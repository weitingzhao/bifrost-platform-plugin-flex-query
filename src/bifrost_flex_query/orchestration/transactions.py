"""Fetch IB Flex cash transactions and upsert into raw_broker.transactions.

Window rule (TD-88): a scheduled run catches up from the last stored cash row of
each Flex account, the way the trades job does. It asks IB for

    from = min(last stored day of any account, yesterday - default days)
    to   = yesterday

The last stored day itself is fetched again: rows IB posts later for that day land
too, and the upsert makes the overlap free. A window longer than one IB request may
span is split into consecutive chunks. A fixed "last N days" window can never refill an outage longer than N days.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Tuple

from bifrost_core.portfolio.reader.accounts import upsert_account_transactions

from bifrost_flex_query.client.flex_client import MAX_FLEX_DAYS, fetch_cash_transactions
from bifrost_flex_query.orchestration.config_rw import (
    get_flex_config,
    open_golden_conn,
    postgres_ready,
    resolve_flex_range_days,
)

logger = logging.getLogger(__name__)

# One SendRequest covers at most this many days between fd and td. IB's wording is
# "cannot exceed 365 days"; request_report allows 366, so stay one under both.
CHUNK_MAX_SPAN_DAYS = MAX_FLEX_DAYS - 2

_FMT = "%Y%m%d"


def _as_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def read_last_transaction_dates(gs_conn: Any) -> Dict[str, date]:
    """Last stored cash-transaction day per Flex account.

    Only accounts that also have Flex executions count: an account that dropped out
    of the queries (its last cash row is months old) must not drag every run's
    window back to it. With no Flex executions at all, every stored account counts.
    """
    with gs_conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.account_id, max(t.ts) AS last_ts
            FROM raw_broker.transactions t
            WHERE NOT EXISTS (SELECT 1 FROM raw_broker.executions_raw_flex)
               OR t.account_id IN (
                    SELECT DISTINCT account_id FROM raw_broker.executions_raw_flex
                    WHERE account_id IS NOT NULL
               )
            GROUP BY t.account_id
            """
        )
        rows = cur.fetchall() or []
    gs_conn.rollback()
    out: Dict[str, date] = {}
    for r in rows:
        acct, ts = (r["account_id"], r["last_ts"]) if isinstance(r, Mapping) else (r[0], r[1])
        d = _as_date(ts)
        if acct and d is not None:
            out[str(acct)] = d
    return out


def transactions_window(
    last_dates: Mapping[str, date],
    *,
    default_days: int,
    init_days: int,
    today: Optional[date] = None,
) -> Tuple[date, date, str, int]:
    """(from, to, mode, days) for a scheduled cash-transactions run.

    ``init`` when nothing is stored; otherwise ``incremental``: back to the oldest
    per-account last stored day (inclusive), and never shorter than ``default_days``.
    """
    today = today or date.today()
    yesterday = today - timedelta(days=1)
    if not last_dates:
        start = yesterday - timedelta(days=max(1, int(init_days)))
        return start, yesterday, "init", (yesterday - start).days
    catch_up = min(last_dates.values())
    start = min(catch_up, yesterday - timedelta(days=max(1, int(default_days))))
    return start, yesterday, "incremental", (yesterday - start).days


def split_window(start: date, end: date, max_span_days: int = CHUNK_MAX_SPAN_DAYS) -> List[Tuple[date, date]]:
    """Consecutive, non-overlapping [from, to] chunks with (to - from).days <= max_span_days."""
    if end < start:
        return [(start, end)]
    out: List[Tuple[date, date]] = []
    cur = start
    while cur <= end:
        chunk_end = min(end, cur + timedelta(days=max_span_days))
        out.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return out


def _parse_yyyymmdd(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s.strip()[:8], _FMT).date()
    except (TypeError, ValueError):
        return None


def _scheduled_window(config: dict, today: Optional[date] = None) -> Tuple[date, date, str, int]:
    default_days, init_days = resolve_flex_range_days(config)
    gs = open_golden_conn(config)
    try:
        last_dates = read_last_transaction_dates(gs)
    finally:
        try:
            gs.close()
        except Exception:
            pass
    return transactions_window(last_dates, default_days=default_days, init_days=init_days, today=today)


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() not in ("", "0", "false", "no", "off")
    return bool(value)


def fetch_cash_transactions_from_flex(
    config: dict,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Fetch Flex cash transactions and upsert (no HTTP)."""
    conn = None
    try:
        if not postgres_ready(config):
            return {
                "ok": False,
                "error": "Postgres config required to write account_transactions.",
                "count": 0,
            }
        conn = open_golden_conn(config)
        entries: List[tuple] = []
        flex_list = get_flex_config(conn, purpose="cash_transactions")
        for a in flex_list:
            tok = (a.get("token") or "").strip()
            qid = (a.get("query_id") or "").strip()
            if tok and qid:
                entries.append((tok, qid))
        if not entries:
            return {
                "ok": False,
                "error": (
                    "No Flex credentials for cash_transactions: set FLEX_HOST_TOKEN / "
                    "FLEX_SECONDARY_TOKEN (K8s Secret bifrost-flex-tokens) and "
                    "purpose=cash_transactions query_id rows "
                    "(Settings → IB Connection → Flex Query IDs, or Ops Console Flex Plugin)."
                ),
                "count": 0,
            }
        payload = body or {}
        from_date = (payload.get("from_date") or "").strip() or None
        to_date = (payload.get("to_date") or "").strip() or None
        allow_fallback = _truthy(payload.get("fallback", True))
        range_mode = "manual"
        range_days: Optional[int] = None
        if from_date is None and to_date is None:
            start, end, range_mode, range_days = _scheduled_window(config)
            from_date, to_date = start.strftime(_FMT), end.strftime(_FMT)
        # Explicit and scheduled windows alike go to IB in chunks it accepts; a
        # half-given window is passed through so the client names the mistake.
        fd, td = (_parse_yyyymmdd(from_date), _parse_yyyymmdd(to_date)) if from_date and to_date else (None, None)
        if fd is not None and td is not None:
            chunks: List[Tuple[Optional[str], Optional[str]]] = [
                (a.strftime(_FMT), b.strftime(_FMT)) for a, b in split_window(fd, td)
            ]
        else:
            chunks = [(from_date, to_date)]
        all_rows: List[Dict[str, Any]] = []
        errors: List[str] = []
        for token, query_id in entries:
            for c_from, c_to in chunks:
                err = _fetch_chunk(
                    token,
                    query_id,
                    c_from,
                    c_to,
                    all_rows,
                    allow_fallback=allow_fallback and len(chunks) == 1,
                )
                if err:
                    errors.append(err if len(chunks) == 1 else f"{c_from}..{c_to}: {err}")
        if errors and not all_rows:
            return {"ok": False, "error": "; ".join(errors), "count": 0}
        extra = {
            "range_mode": range_mode,
            "range_days": range_days,
            "range_chunks": len(chunks),
        }
        if not all_rows:
            return {
                "ok": True,
                "count": 0,
                "message": "No cash transactions in report.",
                "errors": errors,
                "by_account": len(entries),
                "range_from": from_date,
                "range_to": to_date,
                **extra,
            }
        written, skipped = upsert_account_transactions(config, all_rows)
        # Parsed rows that all got skipped (missing account_id / ts / report_date) are a
        # failed run, not "upserted 0". A database error raises and is caught below.
        if written == 0:
            detail = f"Parsed {len(all_rows)} cash transaction(s) but wrote 0"
            if skipped:
                detail += f" ({skipped} skipped)"
            return {
                "ok": False,
                "error": detail + ".",
                "count": 0,
                "skipped": skipped,
                "by_account": len(entries),
                "range_from": from_date,
                "range_to": to_date,
                **extra,
            }
        msg = f"Upserted {written} transaction(s) from {len(entries)} Flex account(s)."
        if skipped:
            msg += f" Skipped {skipped} row(s)."
        if len(chunks) > 1:
            msg += f" Window {from_date}..{to_date} in {len(chunks)} requests per account."
        if errors:
            msg += " Partial errors: " + "; ".join(errors)
        return {
            "ok": True,
            "count": written,
            "skipped": skipped,
            "message": msg,
            "errors": errors,
            "by_account": len(entries),
            "range_from": from_date,
            "range_to": to_date,
            **extra,
        }
    except Exception as e:
        logger.exception("fetch_cash_transactions_from_flex failed: %s", e)
        return {"ok": False, "error": str(e), "count": 0}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _fetch_chunk(
    token: str,
    query_id: str,
    from_date: Optional[str],
    to_date: Optional[str],
    sink: List[Dict[str, Any]],
    *,
    allow_fallback: bool,
) -> Optional[str]:
    """Fetch one window for one query into ``sink``; the error text on failure."""
    try:
        sink.extend(fetch_cash_transactions(token, query_id, from_date=from_date, to_date=to_date))
        return None
    except ValueError as e:
        if (
            allow_fallback
            and from_date
            and to_date
            and ("[1003]" in str(e) or "Statement is not available" in str(e))
        ):
            logger.warning(
                "Flex cash date-range rejected for query_id=%s (%s); trying query default",
                query_id,
                e,
            )
            try:
                sink.extend(fetch_cash_transactions(token, query_id))
                return None
            except ValueError as e2:
                return f"{e}; fallback query-default failed: {e2}"
        return str(e)
