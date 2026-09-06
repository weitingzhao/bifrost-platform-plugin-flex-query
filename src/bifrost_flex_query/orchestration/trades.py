"""Fetch IB Flex Trades and upsert raw_broker.executions_raw_flex (or from uploaded XML)."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from bifrost_core.monitor.reader import write_account_executions_to_db

from bifrost_flex_query.client.flex_client import fetch_trades, parse_trades_xml
from bifrost_flex_query.orchestration.config_rw import (
    get_flex_config,
    get_flex_executions_stats,
    get_flex_range_days,
    open_trade_conn,
    postgres_ready,
)
from bifrost_flex_query.orchestration.notify import publish_flex_executions_system_message
from bifrost_flex_query.orchestration.utils import rows_span

logger = logging.getLogger(__name__)


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() not in ("", "0", "false", "no", "off")
    return bool(value)


def _is_flex_statement_unavailable(exc: BaseException) -> bool:
    """IB Flex Web Service error 1003 — date override often unsupported for Activity queries."""
    msg = str(exc)
    return "[1003]" in msg or "Statement is not available" in msg


def _fetch_trades_with_date_fallback(
    token: str,
    query_id: str,
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    allow_fallback: bool = True,
) -> tuple[List[Dict[str, Any]], bool, Optional[str]]:
    """
    Fetch Flex Trades with progressive fallback when IB rejects date overrides.

    Order:
    1. from_date/to_date when both provided
    2. On 1003 or empty: query default period (no fd/td)
    3. On still empty / still failing: period=5 (Last 365 Calendar Days)

    ``allow_fallback=False`` stops after step 1: a scheduled run that meets
    "statement not available" should come back later, not fire two more
    requests that only earn the 1018 throttle. A window with no trades is then
    an honest empty result, not a reason to widen the query.
    """
    rows: List[Dict[str, Any]] = []
    used_fallback = False
    fallback_kind: Optional[str] = None
    primary_err: Optional[ValueError] = None

    if from_date and to_date:
        try:
            rows = fetch_trades(token, query_id, from_date=from_date, to_date=to_date)
        except ValueError as e:
            if not _is_flex_statement_unavailable(e) or not allow_fallback:
                raise
            primary_err = e
            logger.warning(
                "Flex Trades date-range rejected for query_id=%s (%s); trying query default",
                query_id,
                e,
            )
    else:
        rows = fetch_trades(token, query_id, from_date=from_date, to_date=to_date)

    if rows or not allow_fallback:
        return rows, used_fallback, fallback_kind

    try:
        rows = fetch_trades(token, query_id)
        if rows:
            return rows, True, (
                "query_default_after_1003" if primary_err is not None else "query_default_after_empty"
            )
    except ValueError as e_default:
        logger.warning(
            "Flex Trades query-default failed for query_id=%s: %s",
            query_id,
            e_default,
        )
        if primary_err is None:
            primary_err = e_default

    try:
        rows = fetch_trades(token, query_id, period=5)
        if rows:
            return rows, True, "period5_after_empty_or_1003"
    except ValueError as e_period:
        if primary_err is not None:
            raise ValueError(
                f"{primary_err}; fallback query-default/period=5 also failed: {e_period}"
            ) from e_period
        raise

    if primary_err is not None:
        raise primary_err
    return rows, used_fallback, fallback_kind


def fetch_flex_trades_and_upsert_executions(
    config: dict,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Fetch Flex Trades from IB and upsert executions (no HTTP)."""
    if not postgres_ready(config):
        publish_flex_executions_system_message(
            config,
            ok=False,
            title="Flex executions fetch failed",
            message="PostgreSQL is required to write account_executions.",
            reason="no control_via_db",
            level="error",
        )
        return {"ok": False, "error": "PostgreSQL is required to write account_executions.", "count": 0}
    conn = None
    try:
        conn = open_trade_conn(config)
        entries: List[Dict[str, Any]] = []
        flex_list = get_flex_config(conn, purpose="trades")
        for a in flex_list:
            tok = (a.get("token") or "").strip()
            qid = (a.get("query_id") or "").strip()
            if tok and qid:
                role = (a.get("role") or "").strip() or "unknown"
                label = (a.get("query_label") or "").strip() or None
                entries.append({"token": tok, "query_id": qid, "role": role, "query_label": label})
        if not entries:
            err = (
                "No Flex credentials for trades: set FLEX_HOST_TOKEN / FLEX_SECONDARY_TOKEN "
                "(K8s Secret bifrost-flex-tokens) and purpose=trades query_id rows "
                "(Settings → IB Connection → Flex Query IDs, or Ops Console Flex Plugin)."
            )
            publish_flex_executions_system_message(
                config,
                ok=False,
                title="Flex executions fetch failed",
                message=err,
                reason="no flex trades credentials",
                level="error",
            )
            return {"ok": False, "error": err, "count": 0}
        payload = body or {}
        from_date = (payload.get("from_date") or "").strip() or None
        to_date = (payload.get("to_date") or "").strip() or None
        allow_fallback = _truthy(payload.get("fallback", True))
        range_mode = "manual" if from_date or to_date else "auto"
        range_days = None
        if from_date is None and to_date is None:
            stats_before = get_flex_executions_stats(conn)
            default_days, init_days = get_flex_range_days(conn)
            yesterday = date.today() - timedelta(days=1)
            to_date = yesterday.strftime("%Y%m%d")
            max_date = stats_before.get("max_date") if stats_before else None
            if not stats_before or (stats_before.get("count") or 0) == 0 or max_date is None:
                start = yesterday - timedelta(days=init_days)
                from_date = start.strftime("%Y%m%d")
                range_mode = "init"
                range_days = init_days
            else:
                try:
                    last_date = getattr(max_date, "date", lambda: max_date)()
                except Exception:
                    last_date = yesterday
                days_since_last = max(0, (yesterday - last_date).days)
                total_days = days_since_last + default_days
                start = yesterday - timedelta(days=total_days)
                from_date = start.strftime("%Y%m%d")
                range_mode = "incremental"
                range_days = total_days

        all_rows: List[Dict[str, Any]] = []
        errors: List[str] = []
        rows_per_fetch: List[int] = []
        per_query: List[Dict[str, Any]] = []
        for i, entry in enumerate(entries):
            token = entry["token"]
            query_id = entry["query_id"]
            role = entry.get("role") or "unknown"
            label = entry.get("query_label")
            try:
                rows, used_fallback, fallback_kind = _fetch_trades_with_date_fallback(
                    token,
                    query_id,
                    from_date=from_date,
                    to_date=to_date,
                    allow_fallback=allow_fallback,
                )
                rows_per_fetch.append(len(rows))
                all_rows.extend(rows)
                span_from, span_to = rows_span(rows)
                per_query.append(
                    {
                        "role": role,
                        "query_id": query_id,
                        "label": label,
                        "rows": len(rows),
                        "data_from": span_from,
                        "data_to": span_to,
                        "used_fallback": used_fallback,
                        "fallback_kind": fallback_kind,
                    }
                )
            except ValueError as e:
                rows_per_fetch.append(-1)
                errors.append(f"Flex query {i + 1}/{len(entries)} ({role} {query_id}): {e}")
            except Exception:
                raise

        if errors and not all_rows:
            err = "; ".join(errors)
            publish_flex_executions_system_message(
                config,
                ok=False,
                title="Flex executions fetch failed",
                message=err,
                reason=err[:500],
                level="error",
            )
            return {
                "ok": False,
                "error": err,
                "count": 0,
                "by_account": len(entries),
                "by_account_counts": rows_per_fetch,
            }

        data_from = None
        data_to = None
        if all_rows:
            min_d = None
            max_d = None
            for item in per_query:
                s_from = item.get("data_from")
                s_to = item.get("data_to")
                try:
                    if s_from:
                        d_from = datetime.strptime(s_from, "%Y-%m-%d").date()
                        if min_d is None or d_from < min_d:
                            min_d = d_from
                    if s_to:
                        d_to = datetime.strptime(s_to, "%Y-%m-%d").date()
                        if max_d is None or d_to > max_d:
                            max_d = d_to
                except Exception:
                    continue
            if min_d is not None:
                data_from = min_d.isoformat()
            if max_d is not None:
                data_to = max_d.isoformat()

        if not all_rows:
            msg = "No trades in Flex report."
            publish_flex_executions_system_message(
                config,
                ok=True,
                title="Flex executions import: no rows",
                message=msg,
                reason=None,
                level="warning",
            )
            return {
                "ok": True,
                "count": 0,
                "message": msg,
                "errors": errors,
                "by_account": len(entries),
                "by_account_counts": rows_per_fetch,
                "data_from": data_from,
                "data_to": data_to,
                "raw_count": 0,
                "per_query": per_query,
                "range_mode": range_mode,
                "range_from": from_date,
                "range_to": to_date,
            }

        raw_count = len(all_rows)
        if not write_account_executions_to_db(config, all_rows):
            werr = "Failed to write account_executions."
            publish_flex_executions_system_message(
                config,
                ok=False,
                title="Flex executions write failed",
                message=werr,
                reason="write_account_executions_to_db",
                level="error",
            )
            return {
                "ok": False,
                "error": werr,
                "count": 0,
                "raw_count": raw_count,
                "data_from": data_from,
                "data_to": data_to,
                "by_account": len(entries),
                "by_account_counts": rows_per_fetch,
                "per_query": per_query,
                "range_mode": range_mode,
                "range_days": range_days,
                "range_from": from_date,
                "range_to": to_date,
            }
        updated_accounts = len(
            {(r.get("account_id") or "").strip() for r in all_rows if (r.get("account_id") or "").strip()}
        )
        stats_after = get_flex_executions_stats(conn)
        last_date_after = stats_after.get("max_date") if stats_after else None
        last_date_after_str = None
        if last_date_after is not None:
            try:
                d = getattr(last_date_after, "date", lambda: last_date_after)()
                last_date_after_str = d.isoformat()
            except Exception:
                last_date_after_str = str(last_date_after)
        msg = (
            f"Upserted {len(all_rows)} execution(s) from {len(entries)} Flex account config "
            f"row(s); affected {updated_accounts} account(s)."
        )
        if last_date_after_str:
            msg += f" Latest Flex execution date after update: {last_date_after_str}."
        if data_from and data_to:
            msg += f" Flex data time span: {data_from} .. {data_to}."
        if (
            rows_per_fetch
            and len(rows_per_fetch) > 0
            and rows_per_fetch[0] == 0
            and (len(rows_per_fetch) == 1 or any(c > 0 for c in rows_per_fetch[1:]))
        ):
            msg += (
                " Host (Query ID "
                + str(entries[0]["query_id"])
                + ") returned 0 trades; in Settings > IB Connection > Flex ensure the "
                "purpose=trades row uses a Query that includes Activity > Trades and the "
                "date range covers your trades."
            )
        if errors:
            msg += " Partial errors: " + "; ".join(errors)
        publish_flex_executions_system_message(
            config,
            ok=True,
            title="Flex executions imported",
            message=msg,
            reason=None,
            level="success",
        )
        return {
            "ok": True,
            "count": len(all_rows),
            "raw_count": raw_count,
            "message": msg,
            "errors": errors,
            "by_account": len(entries),
            "by_account_counts": rows_per_fetch,
            "per_query": per_query,
            "updated_accounts": updated_accounts,
            "range_mode": range_mode,
            "range_days": range_days,
            "range_from": from_date,
            "range_to": to_date,
            "last_flex_date_after": last_date_after_str,
            "data_from": data_from,
            "data_to": data_to,
        }
    except Exception as e:
        logger.exception("fetch_flex_trades_and_upsert_executions failed: %s", e)
        publish_flex_executions_system_message(
            config,
            ok=False,
            title="Flex executions fetch failed",
            message=str(e),
            reason=str(e)[:500],
            level="error",
        )
        return {"ok": False, "error": str(e), "count": 0}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def upsert_executions_from_uploaded_flex_xml(
    config: dict,
    raw_xml: str,
) -> Dict[str, Any]:
    """Parse uploaded Flex Trades XML and upsert executions."""
    if not postgres_ready(config):
        publish_flex_executions_system_message(
            config,
            ok=False,
            title="Flex XML upload failed",
            message="PostgreSQL is required to write account_executions.",
            reason="no control_via_db",
            level="error",
        )
        return {"ok": False, "error": "PostgreSQL is required to write account_executions.", "count": 0}
    try:
        raw_xml = (raw_xml or "").strip()
        if not raw_xml:
            publish_flex_executions_system_message(
                config,
                ok=False,
                title="Flex XML upload failed",
                message="Missing xml field in request body.",
                reason="missing xml",
                level="error",
            )
            return {"ok": False, "error": "Missing xml field in request body.", "count": 0}
        rows = parse_trades_xml(raw_xml)
        if not rows:
            err = "No Trade rows parsed from XML. Ensure this is a Flex Trades report (Activity → Trades)."
            publish_flex_executions_system_message(
                config,
                ok=False,
                title="Flex XML upload: no trades",
                message=err,
                reason="parse_trades_xml empty",
                level="error",
            )
            return {"ok": False, "error": err, "count": 0}
        if not write_account_executions_to_db(config, rows):
            werr = "Failed to write account_executions."
            publish_flex_executions_system_message(
                config,
                ok=False,
                title="Flex XML write failed",
                message=werr,
                reason="write_account_executions_to_db",
                level="error",
            )
            return {"ok": False, "error": werr, "count": 0}
        updated_accounts = len(
            {(r.get("account_id") or "").strip() for r in rows if (r.get("account_id") or "").strip()}
        )
        msg = f"Upserted {len(rows)} execution(s) from uploaded Flex XML for {updated_accounts} account(s)."
        publish_flex_executions_system_message(
            config,
            ok=True,
            title="Flex XML executions imported",
            message=msg,
            reason=None,
            level="success",
        )
        return {
            "ok": True,
            "count": len(rows),
            "updated_accounts": updated_accounts,
            "message": msg,
        }
    except Exception as e:
        logger.exception("upsert_executions_from_uploaded_flex_xml failed: %s", e)
        publish_flex_executions_system_message(
            config,
            ok=False,
            title="Flex XML upload failed",
            message=str(e),
            reason=str(e)[:500],
            level="error",
        )
        return {"ok": False, "error": str(e), "count": 0}
