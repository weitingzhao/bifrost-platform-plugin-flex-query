"""Flex token / query-row / range-day config read+write -- all on Golden Source.

Query rows live in ``raw_broker.settings_flex``, range days in ``ops_jobs.flex_settings``
(0.7.0, TD-74); one write updates both in one transaction. Execution stats are read from
``raw_broker.executions_raw_flex``. Since 0.11.0 (TD-116) nothing is read from a Trade env
database: until then the query rows and stats came from ``bifrost_dev`` through its FDW views
back to these same tables, and a failed read was silently turned into "nothing there" (which
widened the trades run to the init window). Reads now raise; the job fails and is retried.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from bifrost_core.persistence.postgres.brokerage_tables import (
    GOLDEN_EXECUTIONS_RAW_FLEX,
    GOLDEN_SETTINGS_FLEX,
)
from bifrost_core.persistence.postgres.connection import get_golden_source_conn_params
from psycopg2.extras import RealDictCursor

from bifrost_flex_query.schema.ddl import (
    SETTINGS_TABLE,
    ensure_flex_ops_schema,
    read_flex_settings,
)

logger = logging.getLogger(__name__)

# Wave 4: prefer K8s Secret / env over Trade DB plaintext columns.
_ENV_HOST_TOKEN = "FLEX_HOST_TOKEN"
_ENV_SECONDARY_TOKEN = "FLEX_SECONDARY_TOKEN"
# Written by scripts/sync_flex_tokens.sh into the same Secret: IB tokens expire
# (error 1012) and nothing else records when they were issued.
_ENV_TOKENS_ISSUED_AT = "FLEX_TOKENS_ISSUED_AT"
_token_source_logged = False


def flex_tokens_issued_at(now: datetime | None = None) -> Tuple[Optional[str], Optional[int]]:
    """(issued_at ISO date, age in days) from the Secret, or (None, None) when unknown."""
    raw = (os.environ.get(_ENV_TOKENS_ISSUED_AT) or "").strip()
    if not raw:
        return None, None
    try:
        issued = datetime.strptime(raw[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return raw, None
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return issued.date().isoformat(), max(0, (now - issued).days)


def resolve_flex_tokens() -> Tuple[str, str, str, str]:
    """Return (host_token, secondary_token, host_source, secondary_source).

    Wave 11: tokens only from K8s Secret / env (`secret` | `none`).
    """
    env_host = (os.environ.get(_ENV_HOST_TOKEN) or "").strip()
    env_sec = (os.environ.get(_ENV_SECONDARY_TOKEN) or "").strip()

    host_tok = env_host
    host_src = "secret" if env_host else "none"
    sec_tok = env_sec
    sec_src = "secret" if env_sec else "none"

    global _token_source_logged
    if not _token_source_logged:
        logger.info("flex tokens: host=%s secondary=%s", host_src, sec_src)
        _token_source_logged = True

    return host_tok, sec_tok, host_src, sec_src


def postgres_ready(config: Optional[dict]) -> bool:
    if not config:
        return False
    return config.get("sink") == "postgres" or bool(config.get("postgres"))


def open_golden_conn(config: dict) -> Any:
    """Golden Source connection from a core-shaped config (``golden_source`` section)."""
    return psycopg2.connect(**{**get_golden_source_conn_params(config), "connect_timeout": 10})


def _end_read(conn: Any) -> None:
    """End a read's transaction so the next read sees what was committed since (TD-117)."""
    try:
        conn.rollback()
    except Exception:  # noqa: BLE001 -- a broken connection fails the next statement anyway
        pass


def _query_entries(rows: List[Dict[str, Any]], host_tok: str, sec_tok: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        label = (r.get("query_label") or "").strip() or None
        purp = (r.get("purpose") or "").strip() or None
        for role, tok, raw in (
            ("host", host_tok, r.get("query_host_id")),
            ("secondary", sec_tok, r.get("query_secondary_id")),
        ):
            if not tok:
                continue
            for qid in [x.strip() for x in (raw or "").split(",") if x.strip()]:
                out.append({"token": tok, "query_id": qid, "role": role, "query_label": label, "purpose": purp})
    return out


def get_flex_config(conn: Any, purpose: Optional[str] = None) -> Any:
    """If purpose is None: return { host_token, secondary_token, rows }.

    If purpose is set: return list of { token, query_id, role, query_label, purpose }.

    ``conn`` is a Golden Source connection (``raw_broker.settings_flex``). A failed read
    raises (TD-116): "no rows" must mean the table is empty, not that it could not be
    read. The read's transaction is ended before returning.

    Token source (Wave 11): env FLEX_HOST_TOKEN / FLEX_SECONDARY_TOKEN only.
    """
    host_tok, sec_tok, _, _ = resolve_flex_tokens()
    sql = f"SELECT query_host_id, query_secondary_id, query_label, purpose FROM {GOLDEN_SETTINGS_FLEX}"
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if purpose is not None:
                cur.execute(sql + " WHERE purpose = %s ORDER BY sort_order, id", (purpose,))
            else:
                cur.execute(sql + " ORDER BY sort_order, id")
            rows = cur.fetchall() or []
    finally:
        _end_read(conn)
    if purpose is not None:
        return _query_entries(rows, host_tok, sec_tok)
    out_rows = []
    for r in rows:
        item = {
            "query_host_id": (r.get("query_host_id") or "").strip(),
            "query_secondary_id": (r.get("query_secondary_id") or "").strip() or None,
        }
        if r.get("query_label") is not None and str(r.get("query_label")).strip():
            item["query_label"] = str(r["query_label"]).strip()
        if r.get("purpose") is not None and str(r.get("purpose")).strip():
            item["purpose"] = str(r["purpose"]).strip()
        out_rows.append(item)
    return {"host_token": host_tok or None, "secondary_token": sec_tok or None, "rows": out_rows}


# The range days a cluster without an ``ops_jobs.flex_settings`` row runs with (0.6.x's).
_DEFAULT_RANGE_DAYS = 30
_DEFAULT_INIT_RANGE_DAYS = 360


def get_flex_range_days(gs_conn: Any) -> Tuple[int, int]:
    """(flex_default_range_days, flex_init_range_days) from ``ops_jobs.flex_settings``.

    No row yet -> the defaults (a fresh cluster; the first config write creates it). A failed
    read raises (TD-116): before 0.11.0 it fell back to a Trade env database and then to the
    defaults, so a lost grant silently changed the window.
    """
    try:
        got = read_flex_settings(gs_conn)
    finally:
        _end_read(gs_conn)
    if got is None:
        return _DEFAULT_RANGE_DAYS, _DEFAULT_INIT_RANGE_DAYS
    return got


def resolve_flex_range_days(config: dict) -> Tuple[int, int]:
    """get_flex_range_days on a short-lived Golden Source connection from ``config``."""
    gs = open_golden_conn(config)
    try:
        return get_flex_range_days(gs)
    finally:
        gs.close()


def get_flex_executions_stats(conn: Any) -> Dict[str, Any]:
    """Stats for executions imported from Flex, on Golden Source ``raw_broker.executions_raw_flex``.

    ``min_date`` / ``max_date`` are trade dates (IB's), not ``exec_time`` cast in the session
    time zone. A failed read raises (TD-116): it used to answer count 0, which switched the
    trades run to init mode (the 270-day window and extra IB requests against the 1018
    throttle). The read's transaction is ended before returning, so a later call sees rows
    committed in between (TD-117).
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT
                    COUNT(*) AS count,
                    COUNT(DISTINCT account_id) AS accounts,
                    MIN(trade_date) AS min_date,
                    MAX(trade_date) AS max_date
                FROM {GOLDEN_EXECUTIONS_RAW_FLEX}
                WHERE source = %s
                """,
                ("flex_trades",),
            )
            row = cur.fetchone() or {}
    finally:
        _end_read(conn)
    return {
        "count": int(row.get("count") or 0),
        "accounts": int(row.get("accounts") or 0),
        "min_date": row.get("min_date"),
        "max_date": row.get("max_date"),
    }


def read_flex_executions_stats(config: dict) -> Dict[str, Any]:
    """get_flex_executions_stats on its own Golden Source connection (opened, read, closed)."""
    gs = open_golden_conn(config)
    try:
        return get_flex_executions_stats(gs)
    finally:
        gs.close()


def write_flex_config(
    status_config: dict,
    accounts: Optional[List[Dict[str, Any]]] = None,
    flex_default_range_days: Optional[int] = None,
    flex_init_range_days: Optional[int] = None,
) -> bool:
    """Write range days and/or replace the query rows, in one Golden Source transaction.

    Range days go to ``ops_jobs.flex_settings``; ``accounts`` replace
    ``raw_broker.settings_flex``. Either both land or neither does. The Trade env
    DBs are neither written (TD-74) nor read (TD-116).

    Tokens are not written here at all (0.8.0, TD-83): they live only in the K8s Secret
    ``bifrost-flex-tokens`` (``FLEX_HOST_TOKEN`` / ``FLEX_SECONDARY_TOKEN``, Wave 11),
    set with ``make sync-flex-tokens``; the HTTP route refuses token fields with 409.
    Returns whether the write landed (nothing to write is success).
    """
    if not postgres_ready(status_config):
        return False
    valid_accounts: Optional[List[Dict[str, Any]]]
    if accounts is None:
        valid_accounts = None
    else:
        valid_accounts = [
            a
            for a in accounts
            if isinstance(a, dict) and (a.get("query_host_id") or "").strip()
        ]
        if not valid_accounts:
            return False

    days_val = max(1, int(flex_default_range_days)) if flex_default_range_days is not None else None
    init_val = max(1, int(flex_init_range_days)) if flex_init_range_days is not None else None
    range_requested = days_val is not None or init_val is not None

    if not range_requested and valid_accounts is None:
        return True

    golden = None
    try:
        golden = open_golden_conn(status_config)
        if range_requested:
            # Creates ops_jobs.flex_settings when this process has not yet; commits.
            ensure_flex_ops_schema(golden)
            insert_vals = (days_val, init_val)
            if days_val is None or init_val is None:
                # First write before the row exists: the half not in the request keeps
                # today's effective value (the defaults).
                current = get_flex_range_days(golden)
                insert_vals = (
                    days_val if days_val is not None else current[0],
                    init_val if init_val is not None else current[1],
                )
        with golden.cursor() as cur:
            if range_requested:
                cur.execute(
                    f"INSERT INTO {SETTINGS_TABLE} AS s "
                    f"(id, flex_default_range_days, flex_init_range_days, updated_at) "
                    f"VALUES (1, %s, %s, now()) "
                    f"ON CONFLICT (id) DO UPDATE SET "
                    f"flex_default_range_days = COALESCE(%s, s.flex_default_range_days), "
                    f"flex_init_range_days = COALESCE(%s, s.flex_init_range_days), "
                    f"updated_at = now()",
                    (insert_vals[0], insert_vals[1], days_val, init_val),
                )
            if valid_accounts is not None:
                cur.execute(f"DELETE FROM {GOLDEN_SETTINGS_FLEX}")
                for i, a in enumerate(valid_accounts):
                    qh = (a.get("query_host_id") or "").strip()
                    qs = (a.get("query_secondary_id") or "").strip() or None
                    query_label = (a.get("query_label") or "").strip() or None
                    purpose = (a.get("purpose") or "cash_transactions").strip() or "cash_transactions"
                    cur.execute(
                        f"INSERT INTO {GOLDEN_SETTINGS_FLEX} "
                        f"(sort_order, query_label, purpose, query_host_id, query_secondary_id) "
                        f"VALUES (%s, %s, %s, %s, %s)",
                        (i, query_label, purpose, qh, qs),
                    )
        golden.commit()
        logger.info(
            "write_flex_config: range=%s gs_rows=%s",
            None if not range_requested else (days_val, init_val),
            None if valid_accounts is None else len(valid_accounts),
        )
        return True
    except Exception as e:
        logger.warning("write_flex_config failed: %s", e)
        if golden is not None:
            try:
                golden.rollback()
            except Exception:
                pass
        return False
    finally:
        if golden is not None:
            golden.close()
