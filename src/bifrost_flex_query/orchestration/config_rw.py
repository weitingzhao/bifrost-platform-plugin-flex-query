"""Flex token / query-row / range-day config read+write.

Range days live in Golden Source ``ops_jobs.flex_settings`` (0.7.0, TD-74), next to
the query rows in ``raw_broker.settings_flex``; one write updates both in one
transaction. Until the settings row is seeded, reads fall back to the Trade env
DB's ``settings`` row, which is where 0.6.x kept them.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from bifrost_core.persistence.postgres.brokerage_tables import (
    EXECUTIONS,
    GOLDEN_SETTINGS_FLEX,
    SETTINGS_FLEX,
)
from bifrost_core.persistence.postgres.connection import (
    get_conn_params,
    get_golden_source_conn_params,
)
from psycopg2.extras import RealDictCursor

from bifrost_flex_query.schema.ddl import (
    SETTINGS_TABLE,
    ensure_flex_ops_schema,
    read_flex_settings,
    seed_flex_settings,
)

logger = logging.getLogger(__name__)

_EXEC_READ_TABLE = EXECUTIONS

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


def open_trade_conn(config: dict) -> Any:
    """Open a psycopg2 connection to the Trade env DB (settings_flex query ids, read-only)."""
    from bifrost_flex_query.config import trade_postgres_connect_kwargs

    # Prefer explicit trade_postgres kwargs so a mis-shaped core config cannot
    # accidentally open Golden Source / empty-password connections.
    try:
        return psycopg2.connect(**trade_postgres_connect_kwargs(config))
    except Exception:
        return psycopg2.connect(**get_conn_params(config))


def postgres_ready(config: Optional[dict]) -> bool:
    if not config:
        return False
    return config.get("sink") == "postgres" or bool(config.get("postgres"))


def get_flex_config(conn: Any, purpose: Optional[str] = None) -> Any:
    """If purpose is None: return { host_token, secondary_token, rows }.

    If purpose is set: return list of { token, query_id, role, query_label, purpose }.

    Token source (Wave 11): env FLEX_HOST_TOKEN / FLEX_SECONDARY_TOKEN only.
    """
    try:
        host_tok, sec_tok, _, _ = resolve_flex_tokens()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if purpose is not None:
                cur.execute(
                    f"SELECT query_host_id, query_secondary_id, query_label, purpose "
                    f"FROM {SETTINGS_FLEX} WHERE purpose = %s ORDER BY sort_order, id",
                    (purpose,),
                )
            else:
                cur.execute(
                    f"SELECT query_host_id, query_secondary_id, query_label, purpose "
                    f"FROM {SETTINGS_FLEX} ORDER BY sort_order, id"
                )
            rows = cur.fetchall()
        if purpose is not None:
            out = []
            for r in rows:
                qh_raw = (r.get("query_host_id") or "").strip()
                qs_raw = (r.get("query_secondary_id") or "").strip()
                label = (r.get("query_label") or "").strip()
                purp = (r.get("purpose") or "").strip()
                qh_ids = [x.strip() for x in qh_raw.split(",") if x.strip()]
                qs_ids = [x.strip() for x in qs_raw.split(",") if x.strip()]
                if host_tok:
                    for qid in qh_ids:
                        out.append(
                            {
                                "token": host_tok,
                                "query_id": qid,
                                "role": "host",
                                "query_label": label or None,
                                "purpose": purp or None,
                            }
                        )
                if sec_tok:
                    for qid in qs_ids:
                        out.append(
                            {
                                "token": sec_tok,
                                "query_id": qid,
                                "role": "secondary",
                                "query_label": label or None,
                                "purpose": purp or None,
                            }
                        )
            return out
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
    except Exception as e:
        logger.warning("get_flex_config failed: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return [] if purpose is not None else {"host_token": None, "secondary_token": None, "rows": []}


# What 0.6.x returned when nothing could be read; kept so a fresh cluster behaves the same.
_DEFAULT_RANGE_DAYS = 30
_DEFAULT_INIT_RANGE_DAYS = 360


def read_trade_flex_range_days(trade_conn: Any) -> Optional[Tuple[int, int]]:
    """Pre-0.7.0 read: ``settings`` id=1 of the Trade env DB. None when unreadable.

    Used only to seed ``ops_jobs.flex_settings`` and as the fallback until it is
    seeded; goes away with the Trade ``settings.flex_*_range_days`` columns.
    """
    try:
        with trade_conn.cursor() as cur:
            cur.execute(
                "SELECT flex_default_range_days, flex_init_range_days FROM settings WHERE id = 1"
            )
            row = cur.fetchone()
        if row is None:
            return None
        if isinstance(row, dict):
            default_days, init_days = row.get("flex_default_range_days"), row.get("flex_init_range_days")
        else:
            default_days, init_days = row[0], row[1]
        if default_days is None or init_days is None:
            return None
        return max(1, int(default_days)), max(1, int(init_days))
    except Exception as e:
        logger.debug("read_trade_flex_range_days failed: %s", e)
        try:
            trade_conn.rollback()
        except Exception:
            pass
        return None


def _read_gs_flex_range_days(gs_conn: Any) -> Optional[Tuple[int, int]]:
    try:
        return read_flex_settings(gs_conn)
    except Exception as e:
        logger.debug("read %s failed: %s", SETTINGS_TABLE, e)
        try:
            gs_conn.rollback()
        except Exception:
            pass
        return None


def get_flex_range_days(trade_conn: Any, gs_conn: Any = None) -> Tuple[int, int]:
    """Return (flex_default_range_days, flex_init_range_days).

    Golden Source ``ops_jobs.flex_settings`` first; until that row is seeded, the
    Trade env DB's ``settings`` row; failing both, the 0.6.x defaults.
    """
    got = _read_gs_flex_range_days(gs_conn) if gs_conn is not None else None
    if got is None and trade_conn is not None:
        got = read_trade_flex_range_days(trade_conn)
    if got is None:
        return _DEFAULT_RANGE_DAYS, _DEFAULT_INIT_RANGE_DAYS
    return got


def open_golden_conn(config: dict) -> Any:
    """Golden Source connection from a core-shaped config (``golden_source`` section)."""
    return psycopg2.connect(**{**get_golden_source_conn_params(config), "connect_timeout": 10})


def resolve_flex_range_days(config: dict, trade_conn: Any) -> Tuple[int, int]:
    """get_flex_range_days with a short-lived Golden Source connection from ``config``."""
    gs = None
    try:
        try:
            gs = open_golden_conn(config)
        except Exception as e:
            logger.warning("flex range days: Golden Source unavailable, using Trade DB: %s", e)
        return get_flex_range_days(trade_conn, gs)
    finally:
        if gs is not None:
            try:
                gs.close()
            except Exception:
                pass


def seed_flex_settings_from_trade(gs_conn: Any, trade_conn: Any) -> str:
    """Copy the Trade env DB's range days into ``ops_jobs.flex_settings`` once.

    Returns ``present`` (row already there), ``seeded`` (this call inserted it) or
    ``pending`` (Trade value unreadable; nothing written, readers keep falling back).
    """
    if _read_gs_flex_range_days(gs_conn) is not None:
        return "present"
    legacy = read_trade_flex_range_days(trade_conn)
    if legacy is None:
        return "pending"
    inserted = seed_flex_settings(gs_conn, legacy[0], legacy[1])
    if inserted:
        logger.info("%s seeded from Trade settings: default=%s init=%s", SETTINGS_TABLE, *legacy)
        return "seeded"
    return "present"


def ensure_flex_settings_seeded(gs_conn: Any, config: dict) -> str:
    """Ensure-path hook: seed from the Trade DB that ``config`` points the plugin at.

    Never raises; a failure leaves the row unseeded and readers on the fallback.
    """
    trade = None
    try:
        if _read_gs_flex_range_days(gs_conn) is not None:
            # End the read so a long-lived connection (the worker's) is not left idle in transaction.
            gs_conn.commit()
            return "present"
        trade = open_trade_conn(config)
        status = seed_flex_settings_from_trade(gs_conn, trade)
        gs_conn.commit()
        return status
    except Exception as e:
        logger.warning("%s seed skipped: %s", SETTINGS_TABLE, e)
        try:
            gs_conn.rollback()
        except Exception:
            pass
        return "pending"
    finally:
        if trade is not None:
            try:
                trade.close()
            except Exception:
                pass


def _range_dates(days: int) -> Tuple[str, str]:
    yesterday = date.today() - timedelta(days=1)
    start = yesterday - timedelta(days=days)
    return start.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")


def get_flex_default_range_dates(config: dict, trade_conn: Any) -> Tuple[str, str]:
    """Return (from_date, to_date) in yyyyMMdd. to_date = yesterday."""
    days, _ = resolve_flex_range_days(config, trade_conn)
    return _range_dates(days)


def get_flex_init_range_dates(config: dict, trade_conn: Any) -> Tuple[str, str]:
    """Return (from_date, to_date) in yyyyMMdd for initial/full pull. to_date = yesterday."""
    _, days = resolve_flex_range_days(config, trade_conn)
    return _range_dates(days)


def get_flex_executions_stats(conn: Any) -> Dict[str, Any]:
    """Stats for executions imported from Flex (source='flex_trades')."""
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT
                    COUNT(*) AS count,
                    COUNT(DISTINCT account_id) AS accounts,
                    MIN(exec_time)::date AS min_date,
                    MAX(exec_time)::date AS max_date
                FROM {_EXEC_READ_TABLE}
                WHERE source = %s
                """,
                ("flex_trades",),
            )
            row = cur.fetchone() or {}
        return {
            "count": int(row.get("count") or 0),
            "accounts": int(row.get("accounts") or 0),
            "min_date": row.get("min_date"),
            "max_date": row.get("max_date"),
        }
    except Exception as e:
        logger.warning("get_flex_executions_stats failed: %s", e)
        return {"count": 0, "accounts": 0, "min_date": None, "max_date": None}


def _flex_token_column_value(token: str) -> Optional[str]:
    return token.strip() or None


def write_flex_config(
    status_config: dict,
    host_token: Optional[str],
    secondary_token: Optional[str],
    accounts: Optional[List[Dict[str, Any]]] = None,
    flex_default_range_days: Optional[int] = None,
    flex_init_range_days: Optional[int] = None,
) -> tuple[bool, Optional[str]]:
    """Write range days and/or replace the query rows, in one Golden Source transaction.

    Range days go to ``ops_jobs.flex_settings``; ``accounts`` replace
    ``raw_broker.settings_flex``. Either both land or neither does. The Trade env
    DBs are no longer written (TD-74).

    Tokens are never persisted here: they live in the K8s Secret (``FLEX_HOST_TOKEN``
    / ``FLEX_SECONDARY_TOKEN``, Wave 11). A write that carries token fields is
    accepted only when that Secret is configured, and reports it as the target.

    Returns ``(ok, token_write_target)`` where ``token_write_target`` is
    ``secret`` when Secret/env is canonical, or ``None`` when no token fields
    were in the write.
    """
    if not postgres_ready(status_config):
        return False, None
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
            return False, None

    token_write_target: Optional[str] = None
    secret_canonical = bool(
        (os.environ.get(_ENV_HOST_TOKEN) or "").strip()
        or (os.environ.get(_ENV_SECONDARY_TOKEN) or "").strip()
    )
    token_fields_requested = host_token is not None or secondary_token is not None

    if token_fields_requested and not secret_canonical:
        logger.warning("write_flex_config: token write rejected — configure K8s Secret env")
        return False, None
    if token_fields_requested and secret_canonical:
        token_write_target = "secret"

    days_val = max(1, int(flex_default_range_days)) if flex_default_range_days is not None else None
    init_val = max(1, int(flex_init_range_days)) if flex_init_range_days is not None else None
    range_requested = days_val is not None or init_val is not None

    if not range_requested and valid_accounts is None:
        return True, token_write_target

    golden = None
    try:
        golden = open_golden_conn(status_config)
        if range_requested:
            # Creates ops_jobs.flex_settings when this process has not yet; commits.
            ensure_flex_ops_schema(golden)
            insert_vals = (days_val, init_val)
            if days_val is None or init_val is None:
                # First write before the row is seeded: the half not in the request
                # keeps today's effective value (Trade DB, else the 0.6.x default).
                current = _read_gs_flex_range_days(golden)
                if current is None:
                    current = _effective_trade_range_days(status_config)
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
            "write_flex_config: range=%s gs_rows=%s token_write_target=%s",
            None if not range_requested else (days_val, init_val),
            None if valid_accounts is None else len(valid_accounts),
            token_write_target,
        )
        return True, token_write_target
    except Exception as e:
        logger.warning("write_flex_config failed: %s", e)
        if golden is not None:
            try:
                golden.rollback()
            except Exception:
                pass
        return False, token_write_target
    finally:
        if golden is not None:
            golden.close()


def _effective_trade_range_days(config: dict) -> Tuple[int, int]:
    trade = None
    try:
        trade = open_trade_conn(config)
        return get_flex_range_days(trade)
    except Exception as e:
        logger.debug("trade range read for first write failed: %s", e)
        return _DEFAULT_RANGE_DAYS, _DEFAULT_INIT_RANGE_DAYS
    finally:
        if trade is not None:
            try:
                trade.close()
            except Exception:
                pass
