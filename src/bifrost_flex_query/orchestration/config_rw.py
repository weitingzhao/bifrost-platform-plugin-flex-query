"""Flex token / query-row / range-day config read+write."""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from bifrost_core.persistence.postgres.brokerage_tables import EXECUTIONS, GOLDEN_SETTINGS_FLEX, SETTINGS_FLEX
from bifrost_core.persistence.postgres.connection import (
    _get_conn_params,
    _get_golden_source_conn_params,
)
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

_EXEC_READ_TABLE = EXECUTIONS

# Wave 4: prefer K8s Secret / env over Trade DB plaintext columns.
_ENV_HOST_TOKEN = "FLEX_HOST_TOKEN"
_ENV_SECONDARY_TOKEN = "FLEX_SECONDARY_TOKEN"
_token_source_logged = False


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
    """Open a psycopg2 connection to the Trade env DB (settings_flex query ids)."""
    from bifrost_flex_query.config import trade_postgres_connect_kwargs

    # Prefer explicit trade_postgres kwargs so a mis-shaped core config cannot
    # accidentally open Golden Source / empty-password connections.
    try:
        return psycopg2.connect(**trade_postgres_connect_kwargs(config))
    except Exception:
        return psycopg2.connect(**_get_conn_params(config))


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


def get_flex_range_days(conn: Any) -> Tuple[int, int]:
    """Return (flex_default_range_days, flex_init_range_days) from settings id=1."""
    default_days = 30
    init_days = 360
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT flex_default_range_days, flex_init_range_days FROM settings WHERE id = 1"
            )
            row = cur.fetchone() or {}
        if row.get("flex_default_range_days") is not None:
            try:
                default_days = max(1, int(row["flex_default_range_days"]))
            except (TypeError, ValueError):
                pass
        if row.get("flex_init_range_days") is not None:
            try:
                init_days = max(1, int(row["flex_init_range_days"]))
            except (TypeError, ValueError):
                pass
    except Exception as e:
        logger.debug("get_flex_range_days failed: %s", e)
    return default_days, init_days


def get_flex_default_range_dates(conn: Any) -> Tuple[str, str]:
    """Return (from_date, to_date) in yyyyMMdd. to_date = yesterday."""
    days, _ = get_flex_range_days(conn)
    yesterday = date.today() - timedelta(days=1)
    start = yesterday - timedelta(days=days)
    return start.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")


def get_flex_init_range_dates(conn: Any) -> Tuple[str, str]:
    """Return (from_date, to_date) in yyyyMMdd for initial/full pull. to_date = yesterday."""
    _, days = get_flex_range_days(conn)
    yesterday = date.today() - timedelta(days=1)
    start = yesterday - timedelta(days=days)
    return start.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")


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
    """Write Flex range days to settings and optionally replace GS query rows.

    Token writes require K8s Secret env (`FLEX_HOST_TOKEN` / `FLEX_SECONDARY_TOKEN`);
    Trade DB token columns were dropped in Wave 11.

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

    sets: List[str] = []
    args: List[Any] = []
    if token_fields_requested and secret_canonical:
        token_write_target = "secret"
    days_val = max(1, int(flex_default_range_days)) if flex_default_range_days is not None else None
    init_val = max(1, int(flex_init_range_days)) if flex_init_range_days is not None else None
    if days_val is not None:
        sets.append("flex_default_range_days = COALESCE(%s, flex_default_range_days)")
        args.append(days_val)
    if init_val is not None:
        sets.append("flex_init_range_days = COALESCE(%s, flex_init_range_days)")
        args.append(init_val)

    if not sets and valid_accounts is None:
        return True, token_write_target

    conn = None
    golden = None
    try:
        if sets:
            params = _get_conn_params(status_config)
            conn = psycopg2.connect(**params)
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE settings SET " + ", ".join(sets) + " WHERE id = 1",
                    tuple(args),
                )
            conn.commit()
        if valid_accounts is not None:
            gs_params = _get_golden_source_conn_params(status_config)
            golden = psycopg2.connect(**{**gs_params, "connect_timeout": 10})
            with golden.cursor() as cur:
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
            "write_flex_config: settings_sets=%d gs_rows=%s token_write_target=%s",
            len(sets),
            None if valid_accounts is None else len(valid_accounts),
            token_write_target,
        )
        return True, token_write_target
    except Exception as e:
        logger.warning("write_flex_config failed: %s", e)
        return False, token_write_target
    finally:
        if conn is not None:
            conn.close()
        if golden is not None:
            golden.close()
