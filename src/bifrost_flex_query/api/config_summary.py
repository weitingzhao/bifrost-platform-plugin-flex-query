"""Flex config summary (read) and write (tokens + query rows + range days)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from bifrost_flex_query.api.deps import db_conn, require_config_write_identity, trade_db_conn
from bifrost_flex_query.config import load_config, trade_config_for_core, trade_token_dbnames

router = APIRouter(prefix="/flex/config", tags=["config"])


def mask_token_last4(token: str | None) -> str | None:
    s = (token or "").strip()
    if not s:
        return None
    return s[-4:]


def _blank_to_none(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


@router.get("/summary")
def config_summary(
    gs_conn: Any = Depends(db_conn),
    trade_conn: Any = Depends(trade_db_conn),
) -> dict[str, Any]:
    host_tok = ""
    sec_tok = ""
    default_days = 30
    init_days = 360
    host_src = "none"
    sec_src = "none"
    try:
        with trade_conn.cursor() as cur:
            cur.execute(
                """
                SELECT ib_flex_host_token, ib_flex_secondary_token,
                       flex_default_range_days, flex_init_range_days
                FROM settings WHERE id = 1
                """
            )
            row = cur.fetchone() or {}
        db_host = str(row.get("ib_flex_host_token") or "").strip()
        db_sec = str(row.get("ib_flex_secondary_token") or "").strip()
        from bifrost_flex_query.orchestration.config_rw import resolve_flex_tokens

        host_tok, sec_tok, host_src, sec_src = resolve_flex_tokens(db_host, db_sec)
        if row.get("flex_default_range_days") is not None:
            default_days = int(row["flex_default_range_days"])
        if row.get("flex_init_range_days") is not None:
            init_days = int(row["flex_init_range_days"])
    except Exception:
        trade_conn.rollback()
        from bifrost_flex_query.orchestration.config_rw import resolve_flex_tokens

        host_tok, sec_tok, host_src, sec_src = resolve_flex_tokens("", "")

    query_rows: list[dict[str, Any]] = []
    try:
        with gs_conn.cursor() as cur:
            cur.execute(
                """
                SELECT query_host_id, query_secondary_id, query_label, purpose
                FROM raw_broker.settings_flex
                ORDER BY sort_order, id
                """
            )
            for r in cur.fetchall() or []:
                query_rows.append(
                    {
                        "purpose": _blank_to_none(r.get("purpose")) or "cash_transactions",
                        "query_label": _blank_to_none(r.get("query_label")),
                        "query_host_id": str(r.get("query_host_id") or "").strip(),
                        "query_secondary_id": _blank_to_none(r.get("query_secondary_id")),
                    }
                )
    except Exception:
        gs_conn.rollback()

    host_last4 = mask_token_last4(host_tok)
    sec_last4 = mask_token_last4(sec_tok)
    # Overall source: secret if either token comes from env; else db if either from DB.
    if host_src == "secret" or sec_src == "secret":
        source = "secret"
    elif host_src == "db" or sec_src == "db":
        source = "db"
    else:
        source = "none"
    return {
        "tokens": {
            "host_token_set": host_last4 is not None,
            "host_token_last4": host_last4,
            "secondary_token_set": sec_last4 is not None,
            "secondary_token_last4": sec_last4,
            "host_source": host_src,
            "secondary_source": sec_src,
        },
        "source": source,
        "range_days": {"default": default_days, "init": init_days},
        "query_rows": query_rows,
    }


def _optional_days(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    return max(1, n)


def normalize_flex_accounts(raw: Any) -> list[dict[str, Any]]:
    """Normalize POST /flex/config/write ``accounts`` rows (skip empty host query ids)."""
    accounts: list[dict[str, Any]] = []
    for a in raw or []:
        if not isinstance(a, dict):
            continue
        qh = str(a.get("query_host_id") or "").strip()
        if not qh:
            continue
        accounts.append(
            {
                "query_host_id": qh,
                "query_secondary_id": str(a.get("query_secondary_id") or "").strip() or None,
                "query_label": str(a.get("query_label") or "").strip() or None,
                "purpose": str(a.get("purpose") or "cash_transactions").strip() or "cash_transactions",
            }
        )
    return accounts


_WRITE_FIELDS = (
    "host_token",
    "secondary_token",
    "accounts",
    "flex_default_range_days",
    "flex_init_range_days",
)
_TOKEN_FIELDS = (
    "host_token",
    "secondary_token",
    "flex_default_range_days",
    "flex_init_range_days",
)


@router.post("/write", dependencies=[Depends(require_config_write_identity)])
def write_flex_config_endpoint(body: dict[str, Any] | None = None) -> dict[str, Any]:
    """Persist Flex tokens (Trade DB ``settings``) and query rows (``raw_broker.settings_flex``).

    ``trade_postgres`` must have UPDATE on ``public.settings`` (same role that reads tokens).
    Tokens fan-out to ``trade_postgres.token_dbnames``; query rows write Golden Source once.
    """
    from bifrost_flex_query.orchestration.config_rw import write_flex_config

    payload = body or {}
    if not any(key in payload for key in _WRITE_FIELDS):
        raise HTTPException(status_code=400, detail="empty flex config write")

    accounts: list[dict[str, Any]] | None
    if "accounts" in payload:
        accounts = normalize_flex_accounts(payload.get("accounts"))
        if not accounts:
            raise HTTPException(
                status_code=400,
                detail="accounts must include at least one query_host_id",
            )
    else:
        accounts = None

    host_token = payload.get("host_token") if "host_token" in payload else None
    secondary_token = payload.get("secondary_token") if "secondary_token" in payload else None
    default_days = _optional_days(payload.get("flex_default_range_days"))
    init_days = _optional_days(payload.get("flex_init_range_days"))
    host_arg = None if host_token is None else str(host_token)
    secondary_arg = None if secondary_token is None else str(secondary_token)

    cfg = load_config()
    if any(key in payload for key in _TOKEN_FIELDS):
        for dbname in trade_token_dbnames(cfg):
            ok = write_flex_config(
                trade_config_for_core(cfg, dbname=dbname),
                host_arg,
                secondary_arg,
                None,
                default_days,
                init_days,
            )
            if not ok:
                raise HTTPException(status_code=500, detail="failed to write flex config")
    if accounts is not None:
        ok = write_flex_config(
            trade_config_for_core(cfg),
            None,
            None,
            accounts,
            None,
            None,
        )
        if not ok:
            raise HTTPException(status_code=500, detail="failed to write flex config")
    return {
        "ok": True,
        "host_token": (str(host_token).strip() or None) if host_token is not None else None,
        "secondary_token": (str(secondary_token).strip() or None) if secondary_token is not None else None,
        "accounts": accounts,
        "flex_default_range_days": default_days,
        "flex_init_range_days": init_days,
        "token_dbnames": trade_token_dbnames(cfg) if any(key in payload for key in _TOKEN_FIELDS) else [],
    }
