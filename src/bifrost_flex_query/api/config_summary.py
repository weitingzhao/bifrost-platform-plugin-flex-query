"""Flex config summary (read) and write (tokens + query rows + range days)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from bifrost_flex_query.api.deps import db_conn, require_write_token, trade_db_conn
from bifrost_flex_query.config import load_config, trade_config_for_core

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
        host_tok = str(row.get("ib_flex_host_token") or "").strip()
        sec_tok = str(row.get("ib_flex_secondary_token") or "").strip()
        if row.get("flex_default_range_days") is not None:
            default_days = int(row["flex_default_range_days"])
        if row.get("flex_init_range_days") is not None:
            init_days = int(row["flex_init_range_days"])
    except Exception:
        trade_conn.rollback()

    query_rows: list[dict[str, Any]] = []
    try:
        with gs_conn.cursor() as cur:
            cur.execute(
                """
                SELECT query_host_id, query_secondary_id, query_label, purpose
                FROM brokerage.settings_flex
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
    return {
        "tokens": {
            "host_token_set": host_last4 is not None,
            "host_token_last4": host_last4,
            "secondary_token_set": sec_last4 is not None,
            "secondary_token_last4": sec_last4,
        },
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


@router.post("/write", dependencies=[Depends(require_write_token)])
def write_flex_config_endpoint(body: dict[str, Any] | None = None) -> dict[str, Any]:
    """Persist Flex tokens (Trade DB ``settings``) and query rows (``brokerage.settings_flex``).

    ``trade_postgres`` must have UPDATE on ``public.settings`` (same role that reads tokens).
    """
    from bifrost_core.monitor.reader import write_flex_config

    payload = body or {}
    accounts = normalize_flex_accounts(payload.get("accounts"))
    host_token = payload.get("host_token")
    secondary_token = payload.get("secondary_token")
    default_days = _optional_days(payload.get("flex_default_range_days"))
    init_days = _optional_days(payload.get("flex_init_range_days"))

    cfg = load_config()
    core_cfg = trade_config_for_core(cfg)
    ok = write_flex_config(
        core_cfg,
        host_token if host_token is None else str(host_token),
        secondary_token if secondary_token is None else str(secondary_token),
        accounts,
        default_days,
        init_days,
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
    }
