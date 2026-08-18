"""Read-only Flex config summary (masked tokens + query rows + range days)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from bifrost_flex_query.api.deps import db_conn, trade_db_conn

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
