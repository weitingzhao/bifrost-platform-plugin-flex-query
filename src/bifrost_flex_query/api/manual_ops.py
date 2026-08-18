"""Manual trigger (online Flex fetch) and XML upload endpoints."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from bifrost_flex_query.api.deps import db_conn, require_write_token
from bifrost_flex_query.config import load_config, trade_config_for_core
from bifrost_flex_query.schema.ddl import update_freshness

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/flex/ingest", tags=["ingest"])


@router.post("/trigger", dependencies=[Depends(require_write_token)])
def trigger_flex_fetch(
    body: dict[str, Any] | None = None,
    conn: Any = Depends(db_conn),
) -> dict[str, Any]:
    """Synchronously fetch Flex trades (and/or transactions) from IB Web Service."""
    from bifrost_flex_query.orchestration.trades import fetch_flex_trades_and_upsert_executions

    payload = body or {}
    kind = str(payload.pop("kind", "trades")).strip()

    cfg = load_config()
    core_cfg = trade_config_for_core(cfg)

    if kind == "trades":
        result = fetch_flex_trades_and_upsert_executions(core_cfg, payload or None)
        inserted = int(result.get("count") or 0)
        if inserted > 0:
            try:
                update_freshness(conn, "flex-trades", inserted)
            except Exception:
                conn.rollback()
        return result

    if kind == "transactions":
        from bifrost_flex_query.orchestration.transactions import (
            fetch_cash_transactions_from_flex,
        )

        result = fetch_cash_transactions_from_flex(core_cfg, payload or None)
        inserted = int(result.get("count") or result.get("inserted") or 0)
        if inserted > 0:
            try:
                update_freshness(conn, "flex-transactions", inserted)
            except Exception:
                conn.rollback()
        return result

    raise HTTPException(status_code=400, detail=f"unsupported kind: {kind!r} (use 'trades' or 'transactions')")


@router.post("/upload-xml", dependencies=[Depends(require_write_token)])
def upload_flex_xml(
    body: dict[str, Any],
    conn: Any = Depends(db_conn),
) -> dict[str, Any]:
    """Parse an uploaded Flex Trades XML and upsert into brokerage.executions_raw_flex."""
    from bifrost_flex_query.orchestration.trades import upsert_executions_from_uploaded_flex_xml

    xml_str = str(body.get("xml") or "").strip()
    if not xml_str:
        raise HTTPException(status_code=400, detail="Missing 'xml' field in request body")

    cfg = load_config()
    core_cfg = trade_config_for_core(cfg)

    result = upsert_executions_from_uploaded_flex_xml(core_cfg, xml_str)
    inserted = int(result.get("count") or 0)
    if inserted > 0:
        try:
            update_freshness(conn, "flex-trades", inserted)
        except Exception:
            conn.rollback()
    return result
