"""Manual trigger (online Flex fetch) and XML upload endpoints.

Both run synchronously — the Trade UI waits for the summary — but each leaves
the same trail a queued job would: a row in ``ops_jobs.job_flex_ingest`` and an
outcome in ``flex_ingest_freshness``. A run nobody can find afterwards is how
the daily hand-maintenance stayed invisible.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from bifrost_flex_query.api.deps import db_conn, require_write_token
from bifrost_flex_query.config import load_config, trade_config_for_core
from bifrost_flex_query.scheduler.enqueue import insert_manual_job
from bifrost_flex_query.schema.ddl import ensure_flex_ops_schema, record_freshness
from bifrost_flex_query.worker.claim import mark_done, mark_failed
from bifrost_flex_query.worker.retry import classify_flex_error

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/flex/ingest", tags=["ingest"])

_KIND_TO_JOB = {"trades": "flex-trades", "transactions": "flex-transactions"}


def _audit_start(conn: Any, job_kind: str, payload: dict[str, Any]) -> int | None:
    try:
        ensure_flex_ops_schema(conn)
        return insert_manual_job(conn, kind=job_kind, payload=payload)
    except Exception as exc:  # noqa: BLE001
        logger.warning("manual %s: audit row not created: %s", job_kind, exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return None


def _audit_finish(conn: Any, job_kind: str, job_id: int | None, result: dict[str, Any]) -> None:
    """Done when the orchestration says ok and every configured query answered."""
    errors = [str(e) for e in (result.get("errors") or []) if str(e).strip()]
    ok = bool(result.get("ok")) and not errors
    processed = int(result.get("count") or result.get("inserted") or 0)
    error = None if ok else str(result.get("error") or "; ".join(errors) or "manual run failed")
    try:
        if job_id is not None:
            summary = {k: result.get(k) for k in ("count", "message", "range_from", "range_to", "data_from", "data_to", "per_query", "errors") if k in result}
            if ok:
                mark_done(conn, job_id, {"inserted": processed, "ok": True, "result": summary})
            else:
                mark_failed(conn, job_id, error=error or "", category=classify_flex_error(error or "").value)
        record_freshness(conn, job_kind, ok=ok, processed_rows=processed, error=error, job_id=job_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("manual %s: outcome not recorded: %s", job_kind, exc)
        try:
            conn.rollback()
        except Exception:
            pass


@router.post("/trigger", dependencies=[Depends(require_write_token)])
def trigger_flex_fetch(
    body: dict[str, Any] | None = None,
    conn: Any = Depends(db_conn),
) -> dict[str, Any]:
    """Synchronously fetch Flex trades (and/or transactions) from IB Web Service."""
    payload = dict(body or {})
    kind = str(payload.pop("kind", "trades")).strip()
    job_kind = _KIND_TO_JOB.get(kind)
    if job_kind is None:
        raise HTTPException(status_code=400, detail=f"unsupported kind: {kind!r} (use 'trades' or 'transactions')")

    cfg = load_config()
    core_cfg = trade_config_for_core(cfg)
    job_id = _audit_start(conn, job_kind, {"source": "trigger", **payload})

    if kind == "trades":
        from bifrost_flex_query.orchestration.trades import fetch_flex_trades_and_upsert_executions

        result = dict(fetch_flex_trades_and_upsert_executions(core_cfg, payload or None))
    else:
        from bifrost_flex_query.orchestration.transactions import fetch_cash_transactions_from_flex

        result = dict(fetch_cash_transactions_from_flex(core_cfg, payload or None))

    _audit_finish(conn, job_kind, job_id, result)
    result["job_id"] = job_id
    return result


@router.post("/upload-xml", dependencies=[Depends(require_write_token)])
def upload_flex_xml(
    body: dict[str, Any],
    conn: Any = Depends(db_conn),
) -> dict[str, Any]:
    """Parse an uploaded Flex Trades XML and upsert into raw_broker.executions_raw_flex."""
    from bifrost_flex_query.orchestration.trades import upsert_executions_from_uploaded_flex_xml

    xml_str = str(body.get("xml") or "").strip()
    if not xml_str:
        raise HTTPException(status_code=400, detail="Missing 'xml' field in request body")

    cfg = load_config()
    core_cfg = trade_config_for_core(cfg)
    job_id = _audit_start(conn, "flex-trades", {"source": "upload-xml", "bytes": len(xml_str)})
    result = dict(upsert_executions_from_uploaded_flex_xml(core_cfg, xml_str))
    _audit_finish(conn, "flex-trades", job_id, result)
    result["job_id"] = job_id
    return result
