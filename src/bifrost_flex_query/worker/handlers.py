"""Thin handlers — dispatch to in-package Flex orchestration, then record the outcome."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Mapping

from bifrost_flex_query.config import postgres_connect_kwargs, trade_config_for_core
from bifrost_flex_query.schema.ddl import record_freshness

logger = logging.getLogger(__name__)

# Where each kind lands, so the worker can count what a run actually added.
_NEW_ROWS_SOURCE = {
    "flex-trades": ("raw_broker.executions_raw_flex", "created_at"),
    "flex-transactions": ("raw_broker.transactions", "created_at"),
}


def _require_ok(result: Mapping[str, Any] | None, *, label: str) -> dict[str, Any]:
    data = dict(result or {})
    if data.get("ok") is False:
        raise RuntimeError(str(data.get("error") or f"{label} failed"))
    # One account landed and the other did not: the job is not done until both
    # are. The rows already written are upserts, so the retry costs nothing.
    errors = [str(e) for e in (data.get("errors") or []) if str(e).strip()]
    if errors:
        raise RuntimeError(f"{label} partial: " + "; ".join(errors))
    inserted = int(data.get("count") or data.get("inserted") or 0)
    return {"inserted": inserted, "ok": True, "result": data}


def handle_flex_trades(payload: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    from bifrost_flex_query.orchestration.trades import fetch_flex_trades_and_upsert_executions

    core_cfg = trade_config_for_core(dict(config))
    result = fetch_flex_trades_and_upsert_executions(core_cfg, dict(payload))
    return _require_ok(result, label="flex-trades")


def handle_flex_transactions(payload: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    from bifrost_flex_query.orchestration.transactions import fetch_cash_transactions_from_flex

    core_cfg = trade_config_for_core(dict(config))
    result = fetch_cash_transactions_from_flex(core_cfg, dict(payload))
    return _require_ok(result, label="flex-transactions")


HANDLERS = {
    "flex-trades": handle_flex_trades,
    "flex-transactions": handle_flex_transactions,
}


def _connect(config: Mapping[str, Any]) -> Any:
    import psycopg2

    return psycopg2.connect(**{**postgres_connect_kwargs(dict(config)), "connect_timeout": 10})


def _db_now(conn: Any) -> datetime | None:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT clock_timestamp()")
            row = cur.fetchone()
        conn.commit()
        if row is None:
            return None
        return row[0] if not isinstance(row, Mapping) else next(iter(row.values()))
    except Exception:  # noqa: BLE001
        try:
            conn.rollback()
        except Exception:
            pass
        return None


def _count_new_rows(conn: Any, kind: str, since: datetime | None) -> int | None:
    src = _NEW_ROWS_SOURCE.get(kind)
    if src is None or since is None:
        return None
    table, col = src
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {table} WHERE {col} >= %s", (since,))
            row = cur.fetchone()
        conn.commit()
        if row is None:
            return None
        return int(row[0] if not isinstance(row, Mapping) else next(iter(row.values())))
    except Exception as exc:  # noqa: BLE001
        logger.debug("new-row count unavailable for %s: %s", kind, exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return None


def dispatch(
    kind: str,
    payload: Mapping[str, Any],
    config: Mapping[str, Any],
    conn: Any | None = None,
    job_id: int | None = None,
) -> dict[str, Any]:
    """Run one job. Raises on failure; on success the freshness row is updated.

    The claim connection is not thread-safe, so the outcome is written on a
    connection of its own.
    """
    _ = conn
    kind_s = str(kind).strip()
    fn = HANDLERS.get(kind_s)
    if fn is None:
        raise ValueError(f"unknown job kind: {kind!r}")
    fresh_conn = _connect(config)
    try:
        since = _db_now(fresh_conn)
        out = fn(payload, config)
        new_rows = _count_new_rows(fresh_conn, kind_s, since)
        out["new_rows"] = new_rows
        record_freshness(
            fresh_conn,
            kind_s,
            ok=True,
            processed_rows=int(out.get("inserted") or 0),
            new_rows=new_rows,
            job_id=job_id,
        )
    finally:
        fresh_conn.close()
    return out


def record_ingest_outcome(
    kind: str,
    config: Mapping[str, Any],
    *,
    ok: bool,
    error: str | None = None,
    job_id: int | None = None,
) -> None:
    """A failed attempt: keep ``latest_ts``, record what went wrong."""
    fresh_conn = _connect(config)
    try:
        record_freshness(fresh_conn, str(kind).strip(), ok=ok, error=error, job_id=job_id)
    finally:
        fresh_conn.close()
