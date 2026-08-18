"""Async worker loop: claim → dispatch → mark done/failed."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any

from bifrost_flex_query.config import load_config, postgres_connect_kwargs
from bifrost_flex_query.schema.ddl import ensure_flex_ops_schema
from bifrost_flex_query.worker.claim import claim_next, mark_done, mark_failed
from bifrost_flex_query.worker.handlers import dispatch
from bifrost_flex_query.worker.health import start_health_server

logger = logging.getLogger(__name__)


def _connect(cfg: dict[str, Any]) -> Any:
    import psycopg2

    return psycopg2.connect(**{**postgres_connect_kwargs(cfg), "connect_timeout": 10})


async def run_forever(*, config_path: str | None = None) -> None:
    cfg = load_config(config_path)
    worker_cfg = dict(cfg.get("worker") or {})
    poll = float(worker_cfg.get("poll_interval_sec") or os.environ.get("FLEX_WORKER_POLL_SEC") or 5)
    health_port = int(os.environ.get("FLEX_WORKER_HEALTH_PORT") or 8080)
    state: dict[str, Any] = {
        "jobs_done": 0,
        "jobs_failed": 0,
        "last_kind": None,
        "last_claim_at": "",
    }
    start_health_server(health_port, state)

    conn = _connect(cfg)
    ensure_flex_ops_schema(conn)
    logger.info("flex-query worker started poll=%.1fs health=:%s", poll, health_port)
    try:
        while True:
            job = claim_next(conn)
            if job is None:
                await asyncio.sleep(poll)
                continue
            jid = int(job["id"])
            kind = str(job["kind"])
            logger.info("claimed job id=%s kind=%s attempt=%s", jid, kind, job["attempts"])
            state["last_claim_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            try:
                result = await asyncio.to_thread(dispatch, kind, job["payload"], cfg, conn)
                mark_done(conn, jid, result)
                state["jobs_done"] = int(state["jobs_done"]) + 1
                state["last_kind"] = kind
                logger.info("job %s done inserted=%s", jid, result.get("inserted"))
            except Exception as exc:
                logger.exception("job %s failed: %s", jid, exc)
                state["jobs_failed"] = int(state["jobs_failed"]) + 1
                mark_failed(
                    conn,
                    jid,
                    error=str(exc),
                    attempts=int(job["attempts"]),
                    max_attempts=int(job["max_attempts"]),
                )
    finally:
        conn.close()
