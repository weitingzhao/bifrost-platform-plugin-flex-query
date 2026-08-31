"""Async worker loop: claim → dispatch → mark done/failed."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any

from bifrost_flex_query.config import load_config, postgres_connect_kwargs
from bifrost_flex_query.schema.ddl import ensure_flex_ops_schema
from bifrost_flex_query.worker.claim import (
    DEFAULT_STALE_RUNNING_SEC,
    claim_next,
    mark_done,
    mark_failed,
    reclaim_stale_running,
)
from bifrost_flex_query.worker.handlers import dispatch
from bifrost_flex_query.worker.health import start_health_server

logger = logging.getLogger(__name__)


def _connect(cfg: dict[str, Any]) -> Any:
    import psycopg2

    return psycopg2.connect(**{**postgres_connect_kwargs(cfg), "connect_timeout": 10})


def _stale_after_sec(worker_cfg: dict[str, Any]) -> int:
    raw = worker_cfg.get("stale_running_sec") or os.environ.get("FLEX_STALE_RUNNING_SEC")
    if raw is None or str(raw).strip() == "":
        return DEFAULT_STALE_RUNNING_SEC
    try:
        return max(60, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_STALE_RUNNING_SEC


async def run_forever(*, config_path: str | None = None) -> None:
    cfg = load_config(config_path)
    worker_cfg = dict(cfg.get("worker") or {})
    poll = float(worker_cfg.get("poll_interval_sec") or os.environ.get("FLEX_WORKER_POLL_SEC") or 5)
    stale_sec = _stale_after_sec(worker_cfg)
    # Reclaim periodically on idle polls (~ every 12 * poll ≈ 1 min with defaults).
    reclaim_every = max(1, int(worker_cfg.get("reclaim_every_n_idle") or 12))
    health_port = int(os.environ.get("FLEX_WORKER_HEALTH_PORT") or 8080)
    state: dict[str, Any] = {
        "jobs_done": 0,
        "jobs_failed": 0,
        "last_kind": None,
        "last_claim_at": "",
        "stale_reclaimed": 0,
    }
    start_health_server(health_port, state)

    conn = _connect(cfg)
    ensure_flex_ops_schema(conn)
    reclaimed = reclaim_stale_running(conn, stale_after_sec=stale_sec)
    state["stale_reclaimed"] = len(reclaimed)
    logger.info(
        "flex-query worker started poll=%.1fs stale_running=%ss health=:%s startup_reclaimed=%s",
        poll,
        stale_sec,
        health_port,
        reclaimed,
    )
    idle_ticks = 0
    try:
        while True:
            job = claim_next(conn)
            if job is None:
                idle_ticks += 1
                if idle_ticks >= reclaim_every:
                    idle_ticks = 0
                    more = reclaim_stale_running(conn, stale_after_sec=stale_sec)
                    if more:
                        state["stale_reclaimed"] = int(state["stale_reclaimed"]) + len(more)
                await asyncio.sleep(poll)
                continue
            idle_ticks = 0
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
