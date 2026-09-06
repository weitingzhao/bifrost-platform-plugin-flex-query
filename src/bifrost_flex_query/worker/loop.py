"""Async worker loop: claim → dispatch → done / retry later / fail."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable

from bifrost_flex_query.config import load_config, postgres_connect_kwargs
from bifrost_flex_query.scheduler.daily import load_schedule
from bifrost_flex_query.schema.ddl import ensure_flex_ops_schema
from bifrost_flex_query.worker.claim import (
    DEFAULT_RECLAIM_REQUEUE_SEC,
    DEFAULT_STALE_RUNNING_SEC,
    claim_next,
    defer_pending,
    mark_done,
    mark_failed,
    mark_retry,
    reclaim_stale_running,
)
from bifrost_flex_query.worker.handlers import dispatch, record_ingest_outcome
from bifrost_flex_query.worker.health import start_health_server
from bifrost_flex_query.worker.retry import RetryPolicy, classify_flex_error, plan_retry

logger = logging.getLogger(__name__)

_RECONNECT_DELAYS = (5, 10, 20, 40, 60)


class WorkerDb:
    """One connection to the queue, re-opened when Postgres goes away.

    A CloudNativePG switchover, an idle-timeout reset or a node outage used to
    surface as an uncaught OperationalError that took the whole process down and
    left the in-flight job ``running`` until the stale reclaim noticed.
    """

    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        self.conn: Any = None
        self.reconnects = 0

    def _connect(self) -> Any:
        import psycopg2

        return psycopg2.connect(**{**postgres_connect_kwargs(self.cfg), "connect_timeout": 10})

    def _drop(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = None

    def call(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        import psycopg2

        last: Exception | None = None
        for i, delay in enumerate((0, *_RECONNECT_DELAYS)):
            if delay:
                logger.warning("postgres unavailable (%s); reconnecting in %ss", last, delay)
                time.sleep(delay)
            try:
                if self.conn is None or getattr(self.conn, "closed", 0):
                    self.conn = self._connect()
                    if i:
                        self.reconnects += 1
                        logger.info("postgres reconnected")
                return fn(self.conn, *args, **kwargs)
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                last = exc
                self._drop()
        raise RuntimeError(f"postgres unavailable after {len(_RECONNECT_DELAYS)} reconnects: {last}")

    def close(self) -> None:
        self._drop()


def _stale_after_sec(worker_cfg: dict[str, Any]) -> int:
    raw = worker_cfg.get("stale_running_sec") or os.environ.get("FLEX_STALE_RUNNING_SEC")
    if raw is None or str(raw).strip() == "":
        return DEFAULT_STALE_RUNNING_SEC
    try:
        return max(60, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_STALE_RUNNING_SEC


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


async def run_forever(*, config_path: str | None = None) -> None:
    cfg = load_config(config_path)
    worker_cfg = dict(cfg.get("worker") or {})
    poll = float(worker_cfg.get("poll_interval_sec") or os.environ.get("FLEX_WORKER_POLL_SEC") or 5)
    stale_sec = _stale_after_sec(worker_cfg)
    requeue_sec = int(worker_cfg.get("reclaim_requeue_sec") or DEFAULT_RECLAIM_REQUEUE_SEC)
    # The delays live next to the crons in schedule.yaml; worker.retry overrides.
    scheduler_cfg = dict(load_schedule().get("scheduler") or {})
    policy = RetryPolicy.from_config({**dict(scheduler_cfg.get("retry") or {}), **dict(worker_cfg.get("retry") or {})})
    # Reclaim periodically on idle polls (~ every 12 * poll ≈ 1 min with defaults).
    reclaim_every = max(1, int(worker_cfg.get("reclaim_every_n_idle") or 12))
    health_port = int(os.environ.get("FLEX_WORKER_HEALTH_PORT") or 8080)
    state: dict[str, Any] = {
        "jobs_done": 0,
        "jobs_failed": 0,
        "jobs_retried": 0,
        "last_kind": None,
        "last_claim_at": "",
        "last_error": "",
        "last_error_category": "",
        "stale_reclaimed": 0,
        "db_reconnects": 0,
    }
    start_health_server(health_port, state)

    db = WorkerDb(cfg)
    db.call(ensure_flex_ops_schema)
    reclaimed = db.call(reclaim_stale_running, stale_after_sec=stale_sec, requeue_delay_sec=requeue_sec)
    state["stale_reclaimed"] = len(reclaimed)
    logger.info(
        "flex-query worker started poll=%.1fs stale_running=%ss retry=%s health=:%s startup_reclaimed=%s",
        poll,
        stale_sec,
        policy,
        health_port,
        reclaimed,
    )
    idle_ticks = 0
    try:
        while True:
            job = db.call(claim_next)
            state["db_reconnects"] = db.reconnects
            if job is None:
                idle_ticks += 1
                if idle_ticks >= reclaim_every:
                    idle_ticks = 0
                    more = db.call(reclaim_stale_running, stale_after_sec=stale_sec, requeue_delay_sec=requeue_sec)
                    if more:
                        state["stale_reclaimed"] = int(state["stale_reclaimed"]) + len(more)
                await asyncio.sleep(poll)
                continue
            idle_ticks = 0
            jid = int(job["id"])
            kind = str(job["kind"])
            attempts = int(job["attempts"])
            max_attempts = int(job["max_attempts"])
            logger.info("claimed job id=%s kind=%s attempt=%s/%s", jid, kind, attempts, max_attempts)
            state["last_claim_at"] = _now_iso()
            state["last_kind"] = kind
            try:
                result = await asyncio.to_thread(dispatch, kind, job["payload"], cfg, None, jid)
            except Exception as exc:
                msg = str(exc)
                category = classify_flex_error(msg)
                plan = plan_retry(category, attempts=attempts, max_attempts=max_attempts, policy=policy)
                state["last_error"] = msg[:500]
                state["last_error_category"] = plan.category.value
                if plan.retry:
                    logger.warning(
                        "job %s %s (%s) — retry in %ss (attempt %s/%s)",
                        jid,
                        plan.category.value,
                        msg[:300],
                        plan.delay_sec,
                        attempts,
                        max_attempts,
                    )
                    db.call(mark_retry, jid, error=msg, category=plan.category.value, delay_sec=plan.delay_sec)
                    if plan.cooldown_all_sec:
                        db.call(defer_pending, delay_sec=plan.cooldown_all_sec, exclude_id=jid)
                    state["jobs_retried"] = int(state["jobs_retried"]) + 1
                else:
                    logger.error("job %s failed for good (%s): %s", jid, plan.category.value, msg[:500])
                    db.call(mark_failed, jid, error=msg, category=plan.category.value)
                    state["jobs_failed"] = int(state["jobs_failed"]) + 1
                try:
                    record_ingest_outcome(kind, cfg, ok=False, error=f"{plan.category.value}: {msg}", job_id=jid)
                except Exception as fexc:  # noqa: BLE001
                    logger.warning("freshness outcome not recorded for job %s: %s", jid, fexc)
                continue
            db.call(mark_done, jid, result)
            state["jobs_done"] = int(state["jobs_done"]) + 1
            state["last_error"] = ""
            state["last_error_category"] = ""
            logger.info(
                "job %s done processed=%s new=%s", jid, result.get("inserted"), result.get("new_rows")
            )
    finally:
        db.close()
