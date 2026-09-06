"""GET /flex/ops/check — the one call an operator makes when something looks wrong.

Reads the queue, freshness, heartbeat, plan and token state in a few small
queries and never touches IB, so it can be pressed as often as anyone likes.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends

from bifrost_flex_query.api.deps import db_conn
from bifrost_flex_query.ops.diagnose import CheckInput, KindInput, diagnose
from bifrost_flex_query.orchestration.config_rw import flex_tokens_issued_at, resolve_flex_tokens
from bifrost_flex_query.scheduler.cronutil import next_fires, previous_fire
from bifrost_flex_query.scheduler.daily import (
    DEFAULT_SLOT_MAX_ATTEMPTS,
    SLOT_KIND,
    catchup_grace_sec,
    load_schedule,
    schedule_timezone,
)
from bifrost_flex_query.schema.ddl import ensure_flex_ops_schema

router = APIRouter(prefix="/flex/ops", tags=["ops"])

_DATA_TABLES = (
    ("executions_raw_flex", "raw_broker.executions_raw_flex", "exec_time"),
    ("transactions", "raw_broker.transactions", "ts"),
)


def collect_check_input(conn: Any, *, now: datetime | None = None) -> CheckInput:
    now = now or datetime.now(timezone.utc)
    schedule = load_schedule()
    scheduler_cfg = dict(schedule.get("scheduler") or {})
    tz = schedule_timezone(scheduler_cfg)
    slots = dict(scheduler_cfg.get("slots") or {})

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (kind) id, kind, status, attempts, max_attempts, error_category, not_before,
                   payload, result, created_at, started_at, finished_at
            FROM ops_jobs.job_flex_ingest
            ORDER BY kind, id DESC
            """
        )
        latest = {str(r["kind"]): dict(r) for r in cur.fetchall() or []}
        cur.execute("SELECT * FROM ops_jobs.flex_ingest_freshness")
        fresh = {str(r["dimension"]): dict(r) for r in cur.fetchall() or []}
        cur.execute("SELECT * FROM ops_jobs.flex_worker_heartbeat ORDER BY seen_at DESC NULLS LAST LIMIT 1")
        hb = cur.fetchone()
        cur.execute(
            """
            SELECT max(not_before) AS until FROM ops_jobs.job_flex_ingest
            WHERE status = 'pending' AND error_category = 'throttled' AND not_before > clock_timestamp()
            """
        )
        cd = cur.fetchone() or {}
    conn.rollback()

    data_latest: dict[str, datetime | None] = {}
    for name, fq, col in _DATA_TABLES:
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT max({col}) AS ts FROM {fq}")
                row = cur.fetchone()
            data_latest[name] = row["ts"] if row else None
        except Exception:
            conn.rollback()
            data_latest[name] = None

    kinds: list[KindInput] = []
    for slot, kind in SLOT_KIND.items():
        scfg = dict(slots.get(slot) or {})
        cron = str(scfg.get("cron") or "")
        planned_last = previous_fire(cron, before=now, tz=tz) if cron else None
        upcoming = next_fires(cron, after=now, count=1, tz=tz) if cron else []
        kinds.append(
            KindInput(
                kind=kind,
                job=latest.get(kind),
                freshness=fresh.get(kind),
                planned_last=planned_last,
                planned_next=upcoming[0] if upcoming else None,
                max_attempts=int(scfg.get("max_attempts") or DEFAULT_SLOT_MAX_ATTEMPTS),
            )
        )

    host_tok, sec_tok, _, _ = resolve_flex_tokens()
    issued_at, age_days = flex_tokens_issued_at(now)
    worker_cfg = {}
    try:
        from bifrost_flex_query.config import load_config

        worker_cfg = dict(load_config().get("worker") or {})
    except Exception:
        worker_cfg = {}
    return CheckInput(
        now=now,
        tz=tz,
        grace_sec=catchup_grace_sec(scheduler_cfg),
        kinds=kinds,
        heartbeat=dict(hb) if hb else None,
        tokens={"host": bool(host_tok), "secondary": bool(sec_tok), "issued_at": issued_at, "age_days": age_days},
        data_latest=data_latest,
        cooldown_until=cd.get("until") if isinstance(cd, dict) else None,
        catchup_enabled=(os.environ.get("FLEX_CATCHUP_DISABLED") or "").strip() not in ("1", "true", "yes"),
        worker_poll_sec=float(worker_cfg.get("poll_interval_sec") or 5),
    )


@router.get("/check")
def check(conn: Any = Depends(db_conn)) -> dict[str, Any]:
    ensure_flex_ops_schema(conn)
    return diagnose(collect_check_input(conn))
