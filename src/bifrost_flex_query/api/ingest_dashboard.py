"""Queue dashboard: plan-vs-actual for Flex CronJob slots."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends

from bifrost_flex_query.api.deps import db_conn
from bifrost_flex_query.scheduler.cronutil import iso_z, next_fires, previous_fire
from bifrost_flex_query.scheduler.daily import SLOT_KIND, load_schedule

router = APIRouter(prefix="/flex/ingest", tags=["ingest"])


@router.get("/queue-dashboard")
def queue_dashboard(conn: Any = Depends(db_conn)) -> dict[str, Any]:
    schedule = load_schedule()
    slots = dict((schedule.get("scheduler") or {}).get("slots") or {})
    now = datetime.now(timezone.utc)
    plans: list[dict[str, Any]] = []
    for slot_name, kind in SLOT_KIND.items():
        scfg = dict(slots.get(slot_name) or {})
        cron = str(scfg.get("cron") or "")
        last_planned = previous_fire(cron, before=now) if cron else None
        upcoming = next_fires(cron, after=now, count=3) if cron else []
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, status, created_at, finished_at, result
                FROM ops_jobs.job_flex_ingest
                WHERE kind = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (kind,),
            )
            last = cur.fetchone()
        last_job = dict(last) if last else None
        adherence = "on_plan"
        if last_planned is not None:
            window_end = last_planned + timedelta(hours=2)
            if now > window_end:
                if last_job is None:
                    adherence = "no_data"
                else:
                    created = last_job.get("created_at")
                    if created is not None and created < last_planned:
                        adherence = "late"
        plans.append(
            {
                "slot": slot_name,
                "kind": kind,
                "cron": cron,
                "last_planned_at": iso_z(last_planned),
                "next_fires": [iso_z(t) for t in upcoming],
                "last_job": last_job,
                "late": adherence == "late",
                "adherence": adherence,
            }
        )
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, count(*)::int AS n
            FROM ops_jobs.job_flex_ingest
            GROUP BY status
            """
        )
        counts = {str(r["status"]): int(r["n"]) for r in cur.fetchall()}
    return {
        "now": iso_z(now),
        "counts": {
            "pending": counts.get("pending", 0),
            "running": counts.get("running", 0),
            "done": counts.get("done", 0),
            "failed": counts.get("failed", 0),
        },
        "slots": plans,
    }
