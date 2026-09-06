"""The worker's own safety net for a slot the scheduler never fired.

Dagster is the trigger; it is also one replica. When a planned slot has come
and gone with no job to show for it, the worker enqueues the slot itself —
deduped on the day, so a late Dagster enqueue for the same day does nothing.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from bifrost_flex_query.scheduler.cronutil import previous_fire
from bifrost_flex_query.scheduler.daily import SLOT_KIND, enqueue_slot, schedule_timezone

logger = logging.getLogger(__name__)


def missed_slots(
    scheduler_cfg: Mapping[str, Any],
    *,
    now: datetime,
    grace_sec: int,
    jobs_since: Mapping[str, datetime | None],
) -> list[dict[str, Any]]:
    """Slots whose last planned fire is older than ``grace_sec`` (and younger
    than a day) with no job of that kind created since it.

    ``jobs_since`` maps kind → newest job created_at (None when no job).
    """
    tz = schedule_timezone(scheduler_cfg)
    slots = dict(scheduler_cfg.get("slots") or {})
    out: list[dict[str, Any]] = []
    for slot, kind in SLOT_KIND.items():
        cron = str(dict(slots.get(slot) or {}).get("cron") or "")
        if not cron:
            continue
        planned = previous_fire(cron, before=now, tz=tz)
        if planned is None:
            continue
        age = (now - planned).total_seconds()
        if age < grace_sec or age > 86400:
            continue
        newest = jobs_since.get(kind)
        if newest is not None and newest >= planned:
            continue
        local_day = planned.astimezone(ZoneInfo(tz) if tz else timezone.utc).date().isoformat()
        out.append({"slot": slot, "kind": kind, "planned_at": planned, "as_of": local_day})
    return out


def _newest_job_per_kind(conn: Any) -> dict[str, datetime | None]:
    with conn.cursor() as cur:
        cur.execute("SELECT kind, max(created_at) AS newest FROM ops_jobs.job_flex_ingest GROUP BY kind")
        rows = cur.fetchall() or []
    conn.rollback()
    out: dict[str, datetime | None] = {}
    for row in rows:
        if isinstance(row, Mapping):
            out[str(row["kind"])] = row.get("newest")
        else:
            out[str(row[0])] = row[1]
    return out


def catchup_missed_slots(
    conn: Any,
    scheduler_cfg: Mapping[str, Any],
    *,
    now: datetime | None = None,
    grace_sec: int,
) -> list[dict[str, Any]]:
    """Enqueue every missed slot; returns what was enqueued (deduped ones excluded)."""
    now = now or datetime.now(timezone.utc)
    missed = missed_slots(scheduler_cfg, now=now, grace_sec=grace_sec, jobs_since=_newest_job_per_kind(conn))
    done: list[dict[str, Any]] = []
    for m in missed:
        result = enqueue_slot(
            conn,
            m["slot"],
            scheduler_cfg=scheduler_cfg,
            payload={"as_of": m["as_of"], "catchup": True},
        )
        if result.get("enqueued"):
            logger.warning(
                "catch-up: slot %s planned %s had no job %ss later — enqueued job %s",
                m["slot"],
                m["planned_at"].isoformat(),
                int((now - m["planned_at"]).total_seconds()),
                result.get("job_id"),
            )
            done.append({**m, "job_id": result.get("job_id")})
    return done


def next_catchup_window(planned_at: datetime, grace_sec: int) -> datetime:
    return planned_at + timedelta(seconds=grace_sec)
