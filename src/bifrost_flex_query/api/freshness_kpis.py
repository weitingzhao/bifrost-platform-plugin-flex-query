"""Aggregated freshness KPIs for the Overview dashboard."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends

from bifrost_flex_query.api.deps import db_conn
from bifrost_flex_query.scheduler.cronutil import iso_z, next_fires, previous_fire
from bifrost_flex_query.scheduler.daily import SLOT_KIND, load_schedule
from bifrost_flex_query.schema.ddl import ensure_flex_ops_schema

router = APIRouter(prefix="/flex/dashboard", tags=["dashboard"])


def _age_seconds(ts: datetime | None) -> float | None:
    if ts is None:
        return None
    now = datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return max(0.0, (now - ts).total_seconds())


def _age_label(secs: float | None) -> str:
    if secs is None:
        return "—"
    if secs < 60:
        return f"{int(secs)}s ago"
    if secs < 3600:
        return f"{int(secs // 60)}m ago"
    if secs < 86400:
        h = int(secs // 3600)
        m = int((secs % 3600) // 60)
        return f"{h}h {m}m ago" if m else f"{h}h ago"
    d = int(secs // 86400)
    h = int((secs % 86400) // 3600)
    return f"{d}d {h}h ago" if h else f"{d}d ago"


def _until_label(secs: float | None) -> str:
    if secs is None:
        return "—"
    if secs <= 0:
        return "overdue"
    if secs < 3600:
        return f"in {int(secs // 60)}m"
    if secs < 86400:
        h = int(secs // 3600)
        m = int((secs % 3600) // 60)
        return f"in {h}h {m}m" if m else f"in {h}h"
    d = int(secs // 86400)
    h = int((secs % 86400) // 3600)
    return f"in {d}d {h}h" if h else f"in {d}d"


@router.get("/freshness-kpis")
def freshness_kpis(conn: Any = Depends(db_conn)) -> dict[str, Any]:
    ensure_flex_ops_schema(conn)
    now = datetime.now(timezone.utc)

    freshness_rows: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT dimension, latest_ts, row_count, updated_at
            FROM ops_jobs.flex_ingest_freshness
            ORDER BY dimension
            """
        )
        freshness_rows = list(cur.fetchall() or [])

    last_success_at: datetime | None = None
    if freshness_rows:
        for row in freshness_rows:
            ts = row.get("latest_ts")
            if ts is not None and (last_success_at is None or ts > last_success_at):
                last_success_at = ts

    if last_success_at is None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT max(finished_at) AS ts
                FROM ops_jobs.job_flex_ingest
                WHERE status = 'done'
                """
            )
            row = cur.fetchone()
            if row and row.get("ts"):
                last_success_at = row["ts"]

    last_run_at: datetime | None = None
    last_run_status: str | None = None
    last_run_kind: str | None = None
    if freshness_rows:
        latest_fresh = max(
            freshness_rows,
            key=lambda r: r.get("latest_ts") or datetime.min.replace(tzinfo=timezone.utc),
        )
        last_run_at = latest_fresh.get("latest_ts")
        last_run_kind = latest_fresh.get("dimension")
        last_run_status = "done"
    else:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT kind, status, finished_at, created_at
                FROM ops_jobs.job_flex_ingest
                ORDER BY id DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if row:
                last_run_at = row.get("finished_at") or row.get("created_at")
                last_run_status = row.get("status")
                last_run_kind = row.get("kind")

    latest_exec_ts: datetime | None = None
    exec_row_count: int | None = None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(exec_time) AS ts, count(*)::bigint AS n "
                "FROM raw_broker.executions_raw_flex"
            )
            row = cur.fetchone()
            if row:
                latest_exec_ts = row.get("ts")
                exec_row_count = int(row["n"]) if row.get("n") is not None else 0
    except Exception:
        conn.rollback()

    latest_txn_ts: datetime | None = None
    txn_row_count: int | None = None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(ts) AS ts, count(*)::bigint AS n FROM raw_broker.transactions"
            )
            row = cur.fetchone()
            if row:
                latest_txn_ts = row.get("ts")
                txn_row_count = int(row["n"]) if row.get("n") is not None else 0
    except Exception:
        conn.rollback()

    schedule = load_schedule()
    slots_cfg = dict((schedule.get("scheduler") or {}).get("slots") or {})
    next_run_at: datetime | None = None
    next_run_slot: str | None = None
    for slot_name in SLOT_KIND:
        scfg = dict(slots_cfg.get(slot_name) or {})
        cron = str(scfg.get("cron") or "")
        if not cron:
            continue
        upcoming = next_fires(cron, after=now, count=1)
        if upcoming and (next_run_at is None or upcoming[0] < next_run_at):
            next_run_at = upcoming[0]
            next_run_slot = slot_name

    last_planned_at: datetime | None = None
    for slot_name in SLOT_KIND:
        scfg = dict(slots_cfg.get(slot_name) or {})
        cron = str(scfg.get("cron") or "")
        if not cron:
            continue
        prev = previous_fire(cron, before=now)
        if prev and (last_planned_at is None or prev > last_planned_at):
            last_planned_at = prev

    last_success_age = _age_seconds(last_success_at)
    last_run_age = _age_seconds(last_run_at)
    latest_exec_age = _age_seconds(latest_exec_ts)
    latest_txn_age = _age_seconds(latest_txn_ts)
    next_run_secs: float | None = None
    if next_run_at is not None:
        next_run_secs = (next_run_at - now).total_seconds()

    return {
        "generated_at": iso_z(now),
        "last_successful_sync": {
            "at": iso_z(last_success_at),
            "age_seconds": last_success_age,
            "age_label": _age_label(last_success_age),
        },
        "last_run": {
            "at": iso_z(last_run_at),
            "age_seconds": last_run_age,
            "age_label": _age_label(last_run_age),
            "status": last_run_status,
            "kind": last_run_kind,
        },
        "latest_execution": {
            "at": iso_z(latest_exec_ts),
            "age_seconds": latest_exec_age,
            "age_label": _age_label(latest_exec_age),
            "row_count": exec_row_count,
        },
        "latest_transaction": {
            "at": iso_z(latest_txn_ts),
            "age_seconds": latest_txn_age,
            "age_label": _age_label(latest_txn_age),
            "row_count": txn_row_count,
        },
        "next_scheduled_run": {
            "at": iso_z(next_run_at),
            "until_seconds": next_run_secs,
            "until_label": _until_label(next_run_secs),
            "slot": next_run_slot,
        },
        "last_planned": {
            "at": iso_z(last_planned_at),
            "age_seconds": _age_seconds(last_planned_at),
            "age_label": _age_label(_age_seconds(last_planned_at)),
        },
    }
