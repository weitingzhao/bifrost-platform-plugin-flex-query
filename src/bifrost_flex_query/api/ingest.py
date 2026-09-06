"""Ingest enqueue + job listing routes."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from bifrost_flex_query.api.deps import db_conn, require_write_token
from bifrost_flex_query.ops.diagnose import fmt_local
from bifrost_flex_query.scheduler.daily import (
    SLOT_KIND,
    SLOT_NAMES,
    enqueue_slot,
    load_schedule,
    schedule_timezone,
)
from bifrost_flex_query.schema.ddl import ensure_flex_ops_schema

router = APIRouter(prefix="/flex/ingest", tags=["ingest"])

KINDS = tuple(SLOT_KIND.values())


@router.get("/kinds")
def list_kinds() -> dict[str, Any]:
    return {"kinds": list(KINDS), "slots": list(SLOT_NAMES)}


@router.post("/enqueue", dependencies=[Depends(require_write_token)])
def enqueue_job(
    body: dict[str, Any],
    conn: Any = Depends(db_conn),
) -> dict[str, Any]:
    kind = str(body.get("kind") or body.get("slot") or "").strip()
    if kind not in KINDS and kind not in SLOT_NAMES:
        raise HTTPException(status_code=400, detail=f"unknown kind: {kind!r}")
    slot = kind if kind in SLOT_NAMES else next(s for s, k in SLOT_KIND.items() if k == kind)
    ensure_flex_ops_schema(conn)
    schedule = load_schedule()
    result = enqueue_slot(
        conn,
        slot,
        scheduler_cfg=dict(schedule.get("scheduler") or {}),
        payload=dict(body.get("payload") or {}),
    )
    return result


@router.get("/jobs")
def list_jobs(
    status: str | None = Query(default=None),
    kind: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    conn: Any = Depends(db_conn),
) -> dict[str, Any]:
    clauses = ["1=1"]
    params: list[Any] = []
    if status:
        clauses.append("status = %s")
        params.append(status)
    if kind:
        clauses.append("kind = %s")
        params.append(kind)
    sql = f"""
        SELECT id, kind, payload, status, attempts, max_attempts, result,
               error_category, not_before,
               created_at, started_at, finished_at
        FROM ops_jobs.job_flex_ingest
        WHERE {' AND '.join(clauses)}
        ORDER BY id DESC
        LIMIT %s
    """
    params.append(int(limit))
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
    return {"jobs": rows}


@router.get("/queue-summary")
def queue_summary(conn: Any = Depends(db_conn)) -> dict[str, Any]:
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
        "pending": counts.get("pending", 0),
        "running": counts.get("running", 0),
        "done": counts.get("done", 0),
        "failed": counts.get("failed", 0),
    }


@router.post("/jobs/{job_id}/run-now", dependencies=[Depends(require_write_token)])
def run_job_now(
    job_id: int,
    force: bool = Query(default=False),
    conn: Any = Depends(db_conn),
) -> dict[str, Any]:
    """Skip a deferred job's wait: clear ``not_before`` so the worker claims it next poll.

    A job cooling down from an IB throttle is refused (the request would fail
    the same way) unless ``force=true``.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, kind, status, error_category, not_before FROM ops_jobs.job_flex_ingest WHERE id = %s",
            (int(job_id),),
        )
        row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"job {job_id} not found")
    if row["status"] != "pending":
        raise HTTPException(status_code=409, detail=f"job {job_id} is {row['status']}, only a pending job can run now")
    tz = schedule_timezone(dict(load_schedule().get("scheduler") or {}))
    nb = row.get("not_before")
    now = datetime.now(timezone.utc)
    if nb is not None and nb.tzinfo is None:
        nb = nb.replace(tzinfo=timezone.utc)
    if row.get("error_category") == "throttled" and nb is not None and nb > now and not force:
        raise HTTPException(
            status_code=409,
            detail=f"IB throttled this token; a request before {fmt_local(nb, tz)} fails again (force=true overrides)",
        )
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ops_jobs.job_flex_ingest
            SET not_before = NULL, updated_at = clock_timestamp()
            WHERE id = %s AND status = 'pending'
            """,
            (int(job_id),),
        )
    conn.commit()
    return {
        "ok": True,
        "job_id": int(job_id),
        "kind": row["kind"],
        "was_not_before": None if nb is None else nb.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "message": "cleared not_before; the worker claims it on its next poll",
    }
