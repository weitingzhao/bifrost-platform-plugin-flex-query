"""Claim pending jobs with SELECT FOR UPDATE SKIP LOCKED."""

from __future__ import annotations

import json
from typing import Any, Mapping


def _row_to_job(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        payload = row.get("payload") or {}
        if isinstance(payload, str):
            payload = json.loads(payload)
        return {
            "id": int(row["id"]),
            "kind": str(row["kind"]),
            "payload": dict(payload),
            "attempts": int(row.get("attempts") or 0),
            "max_attempts": int(row.get("max_attempts") or 3),
        }
    payload = row[2] or {}
    if isinstance(payload, str):
        payload = json.loads(payload)
    return {
        "id": int(row[0]),
        "kind": str(row[1]),
        "payload": dict(payload),
        "attempts": int(row[3] or 0),
        "max_attempts": int(row[4] or 3),
    }


def claim_next(conn: Any) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, kind, payload, attempts, max_attempts
            FROM ops_jobs.job_flex_ingest
            WHERE status = 'pending'
            ORDER BY priority DESC, created_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row is None:
            return None
        job = _row_to_job(row)
        cur.execute(
            """
            UPDATE ops_jobs.job_flex_ingest
            SET status = 'running',
                attempts = attempts + 1,
                started_at = now(),
                updated_at = now()
            WHERE id = %s
            """,
            (job["id"],),
        )
    conn.commit()
    job["attempts"] = int(job["attempts"]) + 1
    return job


def mark_done(conn: Any, job_id: int, result: Mapping[str, Any] | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ops_jobs.job_flex_ingest
            SET status = 'done',
                result = %s::jsonb,
                finished_at = now(),
                updated_at = now()
            WHERE id = %s
            """,
            (json.dumps(dict(result or {})), int(job_id)),
        )
    conn.commit()


def mark_failed(conn: Any, job_id: int, *, error: str, attempts: int, max_attempts: int) -> None:
    retry = attempts < max_attempts
    status = "pending" if retry else "failed"
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ops_jobs.job_flex_ingest
            SET status = %s,
                result = jsonb_build_object('error', %s),
                finished_at = CASE WHEN %s THEN now() ELSE NULL END,
                updated_at = now()
            WHERE id = %s
            """,
            (status, error[:2000], not retry, int(job_id)),
        )
    conn.commit()
