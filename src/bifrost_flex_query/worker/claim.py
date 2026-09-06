"""Claim, defer, finish: the queue side of the worker.

Every timestamp here is ``clock_timestamp()``, not ``now()``: the worker keeps
one connection open, and ``now()`` is frozen at the start of the transaction —
which, on an idle worker, was the last commit hours earlier. That is how
``started_at`` came to predate ``created_at`` on real rows.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Mapping

logger = logging.getLogger(__name__)

# Flex IB calls can run many minutes; zombies after worker crash sit for hours/days.
DEFAULT_STALE_RUNNING_SEC = 7200
# A reclaimed job is retried, not buried: the data it was fetching is still needed.
DEFAULT_RECLAIM_REQUEUE_SEC = 600


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


def _ids(rows: Any) -> list[int]:
    out: list[int] = []
    for row in rows or []:
        if isinstance(row, Mapping):
            out.append(int(row["id"]))
        else:
            out.append(int(row[0]))
    return out


def claim_next(conn: Any) -> dict[str, Any] | None:
    """The next runnable job, or None — and in that case no transaction left open."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, kind, payload, attempts, max_attempts
            FROM ops_jobs.job_flex_ingest
            WHERE status = 'pending'
              AND (not_before IS NULL OR not_before <= clock_timestamp())
            ORDER BY priority DESC, created_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row is None:
            # End the read transaction: left open across idle polls it pins now(),
            # holds the snapshot, and shows up as "idle in transaction" for hours.
            conn.rollback()
            return None
        job = _row_to_job(row)
        cur.execute(
            """
            UPDATE ops_jobs.job_flex_ingest
            SET status = 'running',
                attempts = attempts + 1,
                started_at = clock_timestamp(),
                updated_at = clock_timestamp(),
                not_before = NULL
            WHERE id = %s
            """,
            (job["id"],),
        )
    conn.commit()
    job["attempts"] = int(job["attempts"]) + 1
    return job


def reclaim_stale_running(
    conn: Any,
    *,
    stale_after_sec: int = DEFAULT_STALE_RUNNING_SEC,
    requeue_delay_sec: int = DEFAULT_RECLAIM_REQUEUE_SEC,
) -> list[int]:
    """Requeue ``running`` jobs older than ``stale_after_sec`` (worker crash / lost claim).

    A job with attempts left goes back to ``pending`` after ``requeue_delay_sec``;
    one that has used them all is failed. Returns the reclaimed ids.
    """
    sec = max(60, int(stale_after_sec))
    delay = max(0, int(requeue_delay_sec))
    msg = (
        f"stale running reclaimed after {sec}s — worker likely restarted mid-job; "
        "retried automatically while attempts remain"
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ops_jobs.job_flex_ingest
            SET status = CASE WHEN attempts < max_attempts THEN 'pending' ELSE 'failed' END,
                not_before = CASE WHEN attempts < max_attempts
                                  THEN clock_timestamp() + make_interval(secs => %s) END,
                error_category = 'stale',
                result = jsonb_build_object('error', %s, 'category', 'stale'),
                finished_at = CASE WHEN attempts < max_attempts THEN NULL ELSE clock_timestamp() END,
                updated_at = clock_timestamp()
            WHERE status = 'running'
              AND COALESCE(started_at, created_at, updated_at)
                  < clock_timestamp() - make_interval(secs => %s)
            RETURNING id
            """,
            (delay, msg[:2000], sec),
        )
        rows = cur.fetchall() or []
    conn.commit()
    ids = _ids(rows)
    if ids:
        logger.warning("reclaimed stale running flex jobs: %s", ids)
    return ids


def mark_done(conn: Any, job_id: int, result: Mapping[str, Any] | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ops_jobs.job_flex_ingest
            SET status = 'done',
                result = %s::jsonb,
                error_category = NULL,
                finished_at = clock_timestamp(),
                updated_at = clock_timestamp()
            WHERE id = %s
            """,
            (json.dumps(dict(result or {}), default=str), int(job_id)),
        )
    conn.commit()


def mark_retry(conn: Any, job_id: int, *, error: str, category: str, delay_sec: int) -> None:
    """Back to ``pending``, claimable again after ``delay_sec``."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ops_jobs.job_flex_ingest
            SET status = 'pending',
                not_before = clock_timestamp() + make_interval(secs => %s),
                error_category = %s,
                result = jsonb_build_object('error', %s, 'category', %s, 'retry_after_sec', %s),
                finished_at = NULL,
                updated_at = clock_timestamp()
            WHERE id = %s
            """,
            (int(delay_sec), category, error[:2000], category, int(delay_sec), int(job_id)),
        )
    conn.commit()


def mark_failed(conn: Any, job_id: int, *, error: str, category: str | None = None) -> None:
    """Final: no attempts left, or an error retrying cannot fix."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ops_jobs.job_flex_ingest
            SET status = 'failed',
                error_category = %s,
                result = jsonb_build_object('error', %s, 'category', %s),
                finished_at = clock_timestamp(),
                updated_at = clock_timestamp()
            WHERE id = %s
            """,
            (category, error[:2000], category, int(job_id)),
        )
    conn.commit()


def defer_pending(conn: Any, *, delay_sec: int, exclude_id: int | None = None) -> list[int]:
    """Push every pending job out by ``delay_sec`` — the token is throttled, not one job."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ops_jobs.job_flex_ingest
            SET not_before = GREATEST(
                    COALESCE(not_before, clock_timestamp()),
                    clock_timestamp() + make_interval(secs => %s)
                ),
                updated_at = clock_timestamp()
            WHERE status = 'pending'
              AND (%s::bigint IS NULL OR id <> %s::bigint)
            RETURNING id
            """,
            (int(delay_sec), exclude_id, exclude_id),
        )
        rows = cur.fetchall() or []
    conn.commit()
    ids = _ids(rows)
    if ids:
        logger.warning("deferred pending flex jobs by %ss (token cooldown): %s", delay_sec, ids)
    return ids
