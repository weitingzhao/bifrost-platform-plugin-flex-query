"""Job enqueue helpers: payload_hash, insert with dedup, trim old jobs."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Protocol


class _Connection(Protocol):
    def cursor(self) -> Any: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...


def payload_hash(payload: Mapping[str, Any] | None) -> str:
    data = dict(payload or {})
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def insert_job(
    conn: _Connection,
    *,
    kind: str,
    payload: Mapping[str, Any] | None = None,
    priority: int = 0,
    max_attempts: int = 3,
    hash_payload: Mapping[str, Any] | None = None,
) -> int | None:
    """Insert unless an identical job is already pending/running.

    ``hash_payload`` is what "identical" means when it differs from the stored
    payload — execution knobs such as ``fallback`` are not identity.
    """
    kind_s = str(kind).strip()
    if not kind_s:
        raise ValueError("kind is required")
    body = dict(payload or {})
    ph = payload_hash(body if hash_payload is None else hash_payload)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops_jobs.job_flex_ingest
                    (kind, payload, payload_hash, priority, status, max_attempts)
                VALUES
                    (%s, %s::jsonb, %s, %s, 'pending', %s)
                ON CONFLICT (kind, payload_hash)
                    WHERE status IN ('pending', 'running') AND payload_hash IS NOT NULL
                DO NOTHING
                RETURNING id
                """,
                (kind_s, json.dumps(body), ph, int(priority), int(max_attempts)),
            )
            row = cur.fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    if row is None:
        return None
    if isinstance(row, Mapping):
        return int(row["id"])
    return int(row[0])


def trim_old_jobs(
    conn: _Connection,
    *,
    keep_days: int = 14,
    keep_max: int = 2000,
) -> int:
    deleted = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM ops_jobs.job_flex_ingest
                WHERE status IN ('done', 'failed')
                  AND finished_at IS NOT NULL
                  AND finished_at < now() - (%s || ' days')::interval
                """,
                (int(keep_days),),
            )
            deleted += int(getattr(cur, "rowcount", 0) or 0)
            cur.execute(
                """
                DELETE FROM ops_jobs.job_flex_ingest
                WHERE id IN (
                    SELECT id FROM ops_jobs.job_flex_ingest
                    WHERE status IN ('done', 'failed')
                    ORDER BY finished_at DESC NULLS LAST, id DESC
                    OFFSET %s
                )
                """,
                (int(keep_max),),
            )
            deleted += int(getattr(cur, "rowcount", 0) or 0)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return deleted


def insert_manual_job(conn: _Connection, *, kind: str, payload: Mapping[str, Any] | None = None) -> int:
    """A synchronous manual run, recorded as a job that is already running.

    The Console's queue history and the freshness row then tell one story
    whether a fetch came from Dagster or from the Trade UI's button. No
    payload_hash: a manual run never dedupes against a queued one.
    """
    kind_s = str(kind).strip()
    if not kind_s:
        raise ValueError("kind is required")
    body = {"manual": True, **dict(payload or {})}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops_jobs.job_flex_ingest
                    (kind, payload, payload_hash, priority, status, attempts, max_attempts, started_at)
                VALUES
                    (%s, %s::jsonb, NULL, 0, 'running', 1, 1, clock_timestamp())
                RETURNING id
                """,
                (kind_s, json.dumps(body, default=str)),
            )
            row = cur.fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    if isinstance(row, Mapping):
        return int(row["id"])
    return int(row[0])
