"""DDL for bifrost_golden_source.ops_jobs (Flex ingest queue + freshness)."""

from __future__ import annotations

import logging
from typing import Any, Callable, Mapping, Optional

logger = logging.getLogger(__name__)

SCHEMA = "ops_jobs"
JOB_TABLE = f"{SCHEMA}.job_flex_ingest"
FRESHNESS_TABLE = f"{SCHEMA}.flex_ingest_freshness"

# Idempotent, cheap, and run once per process: an older table grows the columns
# the 0.6.0 worker needs (deferred retries, error categories, outcome-aware
# freshness) without a separate migration step.
_MIGRATIONS: tuple[str, ...] = (
    f"ALTER TABLE {JOB_TABLE} ADD COLUMN IF NOT EXISTS not_before timestamptz",
    f"ALTER TABLE {JOB_TABLE} ADD COLUMN IF NOT EXISTS error_category text",
    f"ALTER TABLE {FRESHNESS_TABLE} ADD COLUMN IF NOT EXISTS last_ok boolean",
    f"ALTER TABLE {FRESHNESS_TABLE} ADD COLUMN IF NOT EXISTS last_error text",
    f"ALTER TABLE {FRESHNESS_TABLE} ADD COLUMN IF NOT EXISTS processed_rows bigint",
    f"ALTER TABLE {FRESHNESS_TABLE} ADD COLUMN IF NOT EXISTS new_rows bigint",
    f"ALTER TABLE {FRESHNESS_TABLE} ADD COLUMN IF NOT EXISTS last_job_id bigint",
    f"ALTER TABLE {FRESHNESS_TABLE} ADD COLUMN IF NOT EXISTS last_finished_at timestamptz",
)
_migrated = False


def _scalar(row: Any) -> int:
    """First column from tuple or RealDictCursor row."""
    if row is None:
        return 0
    if isinstance(row, Mapping):
        return int(next(iter(row.values())))
    return int(row[0])


def ensure_flex_ops_schema(
    conn: Any,
    *,
    log: Optional[Callable[[str], None]] = None,
    migrate: bool = True,
) -> None:
    """Verify ops_jobs flex ingest tables; create only on empty DB when CREATE is granted.

    Never creates legacy flex_ops schema. Column migrations run once per process
    (ALTER takes a table lock even when the column exists, and the API calls this
    on every dashboard request).
    """
    global _migrated
    _log = log or (lambda m: logger.info("%s", m))
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name IN ('job_flex_ingest', 'flex_ingest_freshness')
            """,
            (SCHEMA,),
        )
        row = cur.fetchone()
        present = _scalar(row)
        if present < 2:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            _log(f"schema {SCHEMA}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {JOB_TABLE} (
                    id             bigserial PRIMARY KEY,
                    kind           text NOT NULL,
                    payload        jsonb DEFAULT '{{}}'::jsonb,
                    payload_hash   text,
                    priority       smallint DEFAULT 0,
                    status         text NOT NULL DEFAULT 'pending',
                    attempts       smallint DEFAULT 0,
                    max_attempts   smallint DEFAULT 3,
                    result         jsonb,
                    error_category text,
                    not_before     timestamptz,
                    created_at     timestamptz DEFAULT now(),
                    updated_at     timestamptz DEFAULT now(),
                    started_at     timestamptz,
                    finished_at    timestamptz
                )
                """
            )
            cur.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS job_flex_ingest_kind_hash_active
                ON {JOB_TABLE} (kind, payload_hash)
                WHERE status IN ('pending', 'running') AND payload_hash IS NOT NULL
                """
            )
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS job_flex_ingest_claim
                ON {JOB_TABLE} (status, priority DESC, created_at)
                WHERE status = 'pending'
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {FRESHNESS_TABLE} (
                    dimension        text PRIMARY KEY,
                    latest_ts        timestamptz,
                    row_count        bigint,
                    updated_at       timestamptz DEFAULT now(),
                    last_ok          boolean,
                    last_error       text,
                    processed_rows   bigint,
                    new_rows         bigint,
                    last_job_id      bigint,
                    last_finished_at timestamptz
                )
                """
            )
            _log("tables job_flex_ingest, flex_ingest_freshness")
        else:
            _log(f"schema {SCHEMA} flex ingest tables present")
        if migrate and not _migrated:
            for stmt in _MIGRATIONS:
                cur.execute(stmt)
            _migrated = True
            _log("ops_jobs flex columns up to date")
    conn.commit()


def record_freshness(
    conn: Any,
    dimension: str,
    *,
    ok: bool,
    processed_rows: int = 0,
    new_rows: int | None = None,
    error: str | None = None,
    job_id: int | None = None,
) -> None:
    """One row per kind: when it last succeeded, what the last attempt did.

    Success moves ``latest_ts``; a failure only records ``last_ok`` / ``last_error``,
    so "last successful sync" stays truthful while an attempt is being retried.
    """
    with conn.cursor() as cur:
        if ok:
            cur.execute(
                f"""
                INSERT INTO {FRESHNESS_TABLE}
                    (dimension, latest_ts, row_count, updated_at, last_ok, last_error,
                     processed_rows, new_rows, last_job_id, last_finished_at)
                VALUES (%s, clock_timestamp(), %s, clock_timestamp(), true, NULL, %s, %s, %s, clock_timestamp())
                ON CONFLICT (dimension) DO UPDATE SET
                    latest_ts = EXCLUDED.latest_ts,
                    row_count = EXCLUDED.row_count,
                    updated_at = EXCLUDED.updated_at,
                    last_ok = true,
                    last_error = NULL,
                    processed_rows = EXCLUDED.processed_rows,
                    new_rows = EXCLUDED.new_rows,
                    last_job_id = EXCLUDED.last_job_id,
                    last_finished_at = EXCLUDED.last_finished_at
                """,
                (dimension, int(processed_rows), int(processed_rows), new_rows, job_id),
            )
        else:
            cur.execute(
                f"""
                INSERT INTO {FRESHNESS_TABLE}
                    (dimension, latest_ts, row_count, updated_at, last_ok, last_error,
                     last_job_id, last_finished_at)
                VALUES (%s, NULL, NULL, clock_timestamp(), false, %s, %s, clock_timestamp())
                ON CONFLICT (dimension) DO UPDATE SET
                    updated_at = EXCLUDED.updated_at,
                    last_ok = false,
                    last_error = EXCLUDED.last_error,
                    last_job_id = EXCLUDED.last_job_id,
                    last_finished_at = EXCLUDED.last_finished_at
                """,
                (dimension, (error or "")[:2000] or None, job_id),
            )
    conn.commit()


def update_freshness(conn: Any, dimension: str, row_count: int) -> None:
    """Back-compat: a successful run that processed ``row_count`` rows."""
    record_freshness(conn, dimension, ok=True, processed_rows=int(row_count))
