"""DDL for bifrost_golden_source.ops_jobs (Flex ingest queue, freshness, settings)."""

from __future__ import annotations

import logging
from typing import Any, Callable, Mapping, Optional

logger = logging.getLogger(__name__)

SCHEMA = "ops_jobs"
JOB_TABLE = f"{SCHEMA}.job_flex_ingest"
FRESHNESS_TABLE = f"{SCHEMA}.flex_ingest_freshness"
HEARTBEAT_TABLE = f"{SCHEMA}.flex_worker_heartbeat"
# One row (id=1): the cluster-wide auto-range for Flex pulls. Until 0.7.0 these two
# numbers lived in every Trade env DB's ``settings`` row and were written by a
# non-atomic fan-out; only bifrost_dev's copy was ever read (TD-74).
SETTINGS_TABLE = f"{SCHEMA}.flex_settings"

FLEX_SETTINGS_DDL = f"""CREATE TABLE IF NOT EXISTS {SETTINGS_TABLE} (
        id                      integer     PRIMARY KEY DEFAULT 1 CHECK (id = 1),
        flex_default_range_days integer     NOT NULL DEFAULT 30  CHECK (flex_default_range_days >= 1),
        flex_init_range_days    integer     NOT NULL DEFAULT 360 CHECK (flex_init_range_days >= 1),
        updated_at              timestamptz NOT NULL DEFAULT now()
    )"""
# Guarded so a table someone else created (and owns) never fails the ensure path.
FLEX_SETTINGS_COMMENT = f"""DO $$
    BEGIN
        IF obj_description('{SETTINGS_TABLE}'::regclass, 'pg_class') IS NULL THEN
            COMMENT ON TABLE {SETTINGS_TABLE} IS
                'Flex Query plugin: cluster-wide auto-range for Flex pulls (one row, id=1). '
                'Replaces per-env Trade settings.flex_*_range_days (TD-74).';
        END IF;
    END
    $$"""

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
    # The worker cannot be reached from the API pod (egress policy), so it
    # reports in here; the self-check and /metrics read liveness from this row.
    f"""CREATE TABLE IF NOT EXISTS {HEARTBEAT_TABLE} (
        worker               text PRIMARY KEY,
        pod                  text,
        version              text,
        seen_at              timestamptz,
        started_at           timestamptz,
        jobs_done            bigint DEFAULT 0,
        jobs_failed          bigint DEFAULT 0,
        jobs_retried         bigint DEFAULT 0,
        catchups             bigint DEFAULT 0,
        db_reconnects        bigint DEFAULT 0,
        stale_reclaimed      bigint DEFAULT 0,
        last_error           text,
        last_error_category  text
    )""",
    # 0.7.0: range days move here from the Trade env DBs (TD-74); 0.7.0 seeded the
    # row from bifrost_dev, never from literals. Since 0.11.0 (TD-116) nothing reads
    # a Trade DB: without the row readers use the defaults until the first write.
    FLEX_SETTINGS_DDL,
    FLEX_SETTINGS_COMMENT,
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


def _pair(row: Any) -> tuple[Any, Any]:
    if isinstance(row, Mapping):
        return row.get("flex_default_range_days"), row.get("flex_init_range_days")
    return row[0], row[1]


def read_flex_settings(conn: Any) -> tuple[int, int] | None:
    """(default_days, init_days) from the settings row, or None when it is not there yet.

    Raises when the table itself is missing (the caller decides on the fallback).
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT flex_default_range_days, flex_init_range_days FROM {SETTINGS_TABLE} WHERE id = 1"
        )
        row = cur.fetchone()
    if row is None:
        return None
    default_days, init_days = _pair(row)
    if default_days is None or init_days is None:
        return None
    return max(1, int(default_days)), max(1, int(init_days))


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


WORKER_KEY = "flex-query-worker"


def record_heartbeat(conn: Any, state: Mapping[str, Any], *, pod: str, version: str, started_at: Any) -> None:
    """The worker's pulse: written on every idle tick and after every job."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {HEARTBEAT_TABLE}
                (worker, pod, version, seen_at, started_at, jobs_done, jobs_failed, jobs_retried,
                 catchups, db_reconnects, stale_reclaimed, last_error, last_error_category)
            VALUES (%s, %s, %s, clock_timestamp(), %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (worker) DO UPDATE SET
                pod = EXCLUDED.pod,
                version = EXCLUDED.version,
                seen_at = EXCLUDED.seen_at,
                started_at = EXCLUDED.started_at,
                jobs_done = EXCLUDED.jobs_done,
                jobs_failed = EXCLUDED.jobs_failed,
                jobs_retried = EXCLUDED.jobs_retried,
                catchups = EXCLUDED.catchups,
                db_reconnects = EXCLUDED.db_reconnects,
                stale_reclaimed = EXCLUDED.stale_reclaimed,
                last_error = EXCLUDED.last_error,
                last_error_category = EXCLUDED.last_error_category
            """,
            (
                WORKER_KEY,
                pod,
                version,
                started_at,
                int(state.get("jobs_done") or 0),
                int(state.get("jobs_failed") or 0),
                int(state.get("jobs_retried") or 0),
                int(state.get("catchups") or 0),
                int(state.get("db_reconnects") or 0),
                int(state.get("stale_reclaimed") or 0),
                (str(state.get("last_error") or "")[:500]) or None,
                (str(state.get("last_error_category") or "")) or None,
            ),
        )
    conn.commit()
