"""DDL for bifrost_golden_source.flex_ops (PG-as-broker job queue)."""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

SCHEMA = "flex_ops"
JOB_TABLE = f"{SCHEMA}.job_flex_ingest"
FRESHNESS_TABLE = f"{SCHEMA}.ingest_freshness"


def ensure_flex_ops_schema(
    conn: Any,
    *,
    log: Optional[Callable[[str], None]] = None,
) -> None:
    _log = log or (lambda m: logger.info("%s", m))
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        _log(f"schema {SCHEMA}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {JOB_TABLE} (
                id            bigserial PRIMARY KEY,
                kind          text NOT NULL,
                payload       jsonb DEFAULT '{{}}'::jsonb,
                payload_hash  text,
                priority      smallint DEFAULT 0,
                status        text NOT NULL DEFAULT 'pending',
                attempts      smallint DEFAULT 0,
                max_attempts  smallint DEFAULT 3,
                result        jsonb,
                created_at    timestamptz DEFAULT now(),
                updated_at    timestamptz DEFAULT now(),
                started_at    timestamptz,
                finished_at   timestamptz
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
                dimension  text PRIMARY KEY,
                latest_ts  timestamptz,
                row_count  bigint,
                updated_at timestamptz DEFAULT now()
            )
            """
        )
        _log("tables job_flex_ingest, ingest_freshness")
    conn.commit()


def update_freshness(conn: Any, dimension: str, row_count: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {FRESHNESS_TABLE} (dimension, latest_ts, row_count, updated_at)
            VALUES (%s, now(), %s, now())
            ON CONFLICT (dimension) DO UPDATE SET
                latest_ts = EXCLUDED.latest_ts,
                row_count = EXCLUDED.row_count,
                updated_at = now()
            """,
            (dimension, int(row_count)),
        )
    conn.commit()
