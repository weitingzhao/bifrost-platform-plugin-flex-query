"""Coverage: brokerage table row counts + ingest_freshness."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from bifrost_flex_query.api.deps import db_conn

router = APIRouter(prefix="/flex/coverage", tags=["coverage"])

_TABLES = (
    ("executions_raw_flex", "raw_broker.executions_raw_flex", "exec_time"),
    ("transactions", "raw_broker.transactions", "ts"),
    ("settings_flex", "raw_broker.settings_flex", None),
    ("positions", "raw_broker.positions", "updated_at"),
)


@router.get("/db-summary")
def db_summary(conn: Any = Depends(db_conn)) -> dict[str, Any]:
    tables: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        for name, fq, ts_col in _TABLES:
            try:
                cur.execute(f"SELECT count(*)::bigint AS n FROM {fq}")
                n = int(cur.fetchone()["n"])
            except Exception:
                conn.rollback()
                n = None
            latest = None
            if ts_col:
                try:
                    cur.execute(f"SELECT max({ts_col}) AS ts FROM {fq}")
                    row = cur.fetchone()
                    latest = row["ts"] if row else None
                except Exception:
                    conn.rollback()
                    latest = None
            tables.append({"name": name, "relation": fq, "row_count": n, "latest_ts": latest})
    return {"tables": tables}


@router.get("/freshness")
def freshness(conn: Any = Depends(db_conn)) -> dict[str, Any]:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT dimension, latest_ts, row_count, updated_at
                FROM ops_jobs.flex_ingest_freshness
                ORDER BY dimension
                """
            )
            rows = [dict(r) for r in cur.fetchall()]
        return {"dimensions": rows}
    except Exception:
        conn.rollback()
        return {"dimensions": []}
