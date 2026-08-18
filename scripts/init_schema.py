#!/usr/bin/env python3
"""Apply flex_ops schema on Golden Source."""

from __future__ import annotations

from bifrost_flex_query.config import load_config, postgres_connect_kwargs
from bifrost_flex_query.schema.ddl import ensure_flex_ops_schema


def main() -> int:
    import psycopg2

    cfg = load_config()
    conn = psycopg2.connect(**postgres_connect_kwargs(cfg))
    try:
        ensure_flex_ops_schema(conn, log=print)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
