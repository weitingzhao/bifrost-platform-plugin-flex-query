"""Shared FastAPI dependencies (DB + write token)."""

from __future__ import annotations

import os
from typing import Any, Generator

from fastapi import Header, HTTPException

from bifrost_flex_query.config import (
    load_config,
    postgres_connect_kwargs,
    trade_postgres_connect_kwargs,
)


def get_write_token() -> str:
    cfg = load_config()
    return str(cfg.get("write_token") or os.environ.get("FLEX_QUERY_WRITE_TOKEN") or "").strip()


def _write_token_from_headers(
    x_flex_query_write_token: str | None,
    authorization: str | None,
) -> str:
    got = (x_flex_query_write_token or "").strip()
    if not got and authorization and authorization.lower().startswith("bearer "):
        got = authorization[7:].strip()
    return got


def require_write_token(
    x_flex_query_write_token: str | None = Header(default=None, alias="X-Flex-Query-Write-Token"),
    authorization: str | None = Header(default=None),
    x_bifrost_trade_gateway: str | None = Header(default=None, alias="X-Bifrost-Trade-Gateway"),
) -> None:
    expected = get_write_token()
    if not expected:
        return
    got = _write_token_from_headers(x_flex_query_write_token, authorization)
    if got == expected:
        return
    # Trade Traefik injects this header on /api/plugin/flex-query (LAN same-origin FE).
    if (x_bifrost_trade_gateway or "").strip() == "1":
        return
    raise HTTPException(status_code=401, detail="invalid write token")


def require_config_write_identity(
    x_flex_query_write_token: str | None = Header(default=None, alias="X-Flex-Query-Write-Token"),
    authorization: str | None = Header(default=None),
    x_bifrost_trade_gateway: str | None = Header(default=None, alias="X-Bifrost-Trade-Gateway"),
) -> None:
    """Stricter than require_write_token: empty expected token still needs gateway or bearer."""
    expected = get_write_token()
    got = _write_token_from_headers(x_flex_query_write_token, authorization)
    if expected and got == expected:
        return
    if (x_bifrost_trade_gateway or "").strip() == "1":
        return
    raise HTTPException(status_code=401, detail="invalid write token")


def db_conn() -> Generator[Any, None, None]:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(**{**postgres_connect_kwargs(), "cursor_factory": RealDictCursor})
    try:
        yield conn
    finally:
        conn.close()


def trade_db_conn() -> Generator[Any, None, None]:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(
        **{**trade_postgres_connect_kwargs(), "cursor_factory": RealDictCursor}
    )
    try:
        yield conn
    finally:
        conn.close()
