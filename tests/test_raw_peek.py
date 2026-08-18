"""Unit tests for coverage raw-peek (table whitelist + limit)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi.testclient import TestClient

from bifrost_flex_query.api.app import create_app
from bifrost_flex_query.api.deps import db_conn
from bifrost_flex_query.api.raw_peek import raw_peek


class _Cursor:
    def __init__(self, parent: _Conn) -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        if self.parent.raise_on_execute:
            raise RuntimeError("missing table")
        self.parent.last_query = query
        self.parent.last_params = params
        limit = int(params[0]) if params else 20
        self.parent._rows = self.parent.rows[:limit]

    def fetchall(self) -> list[Any]:
        return self.parent._rows

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Conn:
    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        *,
        raise_on_execute: bool = False,
    ) -> None:
        self.rows = rows or []
        self.raise_on_execute = raise_on_execute
        self.rolled_back = 0
        self.last_query = ""
        self.last_params: Any = None
        self._rows: list[Any] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def rollback(self) -> None:
        self.rolled_back += 1


def test_raw_peek_executions_serializes_and_limits() -> None:
    ts = datetime(2026, 8, 17, 18, 0, tzinfo=timezone.utc)
    conn = _Conn(
        rows=[
            {
                "exec_time": ts,
                "account_id": "U123",
                "contract_key": "AAPL",
                "side": "BOT",
                "quantity": 10,
                "price": 1.25,
                "exec_id": "e1",
                "source": "flex_trades",
            },
            {
                "exec_time": ts,
                "account_id": "U123",
                "contract_key": "MSFT",
                "side": "SLD",
                "quantity": 2,
                "price": 3.5,
                "exec_id": "e2",
                "source": "flex_trades",
            },
        ]
    )
    body = raw_peek(table="executions_raw_flex", limit=1, conn=conn)
    assert body["table"] == "executions_raw_flex"
    assert body["row_count"] == 1
    assert "side" in body["columns"]
    assert "action" not in body["columns"]
    assert body["rows"][0][1] == "U123"
    assert body["rows"][0][0] == ts.isoformat()
    assert "LIMIT %s" in conn.last_query
    assert conn.last_params == (1,)


def test_raw_peek_transactions() -> None:
    conn = _Conn(
        rows=[
            {
                "ts": datetime(2026, 8, 16, tzinfo=timezone.utc),
                "account_id": "U123",
                "amount": -12.5,
                "type": "DIV",
                "description": "Dividend",
                "currency": "USD",
                "symbol": "AAPL",
            }
        ]
    )
    body = raw_peek(table="transactions", limit=20, conn=conn)
    assert body["table"] == "transactions"
    assert body["row_count"] == 1
    assert body["columns"][0] == "ts"
    assert body["rows"][0][2] == -12.5


def test_raw_peek_http_whitelist_and_limit() -> None:
    app = create_app()

    def _gs() -> Any:
        yield _Conn(rows=[])

    app.dependency_overrides[db_conn] = _gs
    client = TestClient(app)

    bad = client.get("/flex/coverage/raw-peek", params={"table": "settings_flex"})
    assert bad.status_code == 422

    over = client.get(
        "/flex/coverage/raw-peek",
        params={"table": "transactions", "limit": 101},
    )
    assert over.status_code == 422

    ok = client.get(
        "/flex/coverage/raw-peek",
        params={"table": "transactions", "limit": 20},
    )
    assert ok.status_code == 200
    body = ok.json()
    assert body["table"] == "transactions"
    assert body["row_count"] == 0
    assert body["rows"] == []
