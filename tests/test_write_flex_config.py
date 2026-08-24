"""write_flex_config: omitted tokens are no-ops; empty accounts refuse GS DELETE."""

from __future__ import annotations

from typing import Any, List, Tuple
from unittest.mock import patch

from bifrost_flex_query.orchestration.config_rw import write_flex_config

CFG = {"sink": "postgres", "postgres": {"dbname": "bifrost_dev"}}


class _Cursor:
    def __init__(self, parent: "_Conn") -> None:
        self.parent = parent

    def execute(self, sql: str, params: Any = None) -> None:
        self.parent.calls.append((sql, params))

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Conn:
    def __init__(self, name: str) -> None:
        self.name = name
        self.calls: List[Tuple[str, Any]] = []
        self.commits = 0
        self.closed = False

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        self.closed = True


def _patch_connect(trade: _Conn, gs: _Conn):
    def fake_connect(**kwargs: Any) -> _Conn:
        db = kwargs.get("dbname")
        if db == "trade":
            return trade
        return gs

    return (
        patch(
            "bifrost_flex_query.orchestration.config_rw.psycopg2.connect",
            side_effect=fake_connect,
        ),
        patch(
            "bifrost_flex_query.orchestration.config_rw._get_conn_params",
            return_value={"dbname": "trade"},
        ),
        patch(
            "bifrost_flex_query.orchestration.config_rw._get_golden_source_conn_params",
            return_value={"dbname": "gs"},
        ),
    )


def test_omit_tokens_does_not_null_columns() -> None:
    trade = _Conn("trade")
    gs = _Conn("gs")
    p_connect, p_trade, p_gs = _patch_connect(trade, gs)
    with p_connect, p_trade, p_gs:
        ok, target = write_flex_config(CFG, None, None, None, 30, None)
    assert ok is True
    assert target is None
    assert len(trade.calls) == 1
    sql, params = trade.calls[0]
    assert "ib_flex_host_token" not in sql
    assert "ib_flex_secondary_token" not in sql
    assert "flex_default_range_days" in sql
    assert params == (30,)
    assert gs.calls == []
    assert trade.commits == 1
    assert trade.closed is True
    assert gs.closed is False


def test_empty_accounts_refuses_without_delete() -> None:
    trade = _Conn("trade")
    gs = _Conn("gs")
    p_connect, p_trade, p_gs = _patch_connect(trade, gs)
    with p_connect, p_trade, p_gs:
        ok, _ = write_flex_config(CFG, "tok", None, [])
    assert ok is False
    assert trade.calls == []
    assert gs.calls == []


def test_blank_query_host_accounts_refuses() -> None:
    trade = _Conn("trade")
    gs = _Conn("gs")
    p_connect, p_trade, p_gs = _patch_connect(trade, gs)
    with p_connect, p_trade, p_gs:
        ok, _ = write_flex_config(
            CFG,
            None,
            None,
            [{"query_host_id": "  ", "purpose": "trades"}],
        )
    assert ok is False
    assert gs.calls == []


def test_explicit_empty_token_writes_null() -> None:
    trade = _Conn("trade")
    gs = _Conn("gs")
    p_connect, p_trade, p_gs = _patch_connect(trade, gs)
    with p_connect, p_trade, p_gs:
        ok, _ = write_flex_config(CFG, "", None, None)
    assert ok is True
    sql, params = trade.calls[0]
    assert "ib_flex_host_token = %s" in sql
    assert "ib_flex_secondary_token" not in sql
    assert params == (None,)
    assert gs.calls == []


def test_accounts_replace_gs_rows() -> None:
    trade = _Conn("trade")
    gs = _Conn("gs")
    p_connect, p_trade, p_gs = _patch_connect(trade, gs)
    with p_connect, p_trade, p_gs:
        ok, _ = write_flex_config(
            CFG,
            "tok",
            "",
            [
                {
                    "query_host_id": "111",
                    "query_secondary_id": "222",
                    "query_label": "Trades",
                    "purpose": "trades",
                }
            ],
            14,
            180,
        )
    assert ok is True
    trade_sql, trade_params = trade.calls[0]
    assert "ib_flex_host_token" in trade_sql
    assert "ib_flex_secondary_token" in trade_sql
    assert trade_params[0] == "tok"
    assert trade_params[1] is None
    assert any("DELETE FROM" in sql for sql, _ in gs.calls)
    insert = [c for c in gs.calls if "INSERT INTO" in c[0]]
    assert len(insert) == 1
    assert insert[0][1][3] == "111"
    assert insert[0][1][4] == "222"
    assert gs.commits == 1


def test_nothing_to_write_is_success_noop() -> None:
    trade = _Conn("trade")
    gs = _Conn("gs")
    p_connect, p_trade, p_gs = _patch_connect(trade, gs)
    with p_connect, p_trade, p_gs:
        ok, _ = write_flex_config(CFG, None, None, None)
    assert ok is True
    assert trade.calls == []
    assert gs.calls == []
