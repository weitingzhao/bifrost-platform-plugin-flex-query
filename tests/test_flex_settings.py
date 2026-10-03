"""ops_jobs.flex_settings: read order, one-time seed from the Trade DB, DDL (TD-74)."""

from __future__ import annotations

from typing import Any, Optional, Tuple

from bifrost_flex_query.orchestration import config_rw
from bifrost_flex_query.orchestration.config_rw import (
    ensure_flex_settings_seeded,
    get_flex_range_days,
    seed_flex_settings_from_trade,
)
from bifrost_flex_query.schema import ddl


class _Cursor:
    def __init__(self, parent: "_Conn") -> None:
        self.parent = parent
        self._one: Any = None
        self.rowcount = 0

    def execute(self, sql: str, params: Any = None) -> None:
        self.parent.calls.append((sql, params))
        if self.parent.raise_on_execute:
            raise RuntimeError("relation does not exist")
        if sql.startswith("SELECT") and "ops_jobs.flex_settings" in sql:
            self._one = self.parent.settings_row
        elif sql.startswith("SELECT") and "FROM settings" in sql:
            self._one = self.parent.trade_row
        elif sql.startswith("INSERT INTO ops_jobs.flex_settings"):
            if self.parent.settings_row is None:
                self.parent.settings_row = (params[0], params[1])
                self.rowcount = 1

    def fetchone(self) -> Any:
        return self._one

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Conn:
    def __init__(
        self,
        *,
        settings_row: Optional[Tuple[int, int]] = None,
        trade_row: Any = None,
        raise_on_execute: bool = False,
    ) -> None:
        self.settings_row = settings_row
        self.trade_row = trade_row
        self.raise_on_execute = raise_on_execute
        self.calls: list[tuple[str, Any]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def test_gs_row_wins_over_trade_row() -> None:
    gs = _Conn(settings_row=(21, 400))
    trade = _Conn(trade_row=(14, 180))
    assert get_flex_range_days(trade, gs) == (21, 400)
    assert trade.calls == []


def test_falls_back_to_trade_row_until_seeded() -> None:
    gs = _Conn(settings_row=None)
    trade = _Conn(trade_row={"flex_default_range_days": 14, "flex_init_range_days": 180})
    assert get_flex_range_days(trade, gs) == (14, 180)


def test_falls_back_when_gs_table_missing() -> None:
    gs = _Conn(raise_on_execute=True)
    trade = _Conn(trade_row=(14, 180))
    assert get_flex_range_days(trade, gs) == (14, 180)
    assert gs.rollbacks == 1


def test_defaults_when_nothing_readable() -> None:
    gs = _Conn(raise_on_execute=True)
    trade = _Conn(raise_on_execute=True)
    assert get_flex_range_days(trade, gs) == (30, 360)


def test_seed_copies_trade_value_once() -> None:
    gs = _Conn(settings_row=None)
    trade = _Conn(trade_row=(14, 180))
    assert seed_flex_settings_from_trade(gs, trade) == "seeded"
    assert gs.settings_row == (14, 180)
    assert gs.commits == 1
    # A later seed never overwrites the row (the Trade value may have drifted).
    trade.trade_row = (99, 999)
    assert seed_flex_settings_from_trade(gs, trade) == "present"
    assert gs.settings_row == (14, 180)


def test_seed_writes_nothing_when_trade_unreadable() -> None:
    gs = _Conn(settings_row=None)
    trade = _Conn(raise_on_execute=True)
    assert seed_flex_settings_from_trade(gs, trade) == "pending"
    assert gs.settings_row is None
    assert not any(sql.startswith("INSERT") for sql, _ in gs.calls)


def test_ensure_seeded_never_raises(monkeypatch) -> None:
    def broken(_config: Any) -> Any:
        raise RuntimeError("no route to host")

    monkeypatch.setattr(config_rw, "open_trade_conn", broken)
    gs = _Conn(settings_row=None)
    assert ensure_flex_settings_seeded(gs, {}) == "pending"


def test_ensure_seeded_skips_trade_when_present(monkeypatch) -> None:
    def must_not_open(_config: Any) -> Any:
        raise AssertionError("Trade DB opened although the row exists")

    monkeypatch.setattr(config_rw, "open_trade_conn", must_not_open)
    assert ensure_flex_settings_seeded(_Conn(settings_row=(30, 270)), {}) == "present"


def test_ensure_seeded_closes_trade_conn(monkeypatch) -> None:
    trade = _Conn(trade_row=(30, 270))
    monkeypatch.setattr(config_rw, "open_trade_conn", lambda _c: trade)
    gs = _Conn(settings_row=None)
    assert ensure_flex_settings_seeded(gs, {}) == "seeded"
    assert trade.closed is True


def test_ddl_single_row_table_in_migrations() -> None:
    blob = "\n".join(ddl._MIGRATIONS)
    assert "CREATE TABLE IF NOT EXISTS ops_jobs.flex_settings" in blob
    assert "CHECK (id = 1)" in blob
    assert "CHECK (flex_default_range_days >= 1)" in blob
    assert "CHECK (flex_init_range_days >= 1)" in blob
    # No upper bound (Owner 2026-10-03) and no literal seed values in the DDL.
    assert "<=" not in ddl.FLEX_SETTINGS_DDL
    assert "INSERT" not in blob
