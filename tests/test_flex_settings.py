"""ops_jobs.flex_settings: the read (TD-74, TD-116) and the DDL."""

from __future__ import annotations

from typing import Any, Optional, Tuple

import pytest

from bifrost_flex_query.orchestration import config_rw
from bifrost_flex_query.orchestration.config_rw import get_flex_range_days
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
        raise_on_execute: bool = False,
    ) -> None:
        self.settings_row = settings_row
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


def test_reads_the_gs_row() -> None:
    gs = _Conn(settings_row=(21, 400))
    assert get_flex_range_days(gs) == (21, 400)
    assert gs.rollbacks == 1  # the read's transaction is ended


def test_defaults_without_a_row() -> None:
    assert get_flex_range_days(_Conn(settings_row=None)) == (30, 360)


def test_a_failed_read_raises() -> None:
    """TD-116: no fallback to a Trade DB or to the defaults when the read itself fails."""
    gs = _Conn(raise_on_execute=True)
    with pytest.raises(RuntimeError):
        get_flex_range_days(gs)
    assert gs.rollbacks == 1


def test_resolve_closes_its_connection(monkeypatch) -> None:
    gs = _Conn(settings_row=(30, 270))
    monkeypatch.setattr(config_rw, "open_golden_conn", lambda _c: gs)
    assert config_rw.resolve_flex_range_days({}) == (30, 270)
    assert gs.closed is True


def test_resolve_raises_when_gs_unreachable(monkeypatch) -> None:
    def broken(_config: Any) -> Any:
        raise RuntimeError("no route to host")

    monkeypatch.setattr(config_rw, "open_golden_conn", broken)
    with pytest.raises(RuntimeError):
        config_rw.resolve_flex_range_days({})


def test_ddl_single_row_table_in_migrations() -> None:
    blob = "\n".join(ddl._MIGRATIONS)
    assert "CREATE TABLE IF NOT EXISTS ops_jobs.flex_settings" in blob
    assert "CHECK (id = 1)" in blob
    assert "CHECK (flex_default_range_days >= 1)" in blob
    assert "CHECK (flex_init_range_days >= 1)" in blob
    # No upper bound (Owner 2026-10-03) and no literal seed values in the DDL.
    assert "<=" not in ddl.FLEX_SETTINGS_DDL
    assert "INSERT" not in blob
