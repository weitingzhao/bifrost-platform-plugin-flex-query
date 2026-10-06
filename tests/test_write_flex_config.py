"""write_flex_config: range days + query rows in one Golden Source transaction (TD-74).

Omitted tokens are no-ops; empty accounts refuse the GS DELETE; no Trade env DB is
written (TD-74) or even opened (TD-116).
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple
from unittest.mock import patch

import pytest

from bifrost_flex_query.orchestration.config_rw import write_flex_config

CFG = {"sink": "postgres", "postgres": {"dbname": "bifrost_golden_source"}}


class _Cursor:
    def __init__(self, parent: "_Conn") -> None:
        self.parent = parent
        self._one: Any = None
        self.rowcount = 0

    def execute(self, sql: str, params: Any = None) -> None:
        if self.parent.fail_on and self.parent.fail_on in sql:
            raise RuntimeError("boom")
        self.parent.calls.append((sql, params))
        if "FROM ops_jobs.flex_settings" in sql:
            self._one = self.parent.settings_row
        elif "FROM settings" in sql:
            self._one = self.parent.trade_row
        else:
            self._one = None

    def fetchone(self) -> Any:
        return self._one

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Conn:
    def __init__(
        self,
        name: str,
        *,
        settings_row: Optional[Tuple[int, int]] = None,
        trade_row: Optional[Tuple[int, int]] = None,
        fail_on: str | None = None,
    ) -> None:
        self.name = name
        self.settings_row = settings_row
        self.trade_row = trade_row
        self.fail_on = fail_on
        self.calls: List[Tuple[str, Any]] = []
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

    def sql(self) -> str:
        return "\n".join(c[0] for c in self.calls)


@pytest.fixture()
def conns():
    """(trade, gs, opened) with connect patched; ``opened`` lists db names connected to."""
    state: dict[str, Any] = {"trade": _Conn("trade"), "gs": _Conn("gs"), "opened": []}

    def fake_connect(**kwargs: Any) -> _Conn:
        db = kwargs.get("dbname")
        state["opened"].append(db)
        return state["gs"] if db == "gs" else state["trade"]

    with (
        patch(
            "bifrost_flex_query.orchestration.config_rw.psycopg2.connect",
            side_effect=fake_connect,
        ),
        patch(
            "bifrost_flex_query.orchestration.config_rw.get_golden_source_conn_params",
            return_value={"dbname": "gs"},
        ),
        patch("bifrost_flex_query.orchestration.config_rw.ensure_flex_ops_schema") as ensure,
    ):
        state["ensure"] = ensure
        yield state


def _upsert(gs: _Conn) -> Tuple[str, Any]:
    hits = [c for c in gs.calls if "INSERT INTO ops_jobs.flex_settings" in c[0]]
    assert len(hits) == 1
    return hits[0]


def test_range_only_updates_gs_row_and_not_trade(conns) -> None:
    conns["gs"].settings_row = (30, 270)
    ok = write_flex_config(CFG, None, 14, None)
    assert ok is True
    sql, params = _upsert(conns["gs"])
    assert "ON CONFLICT (id) DO UPDATE" in sql
    # Insert half only matters when the row is absent; the update keeps init (NULL → COALESCE).
    assert params == (14, 270, 14, None)
    assert conns["gs"].commits == 1
    assert conns["gs"].closed is True
    assert conns["opened"] == ["gs"]
    assert "UPDATE settings" not in conns["trade"].sql()
    conns["ensure"].assert_called_once()


def test_first_write_without_a_row_keeps_the_default_for_the_other_half(conns) -> None:
    conns["gs"].settings_row = None
    ok = write_flex_config(CFG, None, 14, None)
    assert ok is True
    _, params = _upsert(conns["gs"])
    assert params == (14, 360, 14, None)
    # Only Golden Source is opened (TD-116).
    assert conns["opened"] == ["gs"]
    assert conns["trade"].calls == []


def test_range_and_accounts_share_one_transaction(conns) -> None:
    ok = write_flex_config(
        CFG,
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
    gs = conns["gs"]
    _, params = _upsert(gs)
    assert params == (14, 180, 14, 180)
    assert any("DELETE FROM" in sql for sql, _ in gs.calls)
    insert = [c for c in gs.calls if "INSERT INTO raw_broker" in c[0]]
    assert len(insert) == 1
    assert insert[0][1][3] == "111"
    assert insert[0][1][4] == "222"
    assert gs.commits == 1
    assert conns["opened"] == ["gs"]


def test_query_row_failure_rolls_back_range_too(conns) -> None:
    conns["gs"].settings_row = (30, 270)
    conns["gs"].fail_on = "INSERT INTO raw_broker"
    ok = write_flex_config(CFG, [{"query_host_id": "111", "purpose": "trades"}], 14, 180)
    assert ok is False
    gs = conns["gs"]
    _upsert(gs)  # the range statement ran ...
    assert gs.commits == 0  # ... but nothing was committed
    assert gs.rollbacks == 1
    assert gs.closed is True


def test_range_failure_leaves_query_rows_untouched(conns) -> None:
    conns["gs"].settings_row = (30, 270)
    conns["gs"].fail_on = "INSERT INTO ops_jobs.flex_settings"
    ok = write_flex_config(CFG, [{"query_host_id": "111", "purpose": "trades"}], 14, None)
    assert ok is False
    assert "DELETE FROM" not in conns["gs"].sql()
    assert conns["gs"].commits == 0


def test_accounts_only_does_not_touch_settings(conns) -> None:
    ok = write_flex_config(CFG, [{"query_host_id": "111", "purpose": "trades"}])
    assert ok is True
    assert "flex_settings" not in conns["gs"].sql()
    conns["ensure"].assert_not_called()
    assert conns["gs"].commits == 1


def test_empty_accounts_refuses_without_delete(conns) -> None:
    ok = write_flex_config(CFG, [])
    assert ok is False
    assert conns["opened"] == []


def test_blank_query_host_accounts_refuses(conns) -> None:
    ok = write_flex_config(CFG, [{"query_host_id": "  ", "purpose": "trades"}])
    assert ok is False
    assert conns["opened"] == []


def test_the_writer_takes_no_tokens() -> None:
    """0.8.0 (TD-83): tokens live only in the K8s Secret; the writer has no way to take one."""
    import inspect

    params = set(inspect.signature(write_flex_config).parameters)
    assert not {"host_token", "secondary_token"} & params


def test_nothing_to_write_is_success_noop(conns) -> None:
    ok = write_flex_config(CFG, None)
    assert ok is True
    assert conns["opened"] == []
