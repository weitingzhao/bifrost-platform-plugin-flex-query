"""Wave 4: FLEX_*_TOKEN env overrides Trade DB settings columns."""

from __future__ import annotations

from typing import Any

from bifrost_flex_query.api.config_summary import config_summary
from bifrost_flex_query.orchestration.config_rw import get_flex_config, resolve_flex_tokens


class _Cursor:
    def __init__(self, parent: "_Conn") -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        q = query.lower()
        if "from settings" in q:
            self.parent._one = self.parent.settings
            self.parent._rows = [self.parent.settings] if self.parent.settings else []
        elif "settings_flex" in q:
            self.parent._rows = list(self.parent.flex_rows)
            self.parent._one = self.parent.flex_rows[0] if self.parent.flex_rows else None
        else:
            self.parent._one = None
            self.parent._rows = []

    def fetchone(self) -> Any:
        return self.parent._one

    def fetchall(self) -> list[Any]:
        return self.parent._rows

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Conn:
    def __init__(
        self,
        *,
        settings: dict[str, Any] | None = None,
        flex_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.settings = settings
        self.flex_rows = flex_rows or []
        self._one: Any = None
        self._rows: list[Any] = []
        self.rolled_back = 0

    def cursor(self, cursor_factory: Any = None) -> _Cursor:
        return _Cursor(self)

    def rollback(self) -> None:
        self.rolled_back += 1


def test_resolve_flex_tokens_env_over_db(monkeypatch) -> None:
    monkeypatch.setenv("FLEX_HOST_TOKEN", "envHostToken99")
    monkeypatch.setenv("FLEX_SECONDARY_TOKEN", "envSecToken88")
    import bifrost_flex_query.orchestration.config_rw as mod

    mod._token_source_logged = False

    host, sec, hs, ss = resolve_flex_tokens("dbHost", "dbSec")
    assert host == "envHostToken99"
    assert sec == "envSecToken88"
    assert hs == "secret"
    assert ss == "secret"


def test_resolve_flex_tokens_db_fallback(monkeypatch) -> None:
    monkeypatch.delenv("FLEX_HOST_TOKEN", raising=False)
    monkeypatch.delenv("FLEX_SECONDARY_TOKEN", raising=False)
    import bifrost_flex_query.orchestration.config_rw as mod

    mod._token_source_logged = False

    host, sec, hs, ss = resolve_flex_tokens("dbHost", "")
    assert host == "dbHost"
    assert sec == ""
    assert hs == "db"
    assert ss == "none"


def test_get_flex_config_env_override(monkeypatch) -> None:
    monkeypatch.setenv("FLEX_HOST_TOKEN", "fromEnvHOST")
    monkeypatch.delenv("FLEX_SECONDARY_TOKEN", raising=False)
    import bifrost_flex_query.orchestration.config_rw as mod

    mod._token_source_logged = False

    conn = _Conn(
        settings={
            "ib_flex_host_token": "fromDB",
            "ib_flex_secondary_token": "fromDBSec",
        }
    )
    out = get_flex_config(conn)
    assert out["host_token"] == "fromEnvHOST"
    assert out["secondary_token"] == "fromDBSec"


def test_config_summary_source_secret(monkeypatch) -> None:
    monkeypatch.setenv("FLEX_HOST_TOKEN", "envTOKEN9999")
    monkeypatch.delenv("FLEX_SECONDARY_TOKEN", raising=False)
    import bifrost_flex_query.orchestration.config_rw as mod

    mod._token_source_logged = False

    trade = _Conn(
        settings={
            "ib_flex_host_token": "dbTOKEN1111",
            "ib_flex_secondary_token": "",
            "flex_default_range_days": 30,
            "flex_init_range_days": 360,
        }
    )
    gs = _Conn(flex_rows=[])
    body = config_summary(gs_conn=gs, trade_conn=trade)
    assert body["source"] == "secret"
    assert body["tokens"]["host_source"] == "secret"
    assert body["tokens"]["host_token_last4"] == "9999"
    assert "envTOKEN9999" not in str(body)
