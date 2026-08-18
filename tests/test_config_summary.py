"""Unit tests for Flex config summary (token masking + query rows)."""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from bifrost_flex_query.api.app import create_app
from bifrost_flex_query.api.config_summary import config_summary, mask_token_last4
from bifrost_flex_query.api.deps import db_conn, trade_db_conn
from bifrost_flex_query.config import trade_postgres_connect_kwargs


class _Cursor:
    def __init__(self, parent: _Conn) -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        if self.parent.raise_on_execute:
            raise RuntimeError("boom")
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

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Conn:
    def __init__(
        self,
        *,
        settings: dict[str, Any] | None = None,
        flex_rows: list[dict[str, Any]] | None = None,
        raise_on_execute: bool = False,
    ) -> None:
        self.settings = settings
        self.flex_rows = flex_rows or []
        self.raise_on_execute = raise_on_execute
        self.rolled_back = 0
        self._one: Any = None
        self._rows: list[Any] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def rollback(self) -> None:
        self.rolled_back += 1


def test_mask_token_last4() -> None:
    assert mask_token_last4(None) is None
    assert mask_token_last4("") is None
    assert mask_token_last4("   ") is None
    assert mask_token_last4("ab") == "ab"
    assert mask_token_last4("secretTOKEN12") == "EN12"


def test_trade_postgres_connect_kwargs_from_cfg() -> None:
    kw = trade_postgres_connect_kwargs(
        {
            "trade_postgres": {
                "host": "db.lan",
                "port": 30432,
                "dbname": "bifrost_dev",
                "user": "bifrost",
                "password": "x",
            }
        }
    )
    assert kw["host"] == "db.lan"
    assert kw["port"] == 30432
    assert kw["dbname"] == "bifrost_dev"
    assert kw["user"] == "bifrost"
    assert kw["password"] == "x"


def test_config_summary_masks_tokens_and_query_rows() -> None:
    trade = _Conn(
        settings={
            "ib_flex_host_token": "secretTOKEN12",
            "ib_flex_secondary_token": "",
            "flex_default_range_days": 14,
            "flex_init_range_days": 180,
        }
    )
    gs = _Conn(
        flex_rows=[
            {
                "purpose": "trades",
                "query_label": "Trades",
                "query_host_id": "123456",
                "query_secondary_id": None,
            },
            {
                "purpose": "cash_transactions",
                "query_label": "Cash",
                "query_host_id": "789012",
                "query_secondary_id": "789013",
            },
        ]
    )
    body = config_summary(gs_conn=gs, trade_conn=trade)
    assert body["tokens"]["host_token_set"] is True
    assert body["tokens"]["host_token_last4"] == "EN12"
    assert "secret" not in str(body)
    assert body["tokens"]["secondary_token_set"] is False
    assert body["tokens"]["secondary_token_last4"] is None
    assert body["range_days"] == {"default": 14, "init": 180}
    assert len(body["query_rows"]) == 2
    assert body["query_rows"][0]["query_host_id"] == "123456"
    assert body["query_rows"][1]["query_secondary_id"] == "789013"


def test_config_summary_empty_on_query_error() -> None:
    trade = _Conn(raise_on_execute=True)
    gs = _Conn(raise_on_execute=True)
    body = config_summary(gs_conn=gs, trade_conn=trade)
    assert trade.rolled_back == 1
    assert gs.rolled_back == 1
    assert body["tokens"]["host_token_set"] is False
    assert body["query_rows"] == []
    assert body["range_days"]["default"] == 30


def test_config_summary_http_override() -> None:
    app = create_app()

    def _gs() -> Any:
        yield _Conn(
            flex_rows=[
                {
                    "purpose": "trades",
                    "query_label": "Trades",
                    "query_host_id": "1",
                    "query_secondary_id": "",
                }
            ]
        )

    def _trade() -> Any:
        yield _Conn(
            settings={
                "ib_flex_host_token": "abcd1234",
                "ib_flex_secondary_token": "zzzz",
                "flex_default_range_days": 30,
                "flex_init_range_days": 360,
            }
        )

    app.dependency_overrides[db_conn] = _gs
    app.dependency_overrides[trade_db_conn] = _trade
    client = TestClient(app)
    r = client.get("/flex/config/summary")
    assert r.status_code == 200
    body = r.json()
    assert body["tokens"]["host_token_last4"] == "1234"
    assert body["tokens"]["secondary_token_last4"] == "zzzz"
    assert body["query_rows"][0]["query_secondary_id"] is None


def test_normalize_flex_accounts_skips_empty_host() -> None:
    from bifrost_flex_query.api.config_summary import normalize_flex_accounts

    rows = normalize_flex_accounts(
        [
            {"query_host_id": "  ", "purpose": "trades"},
            {
                "query_host_id": "111",
                "query_secondary_id": "222",
                "query_label": "Trades",
                "purpose": "trades",
            },
            "skip-me",
        ]
    )
    assert len(rows) == 1
    assert rows[0]["query_host_id"] == "111"
    assert rows[0]["query_secondary_id"] == "222"
    assert rows[0]["purpose"] == "trades"


def test_config_write_http(monkeypatch: Any) -> None:
    from bifrost_flex_query.api import config_summary as mod

    captured: dict[str, Any] = {}

    def _fake_write(
        status_config: dict[str, Any],
        host_token: str | None,
        secondary_token: str | None,
        accounts: list[dict[str, Any]],
        flex_default_range_days: int | None = None,
        flex_init_range_days: int | None = None,
    ) -> bool:
        captured["host_token"] = host_token
        captured["accounts"] = accounts
        captured["days"] = flex_default_range_days
        captured["init"] = flex_init_range_days
        captured["cfg"] = status_config
        return True

    monkeypatch.setattr(mod, "load_config", lambda: {"trade_postgres": {"host": "db"}})
    monkeypatch.setattr(mod, "trade_config_for_core", lambda cfg: {**cfg, "sink": "postgres"})
    monkeypatch.setattr("bifrost_core.monitor.reader.write_flex_config", _fake_write)

    client = TestClient(create_app())
    r = client.post(
        "/flex/config/write",
        json={
            "host_token": "tok",
            "secondary_token": "",
            "accounts": [{"query_host_id": "999", "purpose": "trades"}],
            "flex_default_range_days": 14,
            "flex_init_range_days": 180,
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["accounts"][0]["query_host_id"] == "999"
    assert captured["host_token"] == "tok"
    assert captured["days"] == 14
    assert captured["init"] == 180


def test_config_write_failure_http(monkeypatch: Any) -> None:
    from bifrost_flex_query.api import config_summary as mod

    monkeypatch.setattr(mod, "load_config", lambda: {})
    monkeypatch.setattr(mod, "trade_config_for_core", lambda cfg: cfg)
    monkeypatch.setattr("bifrost_core.monitor.reader.write_flex_config", lambda *a, **k: False)

    client = TestClient(create_app())
    r = client.post("/flex/config/write", json={"accounts": []})
    assert r.status_code == 500
