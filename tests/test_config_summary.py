"""Unit tests for Flex config summary (token masking + query rows)."""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from bifrost_flex_query.api.app import create_app
from bifrost_flex_query.api.config_summary import config_summary, mask_token_last4
from bifrost_flex_query.api.deps import db_conn


class _Cursor:
    def __init__(self, parent: _Conn) -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        if self.parent.raise_on_execute:
            raise RuntimeError("boom")
        q = query.lower()
        if "ops_jobs.flex_settings" in q:
            self.parent._one = self.parent.flex_settings
            self.parent._rows = [self.parent.flex_settings] if self.parent.flex_settings else []
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
        flex_rows: list[dict[str, Any]] | None = None,
        flex_settings: dict[str, Any] | None = None,
        raise_on_execute: bool = False,
    ) -> None:
        self.flex_settings = flex_settings
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


def test_config_summary_masks_tokens_and_query_rows(monkeypatch) -> None:
    monkeypatch.delenv("FLEX_HOST_TOKEN", raising=False)
    monkeypatch.delenv("FLEX_SECONDARY_TOKEN", raising=False)
    import bifrost_flex_query.orchestration.config_rw as mod

    mod._token_source_logged = False

    gs = _Conn(
        flex_settings={"flex_default_range_days": 14, "flex_init_range_days": 180},
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
    body = config_summary(gs_conn=gs)
    assert body["tokens"]["host_token_set"] is False
    assert body["source"] == "none"
    assert body["tokens"]["host_source"] == "none"
    assert body["tokens"]["secondary_token_set"] is False
    assert body["tokens"]["secondary_token_last4"] is None
    assert body["range_days"] == {"default": 14, "init": 180}
    assert len(body["query_rows"]) == 2
    assert body["query_rows"][0]["query_host_id"] == "123456"
    assert body["query_rows"][1]["query_secondary_id"] == "789013"


def test_config_summary_unreadable_settings_is_503() -> None:
    """TD-116 (0.11.0): a failed range-days read is an error, not the defaults."""
    from fastapi import HTTPException

    gs = _Conn(raise_on_execute=True)
    with pytest.raises(HTTPException) as exc:
        config_summary(gs_conn=gs)
    assert exc.value.status_code == 503
    assert gs.rolled_back == 1


def test_config_summary_defaults_without_settings_row() -> None:
    """A cluster without the ops_jobs.flex_settings row shows the defaults (first write creates it)."""
    body = config_summary(gs_conn=_Conn(flex_settings=None))
    assert body["range_days"] == {"default": 30, "init": 360}


def test_config_summary_range_from_gs_settings_row() -> None:
    gs = _Conn(flex_settings={"flex_default_range_days": 21, "flex_init_range_days": 400})
    body = config_summary(gs_conn=gs)
    assert body["range_days"] == {"default": 21, "init": 400}


def test_config_summary_http_override(monkeypatch) -> None:
    monkeypatch.setenv("FLEX_HOST_TOKEN", "abcd1234")
    monkeypatch.setenv("FLEX_SECONDARY_TOKEN", "zzzz")
    import bifrost_flex_query.orchestration.config_rw as mod

    mod._token_source_logged = False

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

    app.dependency_overrides[db_conn] = _gs
    client = TestClient(app)
    r = client.get("/flex/config/summary")
    assert r.status_code == 200
    body = r.json()
    assert body["source"] == "secret"
    assert body["tokens"]["host_token_last4"] == "1234"
    assert body["tokens"]["secondary_token_last4"] == "zzzz"
    assert body["query_rows"][0]["query_secondary_id"] is None


GATEWAY = {"X-Bifrost-Trade-Gateway": "1"}

FANOUT_CFG = {
    "postgres": {"host": "db", "dbname": "bifrost_golden_source"},
    "golden_source": {"host": "db", "database": "bifrost_golden_source"},
}


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

    calls: list[dict[str, Any]] = []

    def _fake_write(
        status_config: dict[str, Any],
        accounts: list[dict[str, Any]] | None,
        flex_default_range_days: int | None = None,
        flex_init_range_days: int | None = None,
    ) -> bool:
        calls.append(
            {
                "dbname": (status_config.get("postgres") or {}).get("dbname"),
                "accounts": accounts,
                "days": flex_default_range_days,
                "init": flex_init_range_days,
            }
        )
        return True

    monkeypatch.setattr(mod, "load_config", lambda: FANOUT_CFG)
    monkeypatch.setattr(
        "bifrost_flex_query.orchestration.config_rw.write_flex_config", _fake_write
    )

    client = TestClient(create_app())
    r = client.post(
        "/flex/config/write",
        headers=GATEWAY,
        json={
            "accounts": [{"query_host_id": "999", "purpose": "trades"}],
            "flex_default_range_days": 14,
            "flex_init_range_days": 180,
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body == {
        "ok": True,
        "accounts": [{"query_host_id": "999", "query_secondary_id": None, "query_label": None, "purpose": "trades"}],
        "flex_default_range_days": 14,
        "flex_init_range_days": 180,
    }
    # One call: range days and query rows land in one Golden Source transaction.
    assert len(calls) == 1
    assert calls[0]["days"] == 14
    assert calls[0]["init"] == 180
    assert calls[0]["accounts"][0]["query_host_id"] == "999"


@pytest.mark.parametrize(
    "body",
    [
        {"host_token": "tok-made-up-0001"},
        {"secondary_token": ""},
        {"host_token": None, "accounts": [{"query_host_id": "999"}]},
        {"secondary_token": "tok-made-up-0002", "flex_default_range_days": 14},
    ],
)
def test_a_token_field_is_409_and_nothing_is_written(monkeypatch: Any, body: dict[str, Any]) -> None:
    """TD-83 (0.8.0): tokens live only in the K8s Secret. Any token key, whatever its value,
    is refused before the writer runs, and the token is never echoed back."""
    from bifrost_flex_query.api import config_summary as mod

    called: list[Any] = []
    monkeypatch.setattr(mod, "load_config", lambda: FANOUT_CFG)
    monkeypatch.setattr(
        "bifrost_flex_query.orchestration.config_rw.write_flex_config", lambda *a, **k: called.append(a) or True
    )
    r = TestClient(create_app()).post("/flex/config/write", headers=GATEWAY, json=body)
    assert r.status_code == 409
    assert r.json() == {"detail": mod.TOKENS_NOT_STORED}
    assert "make sync-flex-tokens" in r.json()["detail"]
    assert "tok-made-up" not in r.text
    assert called == []


def test_config_write_empty_body_400() -> None:
    client = TestClient(create_app())
    r = client.post("/flex/config/write", headers=GATEWAY, json={})
    assert r.status_code == 400
    assert "empty" in r.json()["detail"]


def test_config_write_empty_accounts_400() -> None:
    client = TestClient(create_app())
    r = client.post("/flex/config/write", headers=GATEWAY, json={"accounts": []})
    assert r.status_code == 400
    assert "query_host_id" in r.json()["detail"]


def test_config_write_without_gateway_401() -> None:
    client = TestClient(create_app())
    r = client.post(
        "/flex/config/write",
        json={"accounts": [{"query_host_id": "1"}]},
    )
    assert r.status_code == 401


def test_config_write_failure_http(monkeypatch: Any) -> None:
    from bifrost_flex_query.api import config_summary as mod

    monkeypatch.setattr(mod, "load_config", lambda: FANOUT_CFG)
    monkeypatch.setattr(
        "bifrost_flex_query.orchestration.config_rw.write_flex_config", lambda *a, **k: False
    )

    client = TestClient(create_app())
    r = client.post(
        "/flex/config/write",
        headers=GATEWAY,
        json={"accounts": [{"query_host_id": "999", "purpose": "trades"}]},
    )
    assert r.status_code == 500
