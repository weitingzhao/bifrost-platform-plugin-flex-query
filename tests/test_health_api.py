"""Plugin API health — no database required."""

from __future__ import annotations

from fastapi.testclient import TestClient

from bifrost_flex_query.api.app import create_app
from bifrost_flex_query.api.ingest import list_kinds


def test_health() -> None:
    client = TestClient(create_app())
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["service"] == "flex-query"


def test_kinds() -> None:
    kinds = list_kinds()
    assert "flex-trades" in kinds["kinds"]
    assert "flex-transactions" in kinds["kinds"]
