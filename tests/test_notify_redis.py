"""TD-266: flex notify must not silently use loopback Redis."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from bifrost_flex_query.orchestration.notify import (
    publish_flex_executions_system_message,
    resolve_message_center_redis_url,
)


@pytest.fixture(autouse=True)
def _clean_redis_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("REDIS_HOST", "REDIS_PORT", "REDIS_DB", "REDIS_PASSWORD", "REDIS_USERNAME"):
        monkeypatch.delenv(key, raising=False)


def test_resolve_url_none_without_explicit_redis() -> None:
    assert resolve_message_center_redis_url({}) is None
    assert resolve_message_center_redis_url({"postgres": {"host": "pg"}}) is None


def test_resolve_url_allows_explicit_loopback_in_yaml() -> None:
    url = resolve_message_center_redis_url({"redis": {"host": "127.0.0.1", "port": 6379, "db": 0}})
    assert url == "redis://127.0.0.1:6379/0"


def test_resolve_url_from_config_host() -> None:
    cfg = {"redis": {"host": "redis-live-prod.data.svc.cluster.local", "port": 6379, "db": 0}}
    assert resolve_message_center_redis_url(cfg) == "redis://redis-live-prod.data.svc.cluster.local:6379/0"


def test_publish_skips_redis_without_explicit_config() -> None:
    with patch("redis.from_url", create=True) as from_url:
        ok = publish_flex_executions_system_message(
            {"postgres": {"host": "pg"}},
            ok=True,
            title="t",
            message="m",
        )
    assert ok is False
    from_url.assert_not_called()


def test_publish_uses_redis_when_host_configured() -> None:
    mock_r = MagicMock()
    with patch("redis.from_url", create=True, return_value=mock_r) as from_url:
        with patch(
            "bifrost_flex_query.orchestration.notify.publish_system_message_event",
            return_value="1-0",
        ):
            ok = publish_flex_executions_system_message(
                {"redis": {"host": "redis-live-prod.data.svc.cluster.local", "port": 6379}},
                ok=True,
                title="t",
                message="m",
            )
    assert ok is True
    from_url.assert_called_once()
    mock_r.close.assert_called_once()
