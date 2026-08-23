"""dispatch must update ingest_freshness even when invoked from a worker thread."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

from bifrost_flex_query.worker.handlers import dispatch


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def cursor(self) -> _FakeConn:
        return self

    def execute(self, *args: Any, **kwargs: Any) -> None:
        _ = args, kwargs

    def commit(self) -> None:
        pass

    def close(self) -> None:
        pass

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *args: object) -> None:
        return None


def test_dispatch_records_freshness_on_separate_connection() -> None:
    fake = _FakeConn()

    def fake_connect(**_kwargs: Any) -> _FakeConn:
        return fake

    with (
        patch("psycopg2.connect", side_effect=fake_connect),
        patch(
            "bifrost_flex_query.worker.handlers.HANDLERS",
            {
                "flex-trades": lambda _p, _c: {"inserted": 7, "ok": True, "result": {}},
            },
        ),
        patch("bifrost_flex_query.worker.handlers.update_freshness") as upd,
    ):
        out = dispatch("flex-trades", {}, {}, conn=object())

    assert out["inserted"] == 7
    upd.assert_called_once_with(fake, "flex-trades", 7)
