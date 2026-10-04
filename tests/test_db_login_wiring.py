"""D6 (2026-10-04): both Deployments take their Postgres login from the Secret.

The switch from bifrost to flex_writer (and its rollback) is one patch of
``flex-query-secrets``: ``postgres-user`` with ``postgres-password`` and
``trade-pg-password``. Each connection must read the user from that key —
the ops_jobs queue (POSTGRES_USER), the Golden Source raw_broker writes that
go through core (GOLDEN_SOURCE_USER; core falls back to the Trade password),
and the Trade database read (FLEX_TRADE_PG_USER) — or it would pair the
ConfigMap's name with the Secret's password after the switch.
"""

from __future__ import annotations

from pathlib import Path

import yaml

BASE = Path(__file__).resolve().parents[1] / "k8s" / "base"
SECRET = "flex-query-secrets"


def _env(path: str) -> dict[str, tuple[str, str, bool]]:
    doc = yaml.safe_load((BASE / path).read_text(encoding="utf-8"))
    (container,) = doc["spec"]["template"]["spec"]["containers"]
    out = {}
    for env in container.get("env") or []:
        ref = (env.get("valueFrom") or {}).get("secretKeyRef")
        if ref:
            out[env["name"]] = (ref["name"], ref["key"], bool(ref.get("optional")))
    return out


def test_every_connection_reads_its_user_from_the_secret() -> None:
    for path in ("deployment-api.yaml", "deployment-worker.yaml"):
        env = _env(path)
        for name in ("POSTGRES_USER", "GOLDEN_SOURCE_USER", "FLEX_TRADE_PG_USER"):
            assert env.get(name) == (SECRET, "postgres-user", True), (path, name)
        assert env["POSTGRES_PASSWORD"][:2] == (SECRET, "postgres-password"), path
        assert env["FLEX_TRADE_PG_PASSWORD"][:2] == (SECRET, "trade-pg-password"), path


def test_api_and_worker_read_the_same_trade_database() -> None:
    """The API used to fall back to the ConfigMap's dbname while the worker read the Secret."""
    assert _env("deployment-api.yaml")["FLEX_TRADE_PG_DB"] == (SECRET, "trade-pg-db", True)
    assert _env("deployment-worker.yaml")["FLEX_TRADE_PG_DB"] == (SECRET, "trade-pg-db", True)


def test_env_user_wins_over_the_configmap(monkeypatch, tmp_path) -> None:
    from bifrost_flex_query.config import (
        load_config,
        postgres_connect_kwargs,
        trade_config_for_core,
        trade_postgres_connect_kwargs,
    )

    cfg_file = tmp_path / "flex-query.yaml"
    cfg_file.write_text(
        "postgres: {host: db, dbname: bifrost_golden_source, user: bifrost, password: ''}\n"
        "trade_postgres: {host: db, dbname: bifrost_dev, user: bifrost, password: ''}\n"
        "golden_source: {host: db, database: bifrost_golden_source, user: bifrost, password: ''}\n",
        encoding="utf-8",
    )
    for name in ("POSTGRES_USER", "GOLDEN_SOURCE_USER", "FLEX_TRADE_PG_USER"):
        monkeypatch.setenv(name, "flex_writer")
    monkeypatch.setenv("POSTGRES_PASSWORD", "pw-invented")
    monkeypatch.setenv("FLEX_TRADE_PG_PASSWORD", "pw-invented")
    cfg = load_config(cfg_file)
    assert postgres_connect_kwargs(cfg)["user"] == "flex_writer"
    assert trade_postgres_connect_kwargs(cfg)["user"] == "flex_writer"

    from bifrost_core.persistence.postgres.connection import _get_golden_source_conn_params

    gs = _get_golden_source_conn_params(trade_config_for_core(cfg))
    assert gs["user"] == "flex_writer"
    assert gs["password"] == "pw-invented"  # the Trade password, as before D6
