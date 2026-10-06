"""D6 (2026-10-04): both Deployments take their Postgres login from the Secret.

The switch from bifrost to flex_writer (and its rollback) is one patch of
``flex-query-secrets``: ``postgres-user`` with ``postgres-password``. Each
connection must read the user from that key — the ops_jobs queue
(POSTGRES_USER) and the Golden Source raw_broker reads and writes that go
through core (GOLDEN_SOURCE_USER; core falls back to the plugin's password) —
or it would pair the ConfigMap's name with the Secret's password after the
switch. Since 0.11.0 (TD-116) there is no Trade database connection at all.
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
        for name in ("POSTGRES_USER", "GOLDEN_SOURCE_USER"):
            assert env.get(name) == (SECRET, "postgres-user", True), (path, name)
        assert env["POSTGRES_PASSWORD"][:2] == (SECRET, "postgres-password"), path


def test_no_trade_database_wiring() -> None:
    """TD-116: neither Deployment names a Trade env database connection."""
    for path in ("deployment-api.yaml", "deployment-worker.yaml"):
        env = _env(path)
        assert not [n for n in env if n.startswith("FLEX_TRADE_PG_")], path
        assert not [k for _, k, _ in env.values() if k.startswith("trade-pg-")], path


def test_env_user_wins_over_the_configmap(monkeypatch, tmp_path) -> None:
    from bifrost_flex_query.config import core_config, load_config, postgres_connect_kwargs

    cfg_file = tmp_path / "flex-query.yaml"
    cfg_file.write_text(
        "postgres: {host: db, dbname: bifrost_golden_source, user: bifrost, password: ''}\n"
        "golden_source: {host: db, database: bifrost_golden_source, user: bifrost, password: ''}\n",
        encoding="utf-8",
    )
    for name in ("POSTGRES_USER", "GOLDEN_SOURCE_USER"):
        monkeypatch.setenv(name, "flex_writer")
    monkeypatch.setenv("POSTGRES_PASSWORD", "pw-invented")
    monkeypatch.delenv("GOLDEN_SOURCE_PASSWORD", raising=False)
    cfg = load_config(cfg_file)
    assert postgres_connect_kwargs(cfg)["user"] == "flex_writer"

    from bifrost_core.persistence.postgres.connection import _get_golden_source_conn_params

    gs = _get_golden_source_conn_params(core_config(cfg))
    assert gs["user"] == "flex_writer"
    assert gs["password"] == "pw-invented"  # the plugin's own password (POSTGRES_PASSWORD)
    assert gs["dbname"] == "bifrost_golden_source"
