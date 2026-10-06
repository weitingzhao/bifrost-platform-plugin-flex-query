"""YAML configuration loading for Flex Query plugin."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


def default_config_path() -> Path | None:
    env = (os.environ.get("FLEX_QUERY_CONFIG") or "").strip()
    if env:
        p = Path(env)
        return p if p.is_file() else None
    here = Path(__file__).resolve().parents[2]
    for candidate in (
        here / "config" / "flex-query.yaml",
        here / "config" / "flex-query.yaml.example",
    ):
        if candidate.is_file():
            return candidate
    return None


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    cfg: dict[str, Any] = {}
    resolved: Path | None
    if path is not None:
        resolved = Path(path)
    else:
        resolved = default_config_path()
    if resolved is not None and resolved.is_file():
        with resolved.open(encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if isinstance(raw, dict):
            cfg = raw

    pg = dict(cfg.get("postgres") or {})
    for key, env_name in (
        ("host", "POSTGRES_HOST"),
        ("port", "POSTGRES_PORT"),
        ("dbname", "POSTGRES_DB"),
        ("user", "POSTGRES_USER"),
        ("password", "POSTGRES_PASSWORD"),
    ):
        val = os.environ.get(env_name)
        if val is not None and str(val).strip() != "":
            pg[key] = int(val) if key == "port" else val
    if pg:
        cfg["postgres"] = pg

    gs = dict(cfg.get("golden_source") or {})
    for key, env_name in (
        ("host", "GOLDEN_SOURCE_HOST"),
        ("port", "GOLDEN_SOURCE_PORT"),
        ("database", "GOLDEN_SOURCE_DATABASE"),
        ("user", "GOLDEN_SOURCE_USER"),
        ("password", "GOLDEN_SOURCE_PASSWORD"),
    ):
        val = os.environ.get(env_name)
        if val is not None and str(val).strip() != "":
            gs[key] = int(val) if key == "port" else val
    if gs:
        cfg["golden_source"] = gs

    tok = os.environ.get("FLEX_QUERY_WRITE_TOKEN")
    if tok:
        cfg["write_token"] = tok.strip()
    return cfg


def postgres_connect_kwargs(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Connect kwargs for Golden Source (ops_jobs queue + raw_broker writes via core)."""
    data = cfg if cfg is not None else load_config()
    pg = dict(data.get("postgres") or {})
    gs = dict(data.get("golden_source") or {})
    return {
        "host": pg.get("host") or gs.get("host") or os.environ.get("POSTGRES_HOST") or "localhost",
        "port": int(pg.get("port") or gs.get("port") or os.environ.get("POSTGRES_PORT") or 5432),
        "dbname": (
            pg.get("dbname")
            or gs.get("database")
            or os.environ.get("POSTGRES_DB")
            or "bifrost_golden_source"
        ),
        "user": pg.get("user") or gs.get("user") or os.environ.get("POSTGRES_USER") or "bifrost",
        "password": pg.get("password") or gs.get("password") or os.environ.get("POSTGRES_PASSWORD") or "",
    }


def core_config(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Shape a config dict that Flex orchestration / core upsert helpers can consume.

    Every database the plugin touches is Golden Source (TD-116, 0.11.0): core's writers
    open it from ``golden_source`` (``GOLDEN_SOURCE_*`` env first), falling back per field
    to ``postgres`` -- set here to the plugin's own Golden Source login, so the password
    comes from ``POSTGRES_PASSWORD``. No Trade env database is named (``trade_config_for_core``
    before 0.11.0 pointed ``postgres`` at ``bifrost_dev``).
    """
    data = dict(cfg if cfg is not None else load_config())
    pg = postgres_connect_kwargs(data)
    gs = dict(data.get("golden_source") or {})
    data["postgres"] = {**pg, "database": pg["dbname"]}
    data["golden_source"] = {
        "host": gs.get("host") or pg["host"],
        "port": gs.get("port") or pg["port"],
        "database": gs.get("database") or gs.get("dbname") or pg["dbname"],
        "user": gs.get("user") or pg["user"],
        "password": gs.get("password") or pg["password"],
    }
    data["sink"] = "postgres"
    if "ib" not in data:
        data["ib"] = {
            "host": {"ip": "127.0.0.1", "port_type": "tws_paper", "client_id": {}},
            "connect_timeout": 60,
        }
    return data
