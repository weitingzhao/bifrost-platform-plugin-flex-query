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

    trade = dict(cfg.get("trade_postgres") or {})
    for key, env_name in (
        ("host", "FLEX_TRADE_PG_HOST"),
        ("port", "FLEX_TRADE_PG_PORT"),
        ("dbname", "FLEX_TRADE_PG_DB"),
        ("user", "FLEX_TRADE_PG_USER"),
        ("password", "FLEX_TRADE_PG_PASSWORD"),
    ):
        val = os.environ.get(env_name)
        if val is not None and str(val).strip() != "":
            trade[key] = int(val) if key == "port" else val
    if trade:
        cfg["trade_postgres"] = trade

    tok = os.environ.get("FLEX_QUERY_WRITE_TOKEN")
    if tok:
        cfg["write_token"] = tok.strip()
    return cfg


def postgres_connect_kwargs(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Connect kwargs for Golden Source (flex_ops + brokerage writes via core)."""
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


def trade_postgres_connect_kwargs(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Connect kwargs for per-env Trade DB (public.settings Flex tokens)."""
    data = cfg if cfg is not None else load_config()
    trade = dict(data.get("trade_postgres") or {})
    pg = dict(data.get("postgres") or {})
    return {
        "host": (
            trade.get("host")
            or os.environ.get("FLEX_TRADE_PG_HOST")
            or pg.get("host")
            or "localhost"
        ),
        "port": int(
            trade.get("port")
            or os.environ.get("FLEX_TRADE_PG_PORT")
            or pg.get("port")
            or 5432
        ),
        "dbname": (
            trade.get("dbname")
            or trade.get("database")
            or os.environ.get("FLEX_TRADE_PG_DB")
            or "bifrost_dev"
        ),
        "user": (
            trade.get("user")
            or os.environ.get("FLEX_TRADE_PG_USER")
            or pg.get("user")
            or "bifrost"
        ),
        "password": (
            trade.get("password")
            or os.environ.get("FLEX_TRADE_PG_PASSWORD")
            or pg.get("password")
            or ""
        ),
    }


def trade_config_for_core(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Shape a config dict that bifrost-core StatusReader / Flex fetch can consume."""
    data = dict(cfg if cfg is not None else load_config())
    trade = dict(data.get("trade_postgres") or {})
    gs = dict(data.get("golden_source") or {})
    pg = dict(data.get("postgres") or {})
    if trade:
        data["postgres"] = {
            "host": trade.get("host") or pg.get("host"),
            "port": trade.get("port") or pg.get("port"),
            "database": trade.get("dbname") or trade.get("database"),
            "dbname": trade.get("dbname") or trade.get("database"),
            "user": trade.get("user") or pg.get("user"),
            "password": trade.get("password") or pg.get("password"),
        }
        data["sink"] = "postgres"
    if not gs:
        data["golden_source"] = {
            "host": pg.get("host"),
            "port": pg.get("port"),
            "database": pg.get("dbname") or "bifrost_golden_source",
            "user": pg.get("user"),
            "password": pg.get("password"),
        }
    if "ib" not in data:
        data["ib"] = {
            "host": {"ip": "127.0.0.1", "port_type": "tws_paper", "client_id": {}},
            "connect_timeout": 60,
        }
    return data
