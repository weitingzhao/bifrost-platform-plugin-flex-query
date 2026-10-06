"""Flex config summary (read) and write (query rows + range days; tokens stay in the Secret).

Tokens are read from the K8s Secret ``bifrost-flex-tokens`` only and are set with
``make sync-flex-tokens``. Since 0.8.0 (TD-83, decision D11-A) the write refuses a token
field with 409 instead of answering ``ok`` without storing it, and no answer carries a
token: the summary shows the last four characters and the issued age.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from bifrost_flex_query.api.deps import db_conn, require_config_write_identity
from bifrost_flex_query.config import core_config, load_config

router = APIRouter(prefix="/flex/config", tags=["config"])


def mask_token_last4(token: str | None) -> str | None:
    s = (token or "").strip()
    if not s:
        return None
    return s[-4:]


def _blank_to_none(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


@router.get("/summary")
def config_summary(gs_conn: Any = Depends(db_conn)) -> dict[str, Any]:
    from bifrost_flex_query.orchestration.config_rw import get_flex_range_days, resolve_flex_tokens

    host_tok, sec_tok, host_src, sec_src = resolve_flex_tokens()
    # ops_jobs.flex_settings (TD-74); a failed read is a 503, not the defaults (TD-116).
    try:
        default_days, init_days = get_flex_range_days(gs_conn)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"flex settings unreadable: {exc}") from exc

    query_rows: list[dict[str, Any]] = []
    try:
        with gs_conn.cursor() as cur:
            cur.execute(
                """
                SELECT query_host_id, query_secondary_id, query_label, purpose
                FROM raw_broker.settings_flex
                ORDER BY sort_order, id
                """
            )
            for r in cur.fetchall() or []:
                query_rows.append(
                    {
                        "purpose": _blank_to_none(r.get("purpose")) or "cash_transactions",
                        "query_label": _blank_to_none(r.get("query_label")),
                        "query_host_id": str(r.get("query_host_id") or "").strip(),
                        "query_secondary_id": _blank_to_none(r.get("query_secondary_id")),
                    }
                )
    except Exception:
        gs_conn.rollback()

    host_last4 = mask_token_last4(host_tok)
    sec_last4 = mask_token_last4(sec_tok)
    if host_src == "secret" or sec_src == "secret":
        source = "secret"
    else:
        source = "none"
    from bifrost_flex_query.orchestration.config_rw import flex_tokens_issued_at

    issued_at, age_days = flex_tokens_issued_at()
    return {
        "tokens": {
            "host_token_set": host_last4 is not None,
            "host_token_last4": host_last4,
            "secondary_token_set": sec_last4 is not None,
            "secondary_token_last4": sec_last4,
            "host_source": host_src,
            "secondary_source": sec_src,
            "issued_at": issued_at,
            "age_days": age_days,
        },
        "source": source,
        "range_days": {"default": default_days, "init": init_days},
        "query_rows": query_rows,
    }


def _optional_days(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    return max(1, n)


def normalize_flex_accounts(raw: Any) -> list[dict[str, Any]]:
    """Normalize POST /flex/config/write ``accounts`` rows (skip empty host query ids)."""
    accounts: list[dict[str, Any]] = []
    for a in raw or []:
        if not isinstance(a, dict):
            continue
        qh = str(a.get("query_host_id") or "").strip()
        if not qh:
            continue
        accounts.append(
            {
                "query_host_id": qh,
                "query_secondary_id": str(a.get("query_secondary_id") or "").strip() or None,
                "query_label": str(a.get("query_label") or "").strip() or None,
                "purpose": str(a.get("purpose") or "cash_transactions").strip() or "cash_transactions",
            }
        )
    return accounts


_TOKEN_FIELDS = ("host_token", "secondary_token")
_RANGE_FIELDS = ("flex_default_range_days", "flex_init_range_days")
_WRITE_FIELDS = ("accounts", *_RANGE_FIELDS)

TOKENS_NOT_STORED = (
    "Flex tokens are not stored by this endpoint. "
    "Set them in the K8s Secret bifrost-flex-tokens (make sync-flex-tokens)."
)


@router.post("/write", dependencies=[Depends(require_config_write_identity)])
def write_flex_config_endpoint(body: dict[str, Any] | None = None) -> dict[str, Any]:
    """Persist range days (``ops_jobs.flex_settings``) and query rows (``raw_broker.settings_flex``).

    Both are in Golden Source and land in one transaction: all or nothing. A body that
    names ``host_token`` or ``secondary_token`` -- whatever the value, null included --
    is 409 before anything is written: tokens live only in the K8s Secret (TD-83).
    """
    from bifrost_flex_query.orchestration.config_rw import write_flex_config

    payload = body or {}
    if any(key in payload for key in _TOKEN_FIELDS):
        raise HTTPException(status_code=409, detail=TOKENS_NOT_STORED)
    if not any(key in payload for key in _WRITE_FIELDS):
        raise HTTPException(status_code=400, detail="empty flex config write")

    accounts: list[dict[str, Any]] | None
    if "accounts" in payload:
        accounts = normalize_flex_accounts(payload.get("accounts"))
        if not accounts:
            raise HTTPException(
                status_code=400,
                detail="accounts must include at least one query_host_id",
            )
    else:
        accounts = None

    default_days = _optional_days(payload.get("flex_default_range_days"))
    init_days = _optional_days(payload.get("flex_init_range_days"))

    cfg = load_config()
    ok = write_flex_config(core_config(cfg), accounts, default_days, init_days)
    if not ok:
        raise HTTPException(status_code=500, detail="failed to write flex config")
    # 0.8.0: no token echo, no token_write_target.
    return {
        "ok": True,
        "accounts": accounts,
        "flex_default_range_days": default_days,
        "flex_init_range_days": init_days,
    }
