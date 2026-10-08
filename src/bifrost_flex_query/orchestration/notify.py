"""Flex ingest notifications via Trade Redis message center."""

from __future__ import annotations

import logging
import os
import uuid
from typing import Optional

from bifrost_core.core.message_center import SystemMessageEvent, publish_system_message_event

logger = logging.getLogger(__name__)

MESSAGE_CENTER_TOPIC_PORTFOLIO_FLEX_EXECUTIONS = "portfolio.flex_executions"

_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


def _explicit_redis_host(config: Optional[dict]) -> bool:
    """True when REDIS_HOST env or config redis.host is set (non-empty)."""
    if (os.environ.get("REDIS_HOST") or "").strip():
        return True
    r = (config or {}).get("redis") or {}
    host = r.get("host")
    return host is not None and str(host).strip() != ""


def resolve_message_center_redis_url(config: Optional[dict]) -> Optional[str]:
    """Redis URL for the Trade live bus, or None if unset / implicit loopback."""
    if not config:
        return None
    from bifrost_core.core.redis_url import effective_redis_dict, format_redis_url

    effective = effective_redis_dict(config, default_db=0)
    host = str(effective.get("host") or "").strip().lower()
    if host in _LOOPBACK_HOSTS and not _explicit_redis_host(config):
        return None
    return format_redis_url(effective)


def build_portfolio_flex_executions_fetch_event(
    *,
    ok: bool,
    title: str,
    message: str,
    reason: Optional[str] = None,
    level: Optional[str] = None,
    occurred_at: Optional[float] = None,
) -> SystemMessageEvent:
    """User-facing summary for Flex trades ingest / XML upload."""
    import time

    lv = level or ("error" if not ok else "success")
    status_to = "complete" if ok else "failed"
    return SystemMessageEvent(
        message_id=uuid.uuid4().hex,
        topic=MESSAGE_CENTER_TOPIC_PORTFOLIO_FLEX_EXECUTIONS,
        level=lv,
        service="portfolio_flex",
        slot="host",
        client_id=None,
        account=None,
        status_from="unknown",
        status_to=status_to,
        title=title,
        message=message,
        reason=reason,
        occurred_at=float(occurred_at or time.time()),
        dedupe_key=f"{MESSAGE_CENTER_TOPIC_PORTFOLIO_FLEX_EXECUTIONS}:{uuid.uuid4().hex}",
    )


def publish_flex_executions_system_message(
    config: Optional[dict],
    *,
    ok: bool,
    title: str,
    message: str,
    reason: Optional[str] = None,
    level: Optional[str] = None,
) -> bool:
    """Best-effort Redis message center (Monitor materializes for SSE).

    Returns True when the event was written to the stream. Ingest must not fail when
    this returns False — callers treat notification as auxiliary to persistence.
    """
    url = resolve_message_center_redis_url(config)
    if not url:
        if config is not None:
            logger.warning(
                "flex executions message center skipped: no explicit Redis host "
                "(set redis.host in flex-query config or REDIS_HOST); refusing loopback default"
            )
        return False
    try:
        import redis as redis_mod

        r = redis_mod.from_url(url, decode_responses=True)
        try:
            ev = build_portfolio_flex_executions_fetch_event(
                ok=ok, title=title, message=message, reason=reason, level=level
            )
            stream_id = publish_system_message_event(r, ev)
            if stream_id is None:
                logger.warning(
                    "flex executions message center publish failed topic=%s (xadd returned no id)",
                    MESSAGE_CENTER_TOPIC_PORTFOLIO_FLEX_EXECUTIONS,
                )
                return False
            return True
        finally:
            r.close()
    except Exception as e:
        logger.warning("flex executions message center publish failed: %s", e)
        return False
