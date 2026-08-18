"""Flex ingest notifications via Trade Redis message center."""

from __future__ import annotations

import logging
import uuid
from typing import Optional

from bifrost_core.core.message_center import SystemMessageEvent, publish_system_message_event

logger = logging.getLogger(__name__)

MESSAGE_CENTER_TOPIC_PORTFOLIO_FLEX_EXECUTIONS = "portfolio.flex_executions"


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
) -> None:
    """Best-effort Redis message center (Monitor materializes for SSE)."""
    if not config:
        return
    try:
        import redis as redis_mod
        from bifrost_core.core.redis_url import effective_redis_dict, format_redis_url

        url = format_redis_url(effective_redis_dict(config, default_db=0))
        if not url:
            return
        r = redis_mod.from_url(url, decode_responses=True)
        try:
            ev = build_portfolio_flex_executions_fetch_event(
                ok=ok, title=title, message=message, reason=reason, level=level
            )
            publish_system_message_event(r, ev)
        finally:
            r.close()
    except Exception as e:
        logger.debug("flex executions message center publish failed: %s", e)
