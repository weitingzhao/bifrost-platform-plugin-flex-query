"""FastAPI application factory."""

from __future__ import annotations

from fastapi import FastAPI

from bifrost_flex_query.api.coverage import router as coverage_router
from bifrost_flex_query.api.health import router as health_router
from bifrost_flex_query.api.ingest import router as ingest_router
from bifrost_flex_query.api.ingest_dashboard import router as dashboard_router


def create_app() -> FastAPI:
    app = FastAPI(title="Bifrost Flex Query Plugin", version="0.1.0")
    app.include_router(health_router)
    app.include_router(ingest_router)
    app.include_router(dashboard_router)
    app.include_router(coverage_router)
    return app
