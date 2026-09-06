"""FastAPI application factory."""

from __future__ import annotations

from fastapi import FastAPI

from bifrost_flex_query import __version__
from bifrost_flex_query.api.config_summary import router as config_router
from bifrost_flex_query.api.coverage import router as coverage_router
from bifrost_flex_query.api.freshness_kpis import router as freshness_kpis_router
from bifrost_flex_query.api.health import router as health_router
from bifrost_flex_query.api.ingest import router as ingest_router
from bifrost_flex_query.api.ingest_dashboard import router as dashboard_router
from bifrost_flex_query.api.manual_ops import router as manual_ops_router
from bifrost_flex_query.api.metrics import router as metrics_router
from bifrost_flex_query.api.raw_peek import router as raw_peek_router


def create_app() -> FastAPI:
    app = FastAPI(title="Bifrost Flex Query Plugin", version=__version__)
    app.include_router(health_router)
    app.include_router(ingest_router)
    app.include_router(dashboard_router)
    app.include_router(coverage_router)
    app.include_router(config_router)
    app.include_router(raw_peek_router)
    app.include_router(freshness_kpis_router)
    app.include_router(manual_ops_router)
    app.include_router(metrics_router)
    return app
