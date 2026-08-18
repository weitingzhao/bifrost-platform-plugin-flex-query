#!/usr/bin/env python3
"""Run Flex Query Plugin API on :8791."""

from __future__ import annotations

import os

import uvicorn

from bifrost_flex_query.api.app import create_app


def main() -> None:
    host = os.environ.get("FLEX_QUERY_API_HOST", "0.0.0.0")
    port = int(os.environ.get("FLEX_QUERY_API_PORT", "8791"))
    uvicorn.run(create_app(), host=host, port=port)


if __name__ == "__main__":
    main()
