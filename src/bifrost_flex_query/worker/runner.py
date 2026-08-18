"""CLI entry for the Flex Query worker."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Flex Query ingest worker")
    parser.add_argument("--config", default=None)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    from bifrost_flex_query.worker.loop import run_forever

    asyncio.run(run_forever(config_path=args.config))
    return 0


if __name__ == "__main__":
    sys.exit(main())
