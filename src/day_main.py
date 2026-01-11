from __future__ import annotations

import asyncio

from .config import load_settings
from .logger import setup_logger
from .competitors_day_parser import run_day_competitors_parser


async def run():
    settings = load_settings()
    logger = setup_logger()
    logger.info("Starting DAY competitors parser...")
    await run_day_competitors_parser(settings, logger)


if __name__ == "__main__":
    asyncio.run(run())