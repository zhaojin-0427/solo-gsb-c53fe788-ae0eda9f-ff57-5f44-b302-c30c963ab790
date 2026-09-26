"""Background loop that closes idle windows and overdue watermarks."""
from __future__ import annotations

import asyncio
import logging

from app.config import get_settings
from app.db import SessionLocal
from app.services.traces import sweep_once

logger = logging.getLogger("tail_sampling.sweeper")


async def run_sweeper(stop: asyncio.Event) -> None:
    settings = get_settings()
    logger.info(
        "sweeper started (window=%sms watermark=%sms interval=%sms)",
        settings.window_ms,
        settings.hard_watermark_ms,
        settings.sweep_interval_ms,
    )
    while not stop.is_set():
        try:
            async with SessionLocal() as session:
                result = await sweep_once(session)
            if result["watermarked"] or result["window_closed"]:
                logger.info("sweep closed traces: %s", result)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("sweep iteration failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.sweep_interval_seconds)
        except asyncio.TimeoutError:
            pass
    logger.info("sweeper stopped")
