"""
APScheduler (AsyncIOScheduler) started/stopped from FastAPI lifespan.
Schedules nightly ingest at 22:00 Asia/Kolkata. Only one worker runs ingest (file lock).
"""

import asyncio
import os

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from imdb_service.ingest import run_ingest_with_lock


_scheduler: AsyncIOScheduler | None = None


def _run_ingest_in_executor() -> None:
    """Sync ingest (with lock); must not block event loop, so run in executor."""
    run_ingest_with_lock()


async def _scheduled_ingest_job() -> None:
    """
    Scheduled job: run ingest in thread pool so the event loop is never blocked.
    File lock ensures only one worker across --workers N actually runs ingest.
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _run_ingest_in_executor)


def get_scheduler() -> AsyncIOScheduler:
    """Return the single AsyncIOScheduler instance (create if needed)."""
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler()
    return _scheduler


def start_scheduler() -> None:
    """
    Start the scheduler with nightly ingest at 22:00 Asia/Kolkata.
    Misfire: coalesce=True, max_instances=1, misfire_grace_time=3600.
    """
    sched = get_scheduler()
    if sched.running:
        return

    tz = "Asia/Kolkata"
    trigger = CronTrigger(hour=22, minute=0, timezone=tz)
    sched.add_job(
        _scheduled_ingest_job,
        trigger=trigger,
        id="nightly_ingest",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=3600,
    )
    sched.start()
    logger.info(f"scheduler started in PID={os.getpid()}")


def stop_scheduler() -> None:
    """Shut down the scheduler (call from lifespan shutdown)."""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        logger.info("scheduler stopped")
