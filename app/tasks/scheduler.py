from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

from ..utils.logging import get_logger

logger = get_logger("scheduler")


class PeriodicTask:
    def __init__(self, interval: timedelta, coro_factory: Callable[[], Awaitable[Any]]):
        self.interval = interval
        self.coro_factory = coro_factory
        self._task: asyncio.Task | None = None
        self._is_running = False

    async def _runner(self) -> None:
        self._is_running = True
        try:
            while self._is_running:
                try:
                    await self.coro_factory()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Periodic task failed; retrying after %s", self.interval)
                await asyncio.sleep(self.interval.total_seconds())
        except asyncio.CancelledError:
            pass
        finally:
            self._is_running = False

    def start(self) -> None:
        if not self._task or self._task.done():
            self._task = asyncio.create_task(self._runner())

    def stop(self) -> None:
        self._is_running = False
        if self._task and not self._task.done():
            self._task.cancel()
