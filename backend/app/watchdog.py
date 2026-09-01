"""Liveness timer: notices robots that have gone quiet.

Deliberately separate from the broker's Last Will, because the two catch different
failures and a system with only one of them has a blind spot:

    LWT      fires the instant a TCP connection breaks. Fast, free, and useless
             against a robot that is still connected.
    watchdog fires when no message has *arrived* for a while. Slower, but it is the
             only thing that catches a wedged process, a hung sensor loop, or a
             modem holding a socket open while passing no data - the failure that
             looks perfectly healthy from the broker's point of view.

Thresholds are two and six missed ticks (robots report every 5s). One missed tick is
not a fault on a cellular link; treating it as one would page an operator several
times an hour and train them to ignore the alerts.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from .config import Settings
from .fleet_state import FleetState

log = logging.getLogger("watchdog")


class Watchdog:
    def __init__(self, settings: Settings, state: FleetState) -> None:
        self.settings = settings
        self.state = state
        self._task: Optional[asyncio.Task] = None

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.settings.watchdog_interval_seconds)
            try:
                updates = await self.state.sweep_liveness(
                    stale_after=self.settings.stale_after_seconds,
                    lost_after=self.settings.lost_after_seconds,
                )
                for update in updates:
                    log.info(
                        "%s -> link=%s (%s)",
                        update.robot.robot_id,
                        update.robot.link,
                        update.robot.link_reason,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - a sweep bug must not kill liveness
                log.exception("watchdog sweep failed; continuing")

    def start(self) -> None:
        self._task = asyncio.create_task(self.run(), name="watchdog")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
