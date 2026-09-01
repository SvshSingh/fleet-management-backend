"""Optional history: every well-formed reading, kept for range queries.

SQLite because this is a single-node service writing ~1.6 rows/second for eight
robots - it needs no operator, no container, and no network hop, and the whole store
is one file you can copy out and open with `sqlite3`. At 500 robots reporting at 1Hz
it stops being the right answer and becomes Postgres or TimescaleDB; that boundary is
in SYSTEM_DESIGN.md rather than hidden here.

Writes are batched off the ingest path. Disk I/O must never sit between a robot and
the operator's screen: `record()` only appends to an in-memory buffer, and a
background task flushes on a row count or a timer, whichever comes first.

Two timestamps are stored, mirroring the two clocks on RobotState:

    reported_ts  when the robot says it happened   <- history is ordered and queried
                                                       on this, because an analyst
                                                       wants the robot's timeline
    received_ts  when we actually got it           <- their difference is arrival lag,
                                                       which is how you see a link
                                                       degrading before it fails

Ordering on reported_ts is what lets a late arrival slot into its correct place in the
record rather than appearing at the end - which is the whole reason history accepts
readings that live state rejects.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

import aiosqlite

from .models import Telemetry

log = logging.getLogger("history")

SCHEMA = """
CREATE TABLE IF NOT EXISTS telemetry (
    robot_id    TEXT    NOT NULL,
    reported_ts REAL    NOT NULL,   -- robot's wall clock at emit; the query axis
    received_ts REAL    NOT NULL,   -- server wall clock at ingest
    reported_t  INTEGER NOT NULL,   -- robot's offset within the recorded window
    cycle       INTEGER NOT NULL,   -- which replay pass; reported_t restarts each one
    seq         INTEGER NOT NULL,
    x           REAL    NOT NULL,
    y           REAL    NOT NULL,
    status      TEXT    NOT NULL,
    battery     REAL    NOT NULL,
    task_event  TEXT
);
-- Every query is "one robot over a time range", so this index is the query plan.
CREATE INDEX IF NOT EXISTS idx_telemetry_robot_ts ON telemetry (robot_id, reported_ts);
"""


class HistoryStore:
    def __init__(self, db_path: Path, flush_rows: int = 200, flush_seconds: float = 1.0) -> None:
        self.db_path = db_path
        self.flush_rows = flush_rows
        self.flush_seconds = flush_seconds
        self._buffer: list[tuple] = []
        self._db: Optional[aiosqlite.Connection] = None
        self._task: Optional[asyncio.Task] = None
        self.rows_written = 0

    async def start(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path)
        # WAL so the flush writer and read queries do not block each other - without
        # it a history request can be stalled behind a batch insert.
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.executescript(SCHEMA)
        await self._db.commit()
        self._task = asyncio.create_task(self._flush_loop(), name="history-flush")
        log.info("history store ready at %s", self.db_path)

    def record(self, msg: Telemetry) -> None:
        """Synchronous and non-blocking by design - called straight from ingest."""
        self._buffer.append(
            (
                msg.robot_id,
                msg.ts,
                time.time(),
                msg.t,
                msg.cycle,
                msg.seq,
                msg.x,
                msg.y,
                msg.status,
                msg.battery,
                msg.task_event,
            )
        )

    async def flush(self) -> int:
        if not self._buffer or self._db is None:
            return 0
        batch, self._buffer = self._buffer, []
        await self._db.executemany(
            "INSERT INTO telemetry"
            " (robot_id, reported_ts, received_ts, reported_t, cycle, seq,"
            "  x, y, status, battery, task_event)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        await self._db.commit()
        self.rows_written += len(batch)
        return len(batch)

    async def _flush_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.flush_seconds)
                if len(self._buffer) >= self.flush_rows or self._buffer:
                    await self.flush()
            except asyncio.CancelledError:
                await self.flush()  # do not lose the tail on shutdown
                raise
            except Exception:  # pragma: no cover
                log.exception("history flush failed; buffer retained")

    async def query(
        self,
        robot_id: str,
        start: Optional[float] = None,
        end: Optional[float] = None,
        limit: int = 1000,
    ) -> list[dict]:
        """Readings for one robot in a `reported_ts` range, inclusive at both ends."""
        if self._db is None:
            return []
        # Flush first so a query issued right after an event actually sees it -
        # otherwise "I just saw r3 error, show me its history" returns nothing for a
        # confusing second.
        await self.flush()

        sql = "SELECT * FROM telemetry WHERE robot_id = ?"
        params: list = [robot_id]
        if start is not None:
            sql += " AND reported_ts >= ?"
            params.append(start)
        if end is not None:
            sql += " AND reported_ts <= ?"
            params.append(end)
        sql += " ORDER BY reported_ts ASC LIMIT ?"
        params.append(limit)

        self._db.row_factory = aiosqlite.Row
        async with self._db.execute(sql, params) as cursor:
            return [dict(row) for row in await cursor.fetchall()]

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._db is not None:
            await self.flush()
            await self._db.close()
