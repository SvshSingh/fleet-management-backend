"""History store: batching, range boundaries, and read-after-write."""

from __future__ import annotations

import asyncio
import time

import pytest

from app.history import HistoryStore
from app.models import Telemetry


def _telemetry(robot_id: str, seq: int, ts: float) -> Telemetry:
    return Telemetry(
        robot_id=robot_id,
        robot_type="picker",
        session="s1",
        seq=seq,
        cycle=0,
        t=seq * 5,
        ts=ts,
        x=float(seq),
        y=1.0,
        status="active",
        battery=50.0,
    )


@pytest.fixture
async def store(tmp_path):
    s = HistoryStore(tmp_path / "h.db", flush_rows=100, flush_seconds=60)
    await s.start()
    yield s
    await s.stop()


@pytest.mark.asyncio
async def test_range_is_inclusive_at_both_ends(store):
    for seq, ts in enumerate([100.0, 200.0, 300.0, 400.0], start=1):
        store.record(_telemetry("r1", seq, ts))

    rows = await store.query("r1", start=200.0, end=300.0)
    assert [r["reported_ts"] for r in rows] == [200.0, 300.0]

    assert len(await store.query("r1")) == 4
    assert await store.query("r1", start=500.0) == []


@pytest.mark.asyncio
async def test_query_flushes_pending_writes_first(store):
    """Writes are batched off the ingest path, so a query issued immediately after an
    event would otherwise return nothing - confusing exactly when an operator is
    drilling into a fault they just saw."""
    store.record(_telemetry("r1", 1, 100.0))
    assert store.rows_written == 0, "record() should not touch disk"

    rows = await store.query("r1")
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_history_is_partitioned_by_robot_and_ordered(store):
    for seq, ts in enumerate([300.0, 100.0, 200.0], start=1):
        store.record(_telemetry("r1", seq, ts))
    store.record(_telemetry("r2", 1, 150.0))

    rows = await store.query("r1")
    assert [r["reported_ts"] for r in rows] == [100.0, 200.0, 300.0], "not ordered by time"
    assert all(r["robot_id"] == "r1" for r in rows)
    assert len(await store.query("r2")) == 1


@pytest.mark.asyncio
async def test_out_of_order_readings_are_kept_even_though_live_state_drops_them(store):
    """Deliberate asymmetry, and worth being able to defend.

    Live state drops a late reading because showing a stale position is actively
    misleading. History keeps it, because for after-the-fact analysis a reading that
    arrived late is still a real thing the robot reported, and dropping it would
    silently put holes in the record.
    """
    store.record(_telemetry("r1", 5, 500.0))
    store.record(_telemetry("r1", 3, 300.0))  # late arrival

    rows = await store.query("r1")
    assert [r["seq"] for r in rows] == [3, 5]


@pytest.mark.asyncio
async def test_late_reading_reaches_history_even_though_live_state_rejects_it(tmp_path, telemetry):
    """The asymmetry, exercised through the real ingest path rather than the store.

    `IngestGuard` drops an out-of-order reading so the operator never sees a robot jump
    backwards - but history is written before the guard runs, so the record keeps every
    well-formed reading a known robot sent. If history were written after the guard,
    the record would silently inherit live state's holes.
    """
    from app.config import Settings
    from app.main import build_context
    from tests.conftest import ROSTER, topic_for

    cfg = Settings(data_dir=tmp_path, db_path=tmp_path / "h.db", history_enabled=True)
    ctx = build_context(cfg, ROSTER, with_history=True)
    await ctx.history.start()
    try:
        # A genuinely late message was EMITTED earlier and merely arrived second, so
        # its reported_ts is older. Getting this wrong in the test was how I noticed
        # history was ordering on the wrong clock.
        now = time.time()
        await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(seq=5, x=500.0, ts=now))
        # The two handle_raw calls are microseconds apart, but `received_ts` comes from
        # time.time(), whose tick is ~15.6ms on Windows - back-to-back calls land on the
        # SAME tick and the arrival-order assertion below becomes unprovable. Sleeping
        # past one tick makes the late arrival actually late in wall-clock terms, which
        # is what the scenario claims; asserting >= instead would let the assertion pass
        # even if received_ts were wrongly filled from the reported clock.
        await asyncio.sleep(0.05)
        await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(seq=3, x=300.0, ts=now - 10))

        assert (await ctx.state.get("r1")).x == 500.0, "live state rewound"
        assert ctx.ingestor.metrics.out_of_order == 1

        rows = await ctx.history.query("r1")
        assert [r["seq"] for r in rows] == [3, 5], "history inherited live state's hole"
        assert rows[0]["received_ts"] > rows[1]["received_ts"], (
            "received_ts should show that the older reading arrived later"
        )
    finally:
        await ctx.history.stop()


@pytest.mark.asyncio
async def test_off_roster_publisher_cannot_write_to_history(tmp_path, telemetry):
    """An unknown robot is screened before the history write, so a misconfigured
    publisher cannot pollute the record either."""
    from app.config import Settings
    from app.main import build_context
    from tests.conftest import ROSTER, topic_for

    cfg = Settings(data_dir=tmp_path, db_path=tmp_path / "h2.db", history_enabled=True)
    ctx = build_context(cfg, ROSTER, with_history=True)
    await ctx.history.start()
    try:
        await ctx.ingestor.handle_raw(topic_for("r99"), telemetry(robot_id="r99"))
        assert ctx.ingestor.metrics.unknown_robot == 1
        assert await ctx.history.query("r99") == []
    finally:
        await ctx.history.stop()


@pytest.mark.asyncio
async def test_limit_is_honoured(store):
    for seq in range(1, 60):
        store.record(_telemetry("r1", seq, float(seq)))

    assert len(await store.query("r1", limit=10)) == 10
