"""The sequencing guards: what happens when MQTT's at-least-once delivery hands us
duplicates, reordering, gaps, or a robot that rebooted.

This is one of the two trickiest parts of the service (the other is
test_consistency.py). Every case here is a thing the broker is *allowed* to do, not a
bug, and each one corrupts state if applied naively.
"""

from __future__ import annotations

import json

import pytest

from app.ingest import Verdict
from tests.conftest import topic_for


@pytest.mark.asyncio
async def test_accepts_first_message_and_applies_it(ctx, telemetry):
    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(seq=1, x=111.0))

    robot = await ctx.state.get("r1")
    assert robot.x == 111.0
    assert robot.last_seq == 1
    assert robot.link == "live"          # hearing from it IS the liveness signal
    assert ctx.ingestor.metrics.accepted == 1


@pytest.mark.asyncio
async def test_duplicate_is_dropped_not_reapplied(ctx, telemetry):
    """QoS 1 means the broker may redeliver. Applying the same seq twice must be a
    no-op, and must not burn a version - a spurious version bump would push a
    meaningless update to every connected client."""
    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(seq=1, x=100.0))
    version_after_first = ctx.state.version

    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(seq=1, x=999.0))

    robot = await ctx.state.get("r1")
    assert robot.x == 100.0, "duplicate overwrote state"
    assert ctx.state.version == version_after_first, "duplicate burned a version"
    assert ctx.ingestor.metrics.duplicate == 1


@pytest.mark.asyncio
async def test_out_of_order_message_does_not_rewind_position(ctx, telemetry):
    """A publisher flushing its offline buffer can deliver an old reading after a new
    one. Applying it would make the robot visibly jump backwards on the operator's
    screen - the freshest reading has to win."""
    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(seq=5, x=500.0))
    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(seq=3, x=300.0))

    robot = await ctx.state.get("r1")
    assert robot.x == 500.0
    assert robot.last_seq == 5
    assert ctx.ingestor.metrics.out_of_order == 1


@pytest.mark.asyncio
async def test_gap_is_accepted_but_counted(ctx, telemetry):
    """Loss must be visible. We take the new reading (it is current and useful) but
    record exactly how many we never received, so /metrics can show it."""
    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(seq=1))
    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(seq=6, x=606.0))

    robot = await ctx.state.get("r1")
    assert robot.x == 606.0
    assert ctx.ingestor.metrics.gaps_detected == 1
    assert ctx.ingestor.metrics.messages_lost_estimate == 4  # seqs 2,3,4,5


@pytest.mark.asyncio
async def test_new_session_resets_the_cursor(ctx, telemetry):
    """A robot that reboots restarts its seq at 1. Without the session id that is
    indistinguishable from a flood of duplicates, and we would ignore the robot until
    its counter climbed past the old high-water mark - potentially forever."""
    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(session="old", seq=50, x=50.0))
    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(session="new", seq=1, x=1.0))

    robot = await ctx.state.get("r1")
    assert robot.x == 1.0, "post-reboot telemetry was rejected as stale"
    assert robot.session == "new"
    assert robot.last_seq == 1
    assert ctx.ingestor.metrics.duplicate == 0
    assert ctx.ingestor.metrics.out_of_order == 0


@pytest.mark.asyncio
async def test_unknown_robot_is_rejected(ctx, telemetry):
    await ctx.ingestor.handle_raw(topic_for("r99"), telemetry(robot_id="r99"))

    assert ctx.ingestor.metrics.unknown_robot == 1
    assert ctx.ingestor.metrics.accepted == 0
    assert await ctx.state.get("r99") is None


@pytest.mark.asyncio
async def test_malformed_payloads_do_not_crash_ingest(ctx, telemetry):
    """A publisher shipping garbage must not be able to take the fleet view down."""
    await ctx.ingestor.handle_raw(topic_for("r1"), b"{not json")
    await ctx.ingestor.handle_raw(topic_for("r1"), json.dumps({"robot_id": "r1"}).encode())
    await ctx.ingestor.handle_raw(
        topic_for("r1"), telemetry(status="teleporting")  # not in the status enum
    )

    assert ctx.ingestor.metrics.malformed == 3
    assert ctx.ingestor.metrics.accepted == 0

    # ...and a good message right afterwards still works.
    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(seq=1))
    assert ctx.ingestor.metrics.accepted == 1


@pytest.mark.asyncio
async def test_verdicts_are_reported_for_each_case(ctx, telemetry):
    """The guard is tested directly too, so a refactor of handle_raw cannot quietly
    change what each verdict means."""
    from app.models import Telemetry

    def parse(payload: bytes) -> Telemetry:
        return Telemetry.model_validate(json.loads(payload))

    guard = ctx.ingestor.guard
    assert guard.judge(parse(telemetry(seq=1))) is Verdict.ACCEPT
    assert guard.judge(parse(telemetry(seq=1))) is Verdict.DUPLICATE
    assert guard.judge(parse(telemetry(seq=0))) is Verdict.OUT_OF_ORDER
    assert guard.judge(parse(telemetry(seq=9))) is Verdict.ACCEPT  # gap, still accepted
    assert guard.judge(parse(telemetry(robot_id="r99"))) is Verdict.UNKNOWN_ROBOT
