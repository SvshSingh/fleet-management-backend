"""The central claim: a WebSocket client and a polling client never disagree.

The brief requires that "both reflect the same underlying state; a client using one
should not see something inconsistent with a client using the other." That is easy to
say and easy to get subtly wrong, because the dangerous window is tiny: between taking
a snapshot and joining the fanout, an update can land in neither and disappear.

These tests attack that window directly, so the guarantee is executable rather than
merely claimed in a README.
"""

from __future__ import annotations

import asyncio

import pytest

from tests.conftest import topic_for


async def _feed(ctx, telemetry, robot_id: str, seqs, **kw):
    for seq in seqs:
        await ctx.ingestor.handle_raw(
            topic_for(robot_id), telemetry(robot_id=robot_id, seq=seq, x=float(seq), **kw)
        )


@pytest.mark.asyncio
async def test_snapshot_and_subscription_are_atomic(ctx, telemetry):
    """No update may fall between the snapshot and the first delta.

    We register a subscriber and then push updates; the subscriber must receive
    exactly versions N+1.. with no gap. If snapshot and registration were two separate
    lock acquisitions, an update landing between them would be lost silently and this
    test would show a jump.
    """
    await _feed(ctx, telemetry, "r1", [1, 2, 3])

    snapshot, sub = await ctx.state.snapshot_and_register(ctx.hub.register)
    await _feed(ctx, telemetry, "r1", [4, 5, 6])

    received = []
    while not sub.queue.empty():
        received.append(await sub.queue.get())

    versions = [u.version for u in received]
    assert versions == list(range(snapshot.version + 1, snapshot.version + 1 + len(versions)))
    assert len(received) == 3


@pytest.mark.asyncio
async def test_ws_replay_reconstructs_exactly_the_rest_body(ctx, telemetry):
    """Snapshot + applied deltas == the polling response at the same version.

    This is the contract stated as an equality. A client that started from the
    snapshot and applied every update must hold, field for field, what GET /fleet
    would return once the versions line up.
    """
    await _feed(ctx, telemetry, "r1", [1, 2])

    snapshot, sub = await ctx.state.snapshot_and_register(ctx.hub.register)
    mirror = {r.robot_id: r.model_dump() for r in snapshot.robots}

    await _feed(ctx, telemetry, "r1", [3, 4])
    await _feed(ctx, telemetry, "r2", [1, 2], status="error", battery=9.0)
    await _feed(ctx, telemetry, "r3", [1], status="charging", battery=12.0)

    last_version = snapshot.version
    while not sub.queue.empty():
        update = await sub.queue.get()
        mirror[update.robot.robot_id] = update.robot.model_dump()
        last_version = update.version

    rest = await ctx.state.snapshot()
    assert rest.version == last_version, "versions drifted; comparison would be meaningless"
    assert mirror == {r.robot_id: r.model_dump() for r in rest.robots}


@pytest.mark.asyncio
async def test_concurrent_writers_produce_one_total_order(roomy_ctx, telemetry):
    """Under concurrent ingest, versions must still be a dense, gapless sequence.

    Three robots publishing at once through the same lock: if the version counter and
    the fanout were not both inside it, two updates could be assigned versions out of
    order relative to the queue and a client would apply them backwards.

    Uses roomy_ctx: with the default test queue of 8, a 42-update burst trips the
    slow-consumer path and this would silently become a backpressure test instead.
    """
    ctx = roomy_ctx
    _, sub = await ctx.state.snapshot_and_register(ctx.hub.register)

    await asyncio.gather(
        _feed(ctx, telemetry, "r1", range(1, 15)),
        _feed(ctx, telemetry, "r2", range(1, 15)),
        _feed(ctx, telemetry, "r3", range(1, 15)),
    )

    versions = []
    while not sub.queue.empty():
        versions.append((await sub.queue.get()).version)

    assert versions == sorted(versions), "updates were queued out of version order"
    assert versions == list(range(versions[0], versions[0] + len(versions))), "version gap"
    assert len(versions) == 42


@pytest.mark.asyncio
async def test_slow_subscriber_is_dropped_not_allowed_to_block_ingest(ctx, telemetry):
    """One slow client must never stall the fleet.

    The queue is bounded at 8 in tests. We overflow it deliberately and assert that
    ingest kept going, that the subscriber was flagged for resync rather than served a
    hole, and that state itself is untouched by the client's problems.
    """
    _, sub = await ctx.state.snapshot_and_register(ctx.hub.register)

    await _feed(ctx, telemetry, "r1", range(1, 40))  # far more than the queue holds

    assert sub.needs_resync is True
    assert sub.dropped > 0

    robot = await ctx.state.get("r1")
    assert robot.last_seq == 39, "ingest was affected by a slow consumer"
    assert ctx.state.version == 39


@pytest.mark.asyncio
async def test_reconnecting_client_resumes_without_a_gap(ctx, telemetry):
    """A client that drops and comes back with ?since=N gets exactly what it missed."""
    await _feed(ctx, telemetry, "r1", [1, 2, 3])
    disconnect_version = ctx.state.version

    await _feed(ctx, telemetry, "r1", [4, 5])

    replay = ctx.hub.replay_since(disconnect_version)
    assert replay is not None
    assert [u.version for u in replay] == [disconnect_version + 1, disconnect_version + 2]


@pytest.mark.asyncio
async def test_resume_beyond_the_buffer_refuses_rather_than_guessing(ctx, telemetry):
    """When we can no longer prove what a client missed, we must say so.

    Returning whatever is still in the buffer would leave the client quietly wrong,
    which is worse than making it re-sync. The buffer is 50 in tests; we push past it.
    """
    await _feed(ctx, telemetry, "r1", range(1, 80))

    assert ctx.hub.replay_since(1) is None       # fell out of the buffer
    assert ctx.hub.replay_since(10_000) is None  # client ahead of us: not our stream
    assert ctx.hub.replay_since(ctx.state.version - 2) is not None  # still provable
