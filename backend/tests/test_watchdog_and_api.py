"""Liveness ageing, the attention policy, and the HTTP surface."""

from __future__ import annotations

import time

import httpx
import pytest
from httpx import ASGITransport

from app.models import LinkAnnouncement, RobotState, derive_attention
from tests.conftest import topic_for


@pytest.mark.asyncio
async def test_link_ages_live_to_stale_to_lost(ctx, telemetry):
    """The watchdog is the only thing that catches a robot which is still *connected*
    but has stopped sending - a wedged process, or a modem holding a socket open while
    passing no data. The broker's Last Will cannot see that failure at all."""
    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(seq=1))
    assert (await ctx.state.get("r1")).link == "live"

    now = time.time()

    updates = await ctx.state.sweep_liveness(12, 30, now=now + 15)
    assert [u.robot.link for u in updates] == ["stale"]
    assert (await ctx.state.get("r1")).link_reason == "watchdog_stale"

    updates = await ctx.state.sweep_liveness(12, 30, now=now + 45)
    assert [u.robot.link for u in updates] == ["lost"]


@pytest.mark.asyncio
async def test_watchdog_does_not_re_emit_a_state_it_already_holds(ctx, telemetry):
    """Sweeps run every 2 seconds. Emitting an update each time would flood every
    connected client with identical frames for a robot that has simply stayed down."""
    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(seq=1))
    now = time.time()

    assert len(await ctx.state.sweep_liveness(12, 30, now=now + 45)) == 1
    version_after = ctx.state.version
    assert await ctx.state.sweep_liveness(12, 30, now=now + 47) == []
    assert ctx.state.version == version_after


@pytest.mark.asyncio
async def test_watchdog_never_downgrades_an_lwt_lost_robot_to_stale(ctx, telemetry):
    """An LWT `lost` must survive the stale age band.

    Found by killing three publishers with SIGKILL against a live broker: the broker
    published r3's will, the board showed `lost/lwt`, and six seconds later the sweep
    put it back to `stale` - the robot appeared to RECOVER while it was in fact dead.
    The two signals answer different questions (the broker saw the socket break; the
    watchdog only knows nobody has spoken) and when they disagree the observation beats
    the inference. Only telemetry may clear it, which the test below covers.
    """
    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(seq=1, session="s1"))
    now = time.time()

    await ctx.state.apply_link(
        LinkAnnouncement(robot_id="r1", link="lost", reason="lwt", session="s1")
    )
    assert (await ctx.state.get("r1")).link == "lost"

    # now + 15 lands between stale_after=12 and lost_after=30 - the exact window the
    # bug lived in. A sweep here has nothing to say about a robot already known lost.
    assert await ctx.state.sweep_liveness(12, 30, now=now + 15) == []
    r1 = await ctx.state.get("r1")
    assert r1.link == "lost", "watchdog invented a recovery"
    assert r1.link_reason == "lwt", "watchdog overwrote the more specific cause"


@pytest.mark.asyncio
async def test_telemetry_revives_a_lost_robot(ctx, telemetry):
    """Recovery needs no special signal: hearing from a robot is proof it is back."""
    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(seq=1))
    await ctx.state.sweep_liveness(12, 30, now=time.time() + 45)
    assert (await ctx.state.get("r1")).link == "lost"

    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(seq=2))
    assert (await ctx.state.get("r1")).link == "live"


@pytest.mark.asyncio
async def test_stale_will_from_a_previous_incarnation_is_ignored(ctx, telemetry):
    """A retained Last Will can arrive after the robot has already reconnected under a
    new session. Applying it would let a dead process kill its own replacement."""
    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(session="new", seq=1))

    await ctx.state.apply_link(
        LinkAnnouncement(robot_id="r1", link="lost", reason="lwt", session="old")
    )
    assert (await ctx.state.get("r1")).link == "live", "ghost of an old session killed r1"

    await ctx.state.apply_link(
        LinkAnnouncement(robot_id="r1", link="lost", reason="lwt", session="new")
    )
    assert (await ctx.state.get("r1")).link == "lost"


def test_charging_robot_with_low_battery_needs_no_attention():
    """A robot on its dock at 4% is behaving correctly. Paging an operator for it is
    how alerts get ignored. Verified against the recording: battery rises in exactly
    73 samples and every one of them is `charging`."""
    charging = RobotState(robot_id="r1", x=0, y=0, status="charging", battery=4.0, link="live")
    needs, reasons = derive_attention(charging, low_battery_pct=20.0)
    assert needs is False and reasons == []

    working = RobotState(robot_id="r2", x=0, y=0, status="on_mission", battery=4.0, link="live")
    needs, reasons = derive_attention(working, low_battery_pct=20.0)
    assert needs is True and reasons == ["battery:4%"]


def test_reported_offline_and_lost_link_are_different_problems():
    """The distinction the whole state model is built on. Verified against the
    recording: `offline` robots never move but keep publishing, so `offline` is a
    self-report, not a transport failure."""
    parked = RobotState(robot_id="r1", x=0, y=0, status="offline", battery=90.0, link="live")
    silent = RobotState(robot_id="r2", x=0, y=0, status="on_mission", battery=90.0, link="lost")

    assert derive_attention(parked, 20.0)[1] == ["status:offline"]
    assert derive_attention(silent, 20.0)[1] == ["link:lost"]


@pytest.fixture
async def client(ctx):
    from fastapi import FastAPI

    from app.api import router as rest_router

    # A bare app with no lifespan: no broker, no background tasks, just the routes
    # reading the same FleetState the ingest tests drive.
    app = FastAPI()
    app.state.ctx = ctx
    app.include_router(rest_router)
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


@pytest.mark.asyncio
async def test_rest_surface(ctx, client, telemetry):
    await ctx.ingestor.handle_raw(topic_for("r1"), telemetry(seq=1, status="on_mission"))
    await ctx.ingestor.handle_raw(
        topic_for("r2"), telemetry(robot_id="r2", seq=1, status="error", battery=5.0)
    )

    fleet = (await client.get("/fleet")).json()
    assert fleet["version"] == ctx.state.version
    assert len(fleet["robots"]) == 3
    assert fleet["summary"]["working"] == 1
    assert fleet["summary"]["by_status"]["error"] == 1

    attention = (await client.get("/robots", params={"needs_attention": True})).json()
    ids = {r["robot_id"] for r in attention}
    assert "r2" in ids       # errored
    assert "r3" in ids       # never heard from
    assert "r1" not in ids   # on mission, healthy battery, live link

    assert (await client.get("/robots/r1")).json()["status"] == "on_mission"
    assert (await client.get("/robots/nope")).status_code == 404

    metrics = (await client.get("/metrics")).json()
    assert metrics["ingest"]["accepted"] == 2
