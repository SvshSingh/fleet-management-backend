"""WebSocket surface - the push half of the contract.

Frame protocol (server -> client):

    {"type":"snapshot", "version":N, "robots":[...], "summary":{...}}
    {"type":"update",   "version":N+1, "robot":{...}, "cause":"telemetry|link|watchdog"}
    {"type":"ping",     "version":N}          heartbeat, every 10s
    {"type":"resync",   "reason":"..."}       always followed by a snapshot

Connect with `?since=<version>` to resume after a dropped connection. If the hub can
still prove what the client missed it gets exactly those updates; otherwise it gets a
`resync` notice and a fresh snapshot. It is never left guessing which happened.

Everything that decides "what does this client hold" happens inside one
`snapshot_and_register` call, i.e. under FleetState's lock. Reading the replay buffer
outside that lock and subscribing after would leave a window where an update belongs
to neither the replay nor the queue, and it would vanish - the exact bug this file is
arranged to prevent.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from .hub import Subscriber, drain

log = logging.getLogger("ws")

router = APIRouter()


@router.websocket("/ws")
async def fleet_stream(websocket: WebSocket, since: int | None = Query(None)) -> None:
    await websocket.accept()
    ctx = websocket.app.state.ctx
    hub = ctx.hub
    sub: Subscriber | None = None

    try:
        resumed: dict = {}

        def _join(version: int) -> Subscriber:
            """Runs while the state lock is held: read the replay buffer and join the
            fanout in one indivisible step, so no update can fall between them."""
            if since is not None:
                resumed["replay"] = hub.replay_since(since)
            return hub.register(version)

        snapshot, sub = await ctx.state.snapshot_and_register(_join)
        replay = resumed.get("replay")

        if since is not None and replay is not None:
            # We can account for every version the client missed, so skip the snapshot
            # and hand it just the delta - the point of resuming at all.
            for update in replay:
                await websocket.send_json({"type": "update", **update.model_dump()})
            sub.last_sent = replay[-1].version if replay else since
        else:
            if since is not None:
                await websocket.send_json(
                    {"type": "resync", "reason": "requested version outside replay buffer"}
                )
            await websocket.send_json({"type": "snapshot", **snapshot.model_dump()})
            sub.last_sent = snapshot.version

        await _pump(websocket, ctx, sub)

    except WebSocketDisconnect:
        pass
    except Exception:  # pragma: no cover - one bad socket must not take the server down
        log.exception("websocket handler failed")
    finally:
        if sub is not None:
            hub.unregister(sub)


async def _pump(websocket: WebSocket, ctx, sub: Subscriber) -> None:
    """Forward updates until the client goes away.

    The heartbeat exists because a TCP connection can be dead for minutes without
    either end noticing. A client that stops seeing pings knows to reconnect, rather
    than sitting in front of a frozen dashboard believing the fleet has gone quiet.
    """
    heartbeat = ctx.settings.hub_heartbeat_seconds

    while True:
        if sub.needs_resync:
            await _resync(websocket, ctx, sub)
            continue

        try:
            update = await asyncio.wait_for(sub.queue.get(), timeout=heartbeat)
        except asyncio.TimeoutError:
            await websocket.send_json({"type": "ping", "version": ctx.state.version})
            continue

        # The hub wakes a flagged-slow subscriber by queueing one more item; that item
        # is superseded by the snapshot we are about to send, so drop it and re-sync.
        if sub.needs_resync:
            await _resync(websocket, ctx, sub)
            continue

        await websocket.send_json({"type": "update", **update.model_dump()})


async def _resync(websocket: WebSocket, ctx, sub: Subscriber) -> None:
    """Recover a client whose backlog we dropped to protect the ingest path.

    Re-anchoring has to happen under the state lock: clearing the queue and taking the
    snapshot atomically is what stops a stale queued update from being applied *after*
    the newer snapshot and rewinding the client's view.
    """

    def _reanchor(version: int) -> Subscriber:
        drain(sub.queue)
        sub.needs_resync = False
        sub.last_sent = version
        return sub

    snapshot, _ = await ctx.state.snapshot_and_register(_reanchor)
    await websocket.send_json({"type": "resync", "reason": "client too slow; state re-sent"})
    await websocket.send_json({"type": "snapshot", **snapshot.model_dump()})
