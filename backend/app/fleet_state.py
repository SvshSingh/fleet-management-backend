"""The authoritative fleet state, and the consistency guarantee that comes with it.

This is the answer to "both need to reflect the same underlying state; a client using
one should not see something inconsistent with a client using the other."

The shape is deliberately boring: a dict of robot_id -> RobotState behind a single
asyncio.Lock, plus a monotonically increasing integer `version` bumped on every
accepted mutation. Boring is the point. The fleet is small and bounded (a site has
tens of robots, not millions), every consumer wants the whole current picture rather
than a slice of history, and lookups are by robot_id - so a dict is the right shape
and anything cleverer would be justifying itself rather than the data.

The version counter is what makes the two transports agree:

    * REST returns {version: N, robots: [...]}  - the state after N mutations.
    * WS sends a snapshot at version N, then updates N+1, N+2, ...

A WebSocket client that has applied every update through version N holds byte-for-byte
what `GET /fleet` would have returned at version N. The subtle part is not the counter,
it is `snapshot_and_register` below: the snapshot and the subscription must be taken
under the *same* lock acquisition, or an update can slip into the gap between them and
be lost forever (or delivered twice). That one line is the whole guarantee.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Optional, TypeVar

from .models import (
    FleetSummary,
    LinkAnnouncement,
    RobotState,
    Snapshot,
    Telemetry,
    Update,
    WORKING_STATUSES,
    derive_attention,
)

T = TypeVar("T")

# Link health is ordered, and the watchdog may only ever move a robot DOWN it.
# The watchdog infers trouble from silence; LWT is the broker telling us the socket
# actually broke. Both can be true at once, and when they disagree the authoritative
# signal has to win - otherwise a robot the broker has confirmed dead appears to
# recover from `lost` to `stale` on the operator's board while nothing improved.
# Only real telemetry clears this, and it does so in apply_telemetry, not here.
_LINK_SEVERITY = {"live": 0, "stale": 1, "lost": 2}

# Called synchronously, while the lock is held, for every accepted mutation. It must
# not block or await - see hub.publish, which only does put_nowait. Holding the lock
# across the fanout is what keeps update order identical to mutation order.
UpdateSink = Callable[[Update], None]


class FleetState:
    def __init__(
        self,
        roster: list[dict],
        low_battery_pct: float = 20.0,
        on_update: Optional[UpdateSink] = None,
    ) -> None:
        self._lock = asyncio.Lock()
        self._version = 0
        self._low_battery_pct = low_battery_pct
        self._on_update = on_update

        # Seeded from robots.json so the fleet is complete from the first request.
        # A robot we have never heard from is `link=lost`, not absent: an operator
        # needs to see that r7 exists and is silent, and an empty list cannot say that.
        self._robots: dict[str, RobotState] = {}
        for entry in roster:
            state = RobotState(
                robot_id=entry["robot_id"],
                robot_type=entry.get("robot_type", "unknown"),
                x=entry["start"]["x"],
                y=entry["start"]["y"],
                status="offline",
                battery=0.0,
                link="lost",
                link_reason="never_seen",
            )
            state.needs_attention, state.attention_reasons = derive_attention(
                state, self._low_battery_pct
            )
            self._robots[state.robot_id] = state

    # ------------------------------------------------------------------ internals

    def set_update_sink(self, sink: UpdateSink) -> None:
        self._on_update = sink

    def knows(self, robot_id: str) -> bool:
        """Roster membership. Read without the lock on purpose: the roster is fixed at
        construction, so this is immutable data and locking it would only add contention
        to the hot path in ingest."""
        return robot_id in self._robots

    def _bump(self, state: RobotState, cause: str) -> Update:
        """Caller MUST hold the lock."""
        self._version += 1
        state.needs_attention, state.attention_reasons = derive_attention(
            state, self._low_battery_pct
        )
        update = Update(
            version=self._version,
            server_time=time.time(),
            robot=state.model_copy(deep=True),  # snapshot the value; callers must not
            cause=cause,                        # observe later mutations through it
        )
        if self._on_update is not None:
            self._on_update(update)
        return update

    def _summarize(self) -> FleetSummary:
        robots = list(self._robots.values())
        total = len(robots)
        by_status: dict[str, int] = {}
        by_link: dict[str, int] = {}
        for robot in robots:
            by_status[robot.status] = by_status.get(robot.status, 0) + 1
            by_link[robot.link] = by_link.get(robot.link, 0) + 1

        working = sum(1 for r in robots if r.status in WORKING_STATUSES and r.link == "live")
        attention = sum(1 for r in robots if r.needs_attention)
        mean_battery = sum(r.battery for r in robots) / total if total else 0.0

        return FleetSummary(
            total=total,
            working=working,
            needs_attention=attention,
            by_status=by_status,
            by_link=by_link,
            working_fraction=(working / total) if total else 0.0,
            mean_battery=round(mean_battery, 2),
        )

    def _build_snapshot(self) -> Snapshot:
        """Caller MUST hold the lock."""
        return Snapshot(
            version=self._version,
            server_time=time.time(),
            robots=[r.model_copy(deep=True) for r in self._robots.values()],
            summary=self._summarize(),
        )

    # -------------------------------------------------------------------- writers

    async def apply_telemetry(self, msg: Telemetry) -> Update:
        """Fold one accepted reading into state. Ordering/dedup is ingest.py's job -
        by the time a message reaches here it has already been judged fresh."""
        async with self._lock:
            state = self._robots[msg.robot_id]
            state.robot_type = msg.robot_type or state.robot_type
            state.x = msg.x
            state.y = msg.y
            state.status = msg.status
            state.battery = msg.battery
            state.session = msg.session
            state.last_seq = msg.seq
            state.cycle = msg.cycle
            state.t = msg.t
            state.reported_ts = msg.ts
            state.last_seen_ts = time.time()

            # Hearing from a robot is itself proof the link is alive, whatever the
            # watchdog thought a moment ago. Recovery is implicit and needs no signal.
            state.link = "live"
            state.link_reason = "telemetry"

            if msg.task_event:
                state.last_task_event = msg.task_event
                state.task_events_seen += 1

            return self._bump(state, "telemetry")

    async def apply_link(self, msg: LinkAnnouncement) -> Optional[Update]:
        """Handle a robot's own connect announcement, or the broker's will on its behalf."""
        async with self._lock:
            state = self._robots.get(msg.robot_id)
            if state is None:
                return None

            # Guard against a stale retained will from a previous incarnation clobbering
            # a robot that has already reconnected with a new session. Without this, a
            # restart can be immediately marked dead by its own predecessor's ghost.
            if (
                msg.link == "lost"
                and msg.session is not None
                and state.session is not None
                and msg.session != state.session
            ):
                return None

            if state.link == msg.link and state.link_reason == msg.reason:
                return None  # nothing changed; do not burn a version on a no-op

            state.link = msg.link
            state.link_reason = msg.reason
            if msg.robot_type:
                state.robot_type = msg.robot_type
            if msg.link == "live":
                state.last_seen_ts = time.time()
            return self._bump(state, "link")

    async def sweep_liveness(
        self, stale_after: float, lost_after: float, now: Optional[float] = None
    ) -> list[Update]:
        """Age out robots we have stopped hearing from.

        This exists *alongside* the broker's Last Will, not instead of it, and the
        difference is the interesting part. LWT fires when a TCP connection breaks -
        fast, but it only catches robots that disconnect. A robot whose process has
        hung, whose sensor loop is wedged, or whose modem is holding a socket open
        while passing no data stays "connected" as far as the broker is concerned and
        would look perfectly healthy forever. Only a timer on the last message we
        actually received catches that one.
        """
        now = time.time() if now is None else now
        updates: list[Update] = []

        async with self._lock:
            for state in self._robots.values():
                if state.last_seen_ts == 0.0:
                    continue  # never heard from; already `lost`/never_seen at seed

                age = now - state.last_seen_ts
                if age > lost_after:
                    target, reason = "lost", "watchdog_timeout"
                elif age > stale_after:
                    target, reason = "stale", "watchdog_stale"
                else:
                    continue  # healthy; telemetry already set link=live

                # `>=` not `==`: equal means "already there, do not re-emit every 2
                # seconds", and greater means this robot is already known to be in
                # WORSE shape than silence alone implies - an LWT `lost` sitting in
                # the stale age band. Downgrading that to `stale` would be inventing
                # a recovery that never happened.
                if _LINK_SEVERITY[state.link] >= _LINK_SEVERITY[target]:
                    continue

                state.link = target
                state.link_reason = reason
                updates.append(self._bump(state, "watchdog"))

        return updates

    # -------------------------------------------------------------------- readers

    async def snapshot(self) -> Snapshot:
        async with self._lock:
            return self._build_snapshot()

    async def get(self, robot_id: str) -> Optional[RobotState]:
        async with self._lock:
            state = self._robots.get(robot_id)
            return state.model_copy(deep=True) if state else None

    async def snapshot_and_register(self, register: Callable[[int], T]) -> tuple[Snapshot, T]:
        """Take a snapshot and subscribe to updates atomically.

        This is the load-bearing method of the whole service. `register` is called with
        the snapshot's version while the lock is still held, so the subscriber is on the
        fanout list before any further mutation can occur. The subscriber is therefore
        guaranteed to receive exactly versions N+1, N+2, ... with no gap and no repeat.

        Do it in two steps instead - snapshot, release, subscribe - and any update
        landing in between is lost silently, which is the kind of bug that only shows
        up under load and is miserable to find.
        """
        async with self._lock:
            snap = self._build_snapshot()
            registered = register(snap.version)
            return snap, registered

    @property
    def version(self) -> int:
        """Lock-free peek, for logs and metrics only. Never make a correctness
        decision on this - use snapshot() so the version and the data match."""
        return self._version
