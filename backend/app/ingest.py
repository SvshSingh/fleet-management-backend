"""MQTT consumer, and the guards that reconcile at-least-once delivery with a
state machine that wants each reading exactly once, in order.

MQTT QoS 1 promises delivery *at least* once. That is the right trade for telemetry -
losing a position update is worse than seeing one twice - but it means the broker is
allowed to hand us duplicates, and a publisher flushing its offline buffer can hand us
readings out of order relative to what we already applied. Neither is an error; both
would corrupt state if applied naively (a robot would visibly jump backwards).

So the publisher stamps a per-robot `seq`, and `IngestGuard.judge` is the one place
that decides what to do with each message. Every rejection is counted rather than
swallowed, and the counters are exposed at /metrics - silent loss is the thing this
whole design exists to avoid.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import aiomqtt
from pydantic import ValidationError

from .config import Settings
from .fleet_state import FleetState
from .history import HistoryStore
from .models import LinkAnnouncement, Telemetry

log = logging.getLogger("ingest")

TELEMETRY_FILTER = "fleet/robots/+/telemetry"
LINK_FILTER = "fleet/robots/+/link"


class Verdict(str, Enum):
    ACCEPT = "accept"
    DUPLICATE = "duplicate"          # seq we have already applied
    OUT_OF_ORDER = "out_of_order"    # older than what we hold; applying it rewinds state
    UNKNOWN_ROBOT = "unknown_robot"  # not on the roster
    MALFORMED = "malformed"


@dataclass
class IngestMetrics:
    accepted: int = 0
    duplicate: int = 0
    out_of_order: int = 0
    unknown_robot: int = 0
    malformed: int = 0
    gaps_detected: int = 0
    messages_lost_estimate: int = 0
    link_messages: int = 0
    broker_reconnects: int = 0
    sessions_seen: int = 0

    def as_dict(self) -> dict:
        return dict(self.__dict__)


@dataclass
class _Cursor:
    """What we have already applied from one robot."""

    session: str
    last_seq: int


@dataclass
class IngestGuard:
    """Per-robot sequencing. Deliberately separate from FleetState so it can be tested
    without an event loop, a broker, or any of the rest of the service."""

    state: FleetState
    metrics: IngestMetrics = field(default_factory=IngestMetrics)
    cursors: dict[str, _Cursor] = field(default_factory=dict)

    def judge(self, msg: Telemetry) -> Verdict:
        if not self.state.knows(msg.robot_id):
            # A robot that is not on the roster is not a robot we can reason about:
            # we do not know its type or where it should be. Refusing it keeps the
            # fleet view honest. Ingestor.handle_raw screens these out earlier (so
            # they cannot reach history either) and counts them there; the check is
            # repeated here so the guard is correct when called on its own.
            return Verdict.UNKNOWN_ROBOT

        cursor = self.cursors.get(msg.robot_id)

        if cursor is None or cursor.session != msg.session:
            # New process on the robot: its seq counter restarted at 1. Without the
            # session id this is indistinguishable from a flood of duplicates, and we
            # would ignore a robot that just came back from a reboot until its seq
            # climbed past the old high-water mark - potentially forever.
            if cursor is not None:
                log.info(
                    "%s new session %s (was %s); resetting sequence cursor",
                    msg.robot_id,
                    msg.session[:8],
                    cursor.session[:8],
                )
            self.metrics.sessions_seen += 1
            self.cursors[msg.robot_id] = _Cursor(session=msg.session, last_seq=msg.seq)
            self.metrics.accepted += 1
            return Verdict.ACCEPT

        if msg.seq == cursor.last_seq:
            self.metrics.duplicate += 1
            return Verdict.DUPLICATE

        if msg.seq < cursor.last_seq:
            # Late arrival. We already hold something newer, so applying this would
            # rewind the robot's position on every operator's screen. Drop it: for
            # live telemetry the freshest reading always wins, and a stale one has no
            # value once superseded. History still keeps it - handle_raw writes to the
            # store before calling us, so the record does not inherit this hole.
            self.metrics.out_of_order += 1
            return Verdict.OUT_OF_ORDER

        if msg.seq > cursor.last_seq + 1:
            # We can see precisely how many readings we never got. Accept the new one -
            # it is current and useful - but record the hole so the loss is measurable
            # rather than invisible.
            missed = msg.seq - cursor.last_seq - 1
            self.metrics.gaps_detected += 1
            self.metrics.messages_lost_estimate += missed
            log.warning("%s gap: missed %d message(s) before seq %d", msg.robot_id, missed, msg.seq)

        cursor.last_seq = msg.seq
        self.metrics.accepted += 1
        return Verdict.ACCEPT


class Ingestor:
    """Owns the broker connection and pumps accepted telemetry into FleetState."""

    def __init__(
        self,
        settings: Settings,
        state: FleetState,
        history: Optional[HistoryStore] = None,
    ) -> None:
        self.settings = settings
        self.state = state
        self.history = history
        self.guard = IngestGuard(state=state)
        self.connected = False
        self._task: Optional[asyncio.Task] = None

    @property
    def metrics(self) -> IngestMetrics:
        return self.guard.metrics

    async def handle_raw(self, topic: str, payload: bytes) -> None:
        """One message off the wire. Split out from the network loop so tests can
        drive the entire ingest path with no broker in sight."""
        try:
            body = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            self.metrics.malformed += 1
            log.warning("malformed payload on %s", topic)
            return

        if topic.endswith("/link"):
            await self._handle_link(body)
            return

        try:
            msg = Telemetry.model_validate(body)
        except ValidationError as exc:
            # A schemaless pipeline is a pipeline that corrupts state quietly. Reject
            # and count; a publisher shipping a bad field should be loud, not subtle.
            self.metrics.malformed += 1
            log.warning("invalid telemetry on %s: %s", topic, exc.error_count())
            return

        if not self.state.knows(msg.robot_id):
            # Checked here as well as in judge() so an off-roster publisher cannot
            # write into history either.
            self.metrics.unknown_robot += 1
            return

        # History is written BEFORE the guard, and deliberately so. Live state and the
        # historical record want different things from a late message: showing a stale
        # position as current is misleading, so `judge` drops it - but for
        # after-the-fact analysis it is still a real thing the robot reported, and
        # dropping it would put a hole in the record. Writing here means history keeps
        # every well-formed reading from a known robot, whatever live state does with
        # it. (Unknown robots are already rejected above; malformed ones never parse.)
        if self.history is not None:
            # Fire-and-forget into a buffer; disk I/O must never sit between a robot
            # and the operator's screen.
            self.history.record(msg)

        verdict = self.guard.judge(msg)
        if verdict is not Verdict.ACCEPT:
            return

        await self.state.apply_telemetry(msg)

    async def _handle_link(self, body: dict) -> None:
        try:
            msg = LinkAnnouncement.model_validate(body)
        except ValidationError:
            self.metrics.malformed += 1
            return
        self.metrics.link_messages += 1
        await self.state.apply_link(msg)

    # ---------------------------------------------------------------- broker loop

    async def run(self) -> None:
        """Subscribe forever, reconnecting on failure.

        The backend must survive the broker restarting under it - that is a routine
        event in any real deployment, not an outage worth crashing over. clean_session
        is False, so the broker holds our subscription and queues QoS-1 messages while
        we are away; combined with retained telemetry we come back to a complete fleet
        rather than an empty board.
        """
        delay = 1.0
        while True:
            try:
                async with aiomqtt.Client(
                    hostname=self.settings.mqtt_host,
                    port=self.settings.mqtt_port,
                    identifier=self.settings.mqtt_client_id,
                    clean_session=self.settings.mqtt_clean_session,
                    keepalive=self.settings.mqtt_keepalive,
                ) as client:
                    self.connected = True
                    delay = 1.0
                    log.info("connected to broker %s:%s", self.settings.mqtt_host, self.settings.mqtt_port)

                    # One wildcard subscription for the whole fleet. This is why going
                    # from 8 robots to 500 needs no change here: the topic filter does
                    # not mention any robot by name.
                    await client.subscribe(TELEMETRY_FILTER, qos=1)
                    await client.subscribe(LINK_FILTER, qos=1)

                    async for message in client.messages:
                        await self.handle_raw(str(message.topic), message.payload)

            except aiomqtt.MqttError as exc:
                self.connected = False
                self.metrics.broker_reconnects += 1
                sleep_for = min(delay, 30) * random.uniform(0.8, 1.2)
                log.warning("broker connection lost (%s); retrying in %.1fs", exc, sleep_for)
                await asyncio.sleep(sleep_for)
                delay = min(delay * 2, 30)
            except asyncio.CancelledError:
                self.connected = False
                raise

    def start(self) -> None:
        self._task = asyncio.create_task(self.run(), name="ingest")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
