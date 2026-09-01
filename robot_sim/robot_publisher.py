"""One simulated robot: a single OS process standing in for one machine on site.

Replays this robot's own recorded events from events.jsonl and publishes them over
MQTT, the way a Qualcomm telematics board on a cellular link would report home.

Everything in here is written for a bad network, because that is the network these
robots actually live on:

  * Last Will and Testament   - the broker announces our death if the socket breaks
                                without a clean DISCONNECT.
  * bounded outbound buffer   - telemetry produced while disconnected is flushed on
                                reconnect instead of vanishing; when the bound is hit
                                we drop the OLDEST and count it, because an unbounded
                                buffer is just a slower crash.
  * per-robot seq + session   - lets the consumer dedup MQTT's at-least-once delivery
                                and tell a genuine restart apart from a replay.
  * backoff with jitter       - 500 robots reconnecting in lockstep after a broker
                                bounce is a self-inflicted DDoS.

Run standalone:  python robot_publisher.py --robot-id r1
Normally spawned by supervisor.py, one process per robot in robots.json.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from collections import deque
from pathlib import Path

import paho.mqtt.client as mqtt

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
BROKER_HOST = os.getenv("MQTT_HOST", "broker")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "15"))

# 5x by default: the recorded 15-minute window replays in 3 minutes, so a reviewer
# sees the fleet move within seconds of `docker compose up`. Set to 1 for real time.
SPEED_FACTOR = float(os.getenv("SPEED_FACTOR", "5"))

# Probability, per published sample, of yanking the socket out to prove recovery.
# Off by default so an evaluation run is clean; turn it on to demo reconnect.
CHAOS_DISCONNECT_PROB = float(os.getenv("CHAOS_DISCONNECT_PROB", "0"))

BUFFER_MAX = int(os.getenv("PUBLISHER_BUFFER_MAX", "200"))

TELEMETRY_TOPIC = "fleet/robots/{robot_id}/telemetry"
LINK_TOPIC = "fleet/robots/{robot_id}/link"

log = logging.getLogger("robot")


def load_events(robot_id: str) -> list[dict]:
    """This robot's own events only, in time order.

    Each publisher reads the shared log but keeps just its own lines: the file is a
    convenience for the exercise, not a channel between robots. Sorting by `t` is
    belt-and-braces - the recording is already ordered, but a publisher that assumes
    file order is a publisher that breaks the day someone concatenates two logs.
    """
    events: list[dict] = []
    with (DATA_DIR / "events.jsonl").open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            if event.get("robot_id") == robot_id:
                events.append(event)
    events.sort(key=lambda e: e["t"])
    if not events:
        raise SystemExit(f"no events found for {robot_id}")
    return events


class RobotPublisher:
    def __init__(self, robot_id: str, robot_type: str) -> None:
        self.robot_id = robot_id
        self.robot_type = robot_type
        self.events = load_events(robot_id)

        # A fresh session id every process start. The consumer uses it to tell "this
        # robot rebooted and its seq restarted at 1" apart from "someone is replaying
        # old traffic at me" - without it, a restart looks exactly like a flood of
        # duplicates and gets silently dropped.
        self.session = uuid.uuid4().hex
        self.seq = 0
        self.cycle = 0

        self.telemetry_topic = TELEMETRY_TOPIC.format(robot_id=robot_id)
        self.link_topic = LINK_TOPIC.format(robot_id=robot_id)

        self.connected = False
        self.buffer: deque[str] = deque(maxlen=BUFFER_MAX)
        self.buffered_dropped = 0
        self.published = 0
        self.reconnects = 0
        self._running = True

        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"robot-{robot_id}-{self.session[:8]}",
            clean_session=True,
        )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

        # The broker publishes this for us if we die without saying goodbye. This is
        # how the backend learns about a yanked power cable or a dropped LTE session
        # without waiting for a timeout to expire.
        self.client.will_set(
            self.link_topic,
            json.dumps(
                {"robot_id": robot_id, "link": "lost", "reason": "lwt", "session": self.session}
            ),
            qos=1,
            retain=True,
        )
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)

    # ---------------------------------------------------------------- callbacks

    def _on_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        if reason_code != 0:
            log.warning("%s connect refused: %s", self.robot_id, reason_code)
            return
        self.connected = True
        log.info("%s connected to %s:%s", self.robot_id, BROKER_HOST, BROKER_PORT)

        # Retained, so a backend that starts later immediately knows we are up rather
        # than showing an empty board until our next tick.
        client.publish(
            self.link_topic,
            json.dumps(
                {
                    "robot_id": self.robot_id,
                    "robot_type": self.robot_type,
                    "link": "live",
                    "session": self.session,
                }
            ),
            qos=1,
            retain=True,
        )
        self._flush_buffer()

    def _on_disconnect(self, client, userdata, *args) -> None:
        # paho changed this signature between API versions; we only care that it fell
        # over, not why, so we swallow the varying tail.
        if self.connected:
            self.reconnects += 1
            log.warning("%s disconnected (reconnect #%d)", self.robot_id, self.reconnects)
        self.connected = False

    # ------------------------------------------------------------------ publish

    def _flush_buffer(self) -> None:
        """Drain telemetry that piled up while we were offline, oldest first."""
        while self.buffer and self.connected:
            payload = self.buffer.popleft()
            info = self.client.publish(self.telemetry_topic, payload, qos=1, retain=True)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                self.buffer.appendleft(payload)  # put it back, try again next connect
                break

    def _emit(self, event: dict) -> None:
        self.seq += 1
        payload = json.dumps(
            {
                "robot_id": self.robot_id,
                "robot_type": self.robot_type,
                "session": self.session,
                "seq": self.seq,
                "cycle": self.cycle,
                "t": event["t"],
                "ts": time.time(),
                "x": event["x"],
                "y": event["y"],
                "status": event["status"],
                "battery": event["battery"],
                "task_event": event.get("task_event"),
            }
        )

        if not self.connected:
            # deque(maxlen=) discards the oldest for us. Count it: silently losing
            # telemetry is exactly the failure mode this whole design is about.
            if len(self.buffer) == self.buffer.maxlen:
                self.buffered_dropped += 1
            self.buffer.append(payload)
            return

        # retain=True so the last known position of every robot survives a backend
        # restart - a new subscriber gets the fleet immediately, not after 5 seconds
        # of blank screen. QoS 1 because losing a position update matters more than
        # occasionally seeing one twice (the consumer dedups on seq).
        info = self.client.publish(self.telemetry_topic, payload, qos=1, retain=True)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            self.buffer.append(payload)
            return
        self.published += 1

    # --------------------------------------------------------------------- loop

    def stop(self, *_) -> None:
        self._running = False

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

        self._connect_with_backoff()
        self.client.loop_start()

        # Wall-clock pacing rather than sleeping the nominal interval: sleeping 5s per
        # sample lets publish latency accumulate until this robot drifts minutes behind
        # its peers. We sleep until the moment each sample is *due*.
        started = time.monotonic()
        base_t = self.events[0]["t"]

        try:
            while self._running:
                for event in self.events:
                    if not self._running:
                        break
                    due = started + (event["t"] - base_t) / SPEED_FACTOR
                    delay = due - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    self._emit(event)
                    self._maybe_chaos()

                # Loop the recording. `cycle` increments so a consumer can tell the
                # second pass from the first; the wall clock keeps advancing so
                # timestamps never go backwards even though `t` resets to 0.
                self.cycle += 1
                span = (self.events[-1]["t"] - base_t) / SPEED_FACTOR
                started += span + 5 / SPEED_FACTOR
                log.info(
                    "%s finished cycle %d (published=%d buffered_dropped=%d reconnects=%d)",
                    self.robot_id,
                    self.cycle,
                    self.published,
                    self.buffered_dropped,
                    self.reconnects,
                )
        finally:
            # Clean shutdown: say we are going, so the broker does NOT fire our will.
            # A robot that was told to stop is not the same event as a robot that fell
            # off the network, and the operator should not be paged for the first.
            if self.connected:
                self.client.publish(
                    self.link_topic,
                    json.dumps(
                        {
                            "robot_id": self.robot_id,
                            "link": "lost",
                            "reason": "shutdown",
                            "session": self.session,
                        }
                    ),
                    qos=1,
                    retain=True,
                )
                time.sleep(0.2)
            self.client.loop_stop()
            self.client.disconnect()

    def _maybe_chaos(self) -> None:
        if CHAOS_DISCONNECT_PROB and random.random() < CHAOS_DISCONNECT_PROB:
            log.warning("%s chaos: dropping socket", self.robot_id)
            # Kill the transport without a DISCONNECT packet so the broker treats it
            # as a real failure and fires our will, exactly like a dead LTE session.
            try:
                self.client._sock_close()
            except Exception:  # pragma: no cover - best effort fault injection
                self.client.disconnect()
            self.connected = False

    def _connect_with_backoff(self) -> None:
        """The broker may not be up yet (or may be restarting). Keep trying.

        Jitter matters: without it, every robot that lost the broker reconnects on the
        same schedule and hammers it in lockstep the moment it comes back.
        """
        delay = 1.0
        while self._running:
            try:
                self.client.connect(BROKER_HOST, BROKER_PORT, keepalive=KEEPALIVE)
                return
            except OSError as exc:
                log.warning("%s broker unreachable (%s); retry in %.1fs", self.robot_id, exc, delay)
                time.sleep(delay)
                delay = min(delay * 2, 30) * random.uniform(0.8, 1.2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--robot-id", required=True)
    parser.add_argument("--robot-type", default="unknown")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s [pid %(process)d] %(message)s",
        stream=sys.stdout,
    )
    RobotPublisher(args.robot_id, args.robot_type).run()


if __name__ == "__main__":
    main()
