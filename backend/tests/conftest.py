"""Shared fixtures.

Every test here runs without Docker, without a broker, and without a network: the
ingest path is driven by handing raw payloads straight to `Ingestor.handle_raw`, the
same function the MQTT loop calls. That is deliberate - a test suite you can only run
with `docker compose up` is a test suite nobody runs.
"""

from __future__ import annotations

import json
import time

import pytest

from app.config import Settings
from app.main import build_context

ROSTER = [
    {"robot_id": "r1", "robot_type": "picker", "start": {"x": 569.9, "y": 33.0}},
    {"robot_id": "r2", "robot_type": "hauler", "start": {"x": 787.3, "y": 65.2}},
    {"robot_id": "r3", "robot_type": "picker", "start": {"x": 382.9, "y": 35.5}},
]


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        data_dir=tmp_path,
        db_path=tmp_path / "fleet.db",
        history_enabled=False,
        stale_after_seconds=12.0,
        lost_after_seconds=30.0,
        low_battery_pct=20.0,
        hub_replay_buffer=50,
        hub_subscriber_queue=8,
    )


@pytest.fixture
def ctx(settings):
    """Deliberately tiny hub limits (queue 8, buffer 50) so the slow-consumer and
    buffer-overflow paths are reachable without pushing thousands of messages."""
    return build_context(settings, ROSTER, with_history=False)


@pytest.fixture
def roomy_ctx(settings):
    """Same wiring with a realistic queue, for tests about ordering rather than
    overflow - with queue=8 a 42-update burst trips the slow-consumer path and you
    end up testing backpressure by accident."""
    return build_context(
        settings.model_copy(update={"hub_subscriber_queue": 256}), ROSTER, with_history=False
    )


@pytest.fixture
def telemetry():
    """Factory for wire-shaped telemetry payloads."""

    def _make(robot_id="r1", session="s1", seq=1, **overrides) -> bytes:
        body = {
            "robot_id": robot_id,
            "robot_type": "picker",
            "session": session,
            "seq": seq,
            "cycle": 0,
            "t": seq * 5,
            "ts": time.time(),
            "x": 100.0 + seq,
            "y": 200.0,
            "status": "active",
            "battery": 80.0,
            "task_event": None,
        }
        body.update(overrides)
        return json.dumps(body).encode()

    return _make


def topic_for(robot_id: str) -> str:
    return f"fleet/robots/{robot_id}/telemetry"
