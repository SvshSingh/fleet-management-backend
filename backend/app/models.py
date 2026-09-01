"""Wire contract and the fleet's state shape.

The important decision in this file is that `status` and `link` are two separate
fields. That is not tidiness, it comes from the data: across all 1448 recorded
samples, robots reporting status "offline" never move but keep publishing. So
"offline" is something a robot says about *itself* - powered down, out of service -
and it is not the same event as the backend being unable to hear it. A robot can be
`status=offline, link=live` (parked, reachable, fine) or `status=on_mission,
link=lost` (mid-task and silent - the one an operator must act on immediately).
Collapsing those into one enum throws away the distinction that matters most.
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field

RobotStatus = Literal[
    "idle", "active", "on_mission", "charging", "blocked", "error", "maintenance", "offline"
]

LinkState = Literal["live", "stale", "lost"]

# Verified against the recording: all 307 samples with movement are `active` or
# `on_mission`, and no other status ever moves. The brief declines to define which
# statuses count as working; the data does it for us.
WORKING_STATUSES: frozenset[str] = frozenset({"active", "on_mission"})

# `charging` is excluded from attention on purpose: a robot on its Tru-Dock at 4%
# battery is behaving correctly and needs nothing from an operator. Verified too -
# battery rises in exactly 73 samples and every one of them is `charging`.
ATTENTION_STATUSES: frozenset[str] = frozenset({"error", "blocked", "maintenance", "offline"})


class Telemetry(BaseModel):
    """One reading as it arrives on the wire from a robot."""

    robot_id: str
    robot_type: str = "unknown"
    session: str
    seq: int
    cycle: int = 0
    t: int
    ts: float
    x: float
    y: float
    status: RobotStatus
    battery: float
    task_event: Optional[str] = None


class LinkAnnouncement(BaseModel):
    """Published by a robot on connect, or by the broker as our will on its behalf."""

    robot_id: str
    link: LinkState
    reason: str = "announce"
    session: Optional[str] = None
    robot_type: Optional[str] = None


class RobotState(BaseModel):
    """The backend's current belief about one robot."""

    robot_id: str
    robot_type: str = "unknown"

    x: float
    y: float
    status: RobotStatus = "offline"
    battery: float = 0.0

    link: LinkState = "lost"
    link_reason: str = "never_seen"

    session: Optional[str] = None
    last_seq: int = 0
    cycle: int = 0
    t: int = 0

    # Two clocks on purpose. `reported_ts` is the robot's own idea of when this
    # happened; `last_seen_ts` is when we actually received it. On a slow link they
    # diverge, and that difference IS the observability - see `staleness_seconds`.
    reported_ts: float = 0.0
    last_seen_ts: float = 0.0

    last_task_event: Optional[str] = None
    task_events_seen: int = 0

    needs_attention: bool = True
    attention_reasons: list[str] = Field(default_factory=list)

    @property
    def is_working(self) -> bool:
        return self.status in WORKING_STATUSES and self.link == "live"


def derive_attention(state: RobotState, low_battery_pct: float) -> tuple[bool, list[str]]:
    """Single place that decides whether an operator needs to look at a robot.

    One function so that the definition lives in exactly one spot: the walkthrough
    question "what counts as needing attention?" has one file and one line number as
    its answer, and changing the policy is a one-function change.
    """
    reasons: list[str] = []

    if state.status in ATTENTION_STATUSES:
        reasons.append(f"status:{state.status}")

    if state.link == "lost":
        reasons.append("link:lost")
    elif state.link == "stale":
        reasons.append("link:stale")

    # A charging robot is supposed to have a low battery; that is what charging is.
    if state.battery < low_battery_pct and state.status != "charging":
        reasons.append(f"battery:{state.battery:.0f}%")

    return bool(reasons), reasons


class FleetSummary(BaseModel):
    """Fleet-level rollup. Cheap to compute for 8 robots, and it is what an operator
    actually looks at first - individual robots are the drill-down, not the headline."""

    total: int
    working: int
    needs_attention: int
    by_status: dict[str, int]
    by_link: dict[str, int]
    working_fraction: float
    mean_battery: float


class Snapshot(BaseModel):
    """A consistent read of the whole fleet at one version."""

    version: int
    server_time: float
    robots: list[RobotState]
    summary: FleetSummary


class Update(BaseModel):
    """One state transition, carrying the version it produced.

    A client that applies every Update in version order holds exactly what a snapshot
    at that version would have given it. That equivalence is the contract between the
    WebSocket stream and the REST endpoint.
    """

    version: int
    server_time: float
    robot: RobotState
    cause: Literal["telemetry", "link", "watchdog"]
