"""REST surface - the polling half of the contract.

Every read goes through FleetState's lock and reports the `version` it was taken at,
which is what lets a caller line a REST body up against a WebSocket stream and check
they agree. A REST response without its version would be unfalsifiable.
"""

from __future__ import annotations

import time
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

from .models import RobotState, Snapshot

router = APIRouter()


def _ctx(request: Request):
    return request.app.state.ctx


@router.get("/health")
async def health(request: Request) -> dict:
    """Liveness plus the one fact that matters operationally: are we hearing anything?

    A backend that is up but disconnected from the broker is not healthy in any useful
    sense, and reporting a bare 200 there would be a lie an orchestrator acts on.
    """
    ctx = _ctx(request)
    snap = await ctx.state.snapshot()
    return {
        "status": "ok" if ctx.ingestor.connected else "degraded",
        "broker_connected": ctx.ingestor.connected,
        "uptime_seconds": round(time.time() - ctx.started_at, 1),
        "version": snap.version,
        "robots_known": len(snap.robots),
        "robots_live": snap.summary.by_link.get("live", 0),
        "ws_subscribers": ctx.hub.subscriber_count,
    }


@router.get("/fleet", response_model=Snapshot)
async def get_fleet(request: Request) -> Snapshot:
    """The whole current picture at one version - the polling counterpart to the
    WebSocket snapshot frame, produced by the identical code path."""
    return await _ctx(request).state.snapshot()


@router.get("/robots", response_model=list[RobotState])
async def list_robots(
    request: Request,
    needs_attention: Optional[bool] = Query(
        None, description="Filter to robots an operator should look at"
    ),
    status: Optional[str] = Query(None, description="Filter by reported status"),
    link: Optional[str] = Query(None, description="Filter by link state: live|stale|lost"),
) -> list[RobotState]:
    snap = await _ctx(request).state.snapshot()
    robots = snap.robots
    if needs_attention is not None:
        robots = [r for r in robots if r.needs_attention == needs_attention]
    if status is not None:
        robots = [r for r in robots if r.status == status]
    if link is not None:
        robots = [r for r in robots if r.link == link]
    return robots


@router.get("/robots/{robot_id}", response_model=RobotState)
async def get_robot(request: Request, robot_id: str) -> RobotState:
    robot = await _ctx(request).state.get(robot_id)
    if robot is None:
        raise HTTPException(status_code=404, detail=f"unknown robot {robot_id!r}")
    return robot


@router.get("/robots/history/{robot_id}")
async def get_history(
    request: Request,
    robot_id: str,
    start: Optional[float] = Query(None, description="Unix seconds on reported_ts, inclusive"),
    end: Optional[float] = Query(None, description="Unix seconds on reported_ts, inclusive"),
    limit: int = Query(1000, ge=1, le=20000),
) -> dict:
    """Stretch goal.

    Range and ordering are on `reported_ts` - the robot's own wall clock at emit - not
    on its `t` field, which restarts every replay cycle and would make ranges
    ambiguous. Each row also carries `received_ts`, so the gap between the two shows
    arrival lag on a degrading link.
    """
    ctx = _ctx(request)
    if not ctx.state.knows(robot_id):
        raise HTTPException(status_code=404, detail=f"unknown robot {robot_id!r}")
    if ctx.history is None:
        raise HTTPException(status_code=503, detail="history is disabled")
    if start is not None and end is not None and start > end:
        raise HTTPException(status_code=400, detail="start must be <= end")

    rows = await ctx.history.query(robot_id, start=start, end=end, limit=limit)
    return {"robot_id": robot_id, "count": len(rows), "start": start, "end": end, "points": rows}


@router.get("/metrics")
async def metrics(request: Request) -> dict:
    """Ingest counters. Cheap, and it makes the guards in ingest.py observable: you can
    watch duplicates and gaps accumulate while you break things."""
    ctx = _ctx(request)
    return {
        "ingest": ctx.ingestor.metrics.as_dict(),
        "broker_connected": ctx.ingestor.connected,
        "state_version": ctx.state.version,
        "ws_subscribers": ctx.hub.subscriber_count,
        "history_rows_written": ctx.history.rows_written if ctx.history else 0,
    }
