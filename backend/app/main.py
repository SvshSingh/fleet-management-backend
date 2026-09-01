"""Application wiring.

The order things are built in matters and is deliberate:

    FleetState  ->  Hub  ->  state.set_update_sink(hub.publish)

FleetState does not import Hub and knows nothing about WebSockets; it just calls a
sink for every mutation it makes. That keeps the state machine testable with a plain
list as the sink, and it means adding a second consumer later (a Kafka bridge, a
webhook for the alerting the brief's "operator, customer and OEM" fanout implies) is
one more sink, not a change to the state machine.
"""

from __future__ import annotations

import json
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional

from fastapi import FastAPI

from .api import router as rest_router
from .config import Settings, settings
from .fleet_state import FleetState
from .history import HistoryStore
from .hub import Hub
from .ingest import Ingestor
from .watchdog import Watchdog
from .ws import router as ws_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
)
log = logging.getLogger("main")


@dataclass
class Context:
    """Everything the request handlers need, assembled once at startup and hung off
    app.state. Explicit over globals: tests build their own Context with a different
    Settings and no broker at all."""

    settings: Settings
    state: FleetState
    hub: Hub
    ingestor: Ingestor
    watchdog: Watchdog
    history: Optional[HistoryStore]
    started_at: float


def load_roster(cfg: Settings) -> list[dict]:
    return json.loads((cfg.data_dir / "robots.json").read_text())


def build_context(cfg: Settings, roster: list[dict], with_history: bool = True) -> Context:
    hub = Hub(replay_buffer=cfg.hub_replay_buffer, subscriber_queue=cfg.hub_subscriber_queue)
    state = FleetState(roster=roster, low_battery_pct=cfg.low_battery_pct)

    # The one line that connects state changes to connected clients. Synchronous, so
    # fanout happens inside the state lock and update order == mutation order.
    state.set_update_sink(hub.publish)

    history = (
        HistoryStore(
            db_path=cfg.db_path,
            flush_rows=cfg.history_flush_rows,
            flush_seconds=cfg.history_flush_seconds,
        )
        if (with_history and cfg.history_enabled)
        else None
    )

    return Context(
        settings=cfg,
        state=state,
        hub=hub,
        ingestor=Ingestor(settings=cfg, state=state, history=history),
        watchdog=Watchdog(settings=cfg, state=state),
        history=history,
        started_at=time.time(),
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    ctx: Context = app.state.ctx
    if ctx.history is not None:
        await ctx.history.start()
    ctx.ingestor.start()
    ctx.watchdog.start()
    log.info("fleet backend up: %d robots on the roster", len(ctx.state._robots))
    try:
        yield
    finally:
        await ctx.ingestor.stop()
        await ctx.watchdog.stop()
        if ctx.history is not None:
            await ctx.history.stop()


def create_app(cfg: Settings | None = None, roster: list[dict] | None = None) -> FastAPI:
    cfg = cfg or settings
    roster = roster if roster is not None else load_roster(cfg)

    app = FastAPI(
        title="Peppermint Fleet Backend",
        version="1.0.0",
        summary="Ingests robot telemetry over MQTT and serves one fleet state over "
        "both a WebSocket stream and a REST endpoint.",
        lifespan=lifespan,
    )
    app.state.ctx = build_context(cfg, roster)
    app.include_router(rest_router)
    app.include_router(ws_router)
    return app


# Served as a factory (`uvicorn app.main:create_app --factory`) rather than a
# module-level `app = create_app()`. A module-level instance would read robots.json at
# import time, which means `pytest` fails on a machine without /data mounted - the
# tests must be runnable without Docker, so importing this module must have no side
# effects.
