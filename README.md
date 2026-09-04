# Fleet Management Backend: Peppermint Robotics

Eight simulated robots publish telemetry over MQTT; a FastAPI
service ingests it, holds one authoritative fleet state, and serves that state over
both a WebSocket stream and a REST endpoint that are guaranteed to agree.

This file covers the architecture and the design decisions. The three written
questions the challenge asks for are answered in `ANSWERS.md`; the five system-design
questions are in `SYSTEM_DESIGN.md`.

```
 robot_sim container              broker            backend container
 ┌──────────────────────┐      ┌──────────┐      ┌───────────────────────────┐
 │ supervisor.py        │      │ mosquitto│      │ ingest.py   (aiomqtt sub) │
 │  ├─ process r1 ──────┼─QoS1─┤          ├──────┤   seq / dedup / gaps      │
 │  ├─ process r2       │retain│   LWT    │fleet/│         ↓                 │
 │  ├─ ...              │      │ keepalive│robots│ fleet_state.py  ← ONE LOCK│
 │  └─ process r8       │      │          │ /+/# │   version: int (monotonic)│
 └──────────────────────┘      └──────────┘      │         ↓        ↓        │
   one OS process per robot                      │  hub.py       api.py      │
                                                 │  WS fanout    REST        │
                                                 │  watchdog.py  history.py  │
                                                 └───────────────────────────┘
```

---

## Quick start

```bash
docker compose up --build
```

That is the whole submission. It starts the broker, the backend, and the eight-robot
simulation: no second terminal, no manual step.

Then:

```bash
curl -s localhost:8000/fleet | jq '.version, .summary'
curl -s "localhost:8000/robots?needs_attention=true" | jq '.[].robot_id'

pip install websockets httpx
python tools/ws_client.py            # watch the live stream
python tools/ws_client.py --verify   # prove WS and REST agree, frame by frame
```

The recorded 15-minute window replays at 5x by default, so the fleet is visibly moving
within seconds. `SPEED_FACTOR=1 docker compose up` for real time. Publishers loop the
recording with an incrementing `cycle` so the demo never goes dead. That is looping,
not fifteen minutes of invented data.

### Tests

```bash
cd backend
pip install -r requirements-dev.txt
pytest -v
```

No broker and no Docker required: tests drive `Ingestor.handle_raw` directly, which is
the same function the MQTT loop calls. A test suite you can only run with
`docker compose up` is a test suite nobody runs.

The trickiest part to get right was the consistency guarantee, and separately the
ordering between the watchdog and the broker's Last Will. Those live in
`backend/tests/test_consistency.py` (`test_snapshot_and_subscription_are_atomic`,
`test_ws_replay_reconstructs_exactly_the_rest_body`) and
`backend/tests/test_watchdog_and_api.py`
(`test_watchdog_never_downgrades_an_lwt_lost_robot_to_stale`), the last of which is a
regression test for the one real bug found while verifying this submission; see the
AI delegation notes below for how.

---

## What the data told me

Before designing anything I checked all 1,448 lines of `events.jsonl`, because the
brief deliberately declines to define which statuses count as working. The recording
answers it:

| Finding | Evidence |
|---|---|
| Battery rises **iff** `charging` | 73 of 73 increases are `charging`; no `charging` sample fails to rise |
| Robots move **only** when `active` or `on_mission` | all 308 samples with movement; every other status is stationary |
| **`offline` robots never move but keep publishing** | 85 `offline` samples, none with movement, all still arriving |
| Cadence is exactly 5s | all 1,440 inter-sample gaps; so any gap the backend sees is a real fault, not jitter |

The third one drives the whole state model, and it is the decision I would most want
to defend. `status` is what a robot says about itself; `link` is whether we can hear
it. They are orthogonal, so both are stored (`models.py`):

* `status=offline, link=live`: parked and reachable. Fine. No action.
* `status=on_mission, link=lost`: mid-task and silent. Send someone now.

Collapsing those into one enum would throw away the distinction that matters most to
an operator. `derive_attention()` in `models.py` is the single place that turns the
two fields plus battery into one `needs_attention` flag, so the policy has one
address. Note it excludes `charging` from the low-battery rule: a robot on its dock at
4% is behaving correctly, and paging an operator for it is how alerts get ignored.

---

## Design decisions

### Why MQTT

Peppermint's robots ship worldwide with a Qualcomm telematics board on a cellular
link, sitting inside a customer's building behind their NAT and firewall. That is the
network this has to work on, and MQTT is built for it: it is what AWS IoT Core, Azure
IoT Hub and Qualcomm's own telematics stacks speak. Concretely it gives:

* **Last Will and Testament**: the broker announces a robot's death on its behalf
  when the socket breaks. `robot_publisher.py` registers it at connect;
  `FleetState.apply_link` consumes it.
* **Keepalive**: dead-peer detection in ~15s. Raw TCP will happily hold a half-open
  socket that looks healthy for a very long time.
* **QoS 1 + `clean_session=False` + retained messages**: the broker queues telemetry
  while the backend restarts, and a fresh backend gets every robot's last known
  position immediately instead of a blank board.
* **Wildcard subscribe**: `fleet/robots/+/telemetry` scales from 8 robots to 500 with
  no code change.

The cost is real and I own it: QoS 1 is *at-least-once*, so duplicates and reordering
are mine to handle. That is what `IngestGuard.judge` exists for, and it is deliberately
my code rather than the broker's magic.

### Topics

| Topic | QoS | Retained | Who publishes |
|---|---|---|---|
| `fleet/robots/{id}/telemetry` | 1 | yes | the robot |
| `fleet/robots/{id}/link` | 1 | yes | the robot on connect; the **broker** as its will on failure |

### The ingest guards (`ingest.py`)

Every message is judged before it can touch state. Each rejection is counted and
exposed at `/metrics`: silent loss is the failure mode this whole design exists to
avoid.

| Case | Action | Why |
|---|---|---|
| unknown `robot_id` | drop | not on the roster; we cannot reason about it |
| new `session` | accept, reset cursor | the robot rebooted and its `seq` restarted at 1. Without the session id that is indistinguishable from a flood of duplicates, and we would ignore a recovered robot **forever** |
| `seq == last` | drop | QoS 1 redelivery |
| `seq < last` | drop | a late arrival would rewind the robot's position on every screen |
| `seq > last + 1` | **accept**, count the hole | the reading is current and useful; the loss is recorded, not hidden |

### The consistency guarantee (`fleet_state.py`)

The brief requires that a WebSocket client and a polling client never disagree. That
is enforced by one method, `FleetState.snapshot_and_register`:

```python
async with self._lock:
    snap = self._build_snapshot()
    registered = register(snap.version)   # join the fanout, same lock acquisition
    return snap, registered
```

Take the snapshot, release the lock, then subscribe (the obvious two-step version),
and any update landing in that window belongs to neither the snapshot nor the queue.
It vanishes, silently, only under load. Doing both under one acquisition means a
subscriber provably receives versions N+1, N+2, … with no gap and nothing twice, so
**snapshot + applied deltas == the REST body at the same version**. That equality is
asserted in `tests/test_consistency.py` and can be watched live with
`tools/ws_client.py --verify`.

`Hub.publish` is synchronous and non-blocking for the same reason: it is called while
the lock is held, so queue order equals mutation order. If it awaited anything, a
client could apply version 8 before version 7.

### Handling flaky networks

The brief asks for a design that assumes the network is bad. Specifically:

| Failure | Response | Where |
|---|---|---|
| Robot's connection drops | broker fires LWT, `link=lost` in ~1s | `robot_publisher.py` will_set, `FleetState.apply_link` |
| Robot connected but frozen | watchdog ages `live -> stale -> lost` on time since last message (the only thing that catches this, since the broker sees a healthy socket) | `watchdog.py`, `FleetState.sweep_liveness` |
| Robot offline briefly | bounded `deque` buffers telemetry and flushes on reconnect; overflow drops oldest **and counts it** | `RobotPublisher._emit` |
| Broker restarts | both sides reconnect with exponential backoff **and jitter** (500 robots reconnecting in lockstep is a self-inflicted DDoS) | `Ingestor.run` on the backend; `robot_publisher.py::_connect_with_backoff` on the publisher |
| Backend restarts | `clean_session=False` queues QoS-1 messages; retained telemetry repopulates the fleet instantly | `config.py` |
| WS client drops | reconnect with `?since=<version>`; the hub replays exactly what was missed, or says it cannot and re-snapshots | `Hub.replay_since`, `ws.py` |
| WS client too slow | drop its backlog, flag for resync (one laptop on bad wifi must never stall ingest) | `Hub.publish`, `ws._resync` |
| Robot restarts | new `session` id resets the sequence cursor | `IngestGuard.judge` |
| Stale will arrives after reconnect | ignored if its session doesn't match (a dead process must not kill its own replacement) | `FleetState.apply_link` |

Set `CHAOS_DISCONNECT_PROB=0.01` to have publishers randomly yank their sockets and
watch recovery happen.

---

## API

| Endpoint | Purpose |
|---|---|
| `GET /health` | liveness + **broker connectivity**: a backend that is up but deaf reports `degraded`, because a bare 200 there is a lie an orchestrator acts on |
| `GET /fleet` | whole fleet at one `version`, plus a rollup |
| `GET /robots?needs_attention=&status=&link=` | filtered list |
| `GET /robots/{id}` | one robot |
| `GET /robots/history/{id}?start=&end=&limit=` | stretch goal; range is on `reported_ts` (the robot's wall clock), since its `t` field restarts each replay cycle |
| `GET /metrics` | ingest counters: accepted, duplicates, gaps, estimated loss, reconnects |
| `WS /ws?since=<version>` | `snapshot` -> `update`… with a 10s `ping` heartbeat |

Interactive docs at `localhost:8000/docs`.

### History (stretch goal)

SQLite, because this is a single node writing ~1.6 rows/second for eight robots: no
operator, no extra container, and the whole store is one file you can open with
`sqlite3`. Writes are batched off the ingest path (`history.py`) so disk I/O never
sits between a robot and the operator's screen. At 500 robots at 1Hz it stops being
the right answer; see SYSTEM_DESIGN.md.

Each row stores two clocks: `reported_ts` (robot's wall clock at emit) and
`received_ts` (server clock at ingest). Their difference is arrival lag, which is how
you watch a link degrade before it fails. Queries range and order on `reported_ts`,
because an analyst wants the robot's timeline, not ours.

One deliberate asymmetry worth asking me about: live state **drops** a late reading,
history **keeps** it. `handle_raw` writes to the store *before* calling
`IngestGuard.judge`, so the record does not inherit live state's holes. Showing a
stale position as current is actively misleading; losing it from the record is just a
hole. Locked in by
`tests/test_history.py::test_late_reading_reaches_history_even_though_live_state_rejects_it`.

---

## Configuration

Everything is env-driven (`config.py`, `.env.example`); defaults work with no setup.

| Variable | Default | Meaning |
|---|---|---|
| `SPEED_FACTOR` | `5` | replay speed; `1` is real time |
| `CHAOS_DISCONNECT_PROB` | `0` | per-sample chance a publisher drops its socket |
| `STALE_AFTER_SECONDS` | `12` | ~2 missed ticks -> `stale` |
| `LOST_AFTER_SECONDS` | `30` | ~6 missed ticks -> `lost` |
| `LOW_BATTERY_PCT` | `20` | attention threshold (ignored while charging) |
| `HUB_REPLAY_BUFFER` | `500` | how far back a WS client can resume |
| `API_PORT` | `8000` | host port for the REST/WS API |
| `BROKER_PORT` | `1883` | host port for the broker |

The last two exist only so a port clash on your machine is a one-word fix rather than
an edit to `docker-compose.yml`; nothing inside the compose network uses them:

```bash
API_PORT=8080 BROKER_PORT=1884 docker compose up --build
```

Thresholds are two and six missed ticks, not one: a single dropped packet on a
cellular link is normal, and treating it as a fault trains operators to ignore alerts.

---

## Verification

```bash
docker compose up --build
docker compose logs robot-fleet | grep spawned     # 8 distinct OS pids
curl -s localhost:8000/fleet | jq .summary
python tools/ws_client.py --verify                 # WS == REST, continuously
```

Failure drills worth running:

```bash
docker compose stop broker      # publishers back off; backend flips robots to lost
docker compose start broker     # everything recovers with no manual step

# Only r3 goes lost, and via LWT rather than the watchdog. Two details matter here:
# it must be SIGKILL, because on SIGTERM the publisher disconnects cleanly and the
# broker is then CORRECT to withhold the will; and procps is not installed in the
# slim image, so the pid comes from /proc instead of pkill. The supervisor restarts
# r3 about two seconds later, so watch for the transition rather than the end state.
docker compose exec robot-fleet sh -c 'for p in /proc/[0-9]*; do pid=${p##*/}; [ "$pid" = "$$" ] && continue; tr "\0" " " < "$p/cmdline" 2>/dev/null | grep -q "robot-id r3 " && kill -9 "$pid"; done; exit 0'
```

---

## AI delegation notes

AI assistance was used substantially, across two kinds of work: writing the thing, and then
trying to break it.

**Delegated: writing.** Initial scaffolding of all modules, both Dockerfiles and the
compose file; first drafts of the test suite; first drafts of this README, `ANSWERS.md`
and `SYSTEM_DESIGN.md`.

**Mine: the decisions.** The analysis of `events.jsonl` that produced the findings
above, and the decision that follows from it to model `status` and `link` as separate
fields; the choice of MQTT and the reasoning about a cellular/NAT deployment topology;
the `snapshot_and_register` consistency design; the watchdog thresholds; the decision to
write history *before* the ingest guard so the record keeps late readings that live
state rejects.

**Delegated: verification, and this is where it earned its keep.** A later session
brought the whole system up under Docker and attacked it. That was worth more than the
drafting, because it found a real bug:

* `FleetState.sweep_liveness` chose a robot's link state from message age alone. A robot
  already `lost` via the broker's Last Will would be **downgraded to `stale`** once its
  age fell in the stale band, so a dead robot appeared to recover on the operator's
  board. Fixed by ordering link severity so the watchdog may only ever escalate; only
  telemetry clears a bad link. `test_watchdog_never_downgrades_an_lwt_lost_robot_to_stale`
  fails without the fix.
* The single-robot failure drill in this README was wrong twice over: it used `pkill`,
  which is not in the slim image, and `SIGTERM`, which is a *clean* disconnect the
  broker is right to withhold a will for. Both corrected; the replacement was run
  verbatim out of this file.

Neither was findable by reading the code. Both came from killing real processes against
a real broker.

**What is actually verified, and how.** Not "it compiles":

* `pytest`: 29 tests green, on Python 3.11 and on 3.12, the version the containers ship.
* `docker compose up --build`: from a clean tree containing only git-tracked files,
  `--no-cache`, stock builder. Broker healthy, 8 distinct OS pids, 8 robots live.
* Real MQTT through Mosquitto: 1,534 messages ingested with 0 malformed, 0 out-of-order,
  0 estimated lost, across a replay-cycle rollover where `t` restarts and `seq` does not.
* Consistency: three concurrent WebSocket clients reconstructed the fleet with zero
  version gaps and landed on the same version; at a quiescent version, a WS snapshot
  matched the REST body across all 144 fields of all 8 robots.
* Reconnect: a client resuming with `?since=N` gets version `N+1` contiguously; one
  resuming from outside the replay buffer is told to `resync` and handed a fresh
  snapshot, never left to guess.
* Broker outage: stopping the broker degrades health, drops all 8 robots to `lost`,
  and leaves the API up. On restart everything recovers in ~15s with no manual step,
  and the 8 retained messages Mosquitto redelivers are all caught as duplicates
  rather than double-counted.
* The publisher was run as a real process and diffed against `events.jsonl`: cycle 0
  reproduces all 181 of r3's samples exactly (the file holds 1,448 events, 181 per robot).

I can explain any line in this repo, including the two fixes above and why the bug was
invisible until the system was actually running.

---

## What I would do next

In priority order, the three things worth doing before this went anywhere near real
robots:

1. **A version field on the wire.** There is nothing today that rejects a payload
   from a publisher running a different schema than the backend expects; it would
   just fail pydantic validation and get silently counted as `malformed`. A `v` field
   checked in `IngestGuard.judge` turns that into an explicit, loud rejection instead.
2. **State out of the process.** `FleetState` lives in memory, so a backend restart
   both loses the version counter's meaning and needs a warm-up window before it is
   authoritative again. Moving it to Redis, with history in Postgres or Timescale
   instead of the local SQLite file, is what makes the backend stateless enough to
   run more than one instance and to restart without a gap.
3. **Hysteresis on `needs_attention`.** `derive_attention()` in `models.py` flips the
   instant a robot's battery crosses 20%, so a robot oscillating right at that line
   would page an operator repeatedly for the same underlying event.

The full list, including what I left out entirely and why, is in ANSWERS.md Q3; the
scaling and bandwidth reasoning behind items 1 and 2 is in SYSTEM_DESIGN.md Q2 and Q3.

