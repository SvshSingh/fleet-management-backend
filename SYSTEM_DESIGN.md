# SYSTEM_DESIGN.md

Answered against the system in this repository, the MQTT publishers in `robot_sim/`
and the FastAPI service in `backend/app/`.

---

## 1. What happens if we ask you to add a new feature later? Does your current design accommodate that, or does it need a rework?

The seam that matters is in **`backend/app/main.py::build_context`**:

```python
state.set_update_sink(hub.publish)
```

`FleetState` does not import `Hub` and knows nothing about WebSockets. It calls one
synchronous sink for every mutation it makes. Everything downstream of state is a
subscriber to that one stream, which is what makes new consumers cheap.

**Worked example: low-battery alerting to the operator, the customer and the OEM**,
which is the behaviour your own preventive-maintenance system already has, so it is
the likeliest real ask.

Today the raw material exists: `derive_attention()` in `models.py` already computes
`needs_attention` and a list of reasons on every mutation, and `Update` carries the
full post-transition `RobotState`. So the feature is:

1. A new `backend/app/alerts.py` with an `AlertSink` class holding the previous
   `needs_attention` value per robot, so it fires on the *transition* into a bad state
   rather than every five seconds while it persists.
2. One line in `build_context`: change `set_update_sink` to fan out to a list:
   `[hub.publish, alerts.on_update]`. Both are synchronous and non-blocking, which is
   the contract; anything slow (an HTTP call to a notification service) goes on an
   `asyncio.Queue` inside the sink, never inline, because the sink runs under the state
   lock.
3. Hysteresis config in `config.py`, a battery threshold that flaps at 20% would page
   three times a minute.

No change to `FleetState`, `ingest.py`, `api.py` or `ws.py`. **That is the test of
whether the seam was real**, and I would say it passes.

Two other shapes of feature, honestly assessed:

* **A new derived field** (say `distance_travelled_today`): add it to `RobotState`,
  compute in `apply_telemetry`, and it appears in REST, WebSocket and history
  automatically because they all serialise the same model. Genuinely a one-file change.
* **A feature needing history rather than current state** (say "which robots errored
  most this week"): this one does *not* slot in neatly, and I would not pretend
  otherwise. `history.py` writes to SQLite on the same node, so it is fine for one
  robot over a range and wrong for aggregate analytics across the fleet. That is a
  Postgres/Timescale migration plus a query layer: a day, not an hour.

The one thing that would force a genuine rework is **multiple backend instances**, and
that is Q2.

---

## 2. What happens if the number of robots grows from eight to five hundred?

Let me be specific about volumes first. Eight robots at 0.2 Hz is 1.6 msg/s. Five
hundred robots at 1 Hz is 500 msg/s, roughly a 300x increase.

**The first thing that breaks is the WebSocket fanout in `Hub.publish`, and it breaks
well before the ingest path does.**

Why that specifically: ingest is O(1) per message, and 500 msg/s of small JSON through
`IngestGuard.judge` and `FleetState.apply_telemetry` is not a lot of work for asyncio.
But `Hub.publish` is O(subscribers) *per message*, and each subscriber gets the whole
`RobotState`, and worse, `_bump()` calls `model_copy(deep=True)` on every mutation.
At 500 msg/s with 20 operators watching, that is 10,000 serialisations per second of
objects that are each ~300 bytes, most of them describing a robot that moved four
centimetres. The queues fill, `needs_resync` starts firing, and every dashboard begins
thrashing between resync snapshots, each of which is now a 500-robot payload. The
failure is not a clean overload; it is a feedback loop where the recovery mechanism
becomes the load.

The second thing, close behind, is that **`GET /fleet` returns the entire fleet**. At
500 robots that is a ~150KB JSON body, and if operators poll it every second it will
dominate the service's CPU in serialisation alone.

Third, and only third, is the **single Mosquitto instance**: a broker handles 500 msg/s
without noticing, but it is a single point of failure and the blast radius grows with
the fleet.

**What I would change, in the order the problems arrive:**

1. **Batch and coalesce the fanout.** Instead of one frame per mutation, accumulate
   changes and flush every 250ms as one `{"type":"batch","version":N,"robots":[…]}`
   containing only robots that actually changed. An operator cannot perceive 250ms,
   and this drops fanout cost by an order of magnitude. It preserves the version
   contract exactly (the batch is still "state at version N"), which is the payoff for
   having made that contract explicit rather than implicit.
2. **Make the WebSocket subscription scoped**: `/ws?zone=A` or `?needs_attention=true`.
   Nobody watches 500 robots at once; they watch a zone or an exception list. This also
   shrinks the resync payload, which is what breaks the feedback loop above.
3. **Drop `model_copy(deep=True)` in `_bump`** in favour of an immutable `RobotState`
   so updates can share the object. Cheap, and it removes the per-mutation allocation.
4. **Then** move state to Redis so the backend is stateless and horizontally
   scalable, with the version counter as a Redis `INCR`. This is the point at which
   the design genuinely changes rather than being tuned. Note that the
   `snapshot_and_register` guarantee has to be re-established there, because the
   snapshot and the subscription are no longer under one process's lock. The Redis
   equivalent is `MULTI`/`EXEC` around a read plus a stream position from `XADD`.
5. **Cluster the broker** (EMQX/NATS) and shard subscriptions by topic prefix.

Something that pointedly does *not* break: the subscription itself.
`fleet/robots/+/telemetry` in `ingest.py` names no robot, so going from 8 to 500
publishers needs zero code change on the consumer side. That was a deliberate choice.

---

## 3. What happens if bandwidth is limited and robots and the backend can only exchange a small amount of data per second?

This is the least hypothetical question here, since your robots are on metered
cellular in customer buildings across three continents.

Current payload from `RobotPublisher._emit` is ~230 bytes of JSON per sample, every 5
seconds: ~46 B/s per robot, ~370 B/s for the fleet. Fine today, and roughly
wasteful per robot per month at 500 robots on LTE, most of it re-transmitting facts
that have not changed.

**What I would change, cheapest and highest-value first:**

1. **Do not send what has not changed.** A robot that is `idle` and stationary
   currently re-sends its identical position every five seconds. Send a full state on
   connect and on any status change, and otherwise send only fields that moved beyond
   a threshold: `{"seq":143,"x":580.9,"y":29.4}` is ~40 bytes. From the recording,
   robots move in only 308 of the 1,440 consecutive readings per robot: **79% of
   readings are a robot repeating itself.** That single change is roughly a 4x reduction, and I would do it first
   because the data says so.
2. **Adapt the rate to the state.** A charging or idle robot does not need 0.2 Hz; a
   30-second heartbeat is plenty. An `on_mission` robot near people arguably deserves
   more than 0.2 Hz. Rate as a function of status, not a constant.
3. **Drop JSON for the uplink.** MessagePack or CBOR is a two-line change to
   `_emit`/`handle_raw` and cuts ~40%; Protobuf cuts ~70% but adds a schema-compilation
   step to the build. I would take MessagePack first: most of the win, none of the
   toolchain.
4. **Shorten the topic and drop redundant fields.** `robot_id` is already in the topic
   (`fleet/robots/r3/telemetry`), so carrying it in the payload as well is pure
   duplication; `handle_raw` receives the topic and can parse it out.
5. **Coalesce during backpressure.** The publisher's outbound `deque` currently holds
   individual samples and flushes them all on reconnect. Under a tight cap it should
   collapse consecutive samples for the same robot into the newest one. The operator
   wants where the robot *is*, not a flipbook of where it was.

**What I would not compromise:** `seq` and `session`. They are ~15 bytes and they are
the entire basis for detecting loss (`IngestGuard.judge`). On the worst link, knowing
what you missed matters most, not least; that is the moment to spend bytes, not save
them.

One consequence to be explicit about: with delta encoding, a lost message means the
backend's position for that robot is wrong rather than merely stale, until the next
keyframe. So delta encoding requires a periodic full state (say every 12th sample) and
a "request keyframe" path when `IngestGuard` detects a gap. That is the real cost of
the optimisation, and it is why I did not do it pre-emptively at this scale.

---

## 4. What happens if a robot goes down mid-task and stops responding?

**How the system finds out**: two independent mechanisms, deliberately, because they
catch different failures.

1. **Last Will and Testament.** `robot_publisher.py.__init__` registers a retained
   will on `fleet/robots/{id}/link`. If the TCP connection breaks without a clean
   DISCONNECT (power cut, modem death, process killed), the broker publishes it for
   us, and `FleetState.apply_link` marks the robot `link=lost` within about a second.
2. **The watchdog.** `watchdog.py` sweeps every 2s and `FleetState.sweep_liveness`
   ages robots on time since last message: `live -> stale` at 12s (~2 missed ticks),
   `-> lost` at 30s (~6).

The second exists precisely because the first is not enough, and this is the part I
would most want to be asked about. LWT only fires when a *connection* breaks. A robot
whose process has hung, whose sensor loop is wedged, or whose modem is holding a socket
open while passing no data is still connected as far as the broker is concerned, and
would look perfectly healthy forever. Only a timer on last-message-received catches
that one. The thresholds are two and six missed ticks rather than one, because a single
dropped packet on a cellular link is normal and alerting on it trains operators to
ignore alerts.

**What the rest of the system does about it:**

* The transition bumps `version` and emits an `Update` with `cause="watchdog"`, so
  every WebSocket client and every subsequent `GET /fleet` sees it, consistently,
  through the same path as telemetry.
* `derive_attention()` flags `needs_attention` with reason `link:lost`, so the robot
  appears in `GET /robots?needs_attention=true` without an operator hunting for it.
* Its **last known position is retained**, not cleared. Where a robot stopped is the
  single most useful fact when you are going to fetch it, and blanking it would be
  actively hostile.

Crucially, the robot's *reported* `status` is left untouched. The distinction matters:
`status=on_mission, link=lost` says "it was mid-task when we lost it": go now, it may
be blocking an aisle. `status=idle, link=lost` says "it was parked anyway": deal with
it tomorrow. Collapsing link loss into the status enum, which is the tempting
simplification, would destroy exactly the information that determines urgency. The
recording justifies keeping them apart: robots reporting `offline` never move but keep
publishing, so `offline` is demonstrably a self-report, not a transport failure.

**Recovery needs no special mechanism.** `apply_telemetry` sets `link=live`
unconditionally: hearing from a robot *is* the proof it is back. One guard exists for
the ugly case: a retained will from a dead process can arrive *after* its replacement
has reconnected, so `apply_link` ignores a `lost` announcement whose `session` does not
match the current one. Without it, a robot's ghost kills its own successor. Covered by
`test_watchdog_and_api.py::test_stale_will_from_a_previous_incarnation_is_ignored`.

**What is missing and I would add:** the backend has no concept of a *task*, so it
cannot reassign the dropped robot's work. The recording only contains two unpaired
`task_event` lines, so there was nothing to build a task model on; I carry them
through as `last_task_event` and a counter, and no further. With a real task service,
`cause="watchdog"` on a robot whose last task event was `task_started` is exactly the
signal to trigger reassignment.

---

## 5. What happens if the connection is slow or unreliable, and updates arrive late, out of order, or not at all for a while?

Taking the three cases separately, because the system treats them differently on
purpose.

**Late.** The reading still arrives, just behind. `RobotState` carries two clocks:
`reported_ts` (the robot's own idea of when it happened) and `last_seen_ts` (when we
actually received it). Their difference is the observability: a growing gap is a
degrading link, visible before it fails outright. The watchdog uses `last_seen_ts`, so
a robot on a slow link correctly goes `stale` even though it is technically still
talking.

**Out of order.** `IngestGuard.judge` drops it (`Verdict.OUT_OF_ORDER`) and counts it.
The alternative, applying it, would make the robot visibly jump backwards on every
operator's screen, and a stale position has no value once a newer one exists. Note the
deliberate asymmetry with `history.py`, which **keeps** late readings: for
after-the-fact analysis a late reading is still a real thing the robot reported, and
dropping it would put holes in the record. Live state optimises for "what is true
now"; history optimises for "what happened".

**Not at all, for a while.** Four things happen, in order:

1. The publisher buffers into a bounded `deque` (`RobotPublisher._emit`) and flushes
   on reconnect. Bounded, and overflow drops the oldest **and increments a counter**:
   an unbounded buffer is just a slower crash, and a silent one is worse.
2. The backend's watchdog ages the robot `live -> stale -> lost`, so the dashboard
   shows uncertainty rather than a frozen position presented as current.
3. On reconnect, the flushed backlog arrives at once. `IngestGuard` accepts the newest
   and drops the rest as out-of-order, which is right for live state, and the reason
   history is written from accepted messages only is a limitation I would revisit.
4. `gaps_detected` and `messages_lost_estimate` at `GET /metrics` record exactly how
   much was never received. I verified this works on real traffic: replaying 4,914
   messages captured from eight live publishers, the detector reported ~54 missing
   purely from sequence numbers, without being told loss had occurred.

**What clients see during the outage:** a robot with `link=stale` or `lost`, its last
known position, its last reported status, and `needs_attention=true` with the reason
spelled out. Not a frozen dashboard that looks fine, which is the actual danger, and
why the WebSocket sends a `ping` every 10 seconds: a TCP connection can be dead for
minutes without either end noticing, and a client that stops seeing pings knows to
reconnect rather than trusting a stale screen.

**Recovery.** Telemetry sets `link=live` immediately; no explicit recovery signal is
needed. For a WebSocket client whose own connection dropped, reconnecting with
`?since=<version>` replays exactly what it missed from `Hub.replay_since`. If its
version has fallen out of the 500-update buffer, the hub returns `None` and the client
gets a `resync` frame plus a fresh snapshot. **Refusing to answer is the important
behaviour there.** Handing back whatever happens to still be in the buffer would leave
the client quietly wrong, which is far worse than making it re-sync. Same principle as
the gap counter: the system's job on a bad network is to know what it does not know.

**The honest gap:** if the backend itself restarts, the version counter restarts at
zero, and a client reconnecting with `?since=4000` gets `None` and re-syncs, correctly,
but only by luck, since `replay_since` rejects versions ahead of the buffer. A
production version needs an epoch or instance id alongside the version so a client can
distinguish "I am ahead of you" from "you are a different backend". That is a small
change to `Snapshot` and `Hub.replay_since`, and it is the first thing I would fix.
