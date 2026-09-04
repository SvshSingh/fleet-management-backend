# ANSWERS.md

## 1. What holds the fleet's current state in your backend, and why that shape, given it has to serve both the WebSocket stream and the polling endpoint consistently?

`FleetState` in **`backend/app/fleet_state.py`**: a `dict[str, RobotState]` behind a
single `asyncio.Lock`, plus a monotonically increasing integer `_version` bumped by
`_bump()` on every accepted mutation.

The dict is deliberately unambitious, and I would defend that. A site has tens of
robots, not millions; every consumer wants the whole current picture rather than a
slice; and every lookup is by `robot_id`. A dict is exactly that access pattern, it
fits in cache, and anything cleverer would be justifying itself rather than the data.
The seeding is worth noting: `FleetState.__init__` populates the dict from
`robots.json` at startup, so a robot we have never heard from appears as
`link=lost, link_reason=never_seen` rather than being absent. An operator needs to see
that r7 exists and is silent, and an empty list cannot say that.

The interesting part is the version counter, because it is what makes the two
transports provably agree. `GET /fleet` (`api.py::get_fleet`) returns
`{version: N, robots: [...]}`, the state after N mutations. A WebSocket client gets a
snapshot at version N and then updates N+1, N+2, … So a client that has applied every
update through version N holds exactly what the REST endpoint would have returned at
version N. That is the contract, stated as an equality rather than a hope, and
`tests/test_consistency.py::test_ws_replay_reconstructs_exactly_the_rest_body`
asserts it directly.

The subtle part is not the counter, it is **`FleetState.snapshot_and_register`**. The
naive implementation takes a snapshot, releases the lock, and then subscribes the
client to the fanout. Any update landing in that window belongs to neither the
snapshot nor the queue, and disappears silently, only under load, which is the worst
possible combination. So the snapshot and the subscription happen under one lock
acquisition: `register(snap.version)` is called before the lock is released. That is
the whole guarantee, in one line, and it is why `Hub.publish`
(**`backend/app/hub.py`**) is synchronous and non-blocking: it is called while the
state lock is held, so queue order equals mutation order. If it awaited anything, two
updates could interleave and a client would apply version 8 before version 7.

One consequence I like: because *both* transports read through the same lock and the
same `_build_snapshot()`, there is no separate "REST view" that can drift from the
"WebSocket view". They are the same code path with different framing.

State also carries `status` and `link` as separate fields (`models.py`), which came
out of reading the data rather than the brief. See Q2 in SYSTEM_DESIGN.md and the
README table.

---

## 2. Name one real tradeoff you made: the mechanism you chose for robots to reach your backend, its delivery guarantees, and how you reconcile that mechanism's semantics with your WebSocket fanout. Argue for the decision, including its cost.

**I chose MQTT (Mosquitto) at QoS 1, and paid for it with duplicate and reordering
handling I had to write myself.**

The argument for it is about your actual deployment, not about convenience. Peppermint
ships robots worldwide with a Qualcomm telematics board on a cellular link, sitting
inside a customer's building behind their NAT and firewall. In that topology, plain
sockets mean owning NAT traversal, negotiating inbound ports with someone else's IT
department, and hand-building heartbeats, backoff and resume. MQTT is designed for
exactly this network, and three of its primitives map directly onto the failure modes
this challenge asks about:

* **Last Will and Testament**: the broker announces a robot's death on its behalf.
  Registered in `robot_publisher.py.__init__` via `will_set`, consumed by
  `FleetState.apply_link`. This is most of the answer to "how would the system even
  find out" for a yanked power cable.
* **Keepalive**: dead-peer detection in ~15s, where a raw TCP socket can stay
  half-open and healthy-looking for a very long time.
* **QoS 1 + `clean_session=False` + retained messages**: the broker queues telemetry
  while the backend restarts, and a fresh backend gets every robot's last known
  position instantly instead of showing a blank board for five seconds.

**The cost.** QoS 1 is *at-least-once*. The broker is allowed to redeliver, and a
publisher flushing its offline buffer can deliver readings out of order relative to
what I have already applied. Neither is a bug; both corrupt state if applied naively:
a duplicate would re-emit an update to every client for no reason, and a late reading
would rewind the robot's position on every operator's screen.

So the reconciliation is explicit and it is mine, not the broker's. Each publisher
stamps a per-robot `seq` and a `session` UUID minted at process start
(`RobotPublisher._emit`), and **`IngestGuard.judge`** in `backend/app/ingest.py` is the
single place that decides what happens to each message:

| Case | Action | Reasoning |
|---|---|---|
| `seq == last_seq` | drop, count `duplicate` | QoS 1 redelivery |
| `seq < last_seq` | drop, count `out_of_order` | freshest reading wins; a stale one has no value once superseded |
| `seq > last_seq + 1` | **accept**, count `gaps` and the exact number missed | the reading is current and useful; the loss becomes measurable rather than invisible |
| new `session` | accept, reset the cursor | the robot rebooted and its `seq` restarted at 1 |

That last row is the one I would flag as load-bearing. Without the session id, a
rebooted robot's `seq=1` is indistinguishable from a flood of duplicates, so it would
be dropped until its counter climbed past the old high-water mark, potentially
forever. A robot that comes back from a power cycle and is then ignored by the
dashboard is a much worse bug than a duplicate.

**How this meets the fanout.** Only accepted messages reach `FleetState`, so by the
time anything touches the version counter it has already been judged fresh and
in-order. The fanout therefore never has to think about MQTT semantics at all: it sees
a clean, gapless, monotonic sequence of state transitions. The at-least-once mess is
absorbed at exactly one boundary, and every rejection is counted and exposed at
`GET /metrics`, so the cost of the choice is visible in production rather than
theoretical. `tests/test_ingest_guards.py` covers all five cases.

I checked this works on real traffic rather than trusting it: replaying 4,914 messages
captured from eight live publisher processes, the gap detector independently reported
~54 missing messages purely from sequence numbers, loss it found without being told
it had happened.

**What it cost beyond code:** one more service in `docker-compose.yml`, a broker that
is a single point of failure at this scale, and an obligation to understand QoS,
retain and `clean_session` well enough to defend them. I took that trade because the
alternative, hand-rolled TCP, would have spent roughly a working day of a two-day
budget re-implementing a worse version of the same primitives, and that day comes
straight out of the consistency guarantee, the tests and these documents.

---

## 3. What did you leave out, and what would you build next given more time?

**Left out, deliberately:**

* **Broker auth and TLS.** `allow_anonymous true` on a closed compose network with
  nothing exposed to the internet. In production this is per-robot X.509 client certs,
  topic ACLs so a robot can only write to its own topic, and mTLS. I left it out
  because it needs a certificate-issuance story more than it needs code, and a
  half-done auth layer is worse than an obviously absent one.
* **Multi-broker HA.** One Mosquitto is a single point of failure. EMQX or NATS
  clustered behind a load balancer fixes it, and is overkill for eight robots.
* **Horizontal scaling of the backend.** State is in-process, so this runs as one
  instance today. SYSTEM_DESIGN.md Q2 walks through precisely where that breaks.
* **A downlink command channel.** Robots only talk upward. Commands need
  request/response correlation and idempotency: a design conversation, not an
  afternoon, and nothing in the brief asked for it.
* **Schema versioning.** No `v` field on the payload. This is the omission I am least
  comfortable with; see below.
* **Prometheus/OTel format**, and **hysteresis on `needs_attention`** (it will flap
  for a robot oscillating around 20% battery).

**What I would build next, in order:**

1. **A `v` field on the wire**, with unknown versions rejected and counted in
   `IngestGuard.judge`. It costs nothing today and a great deal once a second
   publisher implementation exists. Highest value per hour of anything on this list.
2. **Move state to Redis and history to Postgres/TimescaleDB**, making the backend
   stateless so it can scale horizontally and survive a restart without a warm-up
   period. This is the change that unblocks the 500-robot case, and the version
   counter becomes a Redis `INCR`; the consistency argument survives intact, which is
   the point of having made it explicit.
3. **Delta encoding and adaptive publish rates.** Today a stationary robot re-sends
   its full position every five seconds. On metered cellular that is most of the bill
   for none of the information (SYSTEM_DESIGN.md Q3).
4. **Alert rules with hysteresis and an outbound notification path**, which is where
   Peppermint's existing "alerts to the operator, customer and OEM" behaviour would
   plug in, as one more `UpdateSink` alongside `Hub.publish` (SYSTEM_DESIGN.md Q1).

**On the timebox:** I spent it on the consistency guarantee, the failure handling, and
the tests, rather than on breadth. If something here looks thin, it is more likely a
choice than an oversight; ask me and I will tell you which.
