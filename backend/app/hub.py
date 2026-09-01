"""WebSocket fanout: one update in, N subscribers out.

Three things this has to get right, all of them failure-shaped:

  1. Ordering. `publish` is synchronous and non-blocking, and FleetState calls it
     while holding its lock. So updates land in every subscriber's queue in exactly
     the order the state mutated. If this awaited anything, two updates could
     interleave and a client would apply version 8 before version 7.

  2. Slow consumers. A subscriber on a bad connection must never be able to stall
     ingest - one laptop on hotel wifi cannot be allowed to freeze the fleet for
     everyone. Queues are bounded; on overflow we drop that subscriber's backlog and
     flag it for resync. Losing a client's deltas is recoverable (send a fresh
     snapshot); blocking the ingest path is not.

  3. Reconnects. Clients drop. A bounded replay buffer lets one come back with
     `?since=<version>` and pick up exactly where it left off, instead of re-syncing
     the whole fleet every time a train goes through a tunnel.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from collections import deque
from typing import Optional

from .models import Update

log = logging.getLogger("hub")

_ids = itertools.count(1)


class Subscriber:
    """One connected WebSocket client's view of the stream."""

    def __init__(self, from_version: int, maxsize: int) -> None:
        self.id = next(_ids)
        self.queue: asyncio.Queue[Update] = asyncio.Queue(maxsize=maxsize)
        self.from_version = from_version
        self.last_sent = from_version
        # Set when we had to drop this client's backlog. The socket handler notices
        # and sends a fresh snapshot rather than silently continuing from a hole.
        self.needs_resync = False
        self.dropped = 0


class Hub:
    def __init__(self, replay_buffer: int = 500, subscriber_queue: int = 256) -> None:
        self._subscribers: set[Subscriber] = set()
        self._replay: deque[Update] = deque(maxlen=replay_buffer)
        self._queue_size = subscriber_queue

    # --------------------------------------------------------------- publishing

    def publish(self, update: Update) -> None:
        """Fan one update out. MUST stay synchronous and non-blocking - see (1) above.

        FleetState calls this under its lock, which is what guarantees that queue order
        equals mutation order for every subscriber.
        """
        self._replay.append(update)

        for sub in self._subscribers:
            if sub.needs_resync:
                continue  # already behind; the resync will carry the current state
            try:
                sub.queue.put_nowait(update)
                sub.last_sent = update.version
            except asyncio.QueueFull:
                # This client cannot keep up. Drop its backlog and mark it, rather
                # than growing the queue without bound (memory) or awaiting space
                # (which would make one slow client everyone's problem).
                sub.needs_resync = True
                sub.dropped += 1
                drain(sub.queue)
                # Queue one item into the now-empty queue purely to wake the socket
                # handler, which is parked on queue.get(). Without it the client sits
                # on stale data until the next heartbeat timeout. The item itself is
                # discarded - the resync snapshot supersedes it.
                sub.queue.put_nowait(update)
                log.warning("subscriber %d too slow; flagged for resync", sub.id)

    # -------------------------------------------------------------- subscribing

    def register(self, from_version: int) -> Subscriber:
        """Called by FleetState.snapshot_and_register while the state lock is held."""
        sub = Subscriber(from_version=from_version, maxsize=self._queue_size)
        self._subscribers.add(sub)
        return sub

    def unregister(self, sub: Subscriber) -> None:
        self._subscribers.discard(sub)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    # ------------------------------------------------------------------ replay

    def replay_since(self, version: int) -> Optional[list[Update]]:
        """Updates after `version`, or None if we can no longer prove completeness.

        Returning None matters more than returning the list. If the client's version
        has fallen out of the buffer we cannot know what it missed, and handing back
        whatever we still happen to hold would leave it quietly wrong. None means
        "resync from a snapshot" - the honest answer.
        """
        if not self._replay:
            # Nothing buffered. Only safe if the client is already current, which we
            # cannot verify here, so make the caller re-snapshot.
            return None

        oldest = self._replay[0].version
        newest = self._replay[-1].version

        if version > newest:
            return None  # client claims to be ahead of us: it is talking to a
                         # different backend instance, or replaying a stale token
        if version < oldest - 1:
            return None  # the gap predates our buffer; we cannot fill it honestly

        return [u for u in self._replay if u.version > version]


def drain(queue: asyncio.Queue) -> None:
    """Discard everything queued. Callers must be holding whatever lock makes that
    safe - see ws._resync."""
    while True:
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            return
