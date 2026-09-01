"""Spawns one OS process per robot in robots.json and keeps them alive.

The brief is explicit that eight coroutines in one process is not what it means by a
simulated fleet, and it is right to be: coroutines share a heap, a GIL, a socket
buffer and a fate. Eight processes fail independently, which is the whole point -
`docker compose exec robot-fleet pkill -f 'robot_id r3'` kills exactly one robot and
you can watch the backend notice.

This is also the "one service runs a script that starts the simulation" the brief
asks for: it is the container's entrypoint, so nothing is started by hand.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
HERE = Path(__file__).parent

# Stagger process starts so eight robots do not open eight TCP connections in the
# same millisecond. Real fleets power on over minutes; a thundering herd at t=0 is an
# artifact of the simulator, not of the system under test.
START_STAGGER = float(os.getenv("START_STAGGER", "0.25"))
RESTART_DELAY = float(os.getenv("RESTART_DELAY", "2"))

log = logging.getLogger("supervisor")


class Fleet:
    def __init__(self) -> None:
        self.robots = json.loads((DATA_DIR / "robots.json").read_text())
        self.procs: dict[str, subprocess.Popen] = {}
        self._running = True

    def _spawn(self, robot: dict) -> None:
        robot_id = robot["robot_id"]
        proc = subprocess.Popen(
            [
                sys.executable,
                "-u",  # unbuffered, so `docker compose logs` shows output live
                str(HERE / "robot_publisher.py"),
                "--robot-id",
                robot_id,
                "--robot-type",
                robot.get("robot_type", "unknown"),
            ],
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        self.procs[robot_id] = proc
        log.info("spawned %s (%s) as pid %d", robot_id, robot.get("robot_type"), proc.pid)

    def stop(self, *_) -> None:
        self._running = False

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

        log.info("starting %d robot processes", len(self.robots))
        for robot in self.robots:
            self._spawn(robot)
            time.sleep(START_STAGGER)

        by_id = {r["robot_id"]: r for r in self.robots}
        try:
            while self._running:
                time.sleep(1)
                for robot_id, proc in list(self.procs.items()):
                    if proc.poll() is None:
                        continue
                    # A robot process died. Restart it, the way a watchdog on the
                    # robot's own board would - and note that its new process gets a
                    # fresh session id, which is precisely the case ingest.py's
                    # session check exists to handle.
                    log.warning("%s exited rc=%s; restarting", robot_id, proc.returncode)
                    time.sleep(RESTART_DELAY)
                    if self._running:
                        self._spawn(by_id[robot_id])
        finally:
            log.info("shutting down fleet")
            for proc in self.procs.values():
                proc.terminate()
            deadline = time.monotonic() + 5
            for proc in self.procs.values():
                remaining = max(0.0, deadline - time.monotonic())
                try:
                    proc.wait(timeout=remaining)
                except subprocess.TimeoutExpired:
                    proc.kill()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s [supervisor] %(message)s",
        stream=sys.stdout,
    )
    Fleet().run()


if __name__ == "__main__":
    main()
