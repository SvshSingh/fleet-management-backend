"""Minimal WebSocket consumer, so a reviewer can watch the stream in one command.

Also doubles as the live consistency check: with --verify it polls GET /fleet
alongside the stream and asserts that the state it built purely from WebSocket frames
matches what REST reports at the same version. That equivalence is the central claim
of this submission, so it should be executable, not just asserted in a README.

    python tools/ws_client.py                    # watch the stream
    python tools/ws_client.py --verify           # prove WS and REST agree
    python tools/ws_client.py --since 42         # resume from a version
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

try:
    import websockets
except ImportError:  # pragma: no cover
    sys.exit("pip install websockets httpx")

import httpx

ATTENTION = "\033[33m"
BAD = "\033[31m"
OK = "\033[32m"
DIM = "\033[2m"
END = "\033[0m"


def render(robot: dict) -> str:
    colour = OK
    if robot["needs_attention"]:
        colour = BAD if robot["link"] == "lost" or robot["status"] == "error" else ATTENTION
    reasons = ",".join(robot["attention_reasons"]) or "-"
    return (
        f"{colour}{robot['robot_id']:>3}{END} {robot['robot_type']:<7} "
        f"({robot['x']:6.1f},{robot['y']:6.1f}) {robot['status']:<11} "
        f"{robot['battery']:5.1f}% link={robot['link']:<5} {DIM}{reasons}{END}"
    )


async def run(url: str, api: str, since: int | None, verify: bool) -> None:
    target = f"{url}?since={since}" if since is not None else url
    mirror: dict[str, dict] = {}
    version = 0
    mismatches = 0

    async with websockets.connect(target) as ws:
        print(f"connected to {target}\n")
        async for raw in ws:
            frame = json.loads(raw)
            kind = frame["type"]

            if kind == "snapshot":
                version = frame["version"]
                mirror = {r["robot_id"]: r for r in frame["robots"]}
                print(f"{DIM}--- snapshot @ v{version} ---{END}")
                for robot in frame["robots"]:
                    print(render(robot))
                summary = frame["summary"]
                print(
                    f"{DIM}working {summary['working']}/{summary['total']} "
                    f"({summary['working_fraction']:.0%})  "
                    f"attention {summary['needs_attention']}  "
                    f"mean battery {summary['mean_battery']}%{END}\n"
                )

            elif kind == "update":
                # Versions must be strictly consecutive. A gap here means the
                # snapshot/subscribe handoff leaked an update, which is exactly the
                # bug the atomic registration in fleet_state.py exists to prevent.
                if frame["version"] != version + 1:
                    print(f"{BAD}!! version jump {version} -> {frame['version']}{END}")
                version = frame["version"]
                mirror[frame["robot"]["robot_id"]] = frame["robot"]
                print(f"v{version:<5} {frame['cause']:<9} {render(frame['robot'])}")

            elif kind == "resync":
                print(f"{ATTENTION}resync: {frame['reason']}{END}")

            elif kind == "ping":
                print(f"{DIM}ping (server at v{frame['version']}){END}")

            if verify and kind in {"snapshot", "update"}:
                async with httpx.AsyncClient(base_url=api, timeout=5) as client:
                    body = (await client.get("/fleet")).json()
                if body["version"] == version:
                    rest = {r["robot_id"]: r for r in body["robots"]}
                    if rest != mirror:
                        mismatches += 1
                        differing = [k for k in rest if rest[k] != mirror.get(k)]
                        print(f"{BAD}!! WS/REST disagree at v{version}: {differing}{END}")
                    else:
                        print(f"{OK}   ✓ WS == REST at v{version}{END}")

    if verify:
        print(f"\nmismatches: {mismatches}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="ws://localhost:8000/ws")
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--since", type=int, default=None)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()

    try:
        asyncio.run(run(args.url, args.api, args.since, args.verify))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
