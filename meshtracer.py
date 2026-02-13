#!/usr/bin/env python3
"""Run periodic Meshtastic traceroutes to random recently-heard nodes."""

from __future__ import annotations

import argparse
import importlib
import json
import random
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Connect to a Meshtastic node over TCP and run a traceroute every "
            "interval minutes to a random node heard recently."
        )
    )
    parser.add_argument(
        "host",
        help="IP address or hostname of your WiFi-connected Meshtastic node",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Minutes between traceroute attempts (default: 5)",
    )
    parser.add_argument(
        "--heard-window",
        type=int,
        default=120,
        help="Only trace nodes heard within this many minutes (default: 120)",
    )
    parser.add_argument(
        "--hop-limit",
        type=int,
        default=7,
        help="Hop limit for traceroute packets (default: 7)",
    )
    parser.add_argument(
        "--webhook-url",
        default=None,
        help="Optional URL to POST structured JSON when a traceroute completes.",
    )
    parser.add_argument(
        "--webhook-api-token",
        default=None,
        help="Optional API token sent in Authorization and X-API-Token headers.",
    )
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def age_str(seconds: float) -> str:
    total = int(max(seconds, 0))
    hours, rem = divmod(total, 3600)
    minutes, sec = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m {sec}s"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def node_display(node: dict[str, Any]) -> str:
    user = node.get("user", {}) or {}
    long_name = user.get("longName") or "unknown"
    short_name = user.get("shortName")
    node_id = user.get("id") or node.get("id") or "(no-id)"
    node_num = node.get("num", "?")

    if short_name:
        return f"{long_name} ({short_name}) id={node_id} num={node_num}"
    return f"{long_name} id={node_id} num={node_num}"


def node_record_from_node(node: dict[str, Any]) -> dict[str, Any]:
    user = node.get("user", {}) or {}
    return {
        "num": node.get("num"),
        "id": user.get("id") or node.get("id"),
        "long_name": user.get("longName"),
        "short_name": user.get("shortName"),
    }


def node_record_from_num(interface: Any, node_num: int) -> dict[str, Any]:
    node_info = interface.nodesByNum.get(node_num, {}) if hasattr(interface, "nodesByNum") else {}
    if not isinstance(node_info, dict):
        node_info = {}
    record = node_record_from_node(node_info)
    record["num"] = node_num

    if not record["id"] and hasattr(interface, "_nodeNumToId"):
        try:
            record["id"] = interface._nodeNumToId(node_num, False) or f"{node_num:08x}"
        except Exception:
            record["id"] = f"{node_num:08x}"

    return record


def snr_to_db(raw_snr: int | None) -> float | None:
    if raw_snr is None or raw_snr == -128:
        return None
    return raw_snr / 4.0


def build_route_hops(interface: Any, node_nums: list[int], snr_values: list[int]) -> list[dict[str, Any]]:
    hops: list[dict[str, Any]] = []
    for index, node_num in enumerate(node_nums):
        raw_snr = snr_values[index] if index < len(snr_values) else None
        hops.append(
            {
                "node": node_record_from_num(interface, int(node_num)),
                "snr_db": snr_to_db(raw_snr),
            }
        )
    return hops


def parse_traceroute_response(interface: Any, mesh_pb2_mod: Any, packet: dict[str, Any]) -> dict[str, Any]:
    decoded = packet.get("decoded", {}) or {}
    payload = decoded.get("payload")
    if payload is None:
        raise ValueError("traceroute response missing decoded payload")

    route_discovery = mesh_pb2_mod.RouteDiscovery()
    route_discovery.ParseFromString(payload)

    route = [int(x) for x in route_discovery.route]
    snr_towards = [int(x) for x in route_discovery.snr_towards]
    route_back = [int(x) for x in route_discovery.route_back]
    snr_back = [int(x) for x in route_discovery.snr_back]

    to_num = int(packet["to"])
    from_num = int(packet["from"])

    towards_nodes = [to_num] + route + [from_num]
    towards_hops = build_route_hops(interface, towards_nodes, snr_towards)

    back_hops = None
    if "hopStart" in packet and len(snr_back) == len(route_back) + 1:
        back_nodes = [from_num] + route_back + [to_num]
        back_hops = build_route_hops(interface, back_nodes, snr_back)

    return {
        "captured_at_utc": utc_now(),
        "packet": {
            "from": node_record_from_num(interface, from_num),
            "to": node_record_from_num(interface, to_num),
        },
        "route_towards_destination": towards_hops,
        "route_back_to_origin": back_hops,
        "raw": {
            "route": route,
            "snr_towards_raw": snr_towards,
            "route_back": route_back,
            "snr_back_raw": snr_back,
        },
    }


def post_webhook(url: str, api_token: str | None, payload: dict[str, Any]) -> tuple[bool, str]:
    request_body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "meshtracer/1.0",
    }
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
        headers["X-API-Token"] = api_token

    req = urllib.request.Request(url, data=request_body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            status = response.getcode()
            body = response.read(200).decode("utf-8", errors="replace")
            if 200 <= status < 300:
                detail = f"HTTP {status}"
                if body.strip():
                    detail += f" ({body.strip()})"
                return True, detail
            return False, f"HTTP {status}"
    except urllib.error.HTTPError as exc:
        body = exc.read(200).decode("utf-8", errors="replace")
        detail = f"HTTP {exc.code}"
        if body.strip():
            detail += f" ({body.strip()})"
        return False, detail
    except Exception as exc:
        return False, str(exc)


def pick_recent_node(interface: Any, heard_window_seconds: int) -> tuple[dict[str, Any] | None, float | None, int]:
    now = time.time()
    local_num = getattr(getattr(interface, "localNode", None), "nodeNum", None)
    candidates: list[tuple[dict[str, Any], float]] = []

    for node in interface.nodesByNum.values():
        node_num = node.get("num")
        if local_num is not None and node_num == local_num:
            continue

        last_heard = node.get("lastHeard")
        if last_heard is None:
            continue

        age = now - float(last_heard)
        if age <= heard_window_seconds:
            candidates.append((node, age))

    if not candidates:
        return None, None, 0

    picked, age = random.choice(candidates)
    return picked, age, len(candidates)


def main() -> int:
    args = parse_args()

    if args.interval <= 0:
        print("--interval must be > 0 minutes", file=sys.stderr)
        return 2
    if args.heard_window <= 0:
        print("--heard-window must be > 0 minutes", file=sys.stderr)
        return 2
    if args.hop_limit <= 0:
        print("--hop-limit must be > 0", file=sys.stderr)
        return 2
    if args.webhook_api_token and not args.webhook_url:
        print(
            f"[{utc_now()}] Warning: --webhook-api-token was provided without "
            "--webhook-url; token will be ignored.",
            file=sys.stderr,
        )

    interval_seconds = args.interval * 60
    heard_window_seconds = args.heard_window * 60

    try:
        tcp_interface_mod = importlib.import_module("meshtastic.tcp_interface")
        mesh_pb2_mod = importlib.import_module("meshtastic.protobuf.mesh_pb2")
    except ModuleNotFoundError:
        print(
            "Missing dependency: meshtastic\n"
            f"Current interpreter: {sys.executable}\n"
            "Install in this repo venv with:\n"
            "  .venv/bin/pip install -r requirements.txt\n"
            "Run script with:\n"
            "  .venv/bin/python meshtracer.py <NODE_IP>",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:
        print(f"Failed to import meshtastic libraries: {exc}", file=sys.stderr)
        return 1

    interface = None
    try:
        print(f"[{utc_now()}] Connecting to Meshtastic node at {args.host}...")
        interface = tcp_interface_mod.TCPInterface(hostname=args.host)
        print(f"[{utc_now()}] Connected.")
        traceroute_capture: dict[str, Any] = {"result": None}
        original_traceroute_callback = interface.onResponseTraceRoute

        def wrapped_traceroute_callback(packet: dict[str, Any]) -> None:
            try:
                traceroute_capture["result"] = parse_traceroute_response(
                    interface=interface,
                    mesh_pb2_mod=mesh_pb2_mod,
                    packet=packet,
                )
            except Exception as exc:
                traceroute_capture["result"] = None
                print(
                    f"[{utc_now()}] Warning: failed to parse traceroute response "
                    f"for webhook payload: {exc}",
                    file=sys.stderr,
                )
            original_traceroute_callback(packet)

        interface.onResponseTraceRoute = wrapped_traceroute_callback

        effective_timeout = max(1, (interval_seconds - 1) // args.hop_limit)

        if hasattr(interface, "_timeout") and hasattr(interface._timeout, "expireTimeout"):
            interface._timeout.expireTimeout = effective_timeout
            est_wait = effective_timeout * args.hop_limit
            print(
                f"[{utc_now()}] Traceroute timeout base set to {effective_timeout}s "
                f"(~{est_wait}s max wait at hop-limit {args.hop_limit})."
            )

        while True:
            cycle_start = time.time()

            target, last_heard_age, candidate_count = pick_recent_node(
                interface,
                heard_window_seconds=heard_window_seconds,
            )

            if target is None:
                print(
                    f"[{utc_now()}] No eligible nodes heard in the last "
                    f"{age_str(heard_window_seconds)}."
                )
            else:
                print(
                    f"\n[{utc_now()}] Selected {node_display(target)} "
                    f"(last heard {age_str(last_heard_age or 0)} ago, "
                    f"{candidate_count} eligible nodes)."
                )
                print(f"[{utc_now()}] Starting traceroute...")

                try:
                    traceroute_capture["result"] = None
                    interface.sendTraceRoute(
                        dest=target["num"],
                        hopLimit=args.hop_limit,
                    )
                    print(f"[{utc_now()}] Traceroute complete.")

                    if args.webhook_url:
                        if traceroute_capture["result"] is None:
                            print(
                                f"[{utc_now()}] Webhook skipped: no parsed "
                                "traceroute response payload available.",
                                file=sys.stderr,
                            )
                        else:
                            webhook_payload = {
                                "event": "meshtastic_traceroute_complete",
                                "sent_at_utc": utc_now(),
                                "mesh_host": args.host,
                                "interval_minutes": args.interval,
                                "interval_seconds": interval_seconds,
                                "hop_limit": args.hop_limit,
                                "selected_target": node_record_from_node(target),
                                "selected_target_last_heard_age_seconds": round(
                                    float(last_heard_age or 0), 3
                                ),
                                "eligible_candidate_count": candidate_count,
                                "traceroute": traceroute_capture["result"],
                            }
                            delivered, detail = post_webhook(
                                url=args.webhook_url,
                                api_token=args.webhook_api_token,
                                payload=webhook_payload,
                            )
                            if delivered:
                                print(f"[{utc_now()}] Webhook delivered: {detail}")
                            else:
                                print(
                                    f"[{utc_now()}] Webhook delivery failed: {detail}",
                                    file=sys.stderr,
                                )
                except Exception as exc:  # keep loop alive for unexpected runtime errors
                    print(f"[{utc_now()}] Traceroute failed: {exc}", file=sys.stderr)

            elapsed = time.time() - cycle_start
            sleep_seconds = max(0.0, interval_seconds - elapsed)
            print(f"[{utc_now()}] Waiting {sleep_seconds:.1f}s for next run...")
            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        print(f"\n[{utc_now()}] Stopped by user.")
        return 0
    finally:
        if interface is not None:
            interface.close()


if __name__ == "__main__":
    raise SystemExit(main())
