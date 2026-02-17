from __future__ import annotations

import time
import random
from typing import Any

from .common import utc_now


def node_display(node: dict[str, Any]) -> str:
    user = node.get("user", {}) or {}
    long_name = user.get("longName") or "unknown"
    short_name = user.get("shortName")
    node_id = user.get("id") or node.get("id") or "(no-id)"
    node_num = node.get("num", "?")

    if short_name:
        return f"{long_name} ({short_name}) id={node_id} num={node_num}"
    return f"{long_name} id={node_id} num={node_num}"


def _node_num_to_id(interface: Any, node_num: int) -> str | None:
    fn = getattr(interface, "_nodeNumToId", None)
    if not callable(fn):
        return None
    try:
        value = fn(node_num, False)
    except Exception:
        return None
    if isinstance(value, str) and value:
        return value
    return None


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

    if not record["id"]:
        record["id"] = _node_num_to_id(interface, node_num) or f"{node_num:08x}"

    return record


def extract_node_position(node: dict[str, Any]) -> tuple[float | None, float | None]:
    position = node.get("position", {}) or {}
    lat = position.get("latitude")
    lon = position.get("longitude")

    if lat is None and position.get("latitudeI") is not None:
        lat = float(position.get("latitudeI")) / 1e7
    if lon is None and position.get("longitudeI") is not None:
        lon = float(position.get("longitudeI")) / 1e7

    try:
        lat_f = float(lat) if lat is not None else None
        lon_f = float(lon) if lon is not None else None
    except (TypeError, ValueError):
        return None, None

    if lat_f is None or lon_f is None:
        return None, None
    if not (-90.0 <= lat_f <= 90.0 and -180.0 <= lon_f <= 180.0):
        return None, None
    return lat_f, lon_f


def node_summary_from_node(node: dict[str, Any]) -> dict[str, Any]:
    summary = node_record_from_node(node)
    lat, lon = extract_node_position(node)
    summary["lat"] = lat
    summary["lon"] = lon
    summary["last_heard"] = node.get("lastHeard")
    return summary


def node_summary_from_num(interface: Any, node_num: int) -> dict[str, Any]:
    node_info = interface.nodesByNum.get(node_num, {}) if hasattr(interface, "nodesByNum") else {}
    if not isinstance(node_info, dict):
        node_info = {}
    summary = node_summary_from_node(node_info)
    summary["num"] = node_num
    if not summary.get("id"):
        summary["id"] = _node_num_to_id(interface, node_num) or f"{node_num:08x}"
    return summary


def extract_route_nums(hops: Any) -> list[int]:
    nums: list[int] = []
    if not isinstance(hops, list):
        return nums
    for hop in hops:
        if not isinstance(hop, dict):
            continue
        node = hop.get("node")
        if not isinstance(node, dict):
            continue
        try:
            nums.append(int(node["num"]))
        except (KeyError, TypeError, ValueError):
            continue
    return nums


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


def pick_recent_node(interface: Any, heard_window_seconds: int) -> tuple[dict[str, Any] | None, float | None, int]:
    now = time.time()
    # Allow small clock skew but ignore implausibly future timestamps.
    max_future_skew_seconds = 300.0
    local_num = getattr(getattr(interface, "localNode", None), "nodeNum", None)
    try:
        local_num_int = int(local_num) if local_num is not None else None
    except (TypeError, ValueError):
        local_num_int = None

    candidates: list[tuple[dict[str, Any], float]] = []
    nodes_by_num = getattr(interface, "nodesByNum", {})
    if not isinstance(nodes_by_num, dict):
        return None, None, 0

    for node in nodes_by_num.values():
        if not isinstance(node, dict):
            continue
        node_num = node.get("num")
        try:
            node_num_int = int(node_num)
        except (TypeError, ValueError):
            node_num_int = None

        if local_num_int is not None and node_num_int == local_num_int:
            continue

        last_heard = node.get("lastHeard")
        if last_heard is None:
            continue

        try:
            age = now - float(last_heard)
        except (TypeError, ValueError):
            continue

        if age < 0:
            if abs(age) > max_future_skew_seconds:
                continue
            age = 0.0

        if age <= heard_window_seconds:
            candidates.append((node, age))

    if not candidates:
        return None, None, 0

    picked, age = random.choice(candidates)
    return picked, age, len(candidates)


def resolve_mesh_partition_key(interface: Any, fallback_host: str) -> str:
    local_num = getattr(getattr(interface, "localNode", None), "nodeNum", None)
    if local_num is not None:
        try:
            node_num = int(local_num)
            node_id = _node_num_to_id(interface, node_num)
            if node_id:
                return f"node:{node_num}:{node_id}"
            return f"node:{node_num}"
        except (TypeError, ValueError):
            pass
    return f"host:{fallback_host.strip().lower()}"
