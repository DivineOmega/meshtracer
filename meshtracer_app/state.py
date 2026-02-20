from __future__ import annotations

import threading
from collections import deque
from typing import Any

from .common import utc_now
from .meshtastic_helpers import node_summary_from_node, node_summary_from_num
from .storage import SQLiteStore


RUNTIME_LOG_TYPES = {
    "traceroute",
    "telemetry",
    "messaging",
    "position",
    "node_info",
    "other",
}


def normalize_runtime_log_type(raw_type: Any) -> str:
    text = str(raw_type or "").strip().lower().replace("-", "_").replace(" ", "_")
    if text in RUNTIME_LOG_TYPES:
        return text
    return "other"


class RuntimeLogBuffer:
    def __init__(self, max_entries: int = 2000) -> None:
        self._lock = threading.Lock()
        self._entries: deque[dict[str, Any]] = deque(maxlen=max_entries)
        self._next_seq = 1

    def add(self, message: str, stream: str, *, log_type: str = "other") -> None:
        normalized_type = normalize_runtime_log_type(log_type)
        lines = str(message).splitlines() or [str(message)]
        with self._lock:
            for line in lines:
                if not line:
                    continue
                self._entries.append(
                    {
                        "seq": self._next_seq,
                        "at_utc": utc_now(),
                        "stream": stream,
                        "type": normalized_type,
                        "message": line,
                    }
                )
                self._next_seq += 1

    def tail(self, limit: int = 400) -> list[dict[str, Any]]:
        max_items = max(1, int(limit))
        with self._lock:
            if len(self._entries) <= max_items:
                return [item.copy() for item in self._entries]
            return [item.copy() for item in list(self._entries)[-max_items:]]

    def latest_seq(self) -> int:
        with self._lock:
            return max(0, self._next_seq - 1)


class MapState:
    def __init__(
        self,
        store: SQLiteStore,
        mesh_host: str,
        traceroute_retention_hours: int = 720,
        log_buffer: RuntimeLogBuffer | None = None,
    ) -> None:
        self._store = store
        self._mesh_host = mesh_host
        try:
            retention_hours = int(traceroute_retention_hours)
        except (TypeError, ValueError):
            retention_hours = 720
        self._traceroute_retention_hours = max(1, retention_hours)
        self._log_buffer = log_buffer
        self._revision_lock = threading.Lock()
        self._revision = 1
        self._store.prune_traceroutes_older_than(self._mesh_host, self._traceroute_retention_hours)

    @property
    def mesh_host(self) -> str:
        return self._mesh_host

    def _bump_revision(self) -> int:
        with self._revision_lock:
            self._revision += 1
            return self._revision

    def revision(self) -> int:
        with self._revision_lock:
            return self._revision

    def update_nodes_from_interface(self, interface: Any, *, bump_revision: bool = True) -> bool:
        if not hasattr(interface, "nodesByNum"):
            return False
        summaries: list[dict[str, Any]] = []
        for raw_node in interface.nodesByNum.values():
            if not isinstance(raw_node, dict):
                continue
            num = raw_node.get("num")
            if num is None:
                continue
            try:
                node_num = int(num)
            except (TypeError, ValueError):
                continue

            summary = node_summary_from_node(raw_node)
            summary["num"] = node_num
            summaries.append(summary)
        self._store.upsert_nodes(self._mesh_host, summaries)
        if summaries and bump_revision:
            self._bump_revision()
        return bool(summaries)

    def add_traceroute(self, result: dict[str, Any]) -> None:
        self._store.add_traceroute(self._mesh_host, result)
        self._store.prune_traceroutes_older_than(self._mesh_host, self._traceroute_retention_hours)
        self._bump_revision()

    def update_node_from_num(self, interface: Any, node_num: Any, *, bump_revision: bool = True) -> bool:
        try:
            node_num_int = int(node_num)
        except (TypeError, ValueError):
            return False
        summary = node_summary_from_num(interface, node_num_int)
        self._store.upsert_node(self._mesh_host, summary)
        if bump_revision:
            self._bump_revision()
        return True

    def update_node_from_dict(self, node: Any, *, bump_revision: bool = True) -> bool:
        if not isinstance(node, dict):
            return False
        node_num = node.get("num")
        try:
            node_num_int = int(node_num)
        except (TypeError, ValueError):
            return False
        summary = node_summary_from_node(node)
        summary["num"] = node_num_int
        self._store.upsert_node(self._mesh_host, summary)
        if bump_revision:
            self._bump_revision()
        return True

    @staticmethod
    def _pick_telemetry_metrics(telemetry: Any, camel_key: str, snake_key: str) -> dict[str, Any] | None:
        if not isinstance(telemetry, dict):
            return None
        value = telemetry.get(camel_key)
        if not isinstance(value, dict):
            value = telemetry.get(snake_key)
        if not isinstance(value, dict):
            return None
        return value

    @staticmethod
    def _packet_node_num(packet: Any) -> int | None:
        if not isinstance(packet, dict):
            return None
        try:
            return int(packet.get("from"))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _packet_decoded(packet: Any) -> dict[str, Any] | None:
        if not isinstance(packet, dict):
            return None
        decoded = packet.get("decoded")
        return decoded if isinstance(decoded, dict) else None

    @staticmethod
    def _position_payload_to_dict(value: Any) -> dict[str, Any] | None:
        if isinstance(value, dict):
            return dict(value)
        if value is None:
            return None

        # Meshtastic sometimes surfaces protobuf message objects in node caches.
        try:
            from google.protobuf.json_format import MessageToDict  # type: ignore

            as_dict = MessageToDict(value)
            if isinstance(as_dict, dict):
                return dict(as_dict)
        except Exception:
            pass

        result: dict[str, Any] = {}
        key_pairs = (
            ("latitude", "latitude"),
            ("longitude", "longitude"),
            ("latitudeI", "latitudeI"),
            ("longitudeI", "longitudeI"),
            ("latitude_i", "latitudeI"),
            ("longitude_i", "longitudeI"),
            ("altitude", "altitude"),
            ("time", "time"),
            ("timestamp", "timestamp"),
            ("satsInView", "satsInView"),
            ("sats_in_view", "satsInView"),
            ("precisionBits", "precisionBits"),
            ("precision_bits", "precisionBits"),
            ("fixQuality", "fixQuality"),
            ("fix_quality", "fixQuality"),
            ("fixType", "fixType"),
            ("fix_type", "fixType"),
            ("locationSource", "locationSource"),
            ("location_source", "locationSource"),
            ("altitudeSource", "altitudeSource"),
            ("altitude_source", "altitudeSource"),
            ("groundSpeed", "groundSpeed"),
            ("ground_speed", "groundSpeed"),
            ("groundTrack", "groundTrack"),
            ("ground_track", "groundTrack"),
        )
        for attr_name, output_key in key_pairs:
            try:
                attr_value = getattr(value, attr_name)
            except Exception:
                continue
            if attr_value is None:
                continue
            result[output_key] = attr_value
        return result or None

    @staticmethod
    def _packet_number(packet: Any, *keys: str) -> float | None:
        if not isinstance(packet, dict):
            return None
        for key in keys:
            if key not in packet:
                continue
            value = packet.get(key)
            if value is None:
                continue
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue
            if number <= 0:
                continue
            return number
        return None

    def update_node_info_from_packet(
        self,
        interface: Any,
        packet: Any,
        *,
        bump_revision: bool = True,
    ) -> bool:
        node_num = self._packet_node_num(packet)
        if node_num is None:
            return False

        decoded = self._packet_decoded(packet)
        if decoded is None:
            return False

        user = decoded.get("user")
        if not isinstance(user, dict):
            return False

        nodes_by_num = getattr(interface, "nodesByNum", None)
        source_node: dict[str, Any] = {}
        if isinstance(nodes_by_num, dict):
            candidate = nodes_by_num.get(node_num)
            if isinstance(candidate, dict):
                source_node = dict(candidate)

        source_node["num"] = node_num
        source_node["user"] = dict(user)

        packet_rx_time = self._packet_number(packet, "rxTime", "rx_time", "time")
        if packet_rx_time is not None:
            source_node["lastHeard"] = packet_rx_time

        summary = node_summary_from_node(source_node)
        summary["num"] = node_num
        self._store.upsert_node(self._mesh_host, summary)
        if bump_revision:
            self._bump_revision()
        return True

    def update_position_from_packet(
        self,
        interface: Any,
        packet: Any,
        *,
        bump_revision: bool = True,
    ) -> bool:
        node_num = self._packet_node_num(packet)
        if node_num is None:
            return False

        decoded = self._packet_decoded(packet)
        if decoded is None:
            return False
        packet_position = self._position_payload_to_dict(decoded.get("position"))
        if packet_position is None:
            return False

        nodes_by_num = getattr(interface, "nodesByNum", None)
        source_node: dict[str, Any] = {}
        position_payload = dict(packet_position)
        if isinstance(nodes_by_num, dict):
            candidate = nodes_by_num.get(node_num)
            if isinstance(candidate, dict):
                source_node = dict(candidate)
                candidate_position = self._position_payload_to_dict(candidate.get("position"))
                if candidate_position:
                    merged_position = dict(candidate_position)
                    merged_position.update(position_payload)
                    position_payload = merged_position
        source_node["num"] = node_num
        source_node["position"] = dict(position_payload)

        packet_rx_time = self._packet_number(packet, "rxTime", "rx_time", "time")
        if packet_rx_time is not None:
            source_node["lastHeard"] = packet_rx_time

        summary = node_summary_from_node(source_node)
        summary["num"] = node_num
        if summary.get("lat") is None or summary.get("lon") is None:
            return False
        if not self._store.upsert_node_position(self._mesh_host, node_num, position_payload):
            return False
        self._store.upsert_node(self._mesh_host, summary)
        if bump_revision:
            self._bump_revision()
        return True

    def update_telemetry_from_packet(
        self,
        interface: Any,
        packet: Any,
        *,
        bump_revision: bool = True,
    ) -> bool:
        if not isinstance(packet, dict):
            return False
        try:
            node_num = int(packet.get("from"))
        except (TypeError, ValueError):
            return False

        decoded = packet.get("decoded")
        telemetry = decoded.get("telemetry") if isinstance(decoded, dict) else None
        if not isinstance(telemetry, dict):
            return False

        node_info = None
        nodes_by_num = getattr(interface, "nodesByNum", None)
        if isinstance(nodes_by_num, dict):
            candidate = nodes_by_num.get(node_num)
            if isinstance(candidate, dict):
                node_info = candidate

        changed = False
        packet_device = self._pick_telemetry_metrics(telemetry, "deviceMetrics", "device_metrics")
        if packet_device:
            device_metrics = (
                node_info.get("deviceMetrics") if isinstance(node_info, dict) else None
            )
            telemetry_payload = device_metrics if isinstance(device_metrics, dict) else packet_device
            if telemetry_payload and self._store.upsert_node_telemetry(
                self._mesh_host,
                node_num,
                "device",
                telemetry_payload,
            ):
                changed = True

        packet_environment = self._pick_telemetry_metrics(
            telemetry, "environmentMetrics", "environment_metrics"
        )
        if packet_environment:
            environment_metrics = (
                node_info.get("environmentMetrics") if isinstance(node_info, dict) else None
            )
            telemetry_payload = (
                environment_metrics if isinstance(environment_metrics, dict) else packet_environment
            )
            if telemetry_payload and self._store.upsert_node_telemetry(
                self._mesh_host,
                node_num,
                "environment",
                telemetry_payload,
            ):
                changed = True

        packet_power = self._pick_telemetry_metrics(telemetry, "powerMetrics", "power_metrics")
        if packet_power:
            power_metrics = (
                self._pick_telemetry_metrics(node_info, "powerMetrics", "power_metrics")
                if isinstance(node_info, dict)
                else None
            )
            telemetry_payload = power_metrics if isinstance(power_metrics, dict) else packet_power
            if telemetry_payload and self._store.upsert_node_telemetry(
                self._mesh_host,
                node_num,
                "power",
                telemetry_payload,
            ):
                changed = True

        if changed and bump_revision:
            self._bump_revision()
        return changed

    def snapshot(self) -> dict[str, Any]:
        nodes, traces = self._store.snapshot(mesh_host=self._mesh_host)

        nodes_by_num = {int(node["num"]): node for node in nodes if node.get("num") is not None}
        edges: list[dict[str, Any]] = []

        def append_edges(trace: dict[str, Any], nums: list[int], direction: str) -> None:
            for index in range(len(nums) - 1):
                src = nodes_by_num.get(nums[index])
                dst = nodes_by_num.get(nums[index + 1])
                if not src or not dst:
                    continue
                if src.get("lat") is None or src.get("lon") is None:
                    continue
                if dst.get("lat") is None or dst.get("lon") is None:
                    continue
                edges.append(
                    {
                        "trace_id": trace["trace_id"],
                        "direction": direction,
                        "from_num": src["num"],
                        "to_num": dst["num"],
                        "from_coord": [src["lat"], src["lon"]],
                        "to_coord": [dst["lat"], dst["lon"]],
                    }
                )

        for trace in traces:
            append_edges(trace, trace.get("towards_nums", []), "towards")
            append_edges(trace, trace.get("back_nums", []), "back")

        return {
            "generated_at_utc": utc_now(),
            "mesh_host": self._mesh_host,
            "map_revision": self.revision(),
            "node_count": len(nodes),
            "trace_count": len(traces),
            "nodes": nodes,
            "traces": traces,
            "edges": edges,
            "logs": self._log_buffer.tail(limit=500) if self._log_buffer is not None else [],
        }

    def set_traceroute_retention_hours(self, value: Any) -> None:
        changed = False
        try:
            retention_hours = int(value)
        except (TypeError, ValueError):
            retention_hours = self._traceroute_retention_hours
        if retention_hours > 0 and retention_hours != self._traceroute_retention_hours:
            self._traceroute_retention_hours = retention_hours
            changed = True
        pruned = self._store.prune_traceroutes_older_than(
            self._mesh_host,
            self._traceroute_retention_hours,
        )
        if changed or pruned > 0:
            self._bump_revision()
