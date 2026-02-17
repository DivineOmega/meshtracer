from __future__ import annotations

import threading
from collections import deque
from typing import Any

from .common import utc_now
from .meshtastic_helpers import node_summary_from_node, node_summary_from_num
from .storage import SQLiteStore


class RuntimeLogBuffer:
    def __init__(self, max_entries: int = 2000) -> None:
        self._lock = threading.Lock()
        self._entries: deque[dict[str, Any]] = deque(maxlen=max_entries)
        self._next_seq = 1

    def add(self, message: str, stream: str) -> None:
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
        max_traces: int = 500,
        max_stored_traces: int = 50000,
        log_buffer: RuntimeLogBuffer | None = None,
    ) -> None:
        self._store = store
        self._mesh_host = mesh_host
        self._max_traces = max_traces
        self._max_stored_traces = max_stored_traces
        self._log_buffer = log_buffer
        self._revision_lock = threading.Lock()
        self._revision = 1

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
        max_keep = self._max_stored_traces if self._max_stored_traces > 0 else None
        self._store.add_traceroute(self._mesh_host, result, max_keep=max_keep)
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

        if changed and bump_revision:
            self._bump_revision()
        return changed

    def snapshot(self) -> dict[str, Any]:
        nodes, traces = self._store.snapshot(mesh_host=self._mesh_host, max_traces=self._max_traces)

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

    def set_limits(self, *, max_traces: int | None = None, max_stored_traces: int | None = None) -> None:
        changed = False
        if max_traces is not None:
            try:
                max_traces_int = int(max_traces)
            except (TypeError, ValueError):
                max_traces_int = self._max_traces
            if max_traces_int > 0:
                if max_traces_int != self._max_traces:
                    self._max_traces = max_traces_int
                    changed = True
        if max_stored_traces is not None:
            try:
                max_stored_traces_int = int(max_stored_traces)
            except (TypeError, ValueError):
                max_stored_traces_int = self._max_stored_traces
            if max_stored_traces_int >= 0:
                if max_stored_traces_int != self._max_stored_traces:
                    self._max_stored_traces = max_stored_traces_int
                    changed = True
        if changed:
            self._bump_revision()
