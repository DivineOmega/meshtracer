from __future__ import annotations

import importlib
import sys
from copy import deepcopy
from typing import Any

from .common import utc_now
from .controller_defaults import DEFAULT_RUNTIME_CONFIG


def _import_module(module_name: str) -> Any:
    app_mod = sys.modules.get("meshtracer_app.app")
    app_importlib = getattr(app_mod, "importlib", None)
    import_module_fn = getattr(app_importlib, "import_module", None)
    if callable(import_module_fn):
        return import_module_fn(module_name)
    return importlib.import_module(module_name)


class ControllerOperationsMixin:
    def run_traceroute(self, node_num: Any) -> tuple[bool, str]:
        try:
            node_num_int = int(node_num)
        except (TypeError, ValueError):
            return False, "invalid node_num"

        with self._lock:
            interface = self._interface
            worker = self._worker_thread
            wake_event = self._worker_wake
            map_state = self._map_state
            connected = self._connection_state == "connected"

            if (
                not connected
                or interface is None
                or worker is None
                or not worker.is_alive()
                or map_state is None
            ):
                return False, "not connected"

            local_num = getattr(getattr(interface, "localNode", None), "nodeNum", None)
            try:
                local_num_int = int(local_num) if local_num is not None else None
            except (TypeError, ValueError):
                local_num_int = None
            if local_num_int is not None and node_num_int == local_num_int:
                return False, "cannot traceroute the local node"

            if self._current_traceroute_node_num == node_num_int:
                return True, f"traceroute already running for node #{node_num_int}"

            mesh_host = str(map_state.mesh_host or "").strip()
            if not mesh_host:
                return False, "not connected"

        existing = self._store.find_traceroute_queue_entry_by_node(mesh_host, node_num_int)
        if isinstance(existing, dict):
            status = str(existing.get("status") or "").strip().lower()
            if status == "running":
                return True, f"traceroute already running for node #{node_num_int}"
            queue_pos = self._store.queued_position_for_entry(mesh_host, int(existing.get("queue_id") or -1))
            if queue_pos <= 0:
                queue_pos = 1
            return True, f"traceroute already queued for node #{node_num_int} (position {queue_pos})"

        queued = self._store.enqueue_traceroute_target(mesh_host, node_num_int)
        if not isinstance(queued, dict):
            retry = self._store.find_traceroute_queue_entry_by_node(mesh_host, node_num_int)
            if isinstance(retry, dict):
                status = str(retry.get("status") or "").strip().lower()
                if status == "running":
                    return True, f"traceroute already running for node #{node_num_int}"
                queue_pos = self._store.queued_position_for_entry(mesh_host, int(retry.get("queue_id") or -1))
                if queue_pos <= 0:
                    queue_pos = 1
                return True, f"traceroute already queued for node #{node_num_int} (position {queue_pos})"
            return False, "failed to queue traceroute"

        queue_pos = self._store.queued_position_for_entry(mesh_host, int(queued.get("queue_id") or -1))
        if queue_pos <= 0:
            queue_pos = 1
        self._bump_snapshot_revision()

        if wake_event is not None:
            wake_event.set()
        self._emit_typed(
            f"[{utc_now()}] Manual traceroute queued for node #{node_num_int}.",
            log_type="traceroute",
        )
        return True, f"queued traceroute to node #{node_num_int} (position {queue_pos})"


    def request_node_telemetry(self, node_num: Any, telemetry_type: Any) -> tuple[bool, str]:
        try:
            node_num_int = int(node_num)
        except (TypeError, ValueError):
            return False, "invalid node_num"

        telemetry_details = self._telemetry_type(telemetry_type)
        if telemetry_details is None:
            return False, "invalid telemetry_type"
        telemetry_label, send_type = telemetry_details

        with self._lock:
            interface = self._interface
            worker = self._worker_thread
            connected = self._connection_state == "connected"
            if not connected or interface is None or worker is None or not worker.is_alive():
                return False, "not connected"
        interface_connected = getattr(interface, "isConnected", None)
        if interface_connected is not None:
            is_set = getattr(interface_connected, "is_set", None)
            if callable(is_set):
                try:
                    if not bool(is_set()):
                        return False, "meshtastic interface is reconnecting"
                except Exception:
                    pass

        # Use sendData + wantResponse for a non-blocking request path.
        send_data = getattr(interface, "sendData", None)
        if not callable(send_data):
            return False, "telemetry request API unavailable"
        try:
            telemetry_pb2_mod = _import_module("meshtastic.protobuf.telemetry_pb2")
            portnums_pb2_mod = _import_module("meshtastic.protobuf.portnums_pb2")
            payload = telemetry_pb2_mod.Telemetry()
            if send_type == "environment_metrics":
                payload.environment_metrics.CopyFrom(telemetry_pb2_mod.EnvironmentMetrics())
            elif send_type == "power_metrics":
                power_message_ctor = getattr(telemetry_pb2_mod, "PowerMetrics", None)
                if not callable(power_message_ctor) or not hasattr(payload, "power_metrics"):
                    return (
                        False,
                        "power telemetry request unsupported by installed meshtastic version",
                    )
                payload.power_metrics.CopyFrom(power_message_ctor())
            else:
                payload.device_metrics.CopyFrom(telemetry_pb2_mod.DeviceMetrics())

            send_data(
                payload,
                destinationId=node_num_int,
                portNum=portnums_pb2_mod.PortNum.TELEMETRY_APP,
                wantResponse=True,
            )
        except Exception as exc:
            return False, f"telemetry request failed: {exc}"
        target_desc = self._node_log_descriptor(interface, node_num_int)
        self._emit_typed(
            f"[{utc_now()}] Requested {telemetry_label} telemetry from {target_desc}.",
            log_type="telemetry",
        )
        return True, f"requested {telemetry_label} telemetry from node #{node_num_int}"

    def request_node_info(self, node_num: Any) -> tuple[bool, str]:
        try:
            node_num_int = int(node_num)
        except (TypeError, ValueError):
            return False, "invalid node_num"

        with self._lock:
            interface = self._interface
            worker = self._worker_thread
            connected = self._connection_state == "connected"
            if not connected or interface is None or worker is None or not worker.is_alive():
                return False, "not connected"
        interface_connected = getattr(interface, "isConnected", None)
        if interface_connected is not None:
            is_set = getattr(interface_connected, "is_set", None)
            if callable(is_set):
                try:
                    if not bool(is_set()):
                        return False, "meshtastic interface is reconnecting"
                except Exception:
                    pass

        send_data = getattr(interface, "sendData", None)
        if not callable(send_data):
            return False, "node info request API unavailable"
        try:
            mesh_pb2_mod = _import_module("meshtastic.protobuf.mesh_pb2")
            portnums_pb2_mod = _import_module("meshtastic.protobuf.portnums_pb2")
            payload = mesh_pb2_mod.User()
            send_data(
                payload,
                destinationId=node_num_int,
                portNum=portnums_pb2_mod.PortNum.NODEINFO_APP,
                wantResponse=True,
            )
        except Exception as exc:
            return False, f"node info request failed: {exc}"

        target_desc = self._node_log_descriptor(interface, node_num_int)
        self._emit_typed(
            f"[{utc_now()}] Requested node info from {target_desc}.",
            log_type="node_info",
        )
        return True, f"requested node info from node #{node_num_int}"

    def request_node_position(self, node_num: Any) -> tuple[bool, str]:
        try:
            node_num_int = int(node_num)
        except (TypeError, ValueError):
            return False, "invalid node_num"

        with self._lock:
            interface = self._interface
            worker = self._worker_thread
            connected = self._connection_state == "connected"
            if not connected or interface is None or worker is None or not worker.is_alive():
                return False, "not connected"
        interface_connected = getattr(interface, "isConnected", None)
        if interface_connected is not None:
            is_set = getattr(interface_connected, "is_set", None)
            if callable(is_set):
                try:
                    if not bool(is_set()):
                        return False, "meshtastic interface is reconnecting"
                except Exception:
                    pass

        send_data = getattr(interface, "sendData", None)
        if not callable(send_data):
            return False, "position request API unavailable"
        try:
            mesh_pb2_mod = _import_module("meshtastic.protobuf.mesh_pb2")
            portnums_pb2_mod = _import_module("meshtastic.protobuf.portnums_pb2")
            payload = mesh_pb2_mod.Position()
            send_data(
                payload,
                destinationId=node_num_int,
                portNum=portnums_pb2_mod.PortNum.POSITION_APP,
                wantResponse=True,
            )
        except Exception as exc:
            return False, f"position request failed: {exc}"

        target_desc = self._node_log_descriptor(interface, node_num_int)
        self._emit_typed(
            f"[{utc_now()}] Requested position from {target_desc}.",
            log_type="position",
        )
        return True, f"requested position from node #{node_num_int}"

    def get_chat_messages(
        self,
        recipient_kind: Any,
        recipient_id: Any,
        limit: Any = 300,
    ) -> tuple[bool, str, list[dict[str, Any]], int]:
        kind = str(recipient_kind or "").strip().lower()
        if kind not in ("channel", "direct"):
            return False, "invalid recipient_kind", [], 0
        try:
            recipient_id_int = int(recipient_id)
        except (TypeError, ValueError):
            return False, "invalid recipient_id", [], 0
        if kind == "channel" and recipient_id_int < 0:
            return False, "invalid recipient_id", [], 0
        if kind == "direct" and recipient_id_int <= 0:
            return False, "invalid recipient_id", [], 0

        mesh_host = self._active_mesh_host()
        if not mesh_host:
            return False, "no active mesh partition", [], 0

        try:
            limit_int = int(limit)
        except (TypeError, ValueError):
            limit_int = 300
        limit_int = max(1, min(2000, limit_int))

        messages = self._store.list_chat_messages(
            mesh_host,
            recipient_kind=kind,
            recipient_id=recipient_id_int,
            limit=limit_int,
        )
        revision = self._store.latest_chat_revision(mesh_host)
        return True, "ok", messages, revision

    def send_chat_message(
        self,
        recipient_kind: Any,
        recipient_id: Any,
        text: Any,
    ) -> tuple[bool, str]:
        kind = str(recipient_kind or "").strip().lower()
        if kind not in ("channel", "direct"):
            return False, "invalid recipient_kind"
        try:
            recipient_id_int = int(recipient_id)
        except (TypeError, ValueError):
            return False, "invalid recipient_id"

        message_text = str(text or "").strip()
        if not message_text:
            return False, "message cannot be empty"

        with self._lock:
            interface = self._interface
            worker = self._worker_thread
            connected = self._connection_state == "connected"
            map_state = self._map_state
            if not connected or interface is None or worker is None or not worker.is_alive() or map_state is None:
                return False, "not connected"

        local_num = self._interface_local_node_num(interface)
        destination: Any = "^all"
        channel_index: int | None = None
        peer_node_num: int | None = None
        to_node_num: int | None = None

        if kind == "channel":
            if recipient_id_int < 0:
                return False, "invalid recipient_id"
            channel_index = max(0, recipient_id_int)
            destination = "^all"
            to_node_num = 0xFFFFFFFF
        else:
            if recipient_id_int <= 0:
                return False, "invalid recipient_id"
            peer_node_num = recipient_id_int
            if local_num is not None and peer_node_num == local_num:
                return False, "cannot send a direct message to the local node"
            destination = peer_node_num
            to_node_num = peer_node_num

        send_text = getattr(interface, "sendText", None)
        if not callable(send_text):
            return False, "message send API unavailable"

        try:
            send_result = send_text(
                message_text,
                destinationId=destination,
                channelIndex=channel_index if channel_index is not None else 0,
            )
        except Exception as exc:
            return False, f"message send failed: {exc}"

        packet_id: int | None = None
        if isinstance(send_result, dict):
            packet_id = self._packet_int(send_result, "id")
        elif send_result is not None:
            try:
                packet_id = int(send_result)
            except (TypeError, ValueError):
                packet_id = None

        dedupe_key: str | None = None
        if packet_id is not None:
            dedupe_key = self._dedupe_key_for_chat_packet(
                packet_id=packet_id,
                from_node_num=local_num,
                to_node_num=to_node_num,
                message_type=kind,
                channel_index=channel_index,
                peer_node_num=peer_node_num,
                rx_time=None,
                text=message_text,
            )

        mesh_host = str(map_state.mesh_host or "").strip()
        if mesh_host:
            self._store.add_chat_message(
                mesh_host,
                text=message_text,
                message_type=kind,
                direction="outgoing",
                channel_index=channel_index,
                peer_node_num=peer_node_num,
                from_node_num=local_num,
                to_node_num=to_node_num,
                packet_id=packet_id,
                packet={
                    "kind": kind,
                    "destination": destination,
                    "channel": channel_index,
                    "id": packet_id,
                },
                dedupe_key=dedupe_key,
            )

        self._bump_snapshot_revision()

        if kind == "channel":
            self._emit(
                f"[{utc_now()}] Sent channel message on channel #{int(channel_index or 0)}."
            )
            return True, f"sent message to channel #{int(channel_index or 0)}"

        target_desc = self._node_log_descriptor(interface, peer_node_num)
        self._emit(
            f"[{utc_now()}] Sent direct message to {target_desc}."
        )
        return True, f"sent direct message to node #{peer_node_num}"

    def remove_traceroute_queue_entry(self, queue_id: Any) -> tuple[bool, str]:
        try:
            queue_id_int = int(queue_id)
        except (TypeError, ValueError):
            return False, "invalid queue_id"
        if queue_id_int <= 0:
            return False, "invalid queue_id"

        mesh_host = self._active_mesh_host()
        if not mesh_host:
            return False, "no active mesh partition"

        entry = self._store.get_traceroute_queue_entry(mesh_host, queue_id_int)
        if not isinstance(entry, dict):
            return False, f"queue entry #{queue_id_int} not found"

        status = str(entry.get("status") or "").strip().lower()
        if status == "running":
            return False, "cannot remove a running traceroute"

        removed = self._store.remove_traceroute_queue_entry(mesh_host, queue_id_int)
        if not removed:
            return False, f"queue entry #{queue_id_int} not found"

        self._bump_snapshot_revision()
        node_num = int(entry.get("node_num") or 0)
        self._emit_typed(
            f"[{utc_now()}] Removed queued traceroute #{queue_id_int} (node #{node_num}).",
            log_type="traceroute",
        )
        return True, f"removed queued traceroute #{queue_id_int}"

    def reset_database(self) -> tuple[bool, str]:
        try:
            self.disconnect()
        except Exception as exc:
            self._emit_error(f"[{utc_now()}] Warning: disconnect during reset encountered an error: {exc}")

        try:
            self._store.reset_all_data()
        except Exception as exc:
            return False, f"failed to reset database: {exc}"

        with self._lock:
            self._config = deepcopy(DEFAULT_RUNTIME_CONFIG)
            self._map_state = None
            self._connection_state = "disconnected"
            self._connected_host = None
            self._connection_error = None
            self._current_traceroute_node_num = None

        self._bump_snapshot_revision()
        self._emit(f"[{utc_now()}] Database reset: all SQLite data cleared and disconnected.")
        return True, "database reset and disconnected"

    def shutdown(self) -> None:
        try:
            self.disconnect()
        except Exception:
            pass
        try:
            self._discovery.stop()
        except Exception:
            pass

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            map_state = self._map_state
            interface = self._interface
            connection_state = self._connection_state
            connected_host = self._connected_host
            connection_error = self._connection_error
            running_node_num = self._current_traceroute_node_num
            mesh_host = map_state.mesh_host if map_state is not None else None
            snapshot_revision = self._snapshot_revision

        queue_entries: list[dict[str, Any]] = []
        if mesh_host:
            queue_entries = self._store.list_traceroute_queue(str(mesh_host))
        queued_node_nums = [
            int(entry.get("node_num"))
            for entry in queue_entries
            if str(entry.get("status") or "").strip().lower() == "queued"
        ]
        chat_revision = 0
        chat_channels = [0]
        chat_recent_direct_node_nums: list[int] = []
        chat_channel_names_by_index: dict[int, str] = {}
        if mesh_host:
            chat_revision = self._store.latest_chat_revision(str(mesh_host))
            for channel_index in self._store.list_chat_channels(str(mesh_host)):
                if channel_index not in chat_channels:
                    chat_channels.append(channel_index)
            if interface is not None:
                interface_channels, interface_channel_names = self._interface_channel_indexes_and_names(interface)
                for channel_index in interface_channels:
                    if channel_index not in chat_channels:
                        chat_channels.append(channel_index)
                for channel_index, channel_name in interface_channel_names.items():
                    if channel_name:
                        chat_channel_names_by_index[int(channel_index)] = str(channel_name)
            chat_recent_direct_node_nums = self._store.list_recent_direct_nodes(str(mesh_host), limit=30)
        chat_channels = sorted(set(int(value) for value in chat_channels if int(value) >= 0))
        chat_channel_names = {
            str(channel_index): chat_channel_names_by_index[channel_index]
            for channel_index in chat_channels
            if channel_index in chat_channel_names_by_index
        }

        if map_state is not None:
            payload = map_state.snapshot()
        else:
            payload = {
                "generated_at_utc": utc_now(),
                "mesh_host": "-",
                "map_revision": 0,
                "node_count": 0,
                "trace_count": 0,
                "nodes": [],
                "traces": [],
                "edges": [],
                "logs": [],
            }

        # Always attach the latest runtime logs.
        payload["logs"] = self._log_buffer.tail(limit=500)
        try:
            payload["log_revision"] = int(self._log_buffer.latest_seq())
        except Exception:
            payload["log_revision"] = 0
        payload["connected"] = connection_state == "connected"
        payload["connection_state"] = connection_state
        payload["connected_host"] = connected_host
        payload["connection_error"] = connection_error
        payload["discovery"] = self._discovery.snapshot()
        payload["config"] = self.get_public_config()
        payload["config_defaults"] = deepcopy(DEFAULT_RUNTIME_CONFIG)
        payload["server"] = {
            "db_path": str(getattr(self._args, "db_path", "") or ""),
            "map_host": str(getattr(self._args, "map_host", "") or ""),
            "map_port": int(getattr(self._args, "map_port", 0) or 0),
        }
        payload["traceroute_control"] = {
            "running_node_num": running_node_num,
            "queued_node_nums": queued_node_nums,
            "queue_entries": queue_entries,
        }
        payload["chat"] = {
            "revision": int(chat_revision),
            "channels": chat_channels,
            "channel_names": chat_channel_names,
            "recent_direct_node_nums": chat_recent_direct_node_nums,
        }
        payload["snapshot_revision"] = int(snapshot_revision)
        return payload
