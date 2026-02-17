from __future__ import annotations

import hashlib
import importlib
import os
import sys
import threading
import time
import webbrowser
from copy import deepcopy
from typing import Any, Callable

from .cli import parse_args
from .common import age_str, utc_now
from .discovery import LanDiscoverer
from .map_server import start_map_server
from .meshtastic_helpers import (
    extract_node_position,
    node_display,
    node_record_from_node,
    node_record_from_num,
    parse_traceroute_response,
    pick_recent_node,
    resolve_mesh_partition_key,
)
from .state import MapState, RuntimeLogBuffer
from .storage import SQLiteStore
from .webhook import post_webhook


DEFAULT_RUNTIME_CONFIG: dict[str, Any] = {
    "traceroute_behavior": "automatic",
    "interval": 5,
    "heard_window": 120,
    "fresh_window": 120,
    "mid_window": 480,
    "hop_limit": 7,
    "webhook_url": None,
    "webhook_api_token": None,
    "traceroute_retention_hours": 720,
}


class MeshTracerController:
    def __init__(
        self,
        *,
        args: Any,
        store: SQLiteStore,
        log_buffer: RuntimeLogBuffer,
        emit: Callable[[str], None],
        emit_error: Callable[[str], None],
    ) -> None:
        self._args = args
        self._store = store
        self._log_buffer = log_buffer
        self._emit = emit
        self._emit_error = emit_error

        self._lock = threading.Lock()
        self._snapshot_cv = threading.Condition(self._lock)
        self._snapshot_revision = 1
        self._interface: Any | None = None
        self._mesh_pb2_mod: Any | None = None
        self._map_state: MapState | None = None

        self._pub_bus: Any | None = None
        self._node_event_subscriptions: list[tuple[Any, str]] = []

        self._worker_thread: threading.Thread | None = None
        self._worker_stop: threading.Event | None = None
        self._worker_wake: threading.Event | None = None
        self._current_traceroute_node_num: int | None = None

        self._connected_host: str | None = None
        self._connection_state: str = "disconnected"  # disconnected | connecting | connected | error
        self._connection_error: str | None = None
        self._discovery = LanDiscoverer()
        self._discovery.set_enabled(False)
        self._config: dict[str, Any] = deepcopy(DEFAULT_RUNTIME_CONFIG)
        persisted_config: dict[str, Any] | None = None
        try:
            persisted_config = store.get_runtime_config("global")
        except Exception as exc:
            self._emit_error(f"[{utc_now()}] Warning: failed to load saved config from SQLite: {exc}")

        ok, detail, initial_config = self._merge_runtime_config(
            dict(self._config),
            persisted_config if isinstance(persisted_config, dict) else {},
        )
        if not ok or initial_config is None:
            if persisted_config:
                self._emit_error(
                    f"[{utc_now()}] Warning: ignoring invalid saved config from SQLite: {detail}"
                )
            initial_config = dict(self._config)

        arg_overrides = self._config_overrides_from_args(args)
        ok, detail, merged_config = self._merge_runtime_config(initial_config, arg_overrides)
        if not ok or merged_config is None:
            self._emit_error(f"[{utc_now()}] Warning: ignoring invalid CLI config overrides: {detail}")
            merged_config = initial_config

        self._config = merged_config

    def _bump_snapshot_revision_locked(self) -> int:
        self._snapshot_revision += 1
        try:
            self._snapshot_cv.notify_all()
        except Exception:
            pass
        return self._snapshot_revision

    def _bump_snapshot_revision(self) -> int:
        with self._lock:
            return self._bump_snapshot_revision_locked()

    def wait_for_snapshot_revision(self, since_revision: Any, timeout: float = 25.0) -> int:
        try:
            since_int = int(since_revision)
        except (TypeError, ValueError):
            since_int = 0
        try:
            wait_seconds = max(0.0, float(timeout))
        except (TypeError, ValueError):
            wait_seconds = 0.0

        with self._snapshot_cv:
            if self._snapshot_revision <= since_int:
                self._snapshot_cv.wait(timeout=wait_seconds)
            return self._snapshot_revision

    @staticmethod
    def _config_from_args(args: Any) -> dict[str, Any]:
        def pick_float(name: str) -> float:
            value = getattr(args, name, None)
            if value is None:
                return float(DEFAULT_RUNTIME_CONFIG[name])
            try:
                return float(value)
            except (TypeError, ValueError):
                return float(DEFAULT_RUNTIME_CONFIG[name])

        def pick_int(name: str) -> int:
            value = getattr(args, name, None)
            if value is None:
                return int(DEFAULT_RUNTIME_CONFIG[name])
            try:
                return int(value)
            except (TypeError, ValueError):
                return int(DEFAULT_RUNTIME_CONFIG[name])

        def pick_str(name: str) -> str | None:
            value = getattr(args, name, None)
            if value is None:
                return None
            text = str(value).strip()
            return text or None

        def pick_behavior(name: str) -> str:
            value = getattr(args, name, None)
            if value is None:
                return str(DEFAULT_RUNTIME_CONFIG["traceroute_behavior"])
            text = str(value).strip().lower()
            if text in ("automatic", "manual"):
                return text
            return str(DEFAULT_RUNTIME_CONFIG["traceroute_behavior"])

        config = deepcopy(DEFAULT_RUNTIME_CONFIG)
        config["traceroute_behavior"] = pick_behavior("traceroute_behavior")
        config["interval"] = max((1.0 / 60.0), pick_float("interval"))
        config["heard_window"] = max(1, pick_int("heard_window"))
        config["hop_limit"] = max(1, pick_int("hop_limit"))
        config["traceroute_retention_hours"] = max(1, pick_int("traceroute_retention_hours"))
        config["webhook_url"] = pick_str("webhook_url")
        config["webhook_api_token"] = pick_str("webhook_api_token")
        return config

    @staticmethod
    def _config_overrides_from_args(args: Any) -> dict[str, Any]:
        update: dict[str, Any] = {}

        def pick_float(name: str) -> float | None:
            value = getattr(args, name, None)
            if value is None:
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        def pick_int(name: str) -> int | None:
            value = getattr(args, name, None)
            if value is None:
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        def pick_any_str(name: str) -> str | None:
            value = getattr(args, name, None)
            if value is None:
                return None
            return str(value)

        interval = pick_float("interval")
        if interval is not None:
            update["interval"] = interval

        for key in [
            "heard_window",
            "fresh_window",
            "mid_window",
            "hop_limit",
            "traceroute_retention_hours",
        ]:
            value = pick_int(key)
            if value is None:
                continue
            update[key] = value

        webhook_url = pick_any_str("webhook_url")
        if webhook_url is not None:
            update["webhook_url"] = webhook_url

        webhook_api_token = pick_any_str("webhook_api_token")
        if webhook_api_token is not None:
            update["webhook_api_token"] = webhook_api_token

        traceroute_behavior = pick_any_str("traceroute_behavior")
        if traceroute_behavior is not None:
            update["traceroute_behavior"] = traceroute_behavior

        return update

    @staticmethod
    def _sanitize_config_for_public(config: dict[str, Any]) -> dict[str, Any]:
        sanitized = dict(config)
        token_raw = sanitized.get("webhook_api_token")
        token_set = False
        if token_raw is not None:
            try:
                token_set = bool(str(token_raw).strip())
            except Exception:
                token_set = False
        sanitized["webhook_api_token"] = None
        sanitized["webhook_api_token_set"] = token_set
        return sanitized

    @staticmethod
    def _merge_runtime_config(
        current: dict[str, Any], update: dict[str, Any]
    ) -> tuple[bool, str, dict[str, Any] | None]:
        if not isinstance(update, dict):
            return False, "expected an object", None

        def pick_float(name: str) -> float | None:
            if name not in update:
                return None
            value = update.get(name)
            if value is None:
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                raise ValueError(f"invalid {name}")

        def pick_int(name: str) -> int | None:
            if name not in update:
                return None
            value = update.get(name)
            if value is None:
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                raise ValueError(f"invalid {name}")

        def pick_str(name: str) -> str | None:
            if name not in update:
                return None
            value = update.get(name)
            if value is None:
                return None
            text = str(value).strip()
            return text or None

        def pick_behavior(name: str) -> str | None:
            if name not in update:
                return None
            value = update.get(name)
            if value is None:
                return None
            text = str(value).strip().lower()
            if not text:
                return None
            if text not in ("automatic", "manual"):
                raise ValueError("traceroute_behavior must be 'automatic' or 'manual'")
            return text

        try:
            traceroute_behavior = pick_behavior("traceroute_behavior")
            interval = pick_float("interval")
            heard_window = pick_int("heard_window")
            fresh_window = pick_int("fresh_window")
            mid_window = pick_int("mid_window")
            hop_limit = pick_int("hop_limit")
            traceroute_retention_hours = pick_int("traceroute_retention_hours")
        except ValueError as exc:
            return False, str(exc), None

        if interval is not None and interval <= 0:
            return False, "interval must be > 0", None
        if heard_window is not None and heard_window <= 0:
            return False, "heard_window must be > 0", None
        if fresh_window is not None and fresh_window <= 0:
            return False, "fresh_window must be > 0", None
        if mid_window is not None and mid_window <= 0:
            return False, "mid_window must be > 0", None

        candidate_fresh = (
            fresh_window
            if fresh_window is not None
            else int(current.get("fresh_window") or DEFAULT_RUNTIME_CONFIG["fresh_window"])
        )
        candidate_mid = (
            mid_window
            if mid_window is not None
            else int(current.get("mid_window") or DEFAULT_RUNTIME_CONFIG["mid_window"])
        )
        if candidate_mid < candidate_fresh:
            return False, "mid_window must be >= fresh_window", None
        if hop_limit is not None and hop_limit <= 0:
            return False, "hop_limit must be > 0", None
        if traceroute_retention_hours is not None and traceroute_retention_hours <= 0:
            return False, "traceroute_retention_hours must be > 0", None

        webhook_url = pick_str("webhook_url")
        webhook_api_token = pick_str("webhook_api_token")

        new_config = dict(current)
        if traceroute_behavior is not None:
            new_config["traceroute_behavior"] = traceroute_behavior
        if interval is not None:
            new_config["interval"] = interval
        if heard_window is not None:
            new_config["heard_window"] = heard_window
        if fresh_window is not None:
            new_config["fresh_window"] = fresh_window
        if mid_window is not None:
            new_config["mid_window"] = mid_window
        if hop_limit is not None:
            new_config["hop_limit"] = hop_limit
        if traceroute_retention_hours is not None:
            new_config["traceroute_retention_hours"] = traceroute_retention_hours
        if webhook_url is not None or "webhook_url" in update:
            new_config["webhook_url"] = webhook_url
        if webhook_api_token is not None or "webhook_api_token" in update:
            new_config["webhook_api_token"] = webhook_api_token

        # Ensure keys exist even if older DB entries were partial.
        for key, value in DEFAULT_RUNTIME_CONFIG.items():
            if key not in new_config:
                new_config[key] = value
        for deprecated_key in ("max_map_traces", "max_stored_traces"):
            if deprecated_key in new_config:
                del new_config[deprecated_key]

        return True, "updated", new_config

    def get_config(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._config)

    def get_public_config(self) -> dict[str, Any]:
        return self._sanitize_config_for_public(self.get_config())

    def set_config(self, update: dict[str, Any]) -> tuple[bool, str]:
        with self._lock:
            current = dict(self._config)
            interface = self._interface
            map_state = self._map_state
            wake_event = self._worker_wake

        ok, detail, new_config = self._merge_runtime_config(current, update)
        if not ok or new_config is None:
            return False, detail

        try:
            self._store.set_runtime_config(new_config, "global")
        except Exception as exc:
            return False, f"failed to save config to SQLite: {exc}"

        with self._lock:
            self._config = new_config

        if map_state is not None:
            try:
                map_state.set_traceroute_retention_hours(
                    int(
                        new_config["traceroute_retention_hours"]
                        or DEFAULT_RUNTIME_CONFIG["traceroute_retention_hours"]
                    )
                )
            except Exception:
                pass

        if interface is not None:
            try:
                self._apply_interface_timeout(
                    interface,
                    interval_minutes=float(new_config["interval"]),
                    hop_limit=int(new_config["hop_limit"]),
                )
            except Exception:
                pass
        if wake_event is not None:
            wake_event.set()
        self._bump_snapshot_revision()

        webhook_on = "on" if new_config.get("webhook_url") else "off"
        traceroute_behavior = str(
            new_config.get("traceroute_behavior") or DEFAULT_RUNTIME_CONFIG["traceroute_behavior"]
        )
        interval_minutes = float(new_config.get("interval") or DEFAULT_RUNTIME_CONFIG["interval"])
        traceroute_retention_hours = int(
            new_config.get("traceroute_retention_hours")
            or DEFAULT_RUNTIME_CONFIG["traceroute_retention_hours"]
        )
        self._emit(
            f"[{utc_now()}] Config updated: traceroute_behavior={traceroute_behavior} "
            f"interval={interval_minutes:g}m "
            f"heard_window={new_config['heard_window']}m hop_limit={new_config['hop_limit']} "
            f"fresh_window={new_config['fresh_window']}m mid_window={new_config['mid_window']}m "
            f"traceroute_retention_hours={traceroute_retention_hours} "
            f"webhook={webhook_on}"
        )
        return True, "updated"

    def _apply_interface_timeout(self, interface: Any, *, interval_minutes: float, hop_limit: int) -> None:
        try:
            interval_seconds = max(1, int(float(interval_minutes) * 60.0))
        except (TypeError, ValueError):
            interval_seconds = max(1, int(float(DEFAULT_RUNTIME_CONFIG["interval"]) * 60.0))
        hop_limit_int = max(1, int(hop_limit))
        effective_timeout = max(1, (interval_seconds - 1) // hop_limit_int)
        if hasattr(interface, "_timeout") and hasattr(interface._timeout, "expireTimeout"):
            interface._timeout.expireTimeout = effective_timeout
            est_wait = effective_timeout * hop_limit_int
            self._emit(
                f"[{utc_now()}] Traceroute timeout base set to {effective_timeout}s "
                f"(~{est_wait}s max wait at hop-limit {hop_limit_int})."
            )
        else:
            self._emit_error(
                f"[{utc_now()}] Warning: unable to set Meshtastic internal timeout "
                "(private API changed?)."
            )

    def set_discovery_enabled(self, enabled: bool) -> None:
        self._discovery.set_enabled(enabled)
        self._bump_snapshot_revision()

    def rescan_discovery(self) -> tuple[bool, str]:
        self._discovery.trigger_scan()
        self._bump_snapshot_revision()
        return True, "scan_triggered"

    def _active_mesh_host(self) -> str | None:
        with self._lock:
            map_state = self._map_state
            if map_state is None:
                return None
            host = str(map_state.mesh_host or "").strip()
            return host or None

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
        self._emit(f"[{utc_now()}] Manual traceroute queued for node #{node_num_int}.")
        return True, f"queued traceroute to node #{node_num_int} (position {queue_pos})"

    @staticmethod
    def _telemetry_type(raw_type: Any) -> tuple[str, str] | None:
        text = str(raw_type or "").strip().lower().replace("-", "_").replace(" ", "_")
        mapping = {
            "device": ("device", "device_metrics"),
            "device_metrics": ("device", "device_metrics"),
            "environment": ("environment", "environment_metrics"),
            "environment_metrics": ("environment", "environment_metrics"),
        }
        return mapping.get(text)

    @staticmethod
    def _telemetry_packet_types(packet: Any) -> list[str]:
        if not isinstance(packet, dict):
            return []
        decoded = packet.get("decoded")
        telemetry = decoded.get("telemetry") if isinstance(decoded, dict) else None
        if not isinstance(telemetry, dict):
            return []

        telemetry_types: list[str] = []
        if isinstance(telemetry.get("deviceMetrics"), dict) or isinstance(
            telemetry.get("device_metrics"), dict
        ):
            telemetry_types.append("device")
        if isinstance(telemetry.get("environmentMetrics"), dict) or isinstance(
            telemetry.get("environment_metrics"), dict
        ):
            telemetry_types.append("environment")
        return telemetry_types

    @staticmethod
    def _packet_decoded(packet: Any) -> dict[str, Any] | None:
        if not isinstance(packet, dict):
            return None
        decoded = packet.get("decoded")
        return decoded if isinstance(decoded, dict) else None

    @classmethod
    def _packet_portnum(cls, packet: Any) -> str:
        decoded = cls._packet_decoded(packet)
        if not isinstance(decoded, dict):
            return ""
        value = decoded.get("portnum")
        if value is None:
            value = decoded.get("portNum")
        if value is None:
            return ""
        if isinstance(value, (int, float)):
            try:
                value_int = int(value)
            except (TypeError, ValueError):
                value_int = None
            if value_int is not None:
                if value_int == 1:
                    return "TEXT_MESSAGE_APP"
                if value_int == 7:
                    return "TEXT_MESSAGE_COMPRESSED_APP"
                if value_int == 3:
                    return "POSITION_APP"
                if value_int == 4:
                    return "NODEINFO_APP"
                if value_int == 67:
                    return "TELEMETRY_APP"
                return str(value_int)
        text = str(value).strip().upper()
        if text.isdigit():
            return cls._packet_portnum({"decoded": {"portnum": int(text)}})
        return text

    @classmethod
    def _is_node_info_packet(cls, packet: Any) -> bool:
        decoded = cls._packet_decoded(packet)
        if isinstance(decoded, dict):
            if isinstance(decoded.get("user"), dict):
                return True
            if isinstance(decoded.get("nodeInfo"), dict):
                return True
            if isinstance(decoded.get("node_info"), dict):
                return True
            if isinstance(decoded.get("nodeinfo"), dict):
                return True
        return cls._packet_portnum(packet) == "NODEINFO_APP"

    @classmethod
    def _is_position_packet(cls, packet: Any) -> bool:
        decoded = cls._packet_decoded(packet)
        if isinstance(decoded, dict) and isinstance(decoded.get("position"), dict):
            return True
        return cls._packet_portnum(packet) == "POSITION_APP"

    @classmethod
    def _packet_position(cls, packet: Any) -> tuple[float | None, float | None]:
        decoded = cls._packet_decoded(packet)
        if not isinstance(decoded, dict):
            return None, None
        position = decoded.get("position")
        if not isinstance(position, dict):
            return None, None
        return extract_node_position({"position": position})

    @staticmethod
    def _packet_int(packet: Any, key: str) -> int | None:
        if not isinstance(packet, dict):
            return None
        value = packet.get(key)
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _packet_float(packet: Any, *keys: str) -> float | None:
        if not isinstance(packet, dict):
            return None
        for key in keys:
            if key not in packet:
                continue
            value = packet.get(key)
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _interface_local_node_num(interface: Any) -> int | None:
        local_num = getattr(getattr(interface, "localNode", None), "nodeNum", None)
        try:
            return int(local_num) if local_num is not None else None
        except (TypeError, ValueError):
            return None

    @classmethod
    def _is_text_message_packet(cls, packet: Any) -> bool:
        portnum = cls._packet_portnum(packet)
        return portnum in ("TEXT_MESSAGE_APP", "TEXT_MESSAGE_COMPRESSED_APP")

    @classmethod
    def _packet_text(cls, packet: Any) -> str | None:
        decoded = cls._packet_decoded(packet)
        if not isinstance(decoded, dict):
            return None
        text_value = decoded.get("text")
        if isinstance(text_value, str):
            text = text_value.strip()
            return text or None

        payload = decoded.get("payload")
        if isinstance(payload, (bytes, bytearray)):
            try:
                text = bytes(payload).decode("utf-8").strip()
            except Exception:
                return None
            return text or None
        return None

    @staticmethod
    def _is_broadcast_node_num(node_num: int | None) -> bool:
        if node_num is None:
            return False
        return node_num in (-1, 0xFFFFFFFF)

    @classmethod
    def _is_broadcast_packet_destination(cls, packet: Any) -> bool:
        to_num = cls._packet_int(packet, "to")
        if cls._is_broadcast_node_num(to_num):
            return True
        if not isinstance(packet, dict):
            return False
        to_id = str(packet.get("toId") or "").strip().lower()
        if to_id in ("^all", "all", "broadcast", "!ffffffff"):
            return True
        return False

    @classmethod
    def _dedupe_key_for_chat_packet(
        cls,
        *,
        packet_id: int | None,
        from_node_num: int | None,
        to_node_num: int | None,
        message_type: str,
        channel_index: int | None,
        peer_node_num: int | None,
        rx_time: float | None,
        text: str,
    ) -> str:
        normalized_to_node = to_node_num
        if cls._is_broadcast_node_num(normalized_to_node):
            normalized_to_node = 0xFFFFFFFF
        if packet_id is not None:
            scope = f"c{channel_index}" if message_type == "channel" else f"p{peer_node_num}"
            return f"pkt:{packet_id}:{from_node_num}:{normalized_to_node}:{scope}"
        rx_stamp = f"{rx_time:.3f}" if isinstance(rx_time, float) else "-"
        text_hash = hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()[:16]
        scope = f"c{channel_index}" if message_type == "channel" else f"p{peer_node_num}"
        return f"rt:{rx_stamp}:{from_node_num}:{normalized_to_node}:{scope}:{text_hash}"

    @staticmethod
    def _channel_name_text(value: Any) -> str | None:
        text = str(value or "").strip()
        if not text:
            return None
        return text

    @staticmethod
    def _field_value(obj: Any, key: str) -> Any:
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None)

    @staticmethod
    def _modem_preset_label(value: Any) -> str | None:
        by_number = {
            0: "LongFast",
            1: "LongSlow",
            2: "VeryLongSlow",
            3: "MediumSlow",
            4: "MediumFast",
            5: "ShortSlow",
            6: "ShortFast",
            7: "LongModerate",
            8: "ShortTurbo",
            9: "LongTurbo",
        }
        try:
            preset_num = int(value)
        except (TypeError, ValueError):
            preset_num = None
        if preset_num is not None and preset_num in by_number:
            return by_number[preset_num]

        text = str(value or "").strip().upper()
        if not text:
            return None
        if "." in text:
            text = text.split(".")[-1]
        if "_" in text:
            parts = [part for part in text.split("_") if part]
            if parts:
                return "".join(part[:1].upper() + part[1:].lower() for part in parts)
        return None

    @classmethod
    def _interface_primary_channel_label(cls, interface: Any) -> str | None:
        local_node = getattr(interface, "localNode", None)
        local_config = cls._field_value(local_node, "localConfig")
        lora = cls._field_value(local_config, "lora")
        preset_val = cls._field_value(lora, "modem_preset")
        if preset_val is None:
            preset_val = cls._field_value(lora, "modemPreset")
        return cls._modem_preset_label(preset_val)

    @staticmethod
    def _channel_role_text(value: Any) -> str:
        try:
            role_num = int(value)
        except (TypeError, ValueError):
            role_num = None
        if role_num is not None:
            if role_num == 0:
                return "DISABLED"
            if role_num == 1:
                return "PRIMARY"
            if role_num == 2:
                return "SECONDARY"
            if role_num == 3:
                return "ADMIN"
        text = str(value or "").strip().upper()
        if not text:
            return ""
        if "." in text:
            text = text.split(".")[-1]
        return text

    @staticmethod
    def _channel_role_label(role_text: str, channel_index: int) -> str | None:
        role_upper = str(role_text or "").strip().upper()
        if role_upper == "PRIMARY":
            return "Primary"
        if role_upper == "SECONDARY":
            return f"Secondary {channel_index}" if channel_index > 0 else "Secondary"
        if role_upper == "ADMIN":
            return "Admin"
        return None

    @classmethod
    def _interface_channel_indexes_and_names(cls, interface: Any) -> tuple[list[int], dict[int, str]]:
        primary_channel_label = cls._interface_primary_channel_label(interface) or "Primary"
        channels_obj = getattr(getattr(interface, "localNode", None), "channels", None)
        if channels_obj is None:
            return [0], {0: primary_channel_label}
        try:
            channels = list(channels_obj)
        except TypeError:
            return [0], {0: primary_channel_label}

        values: list[int] = []
        names: dict[int, str] = {}
        for channel in channels:
            role_val: Any = None
            index_val: Any = None
            name_val: Any = None
            settings_val: Any = None
            if isinstance(channel, dict):
                role_val = channel.get("role")
                index_val = channel.get("index")
                name_val = channel.get("name")
                settings_val = channel.get("settings")
            else:
                role_val = getattr(channel, "role", None)
                index_val = getattr(channel, "index", None)
                name_val = getattr(channel, "name", None)
                settings_val = getattr(channel, "settings", None)

            role_text = cls._channel_role_text(role_val)
            if role_val == 0 or role_text == "DISABLED":
                continue
            try:
                idx = int(index_val)
            except (TypeError, ValueError):
                continue
            if idx < 0:
                continue
            values.append(idx)
            name_text = cls._channel_name_text(name_val)
            if name_text is None:
                if isinstance(settings_val, dict):
                    name_text = cls._channel_name_text(settings_val.get("name"))
                elif settings_val is not None:
                    name_text = cls._channel_name_text(getattr(settings_val, "name", None))
            if name_text is None:
                if idx == 0:
                    name_text = primary_channel_label
                else:
                    name_text = cls._channel_role_label(role_text, idx)
            if name_text is not None and idx not in names:
                names[idx] = name_text

        if 0 not in values:
            values.insert(0, 0)
        if 0 in values and 0 not in names:
            names[0] = primary_channel_label
        return sorted(set(values)), names

    @classmethod
    def _interface_channel_indexes(cls, interface: Any) -> list[int]:
        values, _names = cls._interface_channel_indexes_and_names(interface)
        return values

    @staticmethod
    def _node_log_descriptor_from_record(node_num: int, record: Any) -> str:
        long_name = ""
        short_name = ""
        if isinstance(record, dict):
            long_name = str(record.get("long_name") or "").strip()
            short_name = str(record.get("short_name") or "").strip()
        if not long_name:
            long_name = "-"
        if not short_name:
            short_name = "-"
        long_name = long_name.replace('"', "'")
        short_name = short_name.replace('"', "'")
        return f'node #{node_num} (long="{long_name}", short="{short_name}")'

    @classmethod
    def _node_log_descriptor(cls, interface: Any, node_num: Any, packet: Any = None) -> str:
        try:
            node_num_int = int(node_num)
        except (TypeError, ValueError):
            return "node #?"

        packet_record: dict[str, Any] | None = None
        decoded = cls._packet_decoded(packet)
        if isinstance(decoded, dict):
            user = decoded.get("user")
            if isinstance(user, dict):
                packet_record = node_record_from_node({"num": node_num_int, "user": user})
                packet_record["num"] = node_num_int

        if packet_record is not None:
            return cls._node_log_descriptor_from_record(node_num_int, packet_record)

        try:
            record = node_record_from_num(interface, node_num_int)
        except Exception:
            record = {"num": node_num_int}
        return cls._node_log_descriptor_from_record(node_num_int, record)

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
            telemetry_pb2_mod = importlib.import_module("meshtastic.protobuf.telemetry_pb2")
            portnums_pb2_mod = importlib.import_module("meshtastic.protobuf.portnums_pb2")
            payload = telemetry_pb2_mod.Telemetry()
            if send_type == "environment_metrics":
                payload.environment_metrics.CopyFrom(telemetry_pb2_mod.EnvironmentMetrics())
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
        self._emit(
            f"[{utc_now()}] Requested {telemetry_label} telemetry from {target_desc}."
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
            mesh_pb2_mod = importlib.import_module("meshtastic.protobuf.mesh_pb2")
            portnums_pb2_mod = importlib.import_module("meshtastic.protobuf.portnums_pb2")
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
        self._emit(f"[{utc_now()}] Requested node info from {target_desc}.")
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
            mesh_pb2_mod = importlib.import_module("meshtastic.protobuf.mesh_pb2")
            portnums_pb2_mod = importlib.import_module("meshtastic.protobuf.portnums_pb2")
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
        self._emit(f"[{utc_now()}] Requested position from {target_desc}.")
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
        self._emit(f"[{utc_now()}] Removed queued traceroute #{queue_id_int} (node #{node_num}).")
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

    def connect(self, host: str) -> tuple[bool, str]:
        host = str(host or "").strip()
        if not host:
            return False, "missing host"

        # Always tear down any current connection so connect() is idempotent.
        self.disconnect()
        if getattr(self._args, "web_ui", False):
            self._discovery.set_enabled(False)

        with self._lock:
            self._connection_state = "connecting"
            self._connected_host = host
            self._connection_error = None
            self._bump_snapshot_revision_locked()

        self._emit(f"[{utc_now()}] Connecting to Meshtastic node at {host}...")

        try:
            tcp_interface_mod = importlib.import_module("meshtastic.tcp_interface")
            mesh_pb2_mod = importlib.import_module("meshtastic.protobuf.mesh_pb2")
        except ModuleNotFoundError:
            detail = (
                "Missing dependency: meshtastic. Install in this repo venv with:\n"
                "  .venv/bin/pip install -r requirements.txt\n"
                "Then run:\n"
                "  .venv/bin/python meshtracer.py\n"
                "and connect from the web UI."
            )
            self._emit_error(detail)
            with self._lock:
                self._connection_state = "error"
                self._connection_error = "meshtastic is not installed"
                self._bump_snapshot_revision_locked()
            if getattr(self._args, "web_ui", False):
                self._discovery.set_enabled(True)
            return False, "meshtastic is not installed"
        except Exception as exc:
            self._emit_error(f"Failed to import meshtastic libraries: {exc}")
            with self._lock:
                self._connection_state = "error"
                self._connection_error = str(exc)
                self._bump_snapshot_revision_locked()
            if getattr(self._args, "web_ui", False):
                self._discovery.set_enabled(True)
            return False, str(exc)

        try:
            interface = tcp_interface_mod.TCPInterface(hostname=host)
        except Exception as exc:
            self._emit_error(f"[{utc_now()}] Connection failed: {exc}")
            with self._lock:
                self._connection_state = "error"
                self._connection_error = str(exc)
                self._bump_snapshot_revision_locked()
            if getattr(self._args, "web_ui", False):
                self._discovery.set_enabled(True)
            return False, str(exc)

        self._emit(f"[{utc_now()}] Connected.")
        partition_key = resolve_mesh_partition_key(interface=interface, fallback_host=host)
        config = self.get_config()
        map_state = MapState(
            store=self._store,
            mesh_host=partition_key,
            traceroute_retention_hours=int(
                config.get("traceroute_retention_hours")
                or DEFAULT_RUNTIME_CONFIG["traceroute_retention_hours"]
            ),
            log_buffer=self._log_buffer,
        )

        self._emit(
            f"[{utc_now()}] SQLite history DB: {os.path.abspath(self._args.db_path)} "
            f"(partition: {partition_key})"
        )
        recovered = self._store.requeue_running_traceroutes(partition_key)
        if recovered > 0:
            self._emit(
                f"[{utc_now()}] Re-queued {recovered} in-progress traceroute(s) from a previous session."
            )

        try:
            map_state.update_nodes_from_interface(interface)
        except Exception as exc:
            self._emit_error(f"[{utc_now()}] Warning: failed to read nodes from interface: {exc}")

        pub_bus, subscriptions = self._setup_pubsub(interface, map_state)

        traceroute_capture: dict[str, Any] = {"result": None}
        original_traceroute_callback = getattr(interface, "onResponseTraceRoute", None)

        def wrapped_traceroute_callback(packet: dict[str, Any]) -> None:
            try:
                traceroute_capture["result"] = parse_traceroute_response(
                    interface=interface,
                    mesh_pb2_mod=mesh_pb2_mod,
                    packet=packet,
                )
                if traceroute_capture["result"] is not None:
                    map_state.add_traceroute(traceroute_capture["result"])
                    self._bump_snapshot_revision()
            except Exception as exc:
                traceroute_capture["result"] = None
                self._emit_error(
                    f"[{utc_now()}] Warning: failed to parse traceroute response "
                    f"for webhook payload: {exc}"
                )
            if callable(original_traceroute_callback):
                try:
                    original_traceroute_callback(packet)
                except Exception:
                    pass

        interface.onResponseTraceRoute = wrapped_traceroute_callback

        self._apply_interface_timeout(
            interface,
            interval_minutes=float(config.get("interval") or DEFAULT_RUNTIME_CONFIG["interval"]),
            hop_limit=int(config.get("hop_limit") or DEFAULT_RUNTIME_CONFIG["hop_limit"]),
        )

        stop_event = threading.Event()
        wake_event = threading.Event()
        worker = threading.Thread(
            target=self._traceroute_worker,
            args=(interface, map_state, traceroute_capture, stop_event, wake_event, host),
            daemon=True,
            name="meshtracer-worker",
        )
        worker.start()

        with self._lock:
            self._interface = interface
            self._mesh_pb2_mod = mesh_pb2_mod
            self._map_state = map_state
            self._pub_bus = pub_bus
            self._node_event_subscriptions = subscriptions
            self._worker_thread = worker
            self._worker_stop = stop_event
            self._worker_wake = wake_event
            self._current_traceroute_node_num = None
            self._connection_state = "connected"
            self._connection_error = None
            self._bump_snapshot_revision_locked()

        return True, "connected"

    def disconnect(self) -> tuple[bool, str]:
        with self._lock:
            stop_event = self._worker_stop
            wake_event = self._worker_wake
            worker = self._worker_thread
            pub_bus = self._pub_bus
            subscriptions = list(self._node_event_subscriptions)
            interface = self._interface

            self._worker_stop = None
            self._worker_wake = None
            self._worker_thread = None
            self._pub_bus = None
            self._node_event_subscriptions = []
            self._interface = None
            self._mesh_pb2_mod = None
            self._current_traceroute_node_num = None

            # Keep _map_state around so history remains visible when disconnected.
            self._connection_state = "disconnected"
            self._connection_error = None
            self._connected_host = None
            self._bump_snapshot_revision_locked()

        if getattr(self._args, "web_ui", False):
            self._discovery.set_enabled(True)

        if stop_event is not None:
            stop_event.set()
        if wake_event is not None:
            wake_event.set()

        if pub_bus is not None:
            for listener, topic in subscriptions:
                try:
                    pub_bus.unsubscribe(listener, topic)
                except Exception:
                    pass

        if interface is not None:
            try:
                interface.close()
            except Exception:
                pass

        if worker is not None and worker.is_alive():
            worker.join(timeout=2.0)

        return True, "disconnected"

    def _capture_chat_from_packet(
        self,
        interface: Any,
        map_state: MapState,
        packet: Any,
    ) -> dict[str, Any] | None:
        if not isinstance(packet, dict):
            return None
        if not self._is_text_message_packet(packet):
            return None

        text = self._packet_text(packet)
        if not text:
            return None

        mesh_host = str(map_state.mesh_host or "").strip()
        if not mesh_host:
            return None

        from_node_num = self._packet_int(packet, "from")
        to_node_num = self._packet_int(packet, "to")
        local_node_num = self._interface_local_node_num(interface)
        packet_id = self._packet_int(packet, "id")
        rx_time = self._packet_float(packet, "rxTime", "rx_time", "time")

        is_broadcast = self._is_broadcast_packet_destination(packet)
        message_type = "channel" if is_broadcast else "direct"
        channel_index = self._packet_int(packet, "channel")
        if message_type == "channel":
            channel_index = 0 if channel_index is None else max(0, int(channel_index))
            peer_node_num = None
        else:
            channel_index = None
            if local_node_num is not None and from_node_num == local_node_num and to_node_num is not None:
                peer_node_num = to_node_num
            elif local_node_num is not None and to_node_num == local_node_num and from_node_num is not None:
                peer_node_num = from_node_num
            elif from_node_num is not None:
                peer_node_num = from_node_num
            else:
                peer_node_num = to_node_num
            if peer_node_num is None:
                return None

        direction = "unknown"
        if local_node_num is not None:
            if from_node_num == local_node_num:
                direction = "outgoing"
            else:
                direction = "incoming"
        elif from_node_num is not None:
            direction = "incoming"

        dedupe_key = self._dedupe_key_for_chat_packet(
            packet_id=packet_id,
            from_node_num=from_node_num,
            to_node_num=to_node_num,
            message_type=message_type,
            channel_index=channel_index,
            peer_node_num=peer_node_num,
            rx_time=rx_time,
            text=text,
        )
        chat_id = self._store.add_chat_message(
            mesh_host,
            text=text,
            message_type=message_type,
            direction=direction,
            channel_index=channel_index,
            peer_node_num=peer_node_num,
            from_node_num=from_node_num,
            to_node_num=to_node_num,
            packet_id=packet_id,
            rx_time=rx_time,
            packet=packet,
            dedupe_key=dedupe_key,
        )
        if chat_id is None:
            return None

        return {
            "chat_id": int(chat_id),
            "message_type": message_type,
            "direction": direction,
            "channel_index": channel_index,
            "peer_node_num": peer_node_num,
            "from_node_num": from_node_num,
            "to_node_num": to_node_num,
            "text": text,
        }

    def _setup_pubsub(self, interface: Any, map_state: MapState) -> tuple[Any | None, list[tuple[Any, str]]]:
        pub_bus: Any | None = None
        subscriptions: list[tuple[Any, str]] = []
        try:
            pubsub_mod = importlib.import_module("pubsub")
            pub_bus = getattr(pubsub_mod, "pub", None)
            if pub_bus is None:
                self._emit_error(
                    f"[{utc_now()}] Warning: pubsub.pub not available; realtime node updates disabled."
                )
                return None, []
        except Exception as exc:
            self._emit_error(
                f"[{utc_now()}] Warning: failed to load pubsub for realtime node updates: {exc}"
            )
            return None, []

        connected_interface = interface

        def on_receive_update(packet: Any = None, interface: Any = None, **_kwargs: Any) -> None:
            if interface is not None and interface is not connected_interface:
                return
            if isinstance(packet, dict):
                telemetry_types = self._telemetry_packet_types(packet)
                node_info_packet = self._is_node_info_packet(packet)
                position_packet = self._is_position_packet(packet)
                chat_message = self._capture_chat_from_packet(connected_interface, map_state, packet)
                map_state.update_node_from_num(connected_interface, packet.get("from"))
                telemetry_updated = map_state.update_telemetry_from_packet(connected_interface, packet)
                node_info_updated = map_state.update_node_info_from_packet(connected_interface, packet)
                position_updated = map_state.update_position_from_packet(connected_interface, packet)
                node_desc = self._node_log_descriptor(
                    connected_interface,
                    packet.get("from"),
                    packet=packet,
                )
                if telemetry_types:
                    suffix = "" if telemetry_updated else " (no data changes)"
                    self._emit(
                        f"[{utc_now()}] Received {', '.join(telemetry_types)} telemetry "
                        f"from {node_desc}{suffix}."
                    )
                if node_info_packet:
                    suffix = "" if node_info_updated else " (no data changes)"
                    self._emit(
                        f"[{utc_now()}] Received node info from {node_desc}{suffix}."
                    )
                if position_packet:
                    lat, lon = self._packet_position(packet)
                    position_text = ""
                    if lat is not None and lon is not None:
                        position_text = f" ({lat:.5f}, {lon:.5f})"
                    suffix = "" if position_updated else " (no data changes)"
                    self._emit(
                        f"[{utc_now()}] Received position from {node_desc}"
                        f"{position_text}{suffix}."
                    )
                if isinstance(chat_message, dict):
                    text = str(chat_message.get("text") or "")
                    text_preview = text if len(text) <= 90 else f"{text[:87]}..."
                    if str(chat_message.get("message_type") or "") == "channel":
                        channel_index = int(chat_message.get("channel_index") or 0)
                        self._emit(
                            f"[{utc_now()}] Received channel message on channel #{channel_index} "
                            f"from {node_desc}: \"{text_preview}\""
                        )
                    else:
                        self._emit(
                            f"[{utc_now()}] Received direct message from {node_desc}: "
                            f"\"{text_preview}\""
                        )
            else:
                map_state.update_nodes_from_interface(connected_interface)
            self._bump_snapshot_revision()

        def on_node_updated(node: Any = None, interface: Any = None, **_kwargs: Any) -> None:
            if interface is not None and interface is not connected_interface:
                return
            if isinstance(node, dict):
                node_num = node.get("num")
                if node_num is not None:
                    map_state.update_node_from_num(connected_interface, node_num)
                else:
                    map_state.update_node_from_dict(node)
            else:
                map_state.update_nodes_from_interface(connected_interface)
            self._bump_snapshot_revision()

        try:
            pub_bus.subscribe(on_receive_update, "meshtastic.receive")
            subscriptions.append((on_receive_update, "meshtastic.receive"))
            pub_bus.subscribe(on_node_updated, "meshtastic.node.updated")
            subscriptions.append((on_node_updated, "meshtastic.node.updated"))
            self._emit(
                f"[{utc_now()}] Realtime node updates enabled "
                "(meshtastic.receive, meshtastic.node.updated)."
            )
        except Exception as exc:
            self._emit_error(f"[{utc_now()}] Warning: failed to subscribe to pubsub events: {exc}")
            return None, []

        return pub_bus, subscriptions

    @staticmethod
    def _target_from_num(interface: Any, node_num: int) -> dict[str, Any]:
        nodes_by_num = getattr(interface, "nodesByNum", {})
        if isinstance(nodes_by_num, dict):
            node = nodes_by_num.get(node_num)
            if isinstance(node, dict):
                target = dict(node)
                target["num"] = node_num
                return target
        return {"num": node_num}

    @staticmethod
    def _node_last_heard_age_seconds(node: dict[str, Any]) -> float | None:
        try:
            last_heard = node.get("lastHeard")
        except Exception:
            return None
        if last_heard is None:
            return None
        try:
            age = time.time() - float(last_heard)
        except (TypeError, ValueError):
            return None
        return max(0.0, age)

    def _traceroute_worker(
        self,
        interface: Any,
        map_state: MapState,
        traceroute_capture: dict[str, Any],
        stop_event: threading.Event,
        wake_event: threading.Event,
        connected_host: str,
    ) -> None:
        while not stop_event.is_set():
            config = self.get_config()
            traceroute_behavior = str(
                config.get("traceroute_behavior") or DEFAULT_RUNTIME_CONFIG["traceroute_behavior"]
            ).strip().lower()
            if traceroute_behavior not in ("automatic", "manual"):
                traceroute_behavior = str(DEFAULT_RUNTIME_CONFIG["traceroute_behavior"])
            manual_only_mode = traceroute_behavior == "manual"
            try:
                interval_minutes = float(config.get("interval") or DEFAULT_RUNTIME_CONFIG["interval"])
            except (TypeError, ValueError):
                interval_minutes = float(DEFAULT_RUNTIME_CONFIG["interval"])
            interval_seconds = max(1, int(interval_minutes * 60.0))
            heard_window_seconds = (
                int(config.get("heard_window") or DEFAULT_RUNTIME_CONFIG["heard_window"]) * 60
            )
            hop_limit = int(config.get("hop_limit") or DEFAULT_RUNTIME_CONFIG["hop_limit"])
            webhook_url = config.get("webhook_url")
            webhook_api_token = config.get("webhook_api_token")

            cycle_start = time.time()

            try:
                map_state.update_nodes_from_interface(interface)
                self._bump_snapshot_revision()
            except Exception as exc:
                self._emit_error(f"[{utc_now()}] Warning: failed to refresh nodes: {exc}")

            queue_mesh_host = str(map_state.mesh_host or "").strip()
            manual_entry = (
                self._store.pop_next_queued_traceroute(queue_mesh_host) if queue_mesh_host else None
            )
            manual_node_num = (
                int(manual_entry.get("node_num"))
                if isinstance(manual_entry, dict) and manual_entry.get("node_num") is not None
                else None
            )
            manual_queue_id = (
                int(manual_entry.get("queue_id"))
                if isinstance(manual_entry, dict) and manual_entry.get("queue_id") is not None
                else None
            )
            manual_triggered = manual_node_num is not None
            if manual_triggered:
                self._bump_snapshot_revision()
                target = self._target_from_num(interface, int(manual_node_num))
                last_heard_age = self._node_last_heard_age_seconds(target)
                candidate_count = 1
            elif manual_only_mode:
                if stop_event.is_set():
                    break
                if wake_event.wait():
                    wake_event.clear()
                continue
            else:
                target, last_heard_age, candidate_count = pick_recent_node(
                    interface,
                    heard_window_seconds=heard_window_seconds,
                )

            if target is None:
                self._emit(
                    f"[{utc_now()}] No eligible nodes heard in the last "
                    f"{age_str(heard_window_seconds)}."
                )
            else:
                if manual_triggered:
                    self._emit(
                        f"\n[{utc_now()}] Manually selected {node_display(target)} "
                        f"(requested from UI)."
                    )
                else:
                    self._emit(
                        f"\n[{utc_now()}] Selected {node_display(target)} "
                        f"(last heard {age_str(last_heard_age or 0)} ago, "
                        f"{candidate_count} eligible nodes)."
                    )
                self._emit(f"[{utc_now()}] Starting traceroute...")

                target_num: int | None = None
                try:
                    target_num = int(target.get("num"))
                    with self._lock:
                        self._current_traceroute_node_num = target_num
                        self._bump_snapshot_revision_locked()
                    traceroute_capture["result"] = None
                    interface.sendTraceRoute(
                        dest=target_num,
                        hopLimit=hop_limit,
                    )
                    self._emit(
                        f"[{utc_now()}] Traceroute complete for "
                        f"{self._node_log_descriptor(interface, target_num)}."
                    )

                    if webhook_url:
                        if traceroute_capture["result"] is None:
                            self._emit_error(
                                f"[{utc_now()}] Webhook skipped: no parsed "
                                "traceroute response payload available."
                            )
                        else:
                            webhook_payload = {
                                "event": "meshtastic_traceroute_complete",
                                "sent_at_utc": utc_now(),
                                "mesh_host": connected_host,
                                "interval_minutes": interval_minutes,
                                "interval_seconds": interval_seconds,
                                "hop_limit": hop_limit,
                                "selected_target": node_record_from_node(target),
                                "selected_target_last_heard_age_seconds": round(
                                    float(last_heard_age or 0), 3
                                ),
                                "eligible_candidate_count": candidate_count,
                                "trigger": "manual" if manual_triggered else "scheduled",
                                "traceroute": traceroute_capture["result"],
                            }
                            delivered, detail = post_webhook(
                                url=str(webhook_url),
                                api_token=str(webhook_api_token) if webhook_api_token else None,
                                payload=webhook_payload,
                            )
                            if delivered:
                                self._emit(f"[{utc_now()}] Webhook delivered: {detail}")
                            else:
                                self._emit_error(f"[{utc_now()}] Webhook delivery failed: {detail}")
                except Exception as exc:  # keep loop alive for unexpected runtime errors
                    if stop_event.is_set():
                        break
                    self._emit_error(f"[{utc_now()}] Traceroute failed: {exc}")
                finally:
                    if manual_triggered and manual_queue_id is not None and queue_mesh_host:
                        self._store.remove_traceroute_queue_entry(queue_mesh_host, manual_queue_id)
                    with self._lock:
                        if target_num is not None and self._current_traceroute_node_num == target_num:
                            self._current_traceroute_node_num = None
                            self._bump_snapshot_revision_locked()
                    if manual_triggered and manual_queue_id is not None:
                        self._bump_snapshot_revision()

            if manual_only_mode:
                continue

            elapsed = time.time() - cycle_start
            sleep_seconds = max(0.0, interval_seconds - elapsed)
            self._emit(f"[{utc_now()}] Waiting {sleep_seconds:.1f}s for next run...")
            if stop_event.is_set():
                break
            if wake_event.wait(timeout=sleep_seconds):
                wake_event.clear()


def _browser_open_url(map_host: str, map_port: int) -> str:
    host = str(map_host or "").strip()
    if host in ("0.0.0.0", "::", "[::]"):
        host = "127.0.0.1"
    return f"http://{host}:{int(map_port)}/"


def main() -> int:
    args = parse_args()
    log_buffer = RuntimeLogBuffer(max_entries=3000)

    def emit(message: str) -> None:
        log_buffer.add(message, stream="stdout")
        print(message, file=sys.stdout, flush=True)

    def emit_error(message: str) -> None:
        log_buffer.add(message, stream="stderr")
        print(message, file=sys.stderr, flush=True)

    if args.interval is not None and args.interval <= 0:
        emit_error("--interval must be > 0 minutes (decimals allowed)")
        return 2
    if args.heard_window is not None and args.heard_window <= 0:
        emit_error("--heard-window must be > 0 minutes")
        return 2
    if args.hop_limit is not None and args.hop_limit <= 0:
        emit_error("--hop-limit must be > 0")
        return 2
    if args.web_ui and not (1 <= args.map_port <= 65535):
        emit_error("--map-port must be between 1 and 65535")
        return 2
    if not args.db_path.strip():
        emit_error("--db-path cannot be empty")
        return 2
    if args.traceroute_retention_hours is not None and args.traceroute_retention_hours <= 0:
        emit_error("--traceroute-retention-hours must be > 0")
        return 2
    if not args.web_ui and not args.host:
        emit_error("Missing required host. Usage: python meshtracer.py <NODE_IP> --no-web")
        return 2

    map_server = None
    store: SQLiteStore | None = None
    controller: MeshTracerController | None = None
    try:
        store = SQLiteStore(args.db_path)
        controller = MeshTracerController(
            args=args,
            store=store,
            log_buffer=log_buffer,
            emit=emit,
            emit_error=emit_error,
        )
        if args.webhook_api_token is not None and not controller.get_config().get("webhook_url"):
            emit_error(
                f"[{utc_now()}] Warning: --webhook-api-token was provided without "
                "an effective webhook URL; token will be ignored."
            )

        if args.web_ui:
            try:
                map_server = start_map_server(
                    controller.snapshot,
                    controller.wait_for_snapshot_revision,
                    controller.connect,
                    controller.disconnect,
                    controller.run_traceroute,
                    controller.send_chat_message,
                    controller.get_chat_messages,
                    controller.request_node_telemetry,
                    controller.request_node_info,
                    controller.request_node_position,
                    controller.reset_database,
                    controller.remove_traceroute_queue_entry,
                    controller.rescan_discovery,
                    controller.get_public_config,
                    controller.set_config,
                    args.map_host,
                    args.map_port,
                )
            except OSError as exc:
                emit_error(
                    f"[{utc_now()}] Failed to start web UI on {args.map_host}:{args.map_port}: {exc}"
                )
                emit_error(f"[{utc_now()}] Try a different --map-port or run with --no-web.")
                return 1
            controller.set_discovery_enabled(True)
            url = _browser_open_url(args.map_host, args.map_port)
            emit(f"[{utc_now()}] Web UI listening at {url}")
            if not args.no_open:
                try:
                    webbrowser.open(url, new=2)
                except Exception as exc:
                    emit_error(f"[{utc_now()}] Warning: failed to open browser: {exc}")

        if args.host and controller is not None:
            ok, detail = controller.connect(args.host)
            if not ok and not args.web_ui:
                emit_error(f"[{utc_now()}] Connect failed: {detail}")
                return 1

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        emit(f"\n[{utc_now()}] Stopped by user.")
        return 0
    finally:
        if controller is not None:
            try:
                controller.shutdown()
            except Exception:
                pass
        if map_server is not None:
            try:
                map_server.shutdown()
                map_server.server_close()
            except Exception:
                pass
        if store is not None:
            try:
                store.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
