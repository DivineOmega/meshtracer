from __future__ import annotations

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
    node_display,
    node_record_from_node,
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
    "max_map_traces": 800,
    "max_stored_traces": 50000,
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
        config["max_map_traces"] = max(1, pick_int("max_map_traces"))
        config["max_stored_traces"] = max(0, pick_int("max_stored_traces"))
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
            "max_map_traces",
            "max_stored_traces",
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
            max_map_traces = pick_int("max_map_traces")
            max_stored_traces = pick_int("max_stored_traces")
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
        if max_map_traces is not None and max_map_traces <= 0:
            return False, "max_map_traces must be > 0", None
        if max_stored_traces is not None and max_stored_traces < 0:
            return False, "max_stored_traces must be >= 0", None

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
        if max_map_traces is not None:
            new_config["max_map_traces"] = max_map_traces
        if max_stored_traces is not None:
            new_config["max_stored_traces"] = max_stored_traces
        if webhook_url is not None or "webhook_url" in update:
            new_config["webhook_url"] = webhook_url
        if webhook_api_token is not None or "webhook_api_token" in update:
            new_config["webhook_api_token"] = webhook_api_token

        # Ensure keys exist even if older DB entries were partial.
        for key, value in DEFAULT_RUNTIME_CONFIG.items():
            if key not in new_config:
                new_config[key] = value

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
                map_state.set_limits(
                    max_traces=int(new_config["max_map_traces"]),
                    max_stored_traces=int(new_config["max_stored_traces"]),
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
        self._emit(
            f"[{utc_now()}] Config updated: traceroute_behavior={traceroute_behavior} "
            f"interval={interval_minutes:g}m "
            f"heard_window={new_config['heard_window']}m hop_limit={new_config['hop_limit']} "
            f"fresh_window={new_config['fresh_window']}m mid_window={new_config['mid_window']}m "
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
            max_traces=int(config.get("max_map_traces") or DEFAULT_RUNTIME_CONFIG["max_map_traces"]),
            max_stored_traces=int(
                config.get("max_stored_traces") or DEFAULT_RUNTIME_CONFIG["max_stored_traces"]
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
                map_state.update_node_from_num(connected_interface, packet.get("from"))
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
                    self._emit(f"[{utc_now()}] Traceroute complete.")

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
    if args.max_map_traces is not None and args.max_map_traces <= 0:
        emit_error("--max-map-traces must be > 0")
        return 2
    if args.max_stored_traces is not None and args.max_stored_traces < 0:
        emit_error("--max-stored-traces must be >= 0")
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
