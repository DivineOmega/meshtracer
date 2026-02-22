from __future__ import annotations

import importlib
import os
import sys
import threading
import time
import webbrowser
from contextlib import contextmanager
from copy import deepcopy
from typing import Any, Callable, Iterator

from .cli import parse_args
from .common import utc_now
from .controller_config import ControllerConfigMixin
from .controller_connection import ControllerConnectionMixin
from .controller_defaults import DEFAULT_RUNTIME_CONFIG
from .controller_operations import ControllerOperationsMixin
from .controller_packets import ControllerPacketMixin
from .controller_worker import ControllerWorkerMixin
from .discovery import LanDiscoverer
from .map_server import start_map_server
from .state import MapState, RuntimeLogBuffer, normalize_runtime_log_type
from .storage import SQLiteStore


class MeshTracerController(
    ControllerConfigMixin,
    ControllerPacketMixin,
    ControllerOperationsMixin,
    ControllerConnectionMixin,
    ControllerWorkerMixin,
):
    def _emit_typed(self, message: str, *, log_type: str = "other") -> None:
        normalized_type = normalize_runtime_log_type(log_type)
        try:
            self._emit(message, log_type=normalized_type)
            return
        except TypeError:
            pass
        try:
            self._emit(message, normalized_type)
            return
        except TypeError:
            pass
        self._emit(message)

    def _emit_error_typed(self, message: str, *, log_type: str = "other") -> None:
        normalized_type = normalize_runtime_log_type(log_type)
        try:
            self._emit_error(message, log_type=normalized_type)
            return
        except TypeError:
            pass
        try:
            self._emit_error(message, normalized_type)
            return
        except TypeError:
            pass
        self._emit_error(message)

    def __init__(
        self,
        *,
        args: Any,
        store: SQLiteStore,
        log_buffer: RuntimeLogBuffer,
        emit: Callable[..., None],
        emit_error: Callable[..., None],
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
        self._discovery = LanDiscoverer(on_change=self._bump_snapshot_revision)
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

def _browser_open_url(map_host: str, map_port: int) -> str:
    host = str(map_host or "").strip()
    if host in ("0.0.0.0", "::", "[::]"):
        host = "127.0.0.1"
    return f"http://{host}:{int(map_port)}/"


@contextmanager
def _browser_launch_env() -> Iterator[None]:
    # PyInstaller one-file binaries on Linux override LD_LIBRARY_PATH.
    # Restore the system value while launching the browser opener so tools
    # like xdg-open/kde-open can resolve their own system libstdc++.
    if not (sys.platform.startswith("linux") and getattr(sys, "frozen", False)):
        yield
        return

    previous_ld_library_path = os.environ.get("LD_LIBRARY_PATH")
    original_ld_library_path = os.environ.get("LD_LIBRARY_PATH_ORIG")
    try:
        if original_ld_library_path:
            os.environ["LD_LIBRARY_PATH"] = original_ld_library_path
        else:
            os.environ.pop("LD_LIBRARY_PATH", None)
        yield
    finally:
        if previous_ld_library_path is None:
            os.environ.pop("LD_LIBRARY_PATH", None)
        else:
            os.environ["LD_LIBRARY_PATH"] = previous_ld_library_path


def main() -> int:
    args = parse_args()
    log_buffer = RuntimeLogBuffer(max_entries=3000)

    def emit(message: str, log_type: str = "other") -> None:
        log_buffer.add(message, stream="stdout", log_type=log_type)
        print(message, file=sys.stdout, flush=True)

    def emit_error(message: str, log_type: str = "other") -> None:
        log_buffer.add(message, stream="stderr", log_type=log_type)
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
                    controller.get_incoming_chat_messages,
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
                    with _browser_launch_env():
                        opened = webbrowser.open(url, new=2)
                    if not opened:
                        emit_error(
                            f"[{utc_now()}] Warning: no browser handler accepted auto-open; "
                            f"open {url} manually."
                        )
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
