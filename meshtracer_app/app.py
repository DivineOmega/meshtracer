from __future__ import annotations

import importlib
import os
import sys
import threading
import time
import webbrowser
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
        self._interface: Any | None = None
        self._mesh_pb2_mod: Any | None = None
        self._map_state: MapState | None = None

        self._pub_bus: Any | None = None
        self._node_event_subscriptions: list[tuple[Any, str]] = []

        self._worker_thread: threading.Thread | None = None
        self._worker_stop: threading.Event | None = None

        self._connected_host: str | None = None
        self._connection_state: str = "disconnected"  # disconnected | connecting | connected | error
        self._connection_error: str | None = None
        self._discovery = LanDiscoverer()
        self._discovery.set_enabled(False)

    def set_discovery_enabled(self, enabled: bool) -> None:
        self._discovery.set_enabled(enabled)

    def rescan_discovery(self) -> tuple[bool, str]:
        self._discovery.trigger_scan()
        return True, "scan_triggered"

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

        if map_state is not None:
            payload = map_state.snapshot()
        else:
            payload = {
                "generated_at_utc": utc_now(),
                "mesh_host": "-",
                "node_count": 0,
                "trace_count": 0,
                "nodes": [],
                "traces": [],
                "edges": [],
                "logs": [],
            }

        # Always attach the latest runtime logs.
        payload["logs"] = self._log_buffer.tail(limit=500)
        payload["connected"] = connection_state == "connected"
        payload["connection_state"] = connection_state
        payload["connected_host"] = connected_host
        payload["connection_error"] = connection_error
        payload["discovery"] = self._discovery.snapshot()
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
            if getattr(self._args, "web_ui", False):
                self._discovery.set_enabled(True)
            return False, "meshtastic is not installed"
        except Exception as exc:
            self._emit_error(f"Failed to import meshtastic libraries: {exc}")
            with self._lock:
                self._connection_state = "error"
                self._connection_error = str(exc)
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
            if getattr(self._args, "web_ui", False):
                self._discovery.set_enabled(True)
            return False, str(exc)

        self._emit(f"[{utc_now()}] Connected.")
        partition_key = resolve_mesh_partition_key(interface=interface, fallback_host=host)
        map_state = MapState(
            store=self._store,
            mesh_host=partition_key,
            max_traces=self._args.max_map_traces,
            max_stored_traces=self._args.max_stored_traces,
            log_buffer=self._log_buffer,
        )

        self._emit(
            f"[{utc_now()}] SQLite history DB: {os.path.abspath(self._args.db_path)} "
            f"(partition: {partition_key})"
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

        interval_seconds = int(self._args.interval) * 60
        effective_timeout = max(1, (interval_seconds - 1) // int(self._args.hop_limit))

        if hasattr(interface, "_timeout") and hasattr(interface._timeout, "expireTimeout"):
            interface._timeout.expireTimeout = effective_timeout
            est_wait = effective_timeout * int(self._args.hop_limit)
            self._emit(
                f"[{utc_now()}] Traceroute timeout base set to {effective_timeout}s "
                f"(~{est_wait}s max wait at hop-limit {self._args.hop_limit})."
            )
        else:
            self._emit_error(
                f"[{utc_now()}] Warning: unable to set Meshtastic internal timeout "
                "(private API changed?)."
            )

        stop_event = threading.Event()
        worker = threading.Thread(
            target=self._traceroute_worker,
            args=(interface, map_state, traceroute_capture, stop_event, host),
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
            self._connection_state = "connected"
            self._connection_error = None

        return True, "connected"

    def disconnect(self) -> tuple[bool, str]:
        with self._lock:
            stop_event = self._worker_stop
            worker = self._worker_thread
            pub_bus = self._pub_bus
            subscriptions = list(self._node_event_subscriptions)
            interface = self._interface

            self._worker_stop = None
            self._worker_thread = None
            self._pub_bus = None
            self._node_event_subscriptions = []
            self._interface = None
            self._mesh_pb2_mod = None

            # Keep _map_state around so history remains visible when disconnected.
            self._connection_state = "disconnected"
            self._connection_error = None
            self._connected_host = None

        if getattr(self._args, "web_ui", False):
            self._discovery.set_enabled(True)

        if stop_event is not None:
            stop_event.set()

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

        def on_node_updated(node: Any = None, interface: Any = None, **_kwargs: Any) -> None:
            if interface is not None and interface is not connected_interface:
                return
            if isinstance(node, dict):
                map_state.update_node_from_dict(node)
            else:
                map_state.update_nodes_from_interface(connected_interface)

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

    def _traceroute_worker(
        self,
        interface: Any,
        map_state: MapState,
        traceroute_capture: dict[str, Any],
        stop_event: threading.Event,
        connected_host: str,
    ) -> None:
        interval_seconds = int(self._args.interval) * 60
        heard_window_seconds = int(self._args.heard_window) * 60

        while not stop_event.is_set():
            cycle_start = time.time()

            try:
                map_state.update_nodes_from_interface(interface)
            except Exception as exc:
                self._emit_error(f"[{utc_now()}] Warning: failed to refresh nodes: {exc}")

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
                self._emit(
                    f"\n[{utc_now()}] Selected {node_display(target)} "
                    f"(last heard {age_str(last_heard_age or 0)} ago, "
                    f"{candidate_count} eligible nodes)."
                )
                self._emit(f"[{utc_now()}] Starting traceroute...")

                try:
                    traceroute_capture["result"] = None
                    interface.sendTraceRoute(
                        dest=target["num"],
                        hopLimit=int(self._args.hop_limit),
                    )
                    self._emit(f"[{utc_now()}] Traceroute complete.")

                    if self._args.webhook_url:
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
                                "interval_minutes": int(self._args.interval),
                                "interval_seconds": interval_seconds,
                                "hop_limit": int(self._args.hop_limit),
                                "selected_target": node_record_from_node(target),
                                "selected_target_last_heard_age_seconds": round(
                                    float(last_heard_age or 0), 3
                                ),
                                "eligible_candidate_count": candidate_count,
                                "traceroute": traceroute_capture["result"],
                            }
                            delivered, detail = post_webhook(
                                url=self._args.webhook_url,
                                api_token=self._args.webhook_api_token,
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

            elapsed = time.time() - cycle_start
            sleep_seconds = max(0.0, interval_seconds - elapsed)
            self._emit(f"[{utc_now()}] Waiting {sleep_seconds:.1f}s for next run...")
            stop_event.wait(sleep_seconds)


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

    if args.interval <= 0:
        emit_error("--interval must be > 0 minutes")
        return 2
    if args.heard_window <= 0:
        emit_error("--heard-window must be > 0 minutes")
        return 2
    if args.hop_limit <= 0:
        emit_error("--hop-limit must be > 0")
        return 2
    if args.web_ui and not (1 <= args.map_port <= 65535):
        emit_error("--map-port must be between 1 and 65535")
        return 2
    if not args.db_path.strip():
        emit_error("--db-path cannot be empty")
        return 2
    if args.max_map_traces <= 0:
        emit_error("--max-map-traces must be > 0")
        return 2
    if args.max_stored_traces < 0:
        emit_error("--max-stored-traces must be >= 0")
        return 2
    if args.webhook_api_token and not args.webhook_url:
        emit_error(
            f"[{utc_now()}] Warning: --webhook-api-token was provided without "
            "--webhook-url; token will be ignored."
        )
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

        if args.web_ui:
            try:
                map_server = start_map_server(
                    controller.snapshot,
                    controller.connect,
                    controller.disconnect,
                    controller.rescan_discovery,
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
