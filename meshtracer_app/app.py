from __future__ import annotations

import importlib
import os
import sys
import time
from typing import Any

from .cli import parse_args
from .common import age_str, utc_now
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


def main() -> int:
    args = parse_args()
    log_buffer = RuntimeLogBuffer(max_entries=3000)

    def emit(message: str, *, error: bool = False) -> None:
        stream = "stderr" if error else "stdout"
        log_buffer.add(message, stream=stream)
        print(message, file=sys.stderr if error else sys.stdout, flush=True)

    if args.interval <= 0:
        emit("--interval must be > 0 minutes", error=True)
        return 2
    if args.heard_window <= 0:
        emit("--heard-window must be > 0 minutes", error=True)
        return 2
    if args.hop_limit <= 0:
        emit("--hop-limit must be > 0", error=True)
        return 2
    if args.serve_map and not (1 <= args.map_port <= 65535):
        emit("--map-port must be between 1 and 65535", error=True)
        return 2
    if not args.db_path.strip():
        emit("--db-path cannot be empty", error=True)
        return 2
    if args.max_map_traces <= 0:
        emit("--max-map-traces must be > 0", error=True)
        return 2
    if args.max_stored_traces < 0:
        emit("--max-stored-traces must be >= 0", error=True)
        return 2
    if args.webhook_api_token and not args.webhook_url:
        emit(
            f"[{utc_now()}] Warning: --webhook-api-token was provided without "
            "--webhook-url; token will be ignored.",
            error=True,
        )

    interval_seconds = args.interval * 60
    heard_window_seconds = args.heard_window * 60

    try:
        tcp_interface_mod = importlib.import_module("meshtastic.tcp_interface")
        mesh_pb2_mod = importlib.import_module("meshtastic.protobuf.mesh_pb2")
    except ModuleNotFoundError:
        emit(
            "Missing dependency: meshtastic\n"
            f"Current interpreter: {sys.executable}\n"
            "Install in this repo venv with:\n"
            "  .venv/bin/pip install -r requirements.txt\n"
            "Run script with:\n"
            "  .venv/bin/python meshtracer.py <NODE_IP>",
            error=True,
        )
        return 1
    except Exception as exc:
        emit(f"Failed to import meshtastic libraries: {exc}", error=True)
        return 1

    pub_bus: Any | None = None
    node_event_subscriptions: list[tuple[Any, str]] = []
    try:
        pubsub_mod = importlib.import_module("pubsub")
        pub_bus = getattr(pubsub_mod, "pub", None)
        if pub_bus is None:
            emit(
                f"[{utc_now()}] Warning: pubsub.pub not available; realtime node updates disabled.",
                error=True,
            )
    except Exception as exc:
        emit(
            f"[{utc_now()}] Warning: failed to load pubsub for realtime node updates: {exc}",
            error=True,
        )

    interface = None
    store: SQLiteStore | None = None
    map_server = None
    map_state: MapState | None = None
    try:
        store = SQLiteStore(args.db_path)
        emit(f"[{utc_now()}] Connecting to Meshtastic node at {args.host}...")
        interface = tcp_interface_mod.TCPInterface(hostname=args.host)
        emit(f"[{utc_now()}] Connected.")
        partition_key = resolve_mesh_partition_key(interface=interface, fallback_host=args.host)
        map_state = MapState(
            store=store,
            mesh_host=partition_key,
            max_traces=args.max_map_traces,
            max_stored_traces=args.max_stored_traces,
            log_buffer=log_buffer,
        )
        emit(
            f"[{utc_now()}] SQLite history DB: {os.path.abspath(args.db_path)} "
            f"(partition: {partition_key})"
        )
        if map_state is not None:
            map_state.update_nodes_from_interface(interface)
        if map_state is not None and pub_bus is not None:
            connected_interface = interface

            def on_receive_update(
                packet: Any = None, interface: Any = None, **_kwargs: Any
            ) -> None:
                if interface is not None and interface is not connected_interface:
                    return
                if map_state is None:
                    return
                if isinstance(packet, dict):
                    map_state.update_node_from_num(connected_interface, packet.get("from"))
                else:
                    map_state.update_nodes_from_interface(connected_interface)

            def on_node_updated(
                node: Any = None, interface: Any = None, **_kwargs: Any
            ) -> None:
                if interface is not None and interface is not connected_interface:
                    return
                if map_state is None:
                    return
                if isinstance(node, dict):
                    map_state.update_node_from_dict(node)
                else:
                    map_state.update_nodes_from_interface(connected_interface)

            pub_bus.subscribe(on_receive_update, "meshtastic.receive")
            node_event_subscriptions.append((on_receive_update, "meshtastic.receive"))
            pub_bus.subscribe(on_node_updated, "meshtastic.node.updated")
            node_event_subscriptions.append((on_node_updated, "meshtastic.node.updated"))
            emit(
                f"[{utc_now()}] Realtime node updates enabled "
                "(meshtastic.receive, meshtastic.node.updated)."
            )
        if args.serve_map and map_state is not None:
            map_server = start_map_server(map_state, args.map_host, args.map_port)
            emit(f"[{utc_now()}] Map server listening at http://{args.map_host}:{args.map_port}/")

        traceroute_capture: dict[str, Any] = {"result": None}
        original_traceroute_callback = getattr(interface, "onResponseTraceRoute", None)

        def wrapped_traceroute_callback(packet: dict[str, Any]) -> None:
            try:
                traceroute_capture["result"] = parse_traceroute_response(
                    interface=interface,
                    mesh_pb2_mod=mesh_pb2_mod,
                    packet=packet,
                )
                if traceroute_capture["result"] is not None and map_state is not None:
                    map_state.add_traceroute(traceroute_capture["result"])
            except Exception as exc:
                traceroute_capture["result"] = None
                emit(
                    f"[{utc_now()}] Warning: failed to parse traceroute response "
                    f"for webhook payload: {exc}",
                    error=True,
                )
            if callable(original_traceroute_callback):
                original_traceroute_callback(packet)

        interface.onResponseTraceRoute = wrapped_traceroute_callback

        effective_timeout = max(1, (interval_seconds - 1) // args.hop_limit)

        if hasattr(interface, "_timeout") and hasattr(interface._timeout, "expireTimeout"):
            interface._timeout.expireTimeout = effective_timeout
            est_wait = effective_timeout * args.hop_limit
            emit(
                f"[{utc_now()}] Traceroute timeout base set to {effective_timeout}s "
                f"(~{est_wait}s max wait at hop-limit {args.hop_limit})."
            )
        else:
            emit(
                f"[{utc_now()}] Warning: unable to set Meshtastic internal timeout "
                "(private API changed?).",
                error=True,
            )

        while True:
            cycle_start = time.time()
            if map_state is not None:
                map_state.update_nodes_from_interface(interface)

            target, last_heard_age, candidate_count = pick_recent_node(
                interface,
                heard_window_seconds=heard_window_seconds,
            )

            if target is None:
                emit(
                    f"[{utc_now()}] No eligible nodes heard in the last "
                    f"{age_str(heard_window_seconds)}."
                )
            else:
                emit(
                    f"\n[{utc_now()}] Selected {node_display(target)} "
                    f"(last heard {age_str(last_heard_age or 0)} ago, "
                    f"{candidate_count} eligible nodes)."
                )
                emit(f"[{utc_now()}] Starting traceroute...")

                try:
                    traceroute_capture["result"] = None
                    interface.sendTraceRoute(
                        dest=target["num"],
                        hopLimit=args.hop_limit,
                    )
                    emit(f"[{utc_now()}] Traceroute complete.")

                    if args.webhook_url:
                        if traceroute_capture["result"] is None:
                            emit(
                                f"[{utc_now()}] Webhook skipped: no parsed "
                                "traceroute response payload available.",
                                error=True,
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
                                emit(f"[{utc_now()}] Webhook delivered: {detail}")
                            else:
                                emit(
                                    f"[{utc_now()}] Webhook delivery failed: {detail}",
                                    error=True,
                                )
                except Exception as exc:  # keep loop alive for unexpected runtime errors
                    emit(f"[{utc_now()}] Traceroute failed: {exc}", error=True)

            elapsed = time.time() - cycle_start
            sleep_seconds = max(0.0, interval_seconds - elapsed)
            emit(f"[{utc_now()}] Waiting {sleep_seconds:.1f}s for next run...")
            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        emit(f"\n[{utc_now()}] Stopped by user.")
        return 0
    finally:
        if pub_bus is not None:
            for listener, topic in node_event_subscriptions:
                try:
                    pub_bus.unsubscribe(listener, topic)
                except Exception:
                    pass
        if map_server is not None:
            map_server.shutdown()
            map_server.server_close()
        if interface is not None:
            interface.close()
        if store is not None:
            store.close()


if __name__ == "__main__":
    raise SystemExit(main())
