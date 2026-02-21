from __future__ import annotations

import inspect
import importlib
import os
import sys
import threading
from typing import Any

from .common import utc_now
from .controller_defaults import DEFAULT_RUNTIME_CONFIG
from .meshtastic_helpers import parse_traceroute_response, resolve_mesh_partition_key
from .state import MapState


class ControllerConnectionMixin:
    @staticmethod
    def _parse_connection_target(raw_target: str) -> tuple[str, str | None, str]:
        target = str(raw_target or "").strip()
        if not target:
            raise ValueError("missing host")

        if "://" not in target:
            return "tcp", target, target

        scheme_raw, _sep, endpoint_raw = target.partition("://")
        scheme = str(scheme_raw or "").strip().lower()
        endpoint = str(endpoint_raw or "").strip()

        if scheme == "tcp":
            if not endpoint:
                raise ValueError("missing TCP host")
            # Keep TCP target normalized to plain host text for compatibility.
            return "tcp", endpoint, endpoint

        if scheme == "ble":
            # Empty BLE endpoint means "connect to the only/first discoverable node".
            normalized = "ble://" if not endpoint else f"ble://{endpoint}"
            return "ble", (endpoint or None), normalized

        raise ValueError(f"unsupported connection scheme '{scheme}' (supported: tcp://, ble://)")

    @staticmethod
    def _enable_ble_start_notify_workaround(interface_mod: Any) -> bool:
        """Force BlueZ StartNotify for Meshtastic BLE to avoid AcquireNotify failures."""
        if not str(sys.platform).startswith("linux"):
            return False

        ble_client_cls = getattr(interface_mod, "BLEClient", None)
        original_start_notify = getattr(ble_client_cls, "start_notify", None)
        if not callable(original_start_notify):
            return False
        if getattr(original_start_notify, "__meshtracer_bluez_start_notify_patch__", False):
            return False
        logradio_uuids = {
            str(getattr(interface_mod, "LOGRADIO_UUID", "") or "").strip().lower(),
            str(getattr(interface_mod, "LEGACY_LOGRADIO_UUID", "") or "").strip().lower(),
        }
        logradio_uuids.discard("")

        def patched_start_notify(self: Any, *args: Any, **kwargs: Any) -> Any:
            char_specifier = args[0] if args else kwargs.get("char_specifier")
            try:
                bleak_start_notify = getattr(getattr(self, "bleak_client", None), "start_notify", None)
                bleak_services = getattr(getattr(self, "bleak_client", None), "services", None)
                get_characteristic = getattr(bleak_services, "get_characteristic", None)
                if callable(get_characteristic) and char_specifier is not None:
                    characteristic = get_characteristic(char_specifier)
                    char_obj = getattr(characteristic, "obj", None)
                    if (
                        isinstance(char_obj, tuple)
                        and len(char_obj) >= 2
                        and isinstance(char_obj[1], dict)
                    ):
                        # Old Bleak backends pick AcquireNotify when this key exists.
                        char_obj[1].pop("NotifyAcquired", None)
                if callable(bleak_start_notify):
                    params = inspect.signature(bleak_start_notify).parameters
                    has_var_kwargs = any(
                        p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
                    )
                    if "bluez" in params or has_var_kwargs:
                        bluez_arg = kwargs.get("bluez")
                        if isinstance(bluez_arg, dict):
                            merged = dict(bluez_arg)
                        else:
                            merged = {}
                        merged.setdefault("use_start_notify", True)
                        kwargs["bluez"] = merged
            except Exception:
                # Never block BLE connect path because of a compatibility hint.
                pass
            try:
                return original_start_notify(self, *args, **kwargs)
            except Exception as exc:
                # LOGRADIO notifications are optional for Meshtracer operation.
                # Some BlueZ stacks report "Notify acquired" on this characteristic
                # even when core data path notifications still work.
                char_key = str(char_specifier or "").strip().lower()
                text = str(exc or "")
                if (
                    char_key in logradio_uuids
                    and "org.bluez.Error.NotPermitted" in text
                    and "Notify acquired" in text
                ):
                    return None
                raise

        setattr(patched_start_notify, "__meshtracer_bluez_start_notify_patch__", True)
        setattr(ble_client_cls, "start_notify", patched_start_notify)
        return True

    @staticmethod
    def _enable_bluez_backend_start_notify_workaround() -> bool:
        """Patch Bleak BlueZ backend to favor StartNotify over AcquireNotify."""
        if not str(sys.platform).startswith("linux"):
            return False

        try:
            bluez_client_mod = importlib.import_module("bleak.backends.bluezdbus.client")
        except Exception:
            return False

        bluez_client_cls = getattr(bluez_client_mod, "BleakClientBlueZDBus", None)
        original_start_notify = getattr(bluez_client_cls, "start_notify", None)
        if not callable(original_start_notify):
            return False
        if getattr(original_start_notify, "__meshtracer_bluez_start_notify_patch__", False):
            return False

        params = inspect.signature(original_start_notify).parameters
        has_var_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
        accepts_bluez = "bluez" in params or has_var_kwargs

        async def patched_start_notify(self: Any, characteristic: Any, callback: Any, **kwargs: Any) -> Any:
            try:
                char_obj = getattr(characteristic, "obj", None)
                if (
                    isinstance(char_obj, tuple)
                    and len(char_obj) >= 2
                    and isinstance(char_obj[1], dict)
                ):
                    char_obj[1].pop("NotifyAcquired", None)
                if accepts_bluez:
                    bluez_arg = kwargs.get("bluez")
                    if isinstance(bluez_arg, dict):
                        merged = dict(bluez_arg)
                    else:
                        merged = {}
                    merged.setdefault("use_start_notify", True)
                    kwargs["bluez"] = merged
            except Exception:
                pass

            if accepts_bluez:
                return await original_start_notify(self, characteristic, callback, **kwargs)
            return await original_start_notify(self, characteristic, callback)

        setattr(patched_start_notify, "__meshtracer_bluez_start_notify_patch__", True)
        setattr(bluez_client_cls, "start_notify", patched_start_notify)
        return True

    def connect(self, host: str) -> tuple[bool, str]:
        target_raw = str(host or "").strip()
        if not target_raw:
            return False, "missing host"
        try:
            transport, endpoint, target = self._parse_connection_target(target_raw)
        except ValueError as exc:
            return False, str(exc)

        # Always tear down any current connection so connect() is idempotent.
        self.disconnect()
        if getattr(self._args, "web_ui", False):
            self._discovery.set_enabled(False)

        with self._lock:
            self._connection_state = "connecting"
            self._connected_host = target
            self._connection_error = None
            self._bump_snapshot_revision_locked()

        self._emit(f"[{utc_now()}] Connecting to Meshtastic node at {target}...")

        try:
            mesh_pb2_mod = importlib.import_module("meshtastic.protobuf.mesh_pb2")
            interface_mod = importlib.import_module(
                "meshtastic.tcp_interface" if transport == "tcp" else "meshtastic.ble_interface"
            )
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
            if transport == "tcp":
                interface = interface_mod.TCPInterface(hostname=str(endpoint))
            else:
                meshtastic_ble_patch = self._enable_ble_start_notify_workaround(interface_mod)
                bleak_backend_patch = self._enable_bluez_backend_start_notify_workaround()
                if meshtastic_ble_patch or bleak_backend_patch:
                    self._emit(
                        f"[{utc_now()}] Linux BLE workaround enabled: forcing StartNotify."
                    )
                interface = interface_mod.BLEInterface(address=endpoint)
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
        partition_key = resolve_mesh_partition_key(interface=interface, fallback_host=target)
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
                self._emit_error_typed(
                    f"[{utc_now()}] Warning: failed to parse traceroute response "
                    f"for webhook payload: {exc}",
                    log_type="traceroute",
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
            args=(interface, map_state, traceroute_capture, stop_event, wake_event, target),
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
        packet_for_storage = packet
        packet_hops_away = self._packet_hops_away(packet)
        if packet_hops_away is not None:
            packet_for_storage = dict(packet)
            packet_for_storage["hopsAway"] = int(packet_hops_away)
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
            packet=packet_for_storage,
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
                    self._emit_typed(
                        f"[{utc_now()}] Received {', '.join(telemetry_types)} telemetry "
                        f"from {node_desc}{suffix}.",
                        log_type="telemetry",
                    )
                if node_info_packet:
                    suffix = "" if node_info_updated else " (no data changes)"
                    self._emit_typed(
                        f"[{utc_now()}] Received node info from {node_desc}{suffix}.",
                        log_type="node_info",
                    )
                if position_packet:
                    lat, lon = self._packet_position(packet)
                    position_text = ""
                    if lat is not None and lon is not None:
                        position_text = f" ({lat:.5f}, {lon:.5f})"
                    suffix = "" if position_updated else " (no data changes)"
                    self._emit_typed(
                        f"[{utc_now()}] Received position from {node_desc}"
                        f"{position_text}{suffix}.",
                        log_type="position",
                    )
                if isinstance(chat_message, dict):
                    text = str(chat_message.get("text") or "")
                    text_preview = text if len(text) <= 90 else f"{text[:87]}..."
                    if str(chat_message.get("message_type") or "") == "channel":
                        channel_index = int(chat_message.get("channel_index") or 0)
                        self._emit_typed(
                            f"[{utc_now()}] Received channel message on channel #{channel_index} "
                            f"from {node_desc}: \"{text_preview}\"",
                            log_type="messaging",
                        )
                    else:
                        self._emit_typed(
                            f"[{utc_now()}] Received direct message from {node_desc}: "
                            f"\"{text_preview}\"",
                            log_type="messaging",
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
