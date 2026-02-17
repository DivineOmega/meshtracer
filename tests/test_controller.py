from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from meshtracer_app.app import MeshTracerController
from meshtracer_app.state import MapState, RuntimeLogBuffer
from meshtracer_app.storage import SQLiteStore


class _DummyWorker:
    def is_alive(self) -> bool:
        return True


class _DummyLocalNode:
    def __init__(self, node_num: int | None) -> None:
        self.nodeNum = node_num


class _DummyInterface:
    def __init__(self, local_num: int | None = None) -> None:
        self.localNode = _DummyLocalNode(local_num)
        self.nodesByNum = {}


class _DummyTraceInterface(_DummyInterface):
    def __init__(self, local_num: int | None = None) -> None:
        super().__init__(local_num=local_num)
        self.trace_calls: list[tuple[int, int]] = []

    def sendTraceRoute(self, *, dest: int, hopLimit: int) -> None:
        self.trace_calls.append((int(dest), int(hopLimit)))


class _DummyChatInterface(_DummyInterface):
    def __init__(self, local_num: int | None = None) -> None:
        super().__init__(local_num=local_num)
        self.sent_messages: list[dict[str, object]] = []
        self._next_id = 2000

    def sendText(self, text: str, **kwargs: object) -> dict[str, int]:
        self.sent_messages.append({"text": text, **kwargs})
        self._next_id += 1
        return {"id": self._next_id}


class _DummyChatNoIdInterface(_DummyChatInterface):
    def sendText(self, text: str, **kwargs: object) -> dict[str, object]:
        self.sent_messages.append({"text": text, **kwargs})
        return {}


class _DummyTelemetryField:
    def __init__(self) -> None:
        self.last_copy = None

    def CopyFrom(self, value: object) -> None:
        self.last_copy = value


class _DummyTelemetryMessage:
    def __init__(self) -> None:
        self.device_metrics = _DummyTelemetryField()
        self.environment_metrics = _DummyTelemetryField()


class _DummyTelemetryInterface(_DummyInterface):
    def __init__(self, local_num: int | None = None) -> None:
        super().__init__(local_num=local_num)
        self.sent_packets: list[dict[str, object]] = []
        self.onResponseTelemetry = lambda _packet=None: None
        self.isConnected = threading.Event()
        self.isConnected.set()

    def sendData(self, data: object, **kwargs: object) -> dict[str, int]:
        self.sent_packets.append({"data": data, **kwargs})
        return {"id": len(self.sent_packets)}


class _DummyPositionObject:
    def __init__(self, latitude_i: int, longitude_i: int) -> None:
        self.latitude_i = latitude_i
        self.longitude_i = longitude_i
        self.altitude = 100


def _args(**overrides: object) -> SimpleNamespace:
    base: dict[str, object] = {
        "traceroute_behavior": None,
        "interval": None,
        "heard_window": None,
        "fresh_window": None,
        "mid_window": None,
        "hop_limit": None,
        "traceroute_retention_hours": None,
        "webhook_url": None,
        "webhook_api_token": None,
        "web_ui": True,
        "db_path": "meshtracer.db",
        "map_host": "127.0.0.1",
        "map_port": 8090,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _set_connected_state(
    controller: MeshTracerController,
    store: SQLiteStore,
    *,
    local_num: int | None,
    mesh_host: str = "test:queue",
) -> threading.Event:
    wake_event = threading.Event()
    map_state = MapState(store=store, mesh_host=mesh_host)
    with controller._lock:
        controller._interface = _DummyInterface(local_num=local_num)
        controller._worker_thread = _DummyWorker()
        controller._worker_wake = wake_event
        controller._map_state = map_state
        controller._connection_state = "connected"
    return wake_event


class ControllerConfigTests(unittest.TestCase):
    def test_cli_override_can_reset_persisted_values_back_to_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                store.set_runtime_config(
                    {
                        "interval": 9,
                        "heard_window": 120,
                        "fresh_window": 120,
                        "mid_window": 480,
                        "hop_limit": 7,
                        "webhook_url": None,
                        "webhook_api_token": None,
                        "traceroute_retention_hours": 720,
                    }
                )
                controller = MeshTracerController(
                    args=_args(
                        interval=5,
                        heard_window=120,
                        hop_limit=7,
                        traceroute_retention_hours=720,
                        db_path=str(db_path),
                    ),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                self.assertEqual(controller.get_config().get("interval"), 5)
                self.assertEqual(controller.get_config().get("traceroute_behavior"), "automatic")
            finally:
                store.close()

    def test_public_config_and_snapshot_redact_webhook_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                store.set_runtime_config(
                    {
                        "interval": 5,
                        "heard_window": 120,
                        "fresh_window": 120,
                        "mid_window": 480,
                        "hop_limit": 7,
                        "webhook_url": "https://example.test/hook",
                        "webhook_api_token": "supersecret",
                        "traceroute_retention_hours": 720,
                    }
                )
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )

                public_config = controller.get_public_config()
                self.assertIsNone(public_config.get("webhook_api_token"))
                self.assertTrue(public_config.get("webhook_api_token_set"))

                snap = controller.snapshot()
                self.assertIsNone((snap.get("config") or {}).get("webhook_api_token"))
                self.assertTrue((snap.get("config") or {}).get("webhook_api_token_set"))

                ok, detail = controller.set_config({"interval": 8})
                self.assertTrue(ok, detail)
                self.assertEqual(controller.get_config().get("webhook_api_token"), "supersecret")
            finally:
                store.close()

    def test_run_traceroute_requires_connected_worker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                ok, detail = controller.run_traceroute(123)
                self.assertFalse(ok)
                self.assertEqual(detail, "not connected")
            finally:
                store.close()

    def test_set_config_validates_traceroute_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )

                ok, detail = controller.set_config({"traceroute_behavior": "automatic"})
                self.assertTrue(ok, detail)
                self.assertEqual(controller.get_config().get("traceroute_behavior"), "automatic")

                ok, detail = controller.set_config({"traceroute_behavior": "invalid"})
                self.assertFalse(ok)
                self.assertIn("traceroute_behavior", detail)
                self.assertEqual(controller.get_config().get("traceroute_behavior"), "automatic")
            finally:
                store.close()

    def test_interval_supports_30_seconds_and_defaults_to_5_minutes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                self.assertAlmostEqual(float(controller.get_config().get("interval") or 0.0), 5.0, places=3)
                self.assertEqual(controller.get_config().get("traceroute_behavior"), "automatic")
                self.assertEqual(controller.get_config().get("traceroute_retention_hours"), 720)

                ok, detail = controller.set_config({"interval": 0.5})
                self.assertTrue(ok, detail)
                self.assertAlmostEqual(float(controller.get_config().get("interval") or 0.0), 0.5, places=3)
            finally:
                store.close()

    def test_run_traceroute_queues_and_wakes_worker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                wake_event = _set_connected_state(controller, store, local_num=99)

                ok, detail = controller.run_traceroute(42)
                self.assertTrue(ok, detail)
                self.assertEqual(detail, "queued traceroute to node #42 (position 1)")
                queue_entries = store.list_traceroute_queue("test:queue")
                self.assertEqual(len(queue_entries), 1)
                self.assertEqual(queue_entries[0].get("node_num"), 42)
                self.assertEqual(queue_entries[0].get("status"), "queued")
                self.assertTrue(wake_event.is_set())
            finally:
                store.close()

    def test_run_traceroute_rejects_local_node(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                _set_connected_state(controller, store, local_num=77)

                ok, detail = controller.run_traceroute(77)
                self.assertFalse(ok)
                self.assertEqual(detail, "cannot traceroute the local node")
            finally:
                store.close()

    def test_run_traceroute_deduplicates_queue_and_running(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                _set_connected_state(controller, store, local_num=77)

                ok, detail = controller.run_traceroute(88)
                self.assertTrue(ok, detail)
                self.assertEqual(detail, "queued traceroute to node #88 (position 1)")

                ok, detail = controller.run_traceroute(88)
                self.assertTrue(ok, detail)
                self.assertEqual(detail, "traceroute already queued for node #88 (position 1)")
                queue_entries = store.list_traceroute_queue("test:queue")
                self.assertEqual(len(queue_entries), 1)
                self.assertEqual(queue_entries[0].get("node_num"), 88)

                queue_entry = queue_entries[0]
                removed = store.remove_traceroute_queue_entry("test:queue", int(queue_entry.get("queue_id") or -1))
                self.assertTrue(removed)
                with controller._lock:
                    controller._current_traceroute_node_num = 88
                ok, detail = controller.run_traceroute(88)
                self.assertTrue(ok, detail)
                self.assertEqual(detail, "traceroute already running for node #88")
            finally:
                store.close()

    def test_request_node_telemetry_uses_non_blocking_send_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )

                telemetry_iface = _DummyTelemetryInterface(local_num=1)
                with controller._lock:
                    controller._interface = telemetry_iface
                    controller._worker_thread = _DummyWorker()
                    controller._connection_state = "connected"

                telemetry_pb2 = SimpleNamespace(
                    Telemetry=_DummyTelemetryMessage,
                    DeviceMetrics=type("DeviceMetrics", (), {}),
                    EnvironmentMetrics=type("EnvironmentMetrics", (), {}),
                )
                portnums_pb2 = SimpleNamespace(
                    PortNum=SimpleNamespace(TELEMETRY_APP=67),
                )

                def import_stub(module_name: str):
                    if module_name == "meshtastic.protobuf.telemetry_pb2":
                        return telemetry_pb2
                    if module_name == "meshtastic.protobuf.portnums_pb2":
                        return portnums_pb2
                    raise ModuleNotFoundError(module_name)

                with mock.patch("meshtracer_app.app.importlib.import_module", side_effect=import_stub):
                    ok, detail = controller.request_node_telemetry(42, "device")

                self.assertTrue(ok, detail)
                self.assertIn("requested device telemetry", detail)
                self.assertEqual(len(telemetry_iface.sent_packets), 1)
                sent = telemetry_iface.sent_packets[0]
                self.assertEqual(sent.get("destinationId"), 42)
                self.assertEqual(sent.get("portNum"), 67)
                self.assertEqual(sent.get("wantResponse"), True)
                self.assertIsNone(sent.get("onResponse"))
            finally:
                store.close()

    def test_request_node_telemetry_validates_type(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                _set_connected_state(controller, store, local_num=1)
                ok, detail = controller.request_node_telemetry(42, "unsupported")
                self.assertFalse(ok)
                self.assertEqual(detail, "invalid telemetry_type")
            finally:
                store.close()

    def test_request_node_telemetry_rejects_when_interface_reconnecting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )

                telemetry_iface = _DummyTelemetryInterface(local_num=1)
                telemetry_iface.isConnected.clear()
                with controller._lock:
                    controller._interface = telemetry_iface
                    controller._worker_thread = _DummyWorker()
                    controller._connection_state = "connected"

                ok, detail = controller.request_node_telemetry(42, "device")
                self.assertFalse(ok)
                self.assertEqual(detail, "meshtastic interface is reconnecting")
                self.assertEqual(telemetry_iface.sent_packets, [])
            finally:
                store.close()

    def test_request_node_info_uses_send_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )

                telemetry_iface = _DummyTelemetryInterface(local_num=1)
                with controller._lock:
                    controller._interface = telemetry_iface
                    controller._worker_thread = _DummyWorker()
                    controller._connection_state = "connected"

                mesh_pb2 = SimpleNamespace(User=type("User", (), {}))
                portnums_pb2 = SimpleNamespace(
                    PortNum=SimpleNamespace(NODEINFO_APP=4),
                )

                def import_stub(module_name: str):
                    if module_name == "meshtastic.protobuf.mesh_pb2":
                        return mesh_pb2
                    if module_name == "meshtastic.protobuf.portnums_pb2":
                        return portnums_pb2
                    raise ModuleNotFoundError(module_name)

                with mock.patch("meshtracer_app.app.importlib.import_module", side_effect=import_stub):
                    ok, detail = controller.request_node_info(42)

                self.assertTrue(ok, detail)
                self.assertIn("requested node info", detail)
                self.assertEqual(len(telemetry_iface.sent_packets), 1)
                sent = telemetry_iface.sent_packets[0]
                self.assertEqual(sent.get("destinationId"), 42)
                self.assertEqual(sent.get("portNum"), 4)
                self.assertEqual(sent.get("wantResponse"), True)
            finally:
                store.close()

    def test_request_node_position_uses_send_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )

                telemetry_iface = _DummyTelemetryInterface(local_num=1)
                with controller._lock:
                    controller._interface = telemetry_iface
                    controller._worker_thread = _DummyWorker()
                    controller._connection_state = "connected"

                mesh_pb2 = SimpleNamespace(Position=type("Position", (), {}))
                portnums_pb2 = SimpleNamespace(
                    PortNum=SimpleNamespace(POSITION_APP=3),
                )

                def import_stub(module_name: str):
                    if module_name == "meshtastic.protobuf.mesh_pb2":
                        return mesh_pb2
                    if module_name == "meshtastic.protobuf.portnums_pb2":
                        return portnums_pb2
                    raise ModuleNotFoundError(module_name)

                with mock.patch("meshtracer_app.app.importlib.import_module", side_effect=import_stub):
                    ok, detail = controller.request_node_position(42)

                self.assertTrue(ok, detail)
                self.assertIn("requested position", detail)
                self.assertEqual(len(telemetry_iface.sent_packets), 1)
                sent = telemetry_iface.sent_packets[0]
                self.assertEqual(sent.get("destinationId"), 42)
                self.assertEqual(sent.get("portNum"), 3)
                self.assertEqual(sent.get("wantResponse"), True)
            finally:
                store.close()

    def test_telemetry_packet_types_detect_supported_fields(self) -> None:
        self.assertEqual(
            MeshTracerController._telemetry_packet_types(
                {"decoded": {"telemetry": {"deviceMetrics": {"batteryLevel": 50}}}}
            ),
            ["device"],
        )
        self.assertEqual(
            MeshTracerController._telemetry_packet_types(
                {"decoded": {"telemetry": {"environment_metrics": {"temperature": 21.2}}}}
            ),
            ["environment"],
        )
        self.assertEqual(
            MeshTracerController._telemetry_packet_types(
                {
                    "decoded": {
                        "telemetry": {
                            "device_metrics": {"batteryLevel": 50},
                            "environmentMetrics": {"temperature": 21.2},
                        }
                    }
                }
            ),
            ["device", "environment"],
        )
        self.assertEqual(MeshTracerController._telemetry_packet_types({"decoded": {}}), [])

    def test_packet_type_helpers_detect_node_info_and_position(self) -> None:
        self.assertTrue(
            MeshTracerController._is_node_info_packet(
                {"decoded": {"portnum": "NODEINFO_APP", "user": {"longName": "Node"}}}
            )
        )
        self.assertTrue(
            MeshTracerController._is_position_packet(
                {
                    "decoded": {
                        "portnum": "POSITION_APP",
                        "position": {"latitudeI": 515001234, "longitudeI": -1234567},
                    }
                }
            )
        )
        self.assertFalse(MeshTracerController._is_node_info_packet({"decoded": {"portnum": "TEXT_MESSAGE_APP"}}))
        self.assertFalse(MeshTracerController._is_position_packet({"decoded": {"portnum": "TEXT_MESSAGE_APP"}}))

        lat, lon = MeshTracerController._packet_position(
            {"decoded": {"position": {"latitudeI": 515001234, "longitudeI": -1234567}}}
        )
        self.assertAlmostEqual(lat or 0.0, 51.5001234, places=6)
        self.assertAlmostEqual(lon or 0.0, -0.1234567, places=6)

    def test_send_chat_message_and_get_chat_messages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                map_state = MapState(store=store, mesh_host="test:chat")
                chat_iface = _DummyChatInterface(local_num=10)
                with controller._lock:
                    controller._interface = chat_iface
                    controller._worker_thread = _DummyWorker()
                    controller._map_state = map_state
                    controller._connection_state = "connected"

                ok, detail = controller.send_chat_message("channel", 0, "hello channel")
                self.assertTrue(ok, detail)
                ok, detail = controller.send_chat_message("direct", 42, "hello direct")
                self.assertTrue(ok, detail)
                self.assertEqual(len(chat_iface.sent_messages), 2)
                self.assertEqual(chat_iface.sent_messages[0].get("destinationId"), "^all")
                self.assertEqual(chat_iface.sent_messages[0].get("channelIndex"), 0)
                self.assertEqual(chat_iface.sent_messages[1].get("destinationId"), 42)

                ok, detail, channel_messages, channel_revision = controller.get_chat_messages("channel", 0, 100)
                self.assertTrue(ok, detail)
                self.assertGreaterEqual(channel_revision, 1)
                self.assertEqual(len(channel_messages), 1)
                self.assertEqual(channel_messages[0].get("text"), "hello channel")
                self.assertEqual(channel_messages[0].get("direction"), "outgoing")

                ok, detail, direct_messages, direct_revision = controller.get_chat_messages("direct", 42, 100)
                self.assertTrue(ok, detail)
                self.assertGreaterEqual(direct_revision, channel_revision)
                self.assertEqual(len(direct_messages), 1)
                self.assertEqual(direct_messages[0].get("text"), "hello direct")
                self.assertEqual(direct_messages[0].get("peer_node_num"), 42)
            finally:
                store.close()

    def test_send_chat_message_allows_repeated_text_without_packet_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                map_state = MapState(store=store, mesh_host="test:chat-no-id")
                chat_iface = _DummyChatNoIdInterface(local_num=10)
                with controller._lock:
                    controller._interface = chat_iface
                    controller._worker_thread = _DummyWorker()
                    controller._map_state = map_state
                    controller._connection_state = "connected"

                ok, detail = controller.send_chat_message("channel", 0, "same text")
                self.assertTrue(ok, detail)
                ok, detail = controller.send_chat_message("channel", 0, "same text")
                self.assertTrue(ok, detail)

                ok, detail, channel_messages, _revision = controller.get_chat_messages("channel", 0, 100)
                self.assertTrue(ok, detail)
                self.assertEqual(len(channel_messages), 2)
                self.assertEqual(channel_messages[0].get("text"), "same text")
                self.assertEqual(channel_messages[1].get("text"), "same text")
            finally:
                store.close()

    def test_capture_chat_from_packet_handles_channel_and_direct(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                map_state = MapState(store=store, mesh_host="test:chat-packets")
                iface = _DummyChatInterface(local_num=10)

                packet_channel = {
                    "id": 3001,
                    "from": 42,
                    "to": 0xFFFFFFFF,
                    "channel": 1,
                    "rxTime": 1739740001,
                    "decoded": {"portnum": "TEXT_MESSAGE_APP", "text": "channel hello"},
                }
                packet_direct = {
                    "id": 3002,
                    "from": 42,
                    "to": 10,
                    "rxTime": 1739740002,
                    "decoded": {"portnum": "TEXT_MESSAGE_APP", "text": "direct hello"},
                }

                saved_channel = controller._capture_chat_from_packet(iface, map_state, packet_channel)
                saved_direct = controller._capture_chat_from_packet(iface, map_state, packet_direct)
                self.assertIsNotNone(saved_channel)
                self.assertIsNotNone(saved_direct)
                self.assertEqual((saved_channel or {}).get("message_type"), "channel")
                self.assertEqual((saved_direct or {}).get("message_type"), "direct")
                self.assertEqual((saved_direct or {}).get("peer_node_num"), 42)

                channel_messages = store.list_chat_messages(
                    "test:chat-packets",
                    recipient_kind="channel",
                    recipient_id=1,
                )
                direct_messages = store.list_chat_messages(
                    "test:chat-packets",
                    recipient_kind="direct",
                    recipient_id=42,
                )
                self.assertEqual(len(channel_messages), 1)
                self.assertEqual(len(direct_messages), 1)
                self.assertEqual(channel_messages[0].get("text"), "channel hello")
                self.assertEqual(direct_messages[0].get("text"), "direct hello")
            finally:
                store.close()

    def test_snapshot_includes_traceroute_control_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                map_state = MapState(store=store, mesh_host="test:queue")
                store.enqueue_traceroute_target("test:queue", 42)
                store.enqueue_traceroute_target("test:queue", 99)
                chat_iface = _DummyChatInterface(local_num=10)
                chat_iface.localNode.channels = [
                    {"index": 0, "role": "PRIMARY", "settings": {"name": "LongFast"}},
                    {"index": 1, "role": "SECONDARY", "settings": {"name": "Ops"}},
                    {"index": 2, "role": "DISABLED", "settings": {"name": "Disabled"}},
                ]
                with controller._lock:
                    controller._map_state = map_state
                    controller._current_traceroute_node_num = 55
                    controller._interface = chat_iface

                snap = controller.snapshot()
                control = snap.get("traceroute_control")
                self.assertIsInstance(control, dict)
                self.assertEqual(control.get("running_node_num"), 55)
                self.assertEqual(control.get("queued_node_nums"), [42, 99])
                queue_entries = control.get("queue_entries") or []
                self.assertEqual(len(queue_entries), 2)
                self.assertEqual(queue_entries[0].get("node_num"), 42)
                self.assertEqual(queue_entries[1].get("node_num"), 99)
                chat = snap.get("chat")
                self.assertIsInstance(chat, dict)
                self.assertIn(0, list(chat.get("channels") or []))
                self.assertIn(1, list(chat.get("channels") or []))
                channel_names = chat.get("channel_names") or {}
                self.assertEqual(channel_names.get("0"), "LongFast")
                self.assertEqual(channel_names.get("1"), "Ops")
                self.assertNotIn("2", channel_names)
            finally:
                store.close()

    def test_remove_traceroute_queue_entry_rejects_running_and_removes_queued(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                _set_connected_state(controller, store, local_num=77)
                first = store.enqueue_traceroute_target("test:queue", 100)
                second = store.enqueue_traceroute_target("test:queue", 101)
                self.assertIsNotNone(first)
                self.assertIsNotNone(second)
                running = store.pop_next_queued_traceroute("test:queue")
                self.assertIsNotNone(running)

                ok, detail = controller.remove_traceroute_queue_entry(int(running.get("queue_id") or -1))
                self.assertFalse(ok)
                self.assertIn("running", detail)

                ok, detail = controller.remove_traceroute_queue_entry(int(second.get("queue_id") or -1))
                self.assertTrue(ok, detail)
                remaining = store.list_traceroute_queue("test:queue")
                self.assertEqual(len(remaining), 1)
                self.assertEqual(remaining[0].get("queue_id"), running.get("queue_id"))
                self.assertEqual(remaining[0].get("status"), "running")
            finally:
                store.close()

    def test_reset_database_clears_db_and_disconnects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                map_state = MapState(store=store, mesh_host="test:reset")
                store.set_runtime_config({"interval": 5, "heard_window": 120, "hop_limit": 7})
                store.upsert_node(
                    "test:reset",
                    {
                        "num": 42,
                        "id": "!n42",
                        "long_name": "Node 42",
                        "short_name": "N42",
                        "lat": 52.0,
                        "lon": -2.0,
                    },
                )
                store.add_traceroute(
                    "test:reset",
                    {
                        "captured_at_utc": "2026-01-01 00:00:00 UTC",
                        "packet": {
                            "from": {"num": 42},
                            "to": {"num": 1},
                        },
                        "route_towards_destination": [
                            {"node": {"num": 1}},
                            {"node": {"num": 42}},
                        ],
                        "route_back_to_origin": [
                            {"node": {"num": 42}},
                            {"node": {"num": 1}},
                        ],
                        "raw": {},
                    },
                )
                store.enqueue_traceroute_target("test:reset", 42)
                with controller._lock:
                    controller._interface = _DummyInterface(local_num=1)
                    controller._map_state = map_state
                    controller._connection_state = "connected"
                    controller._connected_host = "192.168.1.50"

                ok, detail = controller.reset_database()
                self.assertTrue(ok, detail)

                self.assertIsNone(store.get_runtime_config())
                nodes, traces = store.snapshot("test:reset", max_traces=10)
                self.assertEqual(nodes, [])
                self.assertEqual(traces, [])
                self.assertEqual(store.list_traceroute_queue("test:reset"), [])

                snap = controller.snapshot()
                self.assertFalse(bool(snap.get("connected")))
                self.assertEqual(str(snap.get("connection_state") or ""), "disconnected")
                self.assertEqual(list(snap.get("nodes") or []), [])
                self.assertEqual(list(snap.get("traces") or []), [])
            finally:
                store.close()

    def test_snapshot_revision_increments_when_manual_queue_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                _set_connected_state(controller, store, local_num=77)

                rev_before = int(controller.snapshot().get("snapshot_revision") or 0)
                ok, detail = controller.run_traceroute(88)
                self.assertTrue(ok, detail)
                rev_after = int(controller.snapshot().get("snapshot_revision") or 0)
                self.assertGreater(rev_after, rev_before)
            finally:
                store.close()

    def test_wait_for_snapshot_revision_unblocks_after_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                since = int(controller.snapshot().get("snapshot_revision") or 0)

                def bump() -> None:
                    time.sleep(0.05)
                    controller.set_discovery_enabled(False)

                thread = threading.Thread(target=bump, daemon=True)
                thread.start()
                next_revision = controller.wait_for_snapshot_revision(since, timeout=1.0)
                thread.join(timeout=1.0)
                self.assertGreater(next_revision, since)
            finally:
                store.close()

    def test_map_state_updates_telemetry_from_packet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                map_state = MapState(store=store, mesh_host="test:telemetry")
                iface = _DummyInterface(local_num=1)
                iface.nodesByNum = {
                    42: {
                        "num": 42,
                        "id": "!node42",
                        "user": {"id": "!node42", "longName": "Node 42", "shortName": "N42"},
                        "deviceMetrics": {"batteryLevel": 73, "voltage": 3.9},
                        "environmentMetrics": {"temperature": 19.5},
                    }
                }

                map_state.update_node_from_num(iface, 42)
                changed = map_state.update_telemetry_from_packet(
                    iface,
                    {
                        "from": 42,
                        "decoded": {
                            "telemetry": {
                                "deviceMetrics": {"batteryLevel": 70},
                                "environmentMetrics": {"temperature": 18.0},
                            }
                        },
                    },
                )
                self.assertTrue(changed)

                nodes, _traces = store.snapshot("test:telemetry", max_traces=10)
                self.assertEqual(len(nodes), 1)
                node = nodes[0]
                self.assertEqual(node.get("device_telemetry", {}).get("batteryLevel"), 73)
                self.assertEqual(node.get("device_telemetry", {}).get("voltage"), 3.9)
                self.assertEqual(node.get("environment_telemetry", {}).get("temperature"), 19.5)
                self.assertTrue(bool(node.get("device_telemetry_updated_at_utc")))
                self.assertTrue(bool(node.get("environment_telemetry_updated_at_utc")))
            finally:
                store.close()

    def test_map_state_updates_node_info_from_packet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                map_state = MapState(store=store, mesh_host="test:nodeinfo")
                iface = _DummyInterface(local_num=1)
                changed = map_state.update_node_info_from_packet(
                    iface,
                    {
                        "from": 42,
                        "rxTime": 1739740000,
                        "decoded": {
                            "user": {
                                "id": "!node42",
                                "longName": "Node 42",
                                "shortName": "N42",
                                "hwModel": "TBEAM",
                            }
                        },
                    },
                )
                self.assertTrue(changed)

                nodes, _traces = store.snapshot("test:nodeinfo", max_traces=10)
                self.assertEqual(len(nodes), 1)
                node = nodes[0]
                self.assertEqual(node.get("num"), 42)
                self.assertEqual(node.get("id"), "!node42")
                self.assertEqual(node.get("long_name"), "Node 42")
                self.assertEqual(node.get("short_name"), "N42")
                self.assertEqual(node.get("hw_model"), "TBEAM")
                self.assertEqual(node.get("last_heard"), 1739740000.0)
            finally:
                store.close()

    def test_map_state_updates_position_from_packet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                map_state = MapState(store=store, mesh_host="test:position")
                iface = _DummyInterface(local_num=1)
                changed = map_state.update_position_from_packet(
                    iface,
                    {
                        "from": 88,
                        "rxTime": 1739740500,
                        "decoded": {
                            "position": {
                                "latitudeI": 515001234,
                                "longitudeI": -1234567,
                            }
                        },
                    },
                )
                self.assertTrue(changed)

                nodes, _traces = store.snapshot("test:position", max_traces=10)
                self.assertEqual(len(nodes), 1)
                node = nodes[0]
                self.assertEqual(node.get("num"), 88)
                self.assertAlmostEqual(float(node.get("lat") or 0.0), 51.5001234, places=6)
                self.assertAlmostEqual(float(node.get("lon") or 0.0), -0.1234567, places=6)
                self.assertEqual(node.get("last_heard"), 1739740500.0)
            finally:
                store.close()

    def test_map_state_updates_position_from_object_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                map_state = MapState(store=store, mesh_host="test:position-object")
                iface = _DummyInterface(local_num=1)
                changed = map_state.update_position_from_packet(
                    iface,
                    {
                        "from": 77,
                        "rxTime": 1739740600,
                        "decoded": {
                            "position": _DummyPositionObject(
                                latitude_i=515001234,
                                longitude_i=-1234567,
                            )
                        },
                    },
                )
                self.assertTrue(changed)

                nodes, _traces = store.snapshot("test:position-object", max_traces=10)
                self.assertEqual(len(nodes), 1)
                node = nodes[0]
                self.assertEqual(node.get("num"), 77)
                self.assertAlmostEqual(float(node.get("lat") or 0.0), 51.5001234, places=6)
                self.assertAlmostEqual(float(node.get("lon") or 0.0), -0.1234567, places=6)
                position_payload = node.get("position") or {}
                self.assertEqual(position_payload.get("latitudeI"), 515001234)
                self.assertEqual(position_payload.get("longitudeI"), -1234567)
                self.assertEqual(position_payload.get("altitude"), 100)
            finally:
                store.close()

    def test_switching_to_automatic_wakes_worker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                wake_event = _set_connected_state(controller, store, local_num=1)

                ok, detail = controller.set_config({"traceroute_behavior": "manual"})
                self.assertTrue(ok, detail)
                wake_event.clear()

                ok, detail = controller.set_config({"traceroute_behavior": "automatic"})
                self.assertTrue(ok, detail)
                self.assertTrue(wake_event.is_set())
            finally:
                store.close()

    def test_manual_mode_worker_does_not_auto_pick_targets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                # Explicitly set mode to manual to ensure the behavior under test.
                ok, detail = controller.set_config({"traceroute_behavior": "manual"})
                self.assertTrue(ok, detail)

                iface = _DummyTraceInterface(local_num=1)
                map_state = MapState(store=store, mesh_host="test:manual")
                traceroute_capture = {"result": None}
                stop_event = threading.Event()
                wake_event = threading.Event()

                worker = threading.Thread(
                    target=controller._traceroute_worker,
                    args=(iface, map_state, traceroute_capture, stop_event, wake_event, "test-host"),
                    daemon=True,
                )
                worker.start()
                time.sleep(0.08)
                stop_event.set()
                wake_event.set()
                worker.join(timeout=1.0)

                self.assertEqual(iface.trace_calls, [])
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
