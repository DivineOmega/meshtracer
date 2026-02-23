from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from meshtracer_app.app import MeshTracerController
from meshtracer_app.state import RuntimeLogBuffer
from meshtracer_app.storage import SQLiteStore

from controller_test_utils import (
    _DummyTelemetryInterface,
    _DummyTelemetryMessage,
    _DummyWorker,
    _args,
    _set_connected_state,
)


class ControllerConfigAndRequestTests(unittest.TestCase):
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

    def test_set_config_validates_traceroute_visual_style(self) -> None:
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

                ok, detail = controller.set_config({"traceroute_visual_style": "signal"})
                self.assertTrue(ok, detail)
                self.assertEqual(controller.get_config().get("traceroute_visual_style"), "signal")

                persisted = store.get_runtime_config("global") or {}
                self.assertEqual(persisted.get("traceroute_visual_style"), "signal")

                ok, detail = controller.set_config({"traceroute_visual_style": "invalid"})
                self.assertFalse(ok)
                self.assertIn("traceroute_visual_style", detail)
                self.assertEqual(controller.get_config().get("traceroute_visual_style"), "signal")
            finally:
                store.close()

    def test_chat_notification_settings_are_config_backed_and_persisted(self) -> None:
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

                cfg = controller.get_config()
                self.assertFalse(bool(cfg.get("chat_notification_desktop")))
                self.assertFalse(bool(cfg.get("chat_notification_sound")))
                self.assertFalse(bool(cfg.get("chat_notification_notify_focused")))

                ok, detail = controller.set_config(
                    {
                        "chat_notification_desktop": True,
                        "chat_notification_sound": True,
                        "chat_notification_notify_focused": True,
                    }
                )
                self.assertTrue(ok, detail)

                updated = controller.get_config()
                self.assertTrue(bool(updated.get("chat_notification_desktop")))
                self.assertTrue(bool(updated.get("chat_notification_sound")))
                self.assertTrue(bool(updated.get("chat_notification_notify_focused")))

                persisted = store.get_runtime_config("global") or {}
                self.assertTrue(bool(persisted.get("chat_notification_desktop")))
                self.assertTrue(bool(persisted.get("chat_notification_sound")))
                self.assertTrue(bool(persisted.get("chat_notification_notify_focused")))
            finally:
                store.close()

    def test_set_config_rejects_invalid_chat_notification_settings(self) -> None:
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

                ok, detail = controller.set_config({"chat_notification_sound": "definitely"})
                self.assertFalse(ok)
                self.assertIn("chat_notification_sound", detail)
                self.assertFalse(bool(controller.get_config().get("chat_notification_sound")))
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
                self.assertEqual(controller.get_config().get("traceroute_visual_style"), "direction")
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
                    PowerMetrics=type("PowerMetrics", (), {}),
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

    def test_request_node_power_telemetry_uses_non_blocking_send_data(self) -> None:
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
                    PowerMetrics=type("PowerMetrics", (), {}),
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
                    ok, detail = controller.request_node_telemetry(42, "power")

                self.assertTrue(ok, detail)
                self.assertIn("requested power telemetry", detail)
                self.assertEqual(len(telemetry_iface.sent_packets), 1)
                sent = telemetry_iface.sent_packets[0]
                self.assertEqual(sent.get("destinationId"), 42)
                self.assertEqual(sent.get("portNum"), 67)
                self.assertEqual(sent.get("wantResponse"), True)
                payload = sent.get("data")
                self.assertIsNotNone(payload)
                self.assertIsNotNone(getattr(payload, "power_metrics", None))
                self.assertIsNotNone(getattr(payload.power_metrics, "last_copy", None))
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
                {"decoded": {"telemetry": {"power_metrics": {"ch1Voltage": 12.7}}}}
            ),
            ["power"],
        )
        self.assertEqual(
            MeshTracerController._telemetry_packet_types(
                {
                    "decoded": {
                        "telemetry": {
                            "device_metrics": {"batteryLevel": 50},
                            "environmentMetrics": {"temperature": 21.2},
                            "powerMetrics": {"ch1Voltage": 12.7},
                        }
                    }
                }
            ),
            ["device", "environment", "power"],
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
