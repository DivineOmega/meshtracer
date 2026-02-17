from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace

from meshtracer_app.app import MeshTracerController
from meshtracer_app.state import RuntimeLogBuffer
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


def _args(**overrides: object) -> SimpleNamespace:
    base: dict[str, object] = {
        "interval": None,
        "heard_window": None,
        "fresh_window": None,
        "mid_window": None,
        "hop_limit": None,
        "max_map_traces": None,
        "max_stored_traces": None,
        "webhook_url": None,
        "webhook_api_token": None,
        "web_ui": True,
        "db_path": "meshtracer.db",
        "map_host": "127.0.0.1",
        "map_port": 8090,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


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
                        "max_map_traces": 800,
                        "max_stored_traces": 50000,
                    }
                )
                controller = MeshTracerController(
                    args=_args(
                        interval=5,
                        heard_window=120,
                        hop_limit=7,
                        max_map_traces=800,
                        max_stored_traces=50000,
                        db_path=str(db_path),
                    ),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                self.assertEqual(controller.get_config().get("interval"), 5)
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
                        "max_map_traces": 800,
                        "max_stored_traces": 50000,
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
                wake_event = threading.Event()
                with controller._lock:
                    controller._interface = _DummyInterface(local_num=99)
                    controller._worker_thread = _DummyWorker()
                    controller._worker_wake = wake_event
                    controller._connection_state = "connected"

                ok, detail = controller.run_traceroute(42)
                self.assertTrue(ok, detail)
                self.assertEqual(detail, "queued traceroute to node #42 (position 1)")
                self.assertEqual(controller._manual_target_queue, [42])
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
                with controller._lock:
                    controller._interface = _DummyInterface(local_num=77)
                    controller._worker_thread = _DummyWorker()
                    controller._worker_wake = threading.Event()
                    controller._connection_state = "connected"

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
                with controller._lock:
                    controller._interface = _DummyInterface(local_num=77)
                    controller._worker_thread = _DummyWorker()
                    controller._worker_wake = threading.Event()
                    controller._connection_state = "connected"

                ok, detail = controller.run_traceroute(88)
                self.assertTrue(ok, detail)
                self.assertEqual(detail, "queued traceroute to node #88 (position 1)")

                ok, detail = controller.run_traceroute(88)
                self.assertTrue(ok, detail)
                self.assertEqual(detail, "traceroute already queued for node #88 (position 1)")
                self.assertEqual(controller._manual_target_queue, [88])

                with controller._lock:
                    controller._manual_target_queue = []
                    controller._current_traceroute_node_num = 88
                ok, detail = controller.run_traceroute(88)
                self.assertTrue(ok, detail)
                self.assertEqual(detail, "traceroute already running for node #88")
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
                with controller._lock:
                    controller._manual_target_queue = [42, 99]
                    controller._current_traceroute_node_num = 55

                snap = controller.snapshot()
                control = snap.get("traceroute_control")
                self.assertIsInstance(control, dict)
                self.assertEqual(control.get("running_node_num"), 55)
                self.assertEqual(control.get("queued_node_nums"), [42, 99])
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
