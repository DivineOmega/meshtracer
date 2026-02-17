from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

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


def _args(**overrides: object) -> SimpleNamespace:
    base: dict[str, object] = {
        "traceroute_behavior": None,
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
                with controller._lock:
                    controller._map_state = map_state
                    controller._current_traceroute_node_num = 55

                snap = controller.snapshot()
                control = snap.get("traceroute_control")
                self.assertIsInstance(control, dict)
                self.assertEqual(control.get("running_node_num"), 55)
                self.assertEqual(control.get("queued_node_nums"), [42, 99])
                queue_entries = control.get("queue_entries") or []
                self.assertEqual(len(queue_entries), 2)
                self.assertEqual(queue_entries[0].get("node_num"), 42)
                self.assertEqual(queue_entries[1].get("node_num"), 99)
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
