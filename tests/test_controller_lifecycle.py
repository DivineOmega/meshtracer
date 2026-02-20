from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path

from meshtracer_app.app import MeshTracerController
from meshtracer_app.state import MapState, RuntimeLogBuffer
from meshtracer_app.storage import SQLiteStore

from controller_test_utils import (
    _DummyChatInterface,
    _DummyInterface,
    _args,
    _set_connected_state,
)


class ControllerLifecycleTests(unittest.TestCase):
    def test_snapshot_logs_include_backend_type_field(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                log_buffer = RuntimeLogBuffer()
                log_buffer.add("telemetry line", stream="stdout", log_type="telemetry")
                log_buffer.add("message line", stream="stdout", log_type="messaging")
                log_buffer.add("fallback line", stream="stdout", log_type="unexpected")
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=log_buffer,
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )

                logs = list(controller.snapshot().get("logs") or [])
                self.assertEqual(len(logs), 3)
                self.assertEqual(logs[0].get("message"), "telemetry line")
                self.assertEqual(logs[0].get("type"), "telemetry")
                self.assertEqual(logs[1].get("message"), "message line")
                self.assertEqual(logs[1].get("type"), "messaging")
                self.assertEqual(logs[2].get("message"), "fallback line")
                self.assertEqual(logs[2].get("type"), "other")
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
