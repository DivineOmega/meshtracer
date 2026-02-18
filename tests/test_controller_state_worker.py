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
    _DummyInterface,
    _DummyPositionObject,
    _DummyTraceInterface,
    _args,
    _set_connected_state,
)


class ControllerStateAndWorkerTests(unittest.TestCase):
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
                        "powerMetrics": {"ch1Voltage": 13.1},
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
                                "powerMetrics": {"ch1Voltage": 12.8},
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
                self.assertEqual(node.get("power_telemetry", {}).get("ch1Voltage"), 13.1)
                self.assertTrue(bool(node.get("device_telemetry_updated_at_utc")))
                self.assertTrue(bool(node.get("environment_telemetry_updated_at_utc")))
                self.assertTrue(bool(node.get("power_telemetry_updated_at_utc")))
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
