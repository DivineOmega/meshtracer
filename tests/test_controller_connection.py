from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from meshtracer_app.app import MeshTracerController
from meshtracer_app import controller_connection
from meshtracer_app.state import RuntimeLogBuffer
from meshtracer_app.storage import SQLiteStore

from controller_test_utils import _args


class _DummyLocalNode:
    def __init__(self, node_num: int | None) -> None:
        self.nodeNum = node_num


class _FakeInterface:
    def __init__(self) -> None:
        self.localNode = _DummyLocalNode(None)
        self.nodesByNum = {}
        self.onResponseTraceRoute = None

    def close(self) -> None:
        return


class _DummyThread:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self._alive = False
        self.args = args
        self.kwargs = kwargs

    def start(self) -> None:
        self._alive = True

    def is_alive(self) -> bool:
        return self._alive

    def join(self, timeout: float | None = None) -> None:
        self._alive = False


class ControllerConnectionTests(unittest.TestCase):
    def test_parse_connection_target_defaults_to_tcp(self) -> None:
        transport, endpoint, target = MeshTracerController._parse_connection_target("192.168.1.50")
        self.assertEqual(transport, "tcp")
        self.assertEqual(endpoint, "192.168.1.50")
        self.assertEqual(target, "192.168.1.50")

    def test_parse_connection_target_supports_explicit_tcp_and_ble(self) -> None:
        transport, endpoint, target = MeshTracerController._parse_connection_target("tcp://meshtastic.local")
        self.assertEqual(transport, "tcp")
        self.assertEqual(endpoint, "meshtastic.local")
        self.assertEqual(target, "meshtastic.local")

        transport, endpoint, target = MeshTracerController._parse_connection_target(
            "ble://AA:BB:CC:DD:EE:FF"
        )
        self.assertEqual(transport, "ble")
        self.assertEqual(endpoint, "AA:BB:CC:DD:EE:FF")
        self.assertEqual(target, "ble://AA:BB:CC:DD:EE:FF")

    def test_parse_connection_target_allows_ble_without_identifier(self) -> None:
        transport, endpoint, target = MeshTracerController._parse_connection_target("ble://")
        self.assertEqual(transport, "ble")
        self.assertIsNone(endpoint)
        self.assertEqual(target, "ble://")

    def test_parse_connection_target_rejects_unknown_scheme(self) -> None:
        with self.assertRaises(ValueError):
            MeshTracerController._parse_connection_target("serial://ttyUSB0")

    def test_connect_uses_tcp_interface_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            controller = MeshTracerController(
                args=_args(db_path=str(db_path)),
                store=store,
                log_buffer=RuntimeLogBuffer(),
                emit=lambda _message: None,
                emit_error=lambda _message: None,
            )
            try:
                tcp_hosts: list[str] = []
                ble_addresses: list[str | None] = []
                mesh_pb2_mod = SimpleNamespace()

                def tcp_ctor(*, hostname: str) -> _FakeInterface:
                    tcp_hosts.append(hostname)
                    return _FakeInterface()

                def ble_ctor(*, address: str | None) -> _FakeInterface:
                    ble_addresses.append(address)
                    return _FakeInterface()

                def import_stub(module_name: str) -> object:
                    if module_name == "meshtastic.protobuf.mesh_pb2":
                        return mesh_pb2_mod
                    if module_name == "meshtastic.tcp_interface":
                        return SimpleNamespace(TCPInterface=tcp_ctor)
                    if module_name == "meshtastic.ble_interface":
                        return SimpleNamespace(BLEInterface=ble_ctor)
                    raise ModuleNotFoundError(module_name)

                with (
                    mock.patch.object(
                        controller_connection,
                        "importlib",
                        SimpleNamespace(import_module=import_stub),
                    ),
                    mock.patch("meshtracer_app.controller_connection.threading.Thread", _DummyThread),
                    mock.patch.object(controller, "_setup_pubsub", return_value=(None, [])),
                ):
                    ok, detail = controller.connect("192.168.4.20")

                self.assertTrue(ok, detail)
                self.assertEqual(detail, "connected")
                self.assertEqual(tcp_hosts, ["192.168.4.20"])
                self.assertEqual(ble_addresses, [])
                snap = controller.snapshot()
                self.assertEqual(snap.get("connection_state"), "connected")
                self.assertEqual(snap.get("connected_host"), "192.168.4.20")
            finally:
                controller.shutdown()
                store.close()

    def test_connect_uses_ble_interface_for_ble_scheme(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            controller = MeshTracerController(
                args=_args(db_path=str(db_path)),
                store=store,
                log_buffer=RuntimeLogBuffer(),
                emit=lambda _message: None,
                emit_error=lambda _message: None,
            )
            try:
                tcp_hosts: list[str] = []
                ble_addresses: list[str | None] = []
                mesh_pb2_mod = SimpleNamespace()

                def tcp_ctor(*, hostname: str) -> _FakeInterface:
                    tcp_hosts.append(hostname)
                    return _FakeInterface()

                def ble_ctor(*, address: str | None) -> _FakeInterface:
                    ble_addresses.append(address)
                    return _FakeInterface()

                def import_stub(module_name: str) -> object:
                    if module_name == "meshtastic.protobuf.mesh_pb2":
                        return mesh_pb2_mod
                    if module_name == "meshtastic.tcp_interface":
                        return SimpleNamespace(TCPInterface=tcp_ctor)
                    if module_name == "meshtastic.ble_interface":
                        return SimpleNamespace(BLEInterface=ble_ctor)
                    raise ModuleNotFoundError(module_name)

                with (
                    mock.patch.object(
                        controller_connection,
                        "importlib",
                        SimpleNamespace(import_module=import_stub),
                    ),
                    mock.patch("meshtracer_app.controller_connection.threading.Thread", _DummyThread),
                    mock.patch.object(controller, "_setup_pubsub", return_value=(None, [])),
                ):
                    ok, detail = controller.connect("ble://AA:BB:CC:DD:EE:FF")

                self.assertTrue(ok, detail)
                self.assertEqual(detail, "connected")
                self.assertEqual(tcp_hosts, [])
                self.assertEqual(ble_addresses, ["AA:BB:CC:DD:EE:FF"])
                snap = controller.snapshot()
                self.assertEqual(snap.get("connection_state"), "connected")
                self.assertEqual(snap.get("connected_host"), "ble://AA:BB:CC:DD:EE:FF")
            finally:
                controller.shutdown()
                store.close()

    def test_connect_rejects_unknown_scheme(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            controller = MeshTracerController(
                args=_args(db_path=str(db_path)),
                store=store,
                log_buffer=RuntimeLogBuffer(),
                emit=lambda _message: None,
                emit_error=lambda _message: None,
            )
            try:
                ok, detail = controller.connect("serial://ttyUSB0")
                self.assertFalse(ok)
                self.assertIn("unsupported connection scheme", detail)
                snap = controller.snapshot()
                self.assertEqual(snap.get("connection_state"), "disconnected")
            finally:
                controller.shutdown()
                store.close()

    def test_connect_ble_without_identifier_passes_none_address(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            controller = MeshTracerController(
                args=_args(db_path=str(db_path)),
                store=store,
                log_buffer=RuntimeLogBuffer(),
                emit=lambda _message: None,
                emit_error=lambda _message: None,
            )
            try:
                ble_addresses: list[str | None] = []

                def ble_ctor(*, address: str | None) -> _FakeInterface:
                    ble_addresses.append(address)
                    return _FakeInterface()

                def import_stub(module_name: str) -> object:
                    if module_name == "meshtastic.protobuf.mesh_pb2":
                        return SimpleNamespace()
                    if module_name == "meshtastic.tcp_interface":
                        return SimpleNamespace(TCPInterface=lambda **_kwargs: _FakeInterface())
                    if module_name == "meshtastic.ble_interface":
                        return SimpleNamespace(BLEInterface=ble_ctor)
                    raise ModuleNotFoundError(module_name)

                with (
                    mock.patch.object(
                        controller_connection,
                        "importlib",
                        SimpleNamespace(import_module=import_stub),
                    ),
                    mock.patch("meshtracer_app.controller_connection.threading.Thread", _DummyThread),
                    mock.patch.object(controller, "_setup_pubsub", return_value=(None, [])),
                ):
                    ok, detail = controller.connect("ble://")

                self.assertTrue(ok, detail)
                self.assertEqual(detail, "connected")
                self.assertEqual(ble_addresses, [None])
                snap = controller.snapshot()
                self.assertEqual(snap.get("connected_host"), "ble://")
            finally:
                controller.shutdown()
                store.close()


if __name__ == "__main__":
    unittest.main()
