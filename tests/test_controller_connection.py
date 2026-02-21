from __future__ import annotations

import asyncio
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
    @staticmethod
    def _fake_ble_module() -> SimpleNamespace:
        class _BluezNotifyAcquiredError(Exception):
            def __str__(self) -> str:
                return "[org.bluez.Error.NotPermitted] Notify acquired"

        class _FakeServices:
            def __init__(self, characteristics: dict[str, object]) -> None:
                self._characteristics = characteristics

            def get_characteristic(self, specifier: object) -> object | None:
                return self._characteristics.get(str(specifier))

        class _FakeBleakClient:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []
                self._characteristics = {
                    "char-uuid": SimpleNamespace(obj=("/org/bluez/char0", {"NotifyAcquired": True}))
                }
                self.services = _FakeServices(self._characteristics)
                self.fail_for: set[str] = set()

            def start_notify(
                self,
                *args: object,
                bluez: dict[str, object] | None = None,
                **kwargs: object,
            ) -> str:
                specifier = args[0] if args else None
                specifier_key = str(specifier or "").strip().lower()
                if specifier_key in self.fail_for:
                    raise _BluezNotifyAcquiredError()
                char = self.services.get_characteristic(specifier) if specifier is not None else None
                char_obj = getattr(char, "obj", None)
                notify_acquired = None
                if isinstance(char_obj, tuple) and len(char_obj) >= 2 and isinstance(char_obj[1], dict):
                    notify_acquired = "NotifyAcquired" in char_obj[1]
                self.calls.append(
                    {
                        "args": args,
                        "bluez": bluez,
                        "kwargs": kwargs,
                        "notify_acquired": notify_acquired,
                    }
                )
                return "ok"

        class _FakeMeshtasticBLEClient:
            def __init__(self) -> None:
                self.bleak_client = _FakeBleakClient()

            def start_notify(self, *args: object, **kwargs: object) -> object:
                return self.bleak_client.start_notify(*args, **kwargs)

        return SimpleNamespace(
            BLEClient=_FakeMeshtasticBLEClient,
            LOGRADIO_UUID="5a3d6e49-06e6-4423-9944-e9de8cdf9547",
            LEGACY_LOGRADIO_UUID="6c6fd238-78fa-436b-aacf-15c5be1ef2e2",
            FROMNUM_UUID="ed9da18c-a800-4f66-a670-aa7547e34453",
        )

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

    def test_enable_ble_start_notify_workaround_adds_bluez_kwarg_on_linux(self) -> None:
        interface_mod = self._fake_ble_module()
        ble_client_cls = interface_mod.BLEClient
        with mock.patch.object(controller_connection.sys, "platform", "linux"):
            patched = MeshTracerController._enable_ble_start_notify_workaround(interface_mod)
            patched_again = MeshTracerController._enable_ble_start_notify_workaround(interface_mod)

        self.assertTrue(patched)
        self.assertFalse(patched_again)

        client = ble_client_cls()
        client.start_notify("char-uuid", lambda *_args: None)
        call = client.bleak_client.calls[-1]
        self.assertEqual(call.get("bluez"), {"use_start_notify": True})
        self.assertEqual(call.get("notify_acquired"), False)

        client.start_notify("char-uuid", lambda *_args: None, bluez={"foo": "bar"})
        merged_call = client.bleak_client.calls[-1]
        self.assertEqual(merged_call.get("bluez"), {"foo": "bar", "use_start_notify": True})
        self.assertEqual(merged_call.get("notify_acquired"), False)

    def test_enable_ble_start_notify_workaround_is_noop_on_non_linux(self) -> None:
        interface_mod = self._fake_ble_module()
        ble_client_cls = interface_mod.BLEClient
        with mock.patch.object(controller_connection.sys, "platform", "darwin"):
            patched = MeshTracerController._enable_ble_start_notify_workaround(interface_mod)

        self.assertFalse(patched)
        client = ble_client_cls()
        client.start_notify("char-uuid", lambda *_args: None)
        call = client.bleak_client.calls[-1]
        self.assertIsNone(call.get("bluez"))
        self.assertEqual(call.get("notify_acquired"), True)

    def test_ble_workaround_ignores_notify_acquired_for_logradio_only(self) -> None:
        interface_mod = self._fake_ble_module()
        ble_client_cls = interface_mod.BLEClient
        with mock.patch.object(controller_connection.sys, "platform", "linux"):
            patched = MeshTracerController._enable_ble_start_notify_workaround(interface_mod)
        self.assertTrue(patched)

        client = ble_client_cls()
        client.bleak_client.fail_for = {str(interface_mod.LOGRADIO_UUID).lower()}

        # Optional LOGRADIO notify failure should be ignored.
        result = client.start_notify(interface_mod.LOGRADIO_UUID, lambda *_args: None)
        self.assertIsNone(result)

        # Required FROMNUM notify failure should still raise.
        client.bleak_client.fail_for = {str(interface_mod.FROMNUM_UUID).lower()}
        with self.assertRaises(Exception):
            client.start_notify(interface_mod.FROMNUM_UUID, lambda *_args: None)

    def test_enable_bluez_backend_start_notify_workaround_forces_start_notify(self) -> None:
        class _FakeBluezClient:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            async def start_notify(
                self,
                characteristic: object,
                callback: object,
                **kwargs: object,
            ) -> str:
                char_obj = getattr(characteristic, "obj", None)
                notify_acquired = None
                if isinstance(char_obj, tuple) and len(char_obj) >= 2 and isinstance(char_obj[1], dict):
                    notify_acquired = "NotifyAcquired" in char_obj[1]
                self.calls.append(
                    {
                        "notify_acquired": notify_acquired,
                        "bluez": kwargs.get("bluez"),
                    }
                )
                return "ok"

        fake_module = SimpleNamespace(BleakClientBlueZDBus=_FakeBluezClient)

        def import_stub(name: str) -> object:
            if name == "bleak.backends.bluezdbus.client":
                return fake_module
            raise ModuleNotFoundError(name)

        with (
            mock.patch.object(controller_connection.sys, "platform", "linux"),
            mock.patch.object(controller_connection.importlib, "import_module", side_effect=import_stub),
        ):
            patched = MeshTracerController._enable_bluez_backend_start_notify_workaround()
            patched_again = MeshTracerController._enable_bluez_backend_start_notify_workaround()

        self.assertTrue(patched)
        self.assertFalse(patched_again)

        characteristic = SimpleNamespace(obj=("/org/bluez/char0", {"NotifyAcquired": True}))
        client = _FakeBluezClient()
        result = asyncio.run(client.start_notify(characteristic, lambda *_args: None))
        self.assertEqual(result, "ok")
        call = client.calls[-1]
        self.assertEqual(call.get("notify_acquired"), False)
        self.assertEqual(call.get("bluez"), {"use_start_notify": True})

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
