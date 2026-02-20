from __future__ import annotations

import ipaddress
import time
import unittest
from types import SimpleNamespace
from unittest import mock

from meshtracer_app import discovery


class _FakeBleDevice:
    def __init__(self, *, name: str | None = None, address: str | None = None, rssi: int | None = None) -> None:
        self.name = name
        self.address = address
        self.rssi = rssi


class _DummyThread:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self._alive = False

    def start(self) -> None:
        self._alive = True

    def is_alive(self) -> bool:
        return self._alive

    def join(self, timeout: float | None = None) -> None:
        self._alive = False


class DiscoveryTests(unittest.TestCase):
    def test_guess_private_ipv4_networks_ignores_loopback(self) -> None:
        with mock.patch.object(discovery, "_primary_ipv4_address", return_value=None), mock.patch.object(
            discovery.socket,
            "getaddrinfo",
            return_value=[(None, None, None, None, ("127.0.0.1", 0))],
        ):
            nets = discovery._guess_private_ipv4_networks()
        self.assertEqual(nets, [])

    def test_guess_private_ipv4_networks_returns_24_for_private_ip(self) -> None:
        with mock.patch.object(discovery, "_primary_ipv4_address", return_value="192.168.4.23"), mock.patch.object(
            discovery.socket,
            "getaddrinfo",
            return_value=[],
        ):
            nets = discovery._guess_private_ipv4_networks()
        self.assertEqual([str(net) for net in nets], ["192.168.4.0/24"])

    def test_scan_notifies_on_change_during_progress(self) -> None:
        change_events: list[str] = []

        def on_change() -> None:
            change_events.append("changed")

        discoverer = discovery.LanDiscoverer(
            on_change=on_change,
            progress_notify_every=1,
            progress_notify_min_interval_seconds=999.0,
            scan_interval_seconds=999.0,
        )
        discoverer.set_enabled(False)
        try:
            with mock.patch.object(
                discovery,
                "_guess_private_ipv4_networks",
                return_value=[ipaddress.ip_network("192.168.77.0/30")],
            ), mock.patch.object(
                discovery,
                "_guess_private_ipv4_addresses",
                return_value=set(),
            ), mock.patch.object(
                discovery,
                "_check_tcp",
                return_value=(False, 0.01),
            ), mock.patch.object(
                discovery,
                "_discover_meshtastic_ble_candidates",
                return_value={},
            ):
                discoverer._perform_scan()
            snapshot = discoverer.snapshot()
            self.assertEqual(int(snapshot.get("progress_total") or 0), 2)
            self.assertEqual(int(snapshot.get("progress_done") or 0), 2)
            self.assertFalse(bool(snapshot.get("scanning")))
            self.assertEqual(str(snapshot.get("scan_phase") or ""), "idle")
            self.assertGreaterEqual(len(change_events), 3)
        finally:
            discoverer.stop()

    def test_discover_meshtastic_ble_candidates_normalizes_devices(self) -> None:
        ble_module = SimpleNamespace(
            BLEInterface=SimpleNamespace(
                scan=lambda: [
                    _FakeBleDevice(name="Alpha", address="AA:BB:CC:DD:EE:FF", rssi=-70),
                    _FakeBleDevice(name="Bravo", address=None, rssi=None),
                    _FakeBleDevice(name=None, address=None, rssi=None),
                ]
            )
        )
        with mock.patch.object(discovery.importlib, "import_module", return_value=ble_module):
            found = discovery._discover_meshtastic_ble_candidates()

        self.assertEqual(len(found), 2)
        first = found.get("aa:bb:cc:dd:ee:ff") or {}
        self.assertEqual(first.get("identifier"), "AA:BB:CC:DD:EE:FF")
        self.assertEqual(first.get("connect_target"), "ble://AA:BB:CC:DD:EE:FF")
        self.assertEqual(first.get("rssi"), -70)
        second = found.get("name:bravo") or {}
        self.assertEqual(second.get("identifier"), "Bravo")
        self.assertEqual(second.get("connect_target"), "ble://Bravo")

    def test_snapshot_includes_ble_candidates(self) -> None:
        with mock.patch.object(discovery.threading, "Thread", _DummyThread):
            discoverer = discovery.LanDiscoverer(max_results=10)
        try:
            with discoverer._lock:
                discoverer._found = {
                    "192.168.1.5": {
                        "host": "192.168.1.5",
                        "port": 4403,
                        "latency_ms": 8.4,
                        "last_seen_utc": "2026-02-20 00:00:00 UTC",
                        "last_seen_epoch": 10.0,
                    }
                }
                discoverer._ble_found = {
                    "aa:bb": {
                        "identifier": "AA:BB",
                        "name": "Node A",
                        "address": "AA:BB",
                        "rssi": -63,
                        "connect_target": "ble://AA:BB",
                        "last_seen_utc": "2026-02-20 00:00:01 UTC",
                        "last_seen_epoch": 11.0,
                    }
                }
                discoverer._ble_last_scan_utc = "2026-02-20 00:00:01 UTC"
            snap = discoverer.snapshot()
            tcp_candidates = snap.get("candidates") or []
            ble_candidates = snap.get("ble_candidates") or []
            self.assertEqual(len(tcp_candidates), 1)
            self.assertEqual(len(ble_candidates), 1)
            self.assertEqual(str(snap.get("scan_phase") or ""), "idle")
            self.assertEqual((ble_candidates[0] or {}).get("connect_target"), "ble://AA:BB")
            self.assertEqual((ble_candidates[0] or {}).get("rssi"), -63)
            self.assertEqual(snap.get("ble_last_scan_utc"), "2026-02-20 00:00:01 UTC")
        finally:
            discoverer.stop()

    def test_perform_scan_merges_ble_results_even_without_tcp_hosts(self) -> None:
        phase_seen_during_ble_scan: list[str] = []

        def fake_ble_scan() -> dict[str, dict[str, object]]:
            if discoverer is not None:
                with discoverer._lock:
                    phase_seen_during_ble_scan.append(str(discoverer._scan_phase))
            return {
                "aa:bb": {
                    "identifier": "AA:BB",
                    "name": "Node A",
                    "address": "AA:BB",
                    "rssi": -58,
                    "connect_target": "ble://AA:BB",
                    "last_seen_utc": "2026-02-20 00:00:02 UTC",
                    "last_seen_epoch": time.time(),
                }
            }

        discoverer = None
        with (
            mock.patch.object(discovery.threading, "Thread", _DummyThread),
            mock.patch.object(discovery, "_guess_private_ipv4_networks", return_value=[]),
            mock.patch.object(discovery, "_guess_private_ipv4_addresses", return_value=set()),
            mock.patch.object(
                discovery,
                "_discover_meshtastic_ble_candidates",
                side_effect=fake_ble_scan,
            ),
        ):
            discoverer = discovery.LanDiscoverer(max_results=10)
            try:
                discoverer._perform_scan()
                snap = discoverer.snapshot()
                self.assertFalse(bool(snap.get("scanning")))
                self.assertEqual(str(snap.get("scan_phase") or ""), "idle")
                self.assertEqual(len(snap.get("candidates") or []), 0)
                self.assertEqual(len(snap.get("ble_candidates") or []), 1)
                self.assertEqual((snap.get("ble_candidates") or [{}])[0].get("identifier"), "AA:BB")
                self.assertTrue(bool(snap.get("ble_last_scan_utc")))
                self.assertIn("ble", phase_seen_during_ble_scan)
            finally:
                discoverer.stop()


if __name__ == "__main__":
    unittest.main()
