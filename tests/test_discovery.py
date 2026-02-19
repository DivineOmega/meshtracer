from __future__ import annotations

import ipaddress
import unittest
from unittest import mock

from meshtracer_app import discovery


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
            ):
                discoverer._perform_scan()
            snapshot = discoverer.snapshot()
            self.assertEqual(int(snapshot.get("progress_total") or 0), 2)
            self.assertEqual(int(snapshot.get("progress_done") or 0), 2)
            self.assertFalse(bool(snapshot.get("scanning")))
            self.assertGreaterEqual(len(change_events), 3)
        finally:
            discoverer.stop()


if __name__ == "__main__":
    unittest.main()
