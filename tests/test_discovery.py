from __future__ import annotations

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


if __name__ == "__main__":
    unittest.main()

