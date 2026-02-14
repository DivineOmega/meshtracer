from __future__ import annotations

import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Connect to a Meshtastic node over TCP and run a traceroute every "
            "interval minutes to a random node heard recently."
        )
    )
    parser.add_argument(
        "host",
        help="IP address or hostname of your WiFi-connected Meshtastic node",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Minutes between traceroute attempts (default: 5)",
    )
    parser.add_argument(
        "--heard-window",
        type=int,
        default=120,
        help="Only trace nodes heard within this many minutes (default: 120)",
    )
    parser.add_argument(
        "--hop-limit",
        type=int,
        default=7,
        help="Hop limit for traceroute packets (default: 7)",
    )
    parser.add_argument(
        "--webhook-url",
        default=None,
        help="Optional URL to POST structured JSON when a traceroute completes.",
    )
    parser.add_argument(
        "--webhook-api-token",
        default=None,
        help="Optional API token sent in Authorization and X-API-Token headers.",
    )
    parser.add_argument(
        "--serve-map",
        action="store_true",
        help="Serve a live map UI and JSON API from this process.",
    )
    parser.add_argument(
        "--map-host",
        default="127.0.0.1",
        help="Bind address for the map web server (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--map-port",
        type=int,
        default=8090,
        help="Port for the map web server when --serve-map is enabled (default: 8090)",
    )
    parser.add_argument(
        "--db-path",
        default="meshtracer.db",
        help="SQLite database file path for node/traceroute history (default: meshtracer.db)",
    )
    parser.add_argument(
        "--max-map-traces",
        type=int,
        default=800,
        help="Max completed traces exposed by /api/map (default: 800)",
    )
    parser.add_argument(
        "--max-stored-traces",
        type=int,
        default=50000,
        help="Max completed traces kept per connected node in SQLite (default: 50000, 0 disables pruning)",
    )
    return parser.parse_args()
