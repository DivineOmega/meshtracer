from __future__ import annotations

import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the Meshtracer web UI (default) and optionally connect to a "
            "Meshtastic node over TCP to continuously run traceroutes."
        )
    )
    parser.add_argument(
        "host",
        nargs="?",
        default=None,
        help=(
            "Optional IP address or hostname of your WiFi-connected Meshtastic node. "
            "If omitted, the web UI will prompt you to connect."
        ),
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help="Minutes between traceroute attempts (supports decimals; default: 5)",
    )
    parser.add_argument(
        "--heard-window",
        type=int,
        default=None,
        help="Only trace nodes heard within this many minutes (default: 120)",
    )
    parser.add_argument(
        "--hop-limit",
        type=int,
        default=None,
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
        dest="web_ui",
        action="store_true",
        default=True,
        help="Serve the web UI (default: on).",
    )
    parser.add_argument(
        "--no-web",
        dest="web_ui",
        action="store_false",
        help="Disable the web UI (run traceroutes in the terminal only).",
    )
    parser.add_argument(
        "--no-open",
        action="store_true",
        help="Do not auto-open a browser when the web UI starts.",
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
        help="Port for the web UI server (default: 8090)",
    )
    parser.add_argument(
        "--db-path",
        default="meshtracer.db",
        help="SQLite database file path for node/traceroute history (default: meshtracer.db)",
    )
    parser.add_argument(
        "--max-map-traces",
        type=int,
        default=None,
        help="Max completed traces exposed by /api/map (default: 800)",
    )
    parser.add_argument(
        "--max-stored-traces",
        type=int,
        default=None,
        help="Max completed traces kept per connected node in SQLite (default: 50000, 0 disables pruning)",
    )
    return parser.parse_args()
