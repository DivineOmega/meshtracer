# meshtracer

`meshtracer.py` connects to a Meshtastic node over TCP (WiFi/Ethernet), then continuously:

1. Selects a random node heard within a recent time window (default: 2 hours)
2. Runs a traceroute to that node
3. Prints human-readable traceroute output to the terminal
4. Optionally POSTs structured JSON for completed traceroutes to a webhook URL

## Requirements

- Python 3.10+
- A Meshtastic node reachable by IP (for example `192.168.x.x`)
- `meshtastic` Python package (installed via `requirements.txt`)

## Setup

```bash
cd /path/to/meshtracer
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Quick Start

Run with defaults:

```bash
source .venv/bin/activate
python meshtracer.py <NODE_IP>
```

Stop with `Ctrl+C`.

## CLI Reference

```bash
python meshtracer.py <host> [options]
```

Arguments:

- `host` (required): IP address or hostname of your Meshtastic node

Options:

- `--interval <minutes>`
  - Default: `5`
  - Minutes between traceroute attempts (start-to-start cadence target)
- `--heard-window <minutes>`
  - Default: `120` (2 hours)
  - Only nodes heard within this window are eligible
- `--hop-limit <int>`
  - Default: `7`
  - Hop limit passed to Meshtastic traceroute
- `--webhook-url <url>`
  - Default: not set
  - If set, completed traceroutes are sent as JSON via HTTP POST
- `--webhook-api-token <token>`
  - Default: not set
  - Optional token sent in request headers when webhook is enabled

## Runtime Behavior

- The script connects once at startup using `meshtastic.tcp_interface.TCPInterface`.
- Each cycle:
  - Picks one random node from `nodesByNum` heard in the last window.
  - Excludes local node from candidates.
  - Sends traceroute.
  - Prints completion or timeout/failure.
  - Sleeps for remaining interval time.
- If no eligible nodes are heard recently, it logs that and waits until next cycle.

## Traceroute Wait Behavior

The script automatically derives Meshtastic internal timeout from interval and hop limit:

- `effective_timeout = max(1, ((interval_minutes * 60) - 1) // hop_limit)`

This is assigned to `interface._timeout.expireTimeout`.

With defaults (`interval=5` minutes, `hop_limit=7`):

- `effective_timeout = 42`
- Approx max traceroute wait is `42 * 7 = 294` seconds
- Remaining time is used to maintain ~5 minute cadence

## Webhook Integration

Webhook POST is attempted only after a traceroute completes and a response payload is parsed.

### Webhook request

- Method: `POST`
- Content-Type: `application/json`
- User-Agent: `meshtracer/1.0`

If `--webhook-api-token` is set, these headers are added:

- `Authorization: Bearer <token>`
- `X-API-Token: <token>`

Header names are case-insensitive on HTTP receivers.

### Webhook payload JSON format

Top-level object:

- `event` (`string`)
- `sent_at_utc` (`string`, UTC timestamp)
- `mesh_host` (`string`)
- `interval_minutes` (`number`)
- `interval_seconds` (`number`)
- `hop_limit` (`number`)
- `selected_target` (`NodeRecord`)
- `selected_target_last_heard_age_seconds` (`number`)
- `eligible_candidate_count` (`number`)
- `traceroute` (`TracerouteResult`)

`NodeRecord`:

- `num` (`number`)
- `id` (`string|null`)
- `long_name` (`string|null`)
- `short_name` (`string|null`)

`TracerouteResult`:

- `captured_at_utc` (`string`, UTC timestamp)
- `packet`:
  - `from` (`NodeRecord`)
  - `to` (`NodeRecord`)
- `route_towards_destination` (`Hop[]`)
- `route_back_to_origin` (`Hop[]|null`)
- `raw`:
  - `route` (`number[]`)
  - `snr_towards_raw` (`number[]`)
  - `route_back` (`number[]`)
  - `snr_back_raw` (`number[]`)

`Hop`:

- `node` (`NodeRecord`)
- `snr_db` (`number|null`)

Notes:

- Unknown SNR values are represented as `null` for `snr_db`.
- `route_back_to_origin` may be `null` when reverse route info is unavailable.

### Example payload

```json
{
  "event": "meshtastic_traceroute_complete",
  "sent_at_utc": "2026-02-13 00:00:00 UTC",
  "mesh_host": "MESH_NODE_IP",
  "interval_minutes": 5,
  "interval_seconds": 300,
  "hop_limit": 3,
  "selected_target": {
    "num": 1234567890,
    "id": "!abcdef01",
    "long_name": "Target Node",
    "short_name": "TRGT"
  },
  "selected_target_last_heard_age_seconds": 742.2,
  "eligible_candidate_count": 42,
  "traceroute": {
    "captured_at_utc": "2026-02-13 00:00:03 UTC",
    "packet": {
      "from": {
        "num": 1234567890,
        "id": "!abcdef01",
        "long_name": "Target Node",
        "short_name": "TRGT"
      },
      "to": {
        "num": 987654321,
        "id": "!1234abcd",
        "long_name": "Origin Node",
        "short_name": "ORIG"
      }
    },
    "route_towards_destination": [
      {
        "node": {
          "num": 987654321,
          "id": "!1234abcd",
          "long_name": "Origin Node",
          "short_name": "ORIG"
        },
        "snr_db": -11.5
      },
      {
        "node": {
          "num": 1234567890,
          "id": "!abcdef01",
          "long_name": "Target Node",
          "short_name": "TRGT"
        },
        "snr_db": 7.25
      }
    ],
    "route_back_to_origin": [
      {
        "node": {
          "num": 1234567890,
          "id": "!abcdef01",
          "long_name": "Target Node",
          "short_name": "TRGT"
        },
        "snr_db": 6.75
      },
      {
        "node": {
          "num": 192837465,
          "id": "!deadbeef",
          "long_name": "Relay Node",
          "short_name": "RLY1"
        },
        "snr_db": 2.5
      },
      {
        "node": {
          "num": 987654321,
          "id": "!1234abcd",
          "long_name": "Origin Node",
          "short_name": "ORIG"
        },
        "snr_db": -9.0
      }
    ],
    "raw": {
      "route": [],
      "snr_towards_raw": [-46, 29],
      "route_back": [192837465],
      "snr_back_raw": [27, 10, -36]
    }
  }
}
```

## Example Commands

Basic run:

```bash
python meshtracer.py <NODE_IP>
```

Custom interval (minutes) and hop limit:

```bash
python meshtracer.py <NODE_IP> --interval 2 --hop-limit 5
```

With webhook:

```bash
python meshtracer.py <NODE_IP> \
  --webhook-url https://example.com/meshtastic/traceroute \
  --webhook-api-token YOUR_TOKEN
```

## Troubleshooting

Missing dependency:

- Ensure you installed from the same interpreter/venv you use to run.
- Recommended run path:

```bash
source .venv/bin/activate
pip install -r requirements.txt
python meshtracer.py <NODE_IP>
```

Traceroute timeout messages:

- Timeouts are expected for unreachable nodes or weak links.
- The script continues and retries next cycle.

Webhook not delivered:

- Verify `--webhook-url` is reachable from where the script runs.
- Check receiver logs and token validation.
- The script logs HTTP status or request failure reason.
