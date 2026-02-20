# meshtracer

Meshtracer connects to a Meshtastic node over TCP (WiFi/Ethernet) or BLE (Bluetooth LE), stores mesh history in SQLite, and serves a live web UI for map/traceroute/telemetry/chat workflows.

Default web UI address: `http://127.0.0.1:8090/`

## Quick Requirements

- Python `3.10+`
- `git`
- Meshtastic node reachable by IP/hostname over TCP, or reachable over BLE.
- Python dependencies from `requirements.txt` (currently pins `meshtastic==2.7.7`).

## Quick Start

Clone + install:

```bash
git clone https://github.com/Jord-JD/meshtracer.git
cd meshtracer
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Start locally (UI bound to localhost):

```bash
source .venv/bin/activate && python meshtracer.py
```

Start locally with startup auto-connect to a Meshtastic node:

```bash
source .venv/bin/activate && python meshtracer.py <NODE_IP_OR_HOST>
```

Start locally and connect over BLE:

```bash
source .venv/bin/activate && python meshtracer.py ble://<BLE_IDENTIFIER_OR_ADDRESS>
```

Host UI for access from other devices on your LAN:

```bash
source .venv/bin/activate && python meshtracer.py --map-host 0.0.0.0 --map-port 8090 --no-open
```

Then open `http://<THIS_MACHINE_LAN_IP>:8090/` from another device on the same network.

Host UI on LAN and auto-connect to a Meshtastic node:

```bash
source .venv/bin/activate && python meshtracer.py <NODE_IP_OR_HOST> --map-host 0.0.0.0 --map-port 8090 --no-open
```

Stop with `Ctrl+C`.

## Capabilities

- Connect/disconnect to a Meshtastic node over TCP or BLE from UI or CLI startup target.
- Best-effort discovery for Meshtastic TCP LAN targets (port `4403`) and BLE targets.
- Continuous traceroute worker with configurable behavior:
  - `automatic`: periodic random eligible-node traceroutes.
  - `manual`: runs only queued traceroutes.
- Per-node traceroute queue with dedupe, queue-position reporting, and removal.
- Live map with node markers, inferred edges, trace highlighting, and node/trace drill-down.
- Node details view with tabs:
  - `Node Info`
  - `Traceroutes`
  - `Position`
  - `Telemetry` (nested `Device`, `Environment`, and `Power` tabs)
- Node actions from node details:
  - `Request Node Info`
  - `Run Traceroute`
  - `Request Position`
  - `Request Device/Environment/Power Telemetry`
  - Person button to open direct chat to selected node.
- Chat UI:
  - Channel + direct recipients.
  - Channel labels from interface channel metadata when available (including modem-preset style names such as `LongFast` for primary channel); fallback labels are used when unavailable.
  - Clickable sender node labels to jump to node details.
  - Repeated identical message text is supported.
- Runtime log stream with backend-assigned message types (`traceroute`, `telemetry`, `position`, `node_info`, `other`) and per-type filters in the Log tab.
- Traceroute log emphasis: failures are highlighted in red and completion messages are highlighted in green.
- Runtime config modal with persisted settings.
- Optional webhook POST on completed traceroutes with parsed route payload.
- Realtime updates over SSE with polling fallback.

## CLI Reference

```bash
python meshtracer.py [host] [options]
```

`host`:

- Optional when web UI is enabled.
- Required when using `--no-web`.
- Formats:
  - TCP (default): `<NODE_IP_OR_HOST>` or `tcp://<NODE_IP_OR_HOST>`
  - BLE: `ble://<BLE_IDENTIFIER_OR_ADDRESS>` (or `ble://` to connect to the first/only discoverable Meshtastic BLE device)

Options:

| Option | Default | Notes |
|---|---:|---|
| `--interval <minutes>` | `5` | Traceroute cycle interval. Float allowed. Must be `> 0`. |
| `--heard-window <minutes>` | `120` | Only nodes heard within this window are eligible for automatic mode. Must be `> 0`. |
| `--hop-limit <int>` | `7` | Hop limit used for traceroute requests. Must be `> 0`. |
| `--webhook-url <url>` | unset | Enables webhook delivery for completed traceroutes. |
| `--webhook-api-token <token>` | unset | Adds `Authorization: Bearer <token>` and `X-API-Token: <token>` headers when webhook is enabled. |
| `--serve-map` | on | Kept for compatibility; web UI is already default-on. |
| `--no-web` | off | Disable web UI and run worker in terminal mode only. |
| `--no-open` | off | Do not auto-open browser when web UI starts. |
| `--map-host <addr>` | `127.0.0.1` | Web server bind address. |
| `--map-port <port>` | `8090` | Web server port (`1-65535`). |
| `--db-path <path>` | `meshtracer.db` | SQLite database file path. |
| `--traceroute-retention-hours <int>` | `720` | Delete completed traceroutes older than this many hours. Must be `> 0`. |

## Runtime Behavior

### Partitioning and persistence

- Data is partitioned by connected mesh identity:
  - Preferred key: `node:<local_num>[:<local_id>]`
  - Fallback key: `host:<host>`
- Multiple radios/hosts can share one SQLite file without mixing histories.

### Discovery behavior

- Discovery scans likely private `/24` networks for open Meshtastic TCP port `4403`.
- Discovery also attempts a BLE scan for nearby Meshtastic peripherals.
- Discovery is enabled while disconnected in web UI mode.
- Discovery is disabled after successful connect and re-enabled on disconnect.

### Traceroute worker behavior

Runtime config key `traceroute_behavior`:

- `automatic`:
  - Runs on interval.
  - Chooses one random eligible node heard within `heard_window`.
  - Excludes local node.
- `manual`:
  - Does not run scheduled random traces.
  - Executes queued traces only.

Queue details:

- Queue is deduped per target node.
- Running entries from prior sessions are re-queued on startup.
- Queued manual traces are processed in either behavior mode.

### Runtime config (persisted)

Persisted config keys:

- `traceroute_behavior` (`automatic` or `manual`)
- `interval`
- `heard_window`
- `fresh_window`
- `mid_window`
- `hop_limit`
- `traceroute_retention_hours`
- `webhook_url`
- `webhook_api_token`

Validation rules:

- `interval > 0`
- `heard_window > 0`
- `fresh_window > 0`
- `mid_window > 0`
- `mid_window >= fresh_window`
- `hop_limit > 0`
- `traceroute_retention_hours > 0`
- `traceroute_behavior` must be `automatic` or `manual`

Security note:

- Webhook token is persisted in SQLite.
- Public config responses intentionally redact the token and expose `webhook_api_token_set`.

### Traceroute timeout derivation

Meshtracer updates Meshtastic internal timeout base per config:

- `effective_timeout = max(1, ((int(interval_minutes * 60) - 1) // hop_limit))`

With defaults (`interval=5`, `hop_limit=7`):

- `effective_timeout = 42`
- Approximate max wait = `42 * 7 = 294s`

## Web UI Overview

Main areas:

- Onboarding connect panel (host entry + discovery list + rescan).
- Sidebar tabs:
  - `Log`
  - `Nodes` (search + sort)
  - `Traces`
- Log tab supports type filters (`Traceroute`, `Telemetry`, `Position`, `Node Info`, `Other`) and remembers choices in browser local storage.
- Map canvas with node markers and trace lines.
- Top-left details panel for selected trace/node.
- Chat modal.
- Config modal (with inline help).
- Database reset action in config modal.

Node details behavior:

- Person button appears in node details header and opens direct chat to selected node.
- Telemetry tabs (`Device`, `Environment`, `Power`) are nested under top-level `Telemetry` tab.
- Position and node-info request actions are available from their respective tabs.

Chat behavior:

- Recipient groups: `Channels`, `Recently Messaged Nodes`, `Other Nodes`.
- Sender labels in incoming messages are clickable and open node details.
- Enter key sends messages.
- Message history is persisted per mesh partition.

## HTTP API

All endpoints are served by embedded web server.

### GET endpoints

- `/` or `/map`
  - UI HTML.
- `/api/map`
  - Full snapshot payload.
- `/api/events?since=<snapshot_revision>`
  - SSE stream.
  - Events: `snapshot`, `heartbeat`.
- `/api/config`
  - `{ "ok": true, "config": { ...public runtime config... } }`
- `/api/chat/messages?recipient_kind=<channel|direct>&recipient_id=<id>&limit=<n>`
  - Returns chat history for the selected recipient.
- `/healthz`
  - Basic liveness payload.

### POST endpoints

- `/api/connect`
  - Body: `{ "host": "..." }`
  - Host formats: plain TCP host/IP, `tcp://...`, or `ble://...`
- `/api/disconnect`
  - Body: `{}`
- `/api/config`
  - Body: partial runtime config update object.
- `/api/discovery/rescan`
  - Body: `{}`
- `/api/traceroute`
  - Body: `{ "node_num": <int> }`
- `/api/traceroute/queue/remove`
  - Body: `{ "queue_id": <int> }`
- `/api/chat/send`
  - Body: `{ "recipient_kind": "channel|direct", "recipient_id": <int>, "text": "..." }`
- `/api/telemetry/request`
  - Body: `{ "node_num": <int>, "telemetry_type": "device|environment|power" }`
- `/api/nodeinfo/request`
  - Body: `{ "node_num": <int> }`
- `/api/position/request`
  - Body: `{ "node_num": <int> }`
- `/api/database/reset`
  - Body: `{}`

Response pattern:

- Most POST handlers return:
  - `ok` (bool)
  - `detail` (status text)
  - `snapshot` (latest full snapshot)
- Invalid inputs usually return `400` with an error string.

### Snapshot shape (`GET /api/map`)

Top-level keys include:

- map data: `nodes`, `traces`, `edges`
- revisions: `map_revision`, `log_revision`, `snapshot_revision`
- logs: `logs`
  - Each log entry includes: `seq`, `at_utc`, `stream`, `type`, `message`
  - `type` is one of: `traceroute`, `telemetry`, `position`, `node_info`, `other`
- connection: `connected`, `connection_state`, `connected_host`, `connection_error`
- discovery: `discovery`
  - Includes: `enabled`, `scanning`, `progress_done`, `progress_total`, `port`, `networks`, `last_scan_utc`, `candidates`
  - Candidate entry fields: `host`, `port`, `latency_ms`, `last_seen_utc`
- runtime config: `config`, `config_defaults`
- server info: `server` (`db_path`, `map_host`, `map_port`)
- traceroute control: `traceroute_control` (`running_node_num`, `queued_node_nums`, `queue_entries`)
- chat metadata: `chat` (`revision`, `channels`, `channel_names`, `recent_direct_node_nums`)

## SQLite Persistence

Default DB path: `meshtracer.db`

Core tables:

- `runtime_config`
- `nodes`
- `node_telemetry`
- `node_positions`
- `traceroutes`
- `traceroute_queue`
- `chat_messages`

Behavior highlights:

- WAL mode enabled.
- Traceroute retention pruning runs on insert and config updates.
- Chat data and queue state are partitioned by `mesh_host`.

## Webhook Integration

Webhook POST is attempted only when traceroute completes and parsed traceroute payload is available.

Request:

- Method: `POST`
- Content-Type: `application/json`
- User-Agent: `meshtracer/1.0`
- Optional token headers:
  - `Authorization: Bearer <token>`
  - `X-API-Token: <token>`

Payload fields include:

- `event`
- `sent_at_utc`
- `mesh_host`
- `interval_minutes`
- `interval_seconds`
- `hop_limit`
- `selected_target`
- `selected_target_last_heard_age_seconds`
- `eligible_candidate_count`
- `trigger` (`scheduled` or `manual`)
- `traceroute`

## Project Layout

- `meshtracer.py`: launcher entrypoint
- `requirements.txt`: Python dependency pins
- `LICENSE`: license text
- `meshtracer_app/__init__.py`: package marker
- `meshtracer_app/app.py`: controller composition, startup/shutdown, snapshot wiring
- `meshtracer_app/cli.py`: CLI parsing
- `meshtracer_app/common.py`: shared helpers (timestamps, formatting)
- `meshtracer_app/controller_defaults.py`: runtime config defaults
- `meshtracer_app/controller_config.py`: runtime config + discovery control handlers
- `meshtracer_app/controller_connection.py`: connect/disconnect and inbound packet plumbing
- `meshtracer_app/controller_operations.py`: API-exposed operations and snapshot assembly
- `meshtracer_app/controller_packets.py`: packet decode/update handling
- `meshtracer_app/controller_worker.py`: traceroute worker loop and queue execution
- `meshtracer_app/map_server.py`: HTTP API + static asset serving
- `meshtracer_app/discovery.py`: LAN TCP + BLE discovery scanner
- `meshtracer_app/state.py`: map state/revisioning/log buffer
- `meshtracer_app/meshtastic_helpers.py`: Meshtastic parsing/utility helpers
- `meshtracer_app/webhook.py`: webhook delivery helper
- `meshtracer_app/storage.py`: SQLite store facade
- `meshtracer_app/storage_repo_base.py`: shared SQLite repository helpers
- `meshtracer_app/storage_runtime.py`: runtime config persistence
- `meshtracer_app/storage_snapshot.py`: snapshot reads for nodes/traces
- `meshtracer_app/storage_nodes.py`: node + telemetry + position persistence
- `meshtracer_app/storage_traceroutes.py`: traceroute + traceroute queue persistence
- `meshtracer_app/storage_chat.py`: chat persistence + chat history queries
- `meshtracer_app/static/index.html`: frontend shell and markup
- `meshtracer_app/static/app.css`: frontend styling
- `meshtracer_app/static/app.js`: frontend map/UI logic
- `tests/controller_test_utils.py`: shared controller test fixtures/helpers
- `tests/test_controller_chat.py`: controller chat tests
- `tests/test_controller_config.py`: runtime config and validation tests
- `tests/test_controller_lifecycle.py`: startup/shutdown and lifecycle tests
- `tests/test_controller_state_worker.py`: state and worker behavior tests
- `tests/test_discovery.py`: discovery scanner tests
- `tests/test_meshtastic_helpers.py`: Meshtastic helper/unit parser tests
- `tests/test_storage.py`: storage/repository persistence tests

## Development Checks

Run tests:

```bash
python -m unittest discover -s tests
```

Quick syntax validation:

```bash
python -m py_compile meshtracer.py meshtracer_app/*.py tests/*.py
```

## Troubleshooting

- Connect fails:
  - For TCP targets, confirm the host is reachable and Meshtastic TCP is enabled.
  - For BLE targets, confirm Bluetooth is enabled and the node is advertising over BLE.
- `--no-web` mode exits with host error:
  - Provide positional `host` argument.
- Discovery list is empty:
  - Discovery is best-effort and scans likely private `/24` networks.
- Webhook not delivered:
  - Validate URL reachability, receiver status, and token handling.
- Browser not auto-opened:
  - Use `--no-open` intentionally or open `http://127.0.0.1:8090/` manually.
