from __future__ import annotations

import json
import threading
from collections.abc import Callable
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from .common import utc_now

STATIC_DIR = Path(__file__).resolve().parent / "static"
STATIC_FILES: dict[str, tuple[str, Path]] = {
    "index.html": ("text/html; charset=utf-8", STATIC_DIR / "index.html"),
    "app.css": ("text/css; charset=utf-8", STATIC_DIR / "app.css"),
    "app.js": ("application/javascript; charset=utf-8", STATIC_DIR / "app.js"),
}


def start_map_server(
    snapshot: Callable[[], dict[str, Any]],
    wait_for_snapshot_revision: Callable[[int, float], int],
    connect: Callable[[str], tuple[bool, str]],
    disconnect: Callable[[], tuple[bool, str]],
    run_traceroute: Callable[[int], tuple[bool, str]],
    send_chat_message: Callable[[str, int, str], tuple[bool, str]],
    get_chat_messages: Callable[[str, int, int], tuple[bool, str, list[dict[str, Any]], int]],
    get_incoming_chat_messages: Callable[[int, int], tuple[bool, str, list[dict[str, Any]], int]],
    request_node_telemetry: Callable[[int, str], tuple[bool, str]],
    request_node_info: Callable[[int], tuple[bool, str]],
    request_node_position: Callable[[int], tuple[bool, str]],
    reset_database: Callable[[], tuple[bool, str]],
    remove_traceroute_queue_entry: Callable[[int], tuple[bool, str]],
    rescan_discovery: Callable[[], tuple[bool, str]],
    get_config: Callable[[], dict[str, Any]],
    set_config: Callable[[dict[str, Any]], tuple[bool, str]],
    host: str,
    port: int,
) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        _GET_ROUTES: dict[str, str] = {
            "/": "_handle_get_root",
            "/map": "_handle_get_root",
            "/api/map": "_handle_get_api_map",
            "/api/events": "_handle_get_api_events",
            "/api/config": "_handle_get_api_config",
            "/api/chat/messages": "_handle_get_api_chat_messages",
            "/api/chat/incoming": "_handle_get_api_chat_incoming",
            "/healthz": "_handle_get_healthz",
        }
        _POST_ROUTES: dict[str, str] = {
            "/api/config": "_handle_post_config",
            "/api/connect": "_handle_post_connect",
            "/api/disconnect": "_handle_post_disconnect",
            "/api/traceroute": "_handle_post_traceroute",
            "/api/chat/send": "_handle_post_chat_send",
            "/api/telemetry/request": "_handle_post_telemetry_request",
            "/api/nodeinfo/request": "_handle_post_nodeinfo_request",
            "/api/position/request": "_handle_post_position_request",
            "/api/database/reset": "_handle_post_database_reset",
            "/api/traceroute/queue/remove": "_handle_post_traceroute_queue_remove",
            "/api/discovery/rescan": "_handle_post_discovery_rescan",
        }

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def _read_json_body(self) -> tuple[dict[str, Any] | None, str | None]:
            length_raw = self.headers.get("Content-Length", "0")
            try:
                length = int(length_raw) if length_raw else 0
            except (TypeError, ValueError):
                length = 0
            if length <= 0:
                return None, "missing_body"
            try:
                raw = self.rfile.read(length)
            except Exception:
                return None, "read_failed"
            try:
                value = json.loads(raw.decode("utf-8", errors="replace"))
            except json.JSONDecodeError:
                return None, "invalid_json"
            if not isinstance(value, dict):
                return None, "expected_object"
            return value, None

        def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
            self._send_bytes(body, content_type="application/json", status=status)

        def _send_bytes(self, body: bytes, *, content_type: str, status: int = 200) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_static_asset(self, asset_name: str) -> bool:
            entry = STATIC_FILES.get(asset_name)
            if entry is None:
                return False
            content_type, asset_path = entry
            try:
                body = asset_path.read_bytes()
            except OSError:
                self._send_json({"error": "static_asset_unavailable", "asset": asset_name}, status=500)
                return True
            self._send_bytes(body, content_type=content_type, status=200)
            return True

        def _send_sse(self, *, event: str, payload: dict[str, Any], event_id: int | None = None) -> None:
            if event_id is not None:
                self.wfile.write(f"id: {int(event_id)}\n".encode("utf-8"))
            self.wfile.write(f"event: {event}\n".encode("utf-8"))
            data = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
            self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
            self.wfile.flush()

        def _serve_events(self, since_revision: int) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()

            latest = snapshot()
            latest_revision = int(latest.get("snapshot_revision") or 0)
            self._send_sse(event="snapshot", payload=latest, event_id=latest_revision)

            cursor = latest_revision
            heartbeat_seconds = 20.0
            while True:
                next_revision = wait_for_snapshot_revision(cursor, heartbeat_seconds)
                if int(next_revision) <= cursor:
                    self._send_sse(
                        event="heartbeat",
                        payload={"at_utc": utc_now(), "snapshot_revision": cursor},
                    )
                    continue

                payload = snapshot()
                payload_revision = int(payload.get("snapshot_revision") or next_revision)
                cursor = max(cursor, payload_revision)
                self._send_sse(event="snapshot", payload=payload, event_id=cursor)

        def _resolve_handler_name(self, routes: dict[str, str], path: str) -> str | None:
            handler_name = routes.get(path)
            if not handler_name:
                return None
            handler = getattr(self, handler_name, None)
            if not callable(handler):
                return None
            return handler_name

        def _send_not_found(self) -> None:
            self._send_json({"error": "not_found"}, status=404)

        def _handle_get_root(self, _url: Any) -> None:
            served = self._send_static_asset("index.html")
            if not served:
                self._send_json({"error": "static_asset_unavailable", "asset": "index.html"}, status=500)

        def _handle_get_api_map(self, _url: Any) -> None:
            self._send_json(snapshot())

        def _handle_get_api_events(self, url: Any) -> None:
            query = parse_qs(url.query, keep_blank_values=False)
            since_raw = query.get("since", [0])[0]
            try:
                since = int(since_raw)
            except (TypeError, ValueError):
                since = 0
            try:
                self._serve_events(since)
            except (BrokenPipeError, ConnectionResetError, OSError):
                return

        def _handle_get_api_config(self, _url: Any) -> None:
            self._send_json({"ok": True, "config": get_config()})

        def _handle_get_api_chat_messages(self, url: Any) -> None:
            query = parse_qs(url.query, keep_blank_values=False)
            recipient_kind = str(query.get("recipient_kind", ["channel"])[0] or "").strip().lower()
            recipient_id_raw = query.get("recipient_id", [0])[0]
            limit_raw = query.get("limit", [300])[0]
            try:
                recipient_id = int(recipient_id_raw)
            except (TypeError, ValueError):
                self._send_json({"ok": False, "error": "invalid_recipient_id"}, status=400)
                return
            try:
                limit = int(limit_raw)
            except (TypeError, ValueError):
                limit = 300
            ok, detail, messages, revision = get_chat_messages(recipient_kind, recipient_id, limit)
            status = 200 if ok else 400
            self._send_json(
                {
                    "ok": ok,
                    "detail": detail,
                    "recipient_kind": recipient_kind,
                    "recipient_id": recipient_id,
                    "messages": messages,
                    "chat_revision": int(revision),
                },
                status=status,
            )

        def _handle_get_api_chat_incoming(self, url: Any) -> None:
            query = parse_qs(url.query, keep_blank_values=False)
            since_chat_id_raw = query.get("since_chat_id", [0])[0]
            limit_raw = query.get("limit", [200])[0]
            try:
                since_chat_id = int(since_chat_id_raw)
            except (TypeError, ValueError):
                self._send_json({"ok": False, "error": "invalid_since_chat_id"}, status=400)
                return
            try:
                limit = int(limit_raw)
            except (TypeError, ValueError):
                limit = 200
            ok, detail, messages, revision = get_incoming_chat_messages(since_chat_id, limit)
            status = 200 if ok else 400
            self._send_json(
                {
                    "ok": ok,
                    "detail": detail,
                    "since_chat_id": int(since_chat_id),
                    "messages": messages,
                    "chat_revision": int(revision),
                },
                status=status,
            )

        def _handle_get_healthz(self, _url: Any) -> None:
            self._send_json({"ok": True, "at_utc": utc_now()})

        def _handle_post_config(self) -> None:
            body, err = self._read_json_body()
            if err is not None or body is None:
                self._send_json({"ok": False, "error": err or "bad_request"}, status=400)
                return
            ok, detail = set_config(body)
            status = 200 if ok else 400
            self._send_json(
                {
                    "ok": ok,
                    "detail": detail,
                    "config": get_config(),
                    "snapshot": snapshot(),
                },
                status=status,
            )

        def _handle_post_connect(self) -> None:
            body, err = self._read_json_body()
            if err is not None or body is None:
                self._send_json({"ok": False, "error": err or "bad_request"}, status=400)
                return
            host_value = body.get("host")
            host = str(host_value or "").strip()
            if not host:
                self._send_json({"ok": False, "error": "missing_host"}, status=400)
                return
            ok, detail = connect(host)
            status = 200 if ok else 500
            self._send_json({"ok": ok, "detail": detail, "snapshot": snapshot()}, status=status)

        def _handle_post_disconnect(self) -> None:
            ok, detail = disconnect()
            status = 200 if ok else 500
            self._send_json({"ok": ok, "detail": detail, "snapshot": snapshot()}, status=status)

        def _handle_post_traceroute(self) -> None:
            body, err = self._read_json_body()
            if err is not None or body is None:
                self._send_json({"ok": False, "error": err or "bad_request"}, status=400)
                return
            node_num_raw = body.get("node_num")
            try:
                node_num = int(node_num_raw)
            except (TypeError, ValueError):
                self._send_json({"ok": False, "error": "invalid_node_num"}, status=400)
                return
            ok, detail = run_traceroute(node_num)
            status = 200 if ok else 400
            self._send_json({"ok": ok, "detail": detail, "snapshot": snapshot()}, status=status)

        def _handle_post_chat_send(self) -> None:
            body, err = self._read_json_body()
            if err is not None or body is None:
                self._send_json({"ok": False, "error": err or "bad_request"}, status=400)
                return

            recipient_kind = str(body.get("recipient_kind") or "").strip().lower()
            recipient_id_raw = body.get("recipient_id")
            text = str(body.get("text") or "")
            try:
                recipient_id = int(recipient_id_raw)
            except (TypeError, ValueError):
                self._send_json({"ok": False, "error": "invalid_recipient_id"}, status=400)
                return

            ok, detail = send_chat_message(recipient_kind, recipient_id, text)
            status = 200 if ok else 400
            self._send_json({"ok": ok, "detail": detail, "snapshot": snapshot()}, status=status)

        def _handle_post_telemetry_request(self) -> None:
            body, err = self._read_json_body()
            if err is not None or body is None:
                self._send_json({"ok": False, "error": err or "bad_request"}, status=400)
                return
            node_num_raw = body.get("node_num")
            telemetry_type_raw = body.get("telemetry_type")
            try:
                node_num = int(node_num_raw)
            except (TypeError, ValueError):
                self._send_json({"ok": False, "error": "invalid_node_num"}, status=400)
                return
            telemetry_type = str(telemetry_type_raw or "").strip()
            if not telemetry_type:
                self._send_json({"ok": False, "error": "invalid_telemetry_type"}, status=400)
                return
            ok, detail = request_node_telemetry(node_num, telemetry_type)
            status = 200 if ok else 400
            self._send_json({"ok": ok, "detail": detail, "snapshot": snapshot()}, status=status)

        def _handle_post_nodeinfo_request(self) -> None:
            body, err = self._read_json_body()
            if err is not None or body is None:
                self._send_json({"ok": False, "error": err or "bad_request"}, status=400)
                return
            node_num_raw = body.get("node_num")
            try:
                node_num = int(node_num_raw)
            except (TypeError, ValueError):
                self._send_json({"ok": False, "error": "invalid_node_num"}, status=400)
                return
            ok, detail = request_node_info(node_num)
            status = 200 if ok else 400
            self._send_json({"ok": ok, "detail": detail, "snapshot": snapshot()}, status=status)

        def _handle_post_position_request(self) -> None:
            body, err = self._read_json_body()
            if err is not None or body is None:
                self._send_json({"ok": False, "error": err or "bad_request"}, status=400)
                return
            node_num_raw = body.get("node_num")
            try:
                node_num = int(node_num_raw)
            except (TypeError, ValueError):
                self._send_json({"ok": False, "error": "invalid_node_num"}, status=400)
                return
            ok, detail = request_node_position(node_num)
            status = 200 if ok else 400
            self._send_json({"ok": ok, "detail": detail, "snapshot": snapshot()}, status=status)

        def _handle_post_database_reset(self) -> None:
            ok, detail = reset_database()
            status = 200 if ok else 500
            self._send_json({"ok": ok, "detail": detail, "snapshot": snapshot()}, status=status)

        def _handle_post_traceroute_queue_remove(self) -> None:
            body, err = self._read_json_body()
            if err is not None or body is None:
                self._send_json({"ok": False, "error": err or "bad_request"}, status=400)
                return
            queue_id_raw = body.get("queue_id")
            try:
                queue_id = int(queue_id_raw)
            except (TypeError, ValueError):
                self._send_json({"ok": False, "error": "invalid_queue_id"}, status=400)
                return
            ok, detail = remove_traceroute_queue_entry(queue_id)
            status = 200 if ok else 400
            self._send_json({"ok": ok, "detail": detail, "snapshot": snapshot()}, status=status)

        def _handle_post_discovery_rescan(self) -> None:
            ok, detail = rescan_discovery()
            status = 200 if ok else 500
            self._send_json({"ok": ok, "detail": detail, "snapshot": snapshot()}, status=status)

        def do_GET(self) -> None:
            url = urlsplit(self.path)
            path = url.path
            if path.startswith("/static/"):
                asset_name = path[len("/static/") :]
                if not asset_name or "/" in asset_name or asset_name in (".", ".."):
                    self._send_not_found()
                    return
                if self._send_static_asset(asset_name):
                    return
                self._send_not_found()
                return

            handler_name = self._resolve_handler_name(self._GET_ROUTES, path)
            if handler_name is None:
                self._send_not_found()
                return
            getattr(self, handler_name)(url)

        def do_POST(self) -> None:
            path = urlsplit(self.path).path
            handler_name = self._resolve_handler_name(self._POST_ROUTES, path)
            if handler_name is None:
                self._send_not_found()
                return
            getattr(self, handler_name)()

    server = ThreadingHTTPServer((host, port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
