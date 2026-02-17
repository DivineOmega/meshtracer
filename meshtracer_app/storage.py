from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
import sqlite3
import threading
from typing import Any

from .common import utc_now
from .meshtastic_helpers import extract_route_nums


class SQLiteStore:
    _TELEMETRY_TYPES = {"device", "environment"}

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        db_dir = os.path.dirname(os.path.abspath(db_path))
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def _init_schema(self) -> None:
        schema = """
        CREATE TABLE IF NOT EXISTS runtime_config (
          config_key TEXT PRIMARY KEY,
          config_json TEXT NOT NULL,
          updated_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS nodes (
          mesh_host TEXT NOT NULL,
          node_num INTEGER NOT NULL,
          node_id TEXT,
          long_name TEXT,
          short_name TEXT,
          hw_model TEXT,
          role TEXT,
          is_licensed INTEGER,
          is_unmessagable INTEGER,
          public_key TEXT,
          snr REAL,
          hops_away INTEGER,
          channel INTEGER,
          via_mqtt INTEGER,
          is_favorite INTEGER,
          is_ignored INTEGER,
          is_muted INTEGER,
          is_key_manually_verified INTEGER,
          lat REAL,
          lon REAL,
          last_heard REAL,
          updated_at_utc TEXT NOT NULL,
          PRIMARY KEY (mesh_host, node_num)
        );

        CREATE TABLE IF NOT EXISTS node_telemetry (
          mesh_host TEXT NOT NULL,
          node_num INTEGER NOT NULL,
          telemetry_type TEXT NOT NULL,
          telemetry_json TEXT NOT NULL,
          updated_at_utc TEXT NOT NULL,
          PRIMARY KEY (mesh_host, node_num, telemetry_type)
        );

        CREATE INDEX IF NOT EXISTS idx_node_telemetry_host_node
          ON node_telemetry (mesh_host, node_num);

        CREATE TABLE IF NOT EXISTS node_positions (
          mesh_host TEXT NOT NULL,
          node_num INTEGER NOT NULL,
          position_json TEXT NOT NULL,
          updated_at_utc TEXT NOT NULL,
          PRIMARY KEY (mesh_host, node_num)
        );

        CREATE INDEX IF NOT EXISTS idx_node_positions_host_node
          ON node_positions (mesh_host, node_num);

        CREATE TABLE IF NOT EXISTS traceroutes (
          trace_id INTEGER PRIMARY KEY AUTOINCREMENT,
          mesh_host TEXT NOT NULL,
          captured_at_utc TEXT,
          towards_nums_json TEXT NOT NULL,
          back_nums_json TEXT NOT NULL,
          packet_json TEXT NOT NULL,
          result_json TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_traceroutes_host_trace
          ON traceroutes (mesh_host, trace_id);

        CREATE TABLE IF NOT EXISTS traceroute_queue (
          queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
          mesh_host TEXT NOT NULL,
          node_num INTEGER NOT NULL,
          status TEXT NOT NULL,
          created_at_utc TEXT NOT NULL,
          updated_at_utc TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_traceroute_queue_host_node
          ON traceroute_queue (mesh_host, node_num);

        CREATE INDEX IF NOT EXISTS idx_traceroute_queue_host_status_queue
          ON traceroute_queue (mesh_host, status, queue_id);

        CREATE TABLE IF NOT EXISTS chat_messages (
          chat_id INTEGER PRIMARY KEY AUTOINCREMENT,
          mesh_host TEXT NOT NULL,
          dedupe_key TEXT,
          direction TEXT NOT NULL,
          message_type TEXT NOT NULL,
          channel_index INTEGER,
          peer_node_num INTEGER,
          from_node_num INTEGER,
          to_node_num INTEGER,
          packet_id INTEGER,
          rx_time REAL,
          text TEXT NOT NULL,
          packet_json TEXT NOT NULL,
          created_at_utc TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_messages_host_dedupe
          ON chat_messages (mesh_host, dedupe_key);

        CREATE INDEX IF NOT EXISTS idx_chat_messages_host_channel_chat
          ON chat_messages (mesh_host, message_type, channel_index, chat_id);

        CREATE INDEX IF NOT EXISTS idx_chat_messages_host_peer_chat
          ON chat_messages (mesh_host, message_type, peer_node_num, chat_id);
        """
        with self._lock:
            self._conn.executescript(schema)
            # Lightweight migration for DBs created before newer node columns were added.
            node_columns = {
                str(row["name"])
                for row in self._conn.execute("PRAGMA table_info(nodes)").fetchall()
                if row is not None and row["name"] is not None
            }
            expected_columns = {
                "hw_model": "TEXT",
                "role": "TEXT",
                "is_licensed": "INTEGER",
                "is_unmessagable": "INTEGER",
                "public_key": "TEXT",
                "snr": "REAL",
                "hops_away": "INTEGER",
                "channel": "INTEGER",
                "via_mqtt": "INTEGER",
                "is_favorite": "INTEGER",
                "is_ignored": "INTEGER",
                "is_muted": "INTEGER",
                "is_key_manually_verified": "INTEGER",
            }
            for column_name, column_type in expected_columns.items():
                if column_name in node_columns:
                    continue
                self._conn.execute(f"ALTER TABLE nodes ADD COLUMN {column_name} {column_type}")
            self._conn.commit()

    @staticmethod
    def _queue_status_text(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in ("queued", "running"):
            return text
        return "queued"

    @staticmethod
    def _chat_message_type_text(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in ("channel", "direct"):
            return text
        return ""

    @staticmethod
    def _chat_direction_text(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in ("incoming", "outgoing", "unknown"):
            return text
        return "unknown"

    def get_runtime_config(self, config_key: str = "global") -> dict[str, Any] | None:
        key = str(config_key or "").strip() or "global"
        with self._lock:
            row = self._conn.execute(
                "SELECT config_json FROM runtime_config WHERE config_key = ?",
                (key,),
            ).fetchone()
        if not row:
            return None
        raw = self._json_loads(row["config_json"], None)
        return raw if isinstance(raw, dict) else None

    def set_runtime_config(self, config: dict[str, Any], config_key: str = "global") -> None:
        if not isinstance(config, dict):
            raise TypeError("config must be a dict")
        key = str(config_key or "").strip() or "global"
        now_utc = utc_now()
        payload = json.dumps(config, separators=(",", ":"), ensure_ascii=True)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO runtime_config (config_key, config_json, updated_at_utc)
                VALUES (?, ?, ?)
                ON CONFLICT(config_key) DO UPDATE SET
                  config_json = excluded.config_json,
                  updated_at_utc = excluded.updated_at_utc
                """,
                (key, payload, now_utc),
            )
            self._conn.commit()

    def reset_all_data(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                DELETE FROM runtime_config;
                DELETE FROM nodes;
                DELETE FROM node_telemetry;
                DELETE FROM node_positions;
                DELETE FROM traceroutes;
                DELETE FROM traceroute_queue;
                DELETE FROM chat_messages;
                """
            )
            self._conn.commit()

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_bool(value: Any) -> bool | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(int(value))

        text = str(value).strip().lower()
        if not text:
            return None
        if text in ("1", "true", "yes", "y", "on"):
            return True
        if text in ("0", "false", "no", "n", "off"):
            return False
        return None

    @staticmethod
    def _json_loads(value: Any, fallback: Any) -> Any:
        if not value:
            return fallback
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(str(value))
        except json.JSONDecodeError:
            return fallback

    @classmethod
    def _json_safe_value(cls, value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, dict):
            safe: dict[str, Any] = {}
            for key, item in value.items():
                safe[str(key)] = cls._json_safe_value(item)
            return safe
        if isinstance(value, (list, tuple, set)):
            return [cls._json_safe_value(item) for item in value]
        if isinstance(value, (bytes, bytearray)):
            try:
                return bytes(value).decode("utf-8")
            except Exception:
                return bytes(value).hex()

        try:
            from google.protobuf.json_format import MessageToDict  # type: ignore

            as_dict = MessageToDict(value)
            return cls._json_safe_value(as_dict)
        except Exception:
            pass

        return str(value)

    @classmethod
    def _telemetry_type_text(cls, value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in cls._TELEMETRY_TYPES:
            return text
        return ""

    def upsert_nodes(self, mesh_host: str, nodes: list[dict[str, Any]]) -> None:
        if not nodes:
            return
        now_utc = utc_now()
        rows: list[tuple[Any, ...]] = []
        for node in nodes:
            node_num = node.get("num")
            try:
                node_num_int = int(node_num)
            except (TypeError, ValueError):
                continue

            rows.append(
                (
                    mesh_host,
                    node_num_int,
                    node.get("id"),
                    node.get("long_name"),
                    node.get("short_name"),
                    node.get("hw_model"),
                    str(node.get("role")) if node.get("role") is not None else None,
                    self._to_bool(node.get("is_licensed")),
                    self._to_bool(node.get("is_unmessagable")),
                    node.get("public_key"),
                    self._to_float(node.get("snr")),
                    self._to_int(node.get("hops_away")),
                    self._to_int(node.get("channel")),
                    self._to_bool(node.get("via_mqtt")),
                    self._to_bool(node.get("is_favorite")),
                    self._to_bool(node.get("is_ignored")),
                    self._to_bool(node.get("is_muted")),
                    self._to_bool(node.get("is_key_manually_verified")),
                    self._to_float(node.get("lat")),
                    self._to_float(node.get("lon")),
                    self._to_float(node.get("last_heard")),
                    now_utc,
                )
            )
        if not rows:
            return
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO nodes (
                  mesh_host, node_num, node_id, long_name, short_name,
                  hw_model, role, is_licensed, is_unmessagable, public_key,
                  snr, hops_away, channel, via_mqtt, is_favorite, is_ignored,
                  is_muted, is_key_manually_verified, lat, lon, last_heard, updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(mesh_host, node_num) DO UPDATE SET
                  node_id = COALESCE(excluded.node_id, nodes.node_id),
                  long_name = COALESCE(excluded.long_name, nodes.long_name),
                  short_name = COALESCE(excluded.short_name, nodes.short_name),
                  hw_model = COALESCE(excluded.hw_model, nodes.hw_model),
                  role = COALESCE(excluded.role, nodes.role),
                  is_licensed = COALESCE(excluded.is_licensed, nodes.is_licensed),
                  is_unmessagable = COALESCE(excluded.is_unmessagable, nodes.is_unmessagable),
                  public_key = COALESCE(excluded.public_key, nodes.public_key),
                  snr = COALESCE(excluded.snr, nodes.snr),
                  hops_away = COALESCE(excluded.hops_away, nodes.hops_away),
                  channel = COALESCE(excluded.channel, nodes.channel),
                  via_mqtt = COALESCE(excluded.via_mqtt, nodes.via_mqtt),
                  is_favorite = COALESCE(excluded.is_favorite, nodes.is_favorite),
                  is_ignored = COALESCE(excluded.is_ignored, nodes.is_ignored),
                  is_muted = COALESCE(excluded.is_muted, nodes.is_muted),
                  is_key_manually_verified = COALESCE(
                    excluded.is_key_manually_verified,
                    nodes.is_key_manually_verified
                  ),
                  lat = COALESCE(excluded.lat, nodes.lat),
                  lon = COALESCE(excluded.lon, nodes.lon),
                  last_heard = CASE
                    WHEN excluded.last_heard IS NULL THEN nodes.last_heard
                    WHEN nodes.last_heard IS NULL THEN excluded.last_heard
                    ELSE MAX(excluded.last_heard, nodes.last_heard)
                  END,
                  updated_at_utc = excluded.updated_at_utc
                """,
                rows,
            )
            self._conn.commit()

    def upsert_node(self, mesh_host: str, node: dict[str, Any]) -> None:
        self.upsert_nodes(mesh_host, [node])

    def upsert_node_telemetry(
        self,
        mesh_host: str,
        node_num: int,
        telemetry_type: str,
        telemetry: dict[str, Any],
    ) -> bool:
        host = str(mesh_host or "").strip()
        t_type = self._telemetry_type_text(telemetry_type)
        if not host or not t_type or not isinstance(telemetry, dict):
            return False
        try:
            node_num_int = int(node_num)
        except (TypeError, ValueError):
            return False

        with self._lock:
            existing_row = self._conn.execute(
                """
                SELECT telemetry_json
                FROM node_telemetry
                WHERE mesh_host = ? AND node_num = ? AND telemetry_type = ?
                """,
                (host, node_num_int, t_type),
            ).fetchone()

            merged: dict[str, Any] = {}
            if existing_row is not None:
                existing = self._json_loads(existing_row["telemetry_json"], {})
                if isinstance(existing, dict):
                    merged.update(existing)
            merged.update(telemetry)

            self._conn.execute(
                """
                INSERT INTO node_telemetry (
                  mesh_host, node_num, telemetry_type, telemetry_json, updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(mesh_host, node_num, telemetry_type) DO UPDATE SET
                  telemetry_json = excluded.telemetry_json,
                  updated_at_utc = excluded.updated_at_utc
                """,
                (
                    host,
                    node_num_int,
                    t_type,
                    json.dumps(merged, separators=(",", ":"), ensure_ascii=True),
                    utc_now(),
                ),
            )
            self._conn.commit()
        return True

    def get_node_telemetry(self, mesh_host: str, node_num: int, telemetry_type: str) -> dict[str, Any] | None:
        host = str(mesh_host or "").strip()
        t_type = self._telemetry_type_text(telemetry_type)
        if not host or not t_type:
            return None
        try:
            node_num_int = int(node_num)
        except (TypeError, ValueError):
            return None

        with self._lock:
            row = self._conn.execute(
                """
                SELECT telemetry_json, updated_at_utc
                FROM node_telemetry
                WHERE mesh_host = ? AND node_num = ? AND telemetry_type = ?
                """,
                (host, node_num_int, t_type),
            ).fetchone()
        if row is None:
            return None
        telemetry_raw = self._json_loads(row["telemetry_json"], {})
        telemetry = telemetry_raw if isinstance(telemetry_raw, dict) else {}
        return {
            "mesh_host": host,
            "node_num": node_num_int,
            "telemetry_type": t_type,
            "telemetry": telemetry,
            "updated_at_utc": str(row["updated_at_utc"] or ""),
        }

    def upsert_node_position(
        self,
        mesh_host: str,
        node_num: int,
        position: dict[str, Any],
    ) -> bool:
        host = str(mesh_host or "").strip()
        if not host:
            return False
        position_safe = self._json_safe_value(position)
        if not isinstance(position_safe, dict):
            return False
        try:
            node_num_int = int(node_num)
        except (TypeError, ValueError):
            return False

        with self._lock:
            existing_row = self._conn.execute(
                """
                SELECT position_json
                FROM node_positions
                WHERE mesh_host = ? AND node_num = ?
                """,
                (host, node_num_int),
            ).fetchone()

            merged: dict[str, Any] = {}
            if existing_row is not None:
                existing = self._json_loads(existing_row["position_json"], {})
                if isinstance(existing, dict):
                    merged.update(existing)
            merged.update(position_safe)

            self._conn.execute(
                """
                INSERT INTO node_positions (
                  mesh_host, node_num, position_json, updated_at_utc
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(mesh_host, node_num) DO UPDATE SET
                  position_json = excluded.position_json,
                  updated_at_utc = excluded.updated_at_utc
                """,
                (
                    host,
                    node_num_int,
                    json.dumps(merged, separators=(",", ":"), ensure_ascii=True),
                    utc_now(),
                ),
            )
            self._conn.commit()
        return True

    def get_node_position(self, mesh_host: str, node_num: int) -> dict[str, Any] | None:
        host = str(mesh_host or "").strip()
        if not host:
            return None
        try:
            node_num_int = int(node_num)
        except (TypeError, ValueError):
            return None

        with self._lock:
            row = self._conn.execute(
                """
                SELECT position_json, updated_at_utc
                FROM node_positions
                WHERE mesh_host = ? AND node_num = ?
                """,
                (host, node_num_int),
            ).fetchone()
        if row is None:
            return None
        position_raw = self._json_loads(row["position_json"], {})
        position = position_raw if isinstance(position_raw, dict) else {}
        return {
            "mesh_host": host,
            "node_num": node_num_int,
            "position": position,
            "updated_at_utc": str(row["updated_at_utc"] or ""),
        }

    def add_traceroute(self, mesh_host: str, result: dict[str, Any]) -> int:
        trace_row = (
            mesh_host,
            result.get("captured_at_utc"),
            json.dumps(extract_route_nums(result.get("route_towards_destination")), separators=(",", ":"), ensure_ascii=True),
            json.dumps(extract_route_nums(result.get("route_back_to_origin")), separators=(",", ":"), ensure_ascii=True),
            json.dumps(result.get("packet") or {}, separators=(",", ":"), ensure_ascii=True),
            json.dumps(result, separators=(",", ":"), ensure_ascii=True),
        )
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO traceroutes (
                  mesh_host, captured_at_utc, towards_nums_json,
                  back_nums_json, packet_json, result_json
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                trace_row,
            )
            self._conn.commit()
            return int(cursor.lastrowid)

    @staticmethod
    def _utc_cutoff_text_for_hours(hours: Any) -> str | None:
        try:
            hours_f = float(hours)
        except (TypeError, ValueError):
            return None
        if not (hours_f > 0):
            return None
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_f)
        return cutoff.strftime("%Y-%m-%d %H:%M:%S UTC")

    def prune_traceroutes_older_than(self, mesh_host: str, retention_hours: Any) -> int:
        host = str(mesh_host or "").strip()
        cutoff = self._utc_cutoff_text_for_hours(retention_hours)
        if not host or not cutoff:
            return 0
        with self._lock:
            cursor = self._conn.execute(
                """
                DELETE FROM traceroutes
                WHERE mesh_host = ?
                  AND captured_at_utc IS NOT NULL
                  AND captured_at_utc <> ''
                  AND captured_at_utc < ?
                """,
                (host, cutoff),
            )
            self._conn.commit()
            return int(cursor.rowcount or 0)

    def list_traceroute_queue(self, mesh_host: str) -> list[dict[str, Any]]:
        host = str(mesh_host or "").strip()
        if not host:
            return []
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT queue_id, node_num, status, created_at_utc, updated_at_utc
                FROM traceroute_queue
                WHERE mesh_host = ?
                  AND status IN ('queued', 'running')
                ORDER BY
                  CASE status WHEN 'running' THEN 0 ELSE 1 END,
                  queue_id ASC
                """,
                (host,),
            ).fetchall()
        return [
            {
                "queue_id": int(row["queue_id"]),
                "mesh_host": host,
                "node_num": int(row["node_num"]),
                "status": self._queue_status_text(row["status"]),
                "created_at_utc": str(row["created_at_utc"] or ""),
                "updated_at_utc": str(row["updated_at_utc"] or ""),
            }
            for row in rows
        ]

    def get_traceroute_queue_entry(self, mesh_host: str, queue_id: int) -> dict[str, Any] | None:
        host = str(mesh_host or "").strip()
        try:
            queue_id_int = int(queue_id)
        except (TypeError, ValueError):
            return None
        if not host:
            return None
        with self._lock:
            row = self._conn.execute(
                """
                SELECT queue_id, node_num, status, created_at_utc, updated_at_utc
                FROM traceroute_queue
                WHERE mesh_host = ? AND queue_id = ?
                """,
                (host, queue_id_int),
            ).fetchone()
        if row is None:
            return None
        return {
            "queue_id": int(row["queue_id"]),
            "mesh_host": host,
            "node_num": int(row["node_num"]),
            "status": self._queue_status_text(row["status"]),
            "created_at_utc": str(row["created_at_utc"] or ""),
            "updated_at_utc": str(row["updated_at_utc"] or ""),
        }

    def find_traceroute_queue_entry_by_node(self, mesh_host: str, node_num: int) -> dict[str, Any] | None:
        host = str(mesh_host or "").strip()
        try:
            node_num_int = int(node_num)
        except (TypeError, ValueError):
            return None
        if not host:
            return None
        with self._lock:
            row = self._conn.execute(
                """
                SELECT queue_id, node_num, status, created_at_utc, updated_at_utc
                FROM traceroute_queue
                WHERE mesh_host = ? AND node_num = ?
                LIMIT 1
                """,
                (host, node_num_int),
            ).fetchone()
        if row is None:
            return None
        return {
            "queue_id": int(row["queue_id"]),
            "mesh_host": host,
            "node_num": int(row["node_num"]),
            "status": self._queue_status_text(row["status"]),
            "created_at_utc": str(row["created_at_utc"] or ""),
            "updated_at_utc": str(row["updated_at_utc"] or ""),
        }

    def enqueue_traceroute_target(self, mesh_host: str, node_num: int) -> dict[str, Any] | None:
        host = str(mesh_host or "").strip()
        try:
            node_num_int = int(node_num)
        except (TypeError, ValueError):
            return None
        if not host:
            return None

        now_utc = utc_now()
        with self._lock:
            try:
                cursor = self._conn.execute(
                    """
                    INSERT INTO traceroute_queue (
                      mesh_host, node_num, status, created_at_utc, updated_at_utc
                    )
                    VALUES (?, ?, 'queued', ?, ?)
                    """,
                    (host, node_num_int, now_utc, now_utc),
                )
                queue_id = int(cursor.lastrowid)
                self._conn.commit()
            except sqlite3.IntegrityError:
                self._conn.rollback()
                return None

            row = self._conn.execute(
                """
                SELECT queue_id, node_num, status, created_at_utc, updated_at_utc
                FROM traceroute_queue
                WHERE mesh_host = ? AND queue_id = ?
                """,
                (host, queue_id),
            ).fetchone()

        if row is None:
            return None
        return {
            "queue_id": int(row["queue_id"]),
            "mesh_host": host,
            "node_num": int(row["node_num"]),
            "status": self._queue_status_text(row["status"]),
            "created_at_utc": str(row["created_at_utc"] or ""),
            "updated_at_utc": str(row["updated_at_utc"] or ""),
        }

    def queued_position_for_entry(self, mesh_host: str, queue_id: int) -> int:
        host = str(mesh_host or "").strip()
        try:
            queue_id_int = int(queue_id)
        except (TypeError, ValueError):
            return -1
        if not host:
            return -1
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM traceroute_queue
                WHERE mesh_host = ?
                  AND status = 'queued'
                  AND queue_id <= ?
                """,
                (host, queue_id_int),
            ).fetchone()
        if row is None:
            return -1
        try:
            count = int(row["c"])
        except (TypeError, ValueError):
            return -1
        return count if count > 0 else -1

    def pop_next_queued_traceroute(self, mesh_host: str) -> dict[str, Any] | None:
        host = str(mesh_host or "").strip()
        if not host:
            return None
        now_utc = utc_now()
        with self._lock:
            row = self._conn.execute(
                """
                SELECT queue_id, node_num, created_at_utc
                FROM traceroute_queue
                WHERE mesh_host = ? AND status = 'queued'
                ORDER BY queue_id ASC
                LIMIT 1
                """,
                (host,),
            ).fetchone()
            if row is None:
                return None

            queue_id_int = int(row["queue_id"])
            self._conn.execute(
                """
                UPDATE traceroute_queue
                SET status = 'running', updated_at_utc = ?
                WHERE mesh_host = ? AND queue_id = ? AND status = 'queued'
                """,
                (now_utc, host, queue_id_int),
            )
            self._conn.commit()

            updated = self._conn.execute(
                """
                SELECT queue_id, node_num, status, created_at_utc, updated_at_utc
                FROM traceroute_queue
                WHERE mesh_host = ? AND queue_id = ?
                """,
                (host, queue_id_int),
            ).fetchone()

        if updated is None:
            return None
        return {
            "queue_id": int(updated["queue_id"]),
            "mesh_host": host,
            "node_num": int(updated["node_num"]),
            "status": self._queue_status_text(updated["status"]),
            "created_at_utc": str(updated["created_at_utc"] or ""),
            "updated_at_utc": str(updated["updated_at_utc"] or ""),
        }

    def remove_traceroute_queue_entry(self, mesh_host: str, queue_id: int) -> bool:
        host = str(mesh_host or "").strip()
        try:
            queue_id_int = int(queue_id)
        except (TypeError, ValueError):
            return False
        if not host:
            return False
        with self._lock:
            cursor = self._conn.execute(
                "DELETE FROM traceroute_queue WHERE mesh_host = ? AND queue_id = ?",
                (host, queue_id_int),
            )
            self._conn.commit()
            return int(cursor.rowcount or 0) > 0

    def requeue_running_traceroutes(self, mesh_host: str) -> int:
        host = str(mesh_host or "").strip()
        if not host:
            return 0
        now_utc = utc_now()
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE traceroute_queue
                SET status = 'queued', updated_at_utc = ?
                WHERE mesh_host = ? AND status = 'running'
                """,
                (now_utc, host),
            )
            self._conn.commit()
            return int(cursor.rowcount or 0)

    def add_chat_message(
        self,
        mesh_host: str,
        *,
        text: Any,
        message_type: Any,
        direction: Any,
        channel_index: Any = None,
        peer_node_num: Any = None,
        from_node_num: Any = None,
        to_node_num: Any = None,
        packet_id: Any = None,
        rx_time: Any = None,
        packet: Any = None,
        dedupe_key: Any = None,
        created_at_utc: Any = None,
    ) -> int | None:
        host = str(mesh_host or "").strip()
        if not host:
            return None
        message_type_text = self._chat_message_type_text(message_type)
        if not message_type_text:
            return None

        text_value = str(text or "")
        if not text_value:
            return None

        direction_text = self._chat_direction_text(direction)
        channel_index_int = self._to_int(channel_index)
        peer_node_num_int = self._to_int(peer_node_num)
        from_node_num_int = self._to_int(from_node_num)
        to_node_num_int = self._to_int(to_node_num)
        packet_id_int = self._to_int(packet_id)
        rx_time_float = self._to_float(rx_time)

        if message_type_text == "channel":
            if channel_index_int is None:
                channel_index_int = 0
            peer_node_num_int = None
        else:
            channel_index_int = None
            if peer_node_num_int is None:
                return None

        dedupe_key_text = str(dedupe_key or "").strip() or None
        created_text = str(created_at_utc or "").strip() or utc_now()
        packet_safe = self._json_safe_value(packet)
        packet_json = json.dumps(packet_safe, separators=(",", ":"), ensure_ascii=True)

        with self._lock:
            try:
                cursor = self._conn.execute(
                    """
                    INSERT INTO chat_messages (
                      mesh_host, dedupe_key, direction, message_type, channel_index,
                      peer_node_num, from_node_num, to_node_num, packet_id, rx_time,
                      text, packet_json, created_at_utc
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        host,
                        dedupe_key_text,
                        direction_text,
                        message_type_text,
                        channel_index_int,
                        peer_node_num_int,
                        from_node_num_int,
                        to_node_num_int,
                        packet_id_int,
                        rx_time_float,
                        text_value,
                        packet_json,
                        created_text,
                    ),
                )
                self._conn.commit()
                return int(cursor.lastrowid)
            except sqlite3.IntegrityError:
                if dedupe_key_text is None:
                    self._conn.rollback()
                    return None
                row = self._conn.execute(
                    """
                    SELECT chat_id
                    FROM chat_messages
                    WHERE mesh_host = ? AND dedupe_key = ?
                    LIMIT 1
                    """,
                    (host, dedupe_key_text),
                ).fetchone()
                self._conn.rollback()

        if row is None:
            return None
        try:
            return int(row["chat_id"])
        except (TypeError, ValueError):
            return None

    def latest_chat_revision(self, mesh_host: str) -> int:
        host = str(mesh_host or "").strip()
        if not host:
            return 0
        with self._lock:
            row = self._conn.execute(
                """
                SELECT MAX(chat_id) AS max_id
                FROM chat_messages
                WHERE mesh_host = ?
                """,
                (host,),
            ).fetchone()
        if row is None:
            return 0
        try:
            return int(row["max_id"] or 0)
        except (TypeError, ValueError):
            return 0

    def list_chat_channels(self, mesh_host: str) -> list[int]:
        host = str(mesh_host or "").strip()
        if not host:
            return []
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT DISTINCT channel_index
                FROM chat_messages
                WHERE mesh_host = ?
                  AND message_type = 'channel'
                  AND channel_index IS NOT NULL
                ORDER BY channel_index ASC
                """,
                (host,),
            ).fetchall()
        values: list[int] = []
        for row in rows:
            try:
                values.append(int(row["channel_index"]))
            except (TypeError, ValueError):
                continue
        return values

    def list_recent_direct_nodes(self, mesh_host: str, limit: int = 30) -> list[int]:
        host = str(mesh_host or "").strip()
        if not host:
            return []
        try:
            limit_int = int(limit)
        except (TypeError, ValueError):
            limit_int = 30
        limit_int = max(1, min(500, limit_int))
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT peer_node_num, MAX(chat_id) AS last_chat_id
                FROM chat_messages
                WHERE mesh_host = ?
                  AND message_type = 'direct'
                  AND peer_node_num IS NOT NULL
                GROUP BY peer_node_num
                ORDER BY last_chat_id DESC
                LIMIT ?
                """,
                (host, limit_int),
            ).fetchall()
        values: list[int] = []
        for row in rows:
            try:
                values.append(int(row["peer_node_num"]))
            except (TypeError, ValueError):
                continue
        return values

    def list_chat_messages(
        self,
        mesh_host: str,
        *,
        recipient_kind: Any,
        recipient_id: Any,
        limit: int = 300,
    ) -> list[dict[str, Any]]:
        host = str(mesh_host or "").strip()
        kind = self._chat_message_type_text(recipient_kind)
        if not host or kind not in ("channel", "direct"):
            return []
        try:
            recipient_id_int = int(recipient_id)
        except (TypeError, ValueError):
            return []
        try:
            limit_int = int(limit)
        except (TypeError, ValueError):
            limit_int = 300
        limit_int = max(1, min(2000, limit_int))

        sql = """
            SELECT
              chat_id, direction, message_type, channel_index, peer_node_num,
              from_node_num, to_node_num, packet_id, rx_time, text, packet_json, created_at_utc
            FROM chat_messages
            WHERE mesh_host = ?
        """
        params: list[Any] = [host]
        if kind == "channel":
            sql += "\n  AND message_type = 'channel' AND channel_index = ?"
            params.append(recipient_id_int)
        else:
            sql += "\n  AND message_type = 'direct' AND peer_node_num = ?"
            params.append(recipient_id_int)
        sql += "\nORDER BY chat_id DESC\nLIMIT ?"
        params.append(limit_int)

        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()

        messages: list[dict[str, Any]] = []
        for row in reversed(rows):
            packet_raw = self._json_loads(row["packet_json"], {})
            packet_value = packet_raw if isinstance(packet_raw, (dict, list)) else {}
            messages.append(
                {
                    "chat_id": int(row["chat_id"]),
                    "direction": self._chat_direction_text(row["direction"]),
                    "message_type": self._chat_message_type_text(row["message_type"]),
                    "channel_index": self._to_int(row["channel_index"]),
                    "peer_node_num": self._to_int(row["peer_node_num"]),
                    "from_node_num": self._to_int(row["from_node_num"]),
                    "to_node_num": self._to_int(row["to_node_num"]),
                    "packet_id": self._to_int(row["packet_id"]),
                    "rx_time": self._to_float(row["rx_time"]),
                    "text": str(row["text"] or ""),
                    "packet": packet_value,
                    "created_at_utc": str(row["created_at_utc"] or ""),
                }
            )
        return messages

    def snapshot(
        self,
        mesh_host: str,
        max_traces: int | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        trace_limit: int | None = None
        if max_traces is not None:
            try:
                trace_limit = int(max_traces)
            except (TypeError, ValueError):
                trace_limit = None
            if trace_limit is not None and trace_limit <= 0:
                trace_limit = None
        with self._lock:
            node_rows = self._conn.execute(
                """
                SELECT
                  n.node_num,
                  n.node_id,
                  n.long_name,
                  n.short_name,
                  n.hw_model,
                  n.role,
                  n.is_licensed,
                  n.is_unmessagable,
                  n.public_key,
                  n.snr,
                  n.hops_away,
                  n.channel,
                  n.via_mqtt,
                  n.is_favorite,
                  n.is_ignored,
                  n.is_muted,
                  n.is_key_manually_verified,
                  n.lat,
                  n.lon,
                  n.last_heard,
                  td.telemetry_json AS device_telemetry_json,
                  td.updated_at_utc AS device_telemetry_updated_at_utc,
                  te.telemetry_json AS environment_telemetry_json,
                  te.updated_at_utc AS environment_telemetry_updated_at_utc,
                  np.position_json AS position_json,
                  np.updated_at_utc AS position_updated_at_utc
                FROM nodes AS n
                LEFT JOIN node_telemetry AS td
                  ON td.mesh_host = n.mesh_host
                 AND td.node_num = n.node_num
                 AND td.telemetry_type = 'device'
                LEFT JOIN node_telemetry AS te
                  ON te.mesh_host = n.mesh_host
                 AND te.node_num = n.node_num
                 AND te.telemetry_type = 'environment'
                LEFT JOIN node_positions AS np
                  ON np.mesh_host = n.mesh_host
                 AND np.node_num = n.node_num
                WHERE n.mesh_host = ?
                ORDER BY n.node_num ASC
                """,
                (mesh_host,),
            ).fetchall()
            trace_sql = """
                SELECT trace_id, captured_at_utc, towards_nums_json, back_nums_json, packet_json, result_json
                FROM traceroutes
                WHERE mesh_host = ?
                ORDER BY trace_id DESC
                """
            trace_params: tuple[Any, ...]
            if trace_limit is not None:
                trace_sql += "\nLIMIT ?"
                trace_params = (mesh_host, trace_limit)
            else:
                trace_params = (mesh_host,)
            trace_rows = self._conn.execute(trace_sql, trace_params).fetchall()

        nodes: list[dict[str, Any]] = []
        for row in node_rows:
            device_telemetry_raw = self._json_loads(row["device_telemetry_json"], {})
            environment_telemetry_raw = self._json_loads(row["environment_telemetry_json"], {})
            position_raw = self._json_loads(row["position_json"], {})
            device_telemetry = device_telemetry_raw if isinstance(device_telemetry_raw, dict) else {}
            environment_telemetry = (
                environment_telemetry_raw if isinstance(environment_telemetry_raw, dict) else {}
            )
            position = position_raw if isinstance(position_raw, dict) else {}
            nodes.append(
                {
                    "num": int(row["node_num"]),
                    "id": row["node_id"],
                    "long_name": row["long_name"],
                    "short_name": row["short_name"],
                    "hw_model": row["hw_model"],
                    "role": row["role"],
                    "is_licensed": self._to_bool(row["is_licensed"]),
                    "is_unmessagable": self._to_bool(row["is_unmessagable"]),
                    "public_key": row["public_key"],
                    "snr": self._to_float(row["snr"]),
                    "hops_away": self._to_int(row["hops_away"]),
                    "channel": self._to_int(row["channel"]),
                    "via_mqtt": self._to_bool(row["via_mqtt"]),
                    "is_favorite": self._to_bool(row["is_favorite"]),
                    "is_ignored": self._to_bool(row["is_ignored"]),
                    "is_muted": self._to_bool(row["is_muted"]),
                    "is_key_manually_verified": self._to_bool(row["is_key_manually_verified"]),
                    "lat": self._to_float(row["lat"]),
                    "lon": self._to_float(row["lon"]),
                    "last_heard": self._to_float(row["last_heard"]),
                    "device_telemetry": device_telemetry,
                    "device_telemetry_updated_at_utc": str(
                        row["device_telemetry_updated_at_utc"] or ""
                    ),
                    "environment_telemetry": environment_telemetry,
                    "environment_telemetry_updated_at_utc": str(
                        row["environment_telemetry_updated_at_utc"] or ""
                    ),
                    "position": position,
                    "position_updated_at_utc": str(row["position_updated_at_utc"] or ""),
                }
            )

        traces: list[dict[str, Any]] = []
        for row in reversed(trace_rows):
            towards_raw = self._json_loads(row["towards_nums_json"], [])
            back_raw = self._json_loads(row["back_nums_json"], [])
            packet_raw = self._json_loads(row["packet_json"], {})
            result_raw = self._json_loads(row["result_json"], {})
            towards_nums = [
                int(value)
                for value in towards_raw
                if isinstance(value, (int, float, str)) and str(value).lstrip("-").isdigit()
            ]
            back_nums = [
                int(value)
                for value in back_raw
                if isinstance(value, (int, float, str)) and str(value).lstrip("-").isdigit()
            ]
            towards_snr_db: list[float | None] | None = None
            back_snr_db: list[float | None] | None = None
            if isinstance(result_raw, dict):
                towards_hops = result_raw.get("route_towards_destination")
                if isinstance(towards_hops, list):
                    values: list[float | None] = []
                    has_value = False
                    for hop in towards_hops:
                        snr = hop.get("snr_db") if isinstance(hop, dict) else None
                        if snr is None:
                            values.append(None)
                            continue
                        try:
                            snr_f = float(snr)
                        except (TypeError, ValueError):
                            values.append(None)
                            continue
                        values.append(snr_f)
                        has_value = True
                    if has_value:
                        towards_snr_db = values

                back_hops = result_raw.get("route_back_to_origin")
                if isinstance(back_hops, list):
                    values = []
                    has_value = False
                    for hop in back_hops:
                        snr = hop.get("snr_db") if isinstance(hop, dict) else None
                        if snr is None:
                            values.append(None)
                            continue
                        try:
                            snr_f = float(snr)
                        except (TypeError, ValueError):
                            values.append(None)
                            continue
                        values.append(snr_f)
                        has_value = True
                    if has_value:
                        back_snr_db = values
            traces.append(
                {
                    "trace_id": int(row["trace_id"]),
                    "captured_at_utc": row["captured_at_utc"],
                    "towards_nums": towards_nums,
                    "back_nums": back_nums,
                    "packet": packet_raw if isinstance(packet_raw, dict) else {},
                    "towards_snr_db": towards_snr_db,
                    "back_snr_db": back_snr_db,
                }
            )
        return nodes, traces

    def close(self) -> None:
        with self._lock:
            self._conn.close()
