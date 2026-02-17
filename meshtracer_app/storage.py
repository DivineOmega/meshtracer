from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Any

from .common import utc_now
from .meshtastic_helpers import extract_route_nums


class SQLiteStore:
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
          lat REAL,
          lon REAL,
          last_heard REAL,
          updated_at_utc TEXT NOT NULL,
          PRIMARY KEY (mesh_host, node_num)
        );

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
        """
        with self._lock:
            self._conn.executescript(schema)
            self._conn.commit()

    @staticmethod
    def _queue_status_text(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in ("queued", "running"):
            return text
        return "queued"

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

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
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
                  lat, lon, last_heard, updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(mesh_host, node_num) DO UPDATE SET
                  node_id = COALESCE(excluded.node_id, nodes.node_id),
                  long_name = COALESCE(excluded.long_name, nodes.long_name),
                  short_name = COALESCE(excluded.short_name, nodes.short_name),
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

    def add_traceroute(self, mesh_host: str, result: dict[str, Any], max_keep: int | None = None) -> int:
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
            if max_keep is not None and max_keep > 0:
                self._conn.execute(
                    """
                    DELETE FROM traceroutes
                    WHERE mesh_host = ?
                      AND trace_id NOT IN (
                        SELECT trace_id FROM traceroutes
                        WHERE mesh_host = ?
                        ORDER BY trace_id DESC
                        LIMIT ?
                      )
                    """,
                    (mesh_host, mesh_host, int(max_keep)),
                )
            self._conn.commit()
            return int(cursor.lastrowid)

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

    def snapshot(self, mesh_host: str, max_traces: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        trace_limit = max(1, int(max_traces))
        with self._lock:
            node_rows = self._conn.execute(
                """
                SELECT node_num, node_id, long_name, short_name, lat, lon, last_heard
                FROM nodes
                WHERE mesh_host = ?
                ORDER BY node_num ASC
                """,
                (mesh_host,),
            ).fetchall()
            trace_rows = self._conn.execute(
                """
                SELECT trace_id, captured_at_utc, towards_nums_json, back_nums_json, packet_json, result_json
                FROM traceroutes
                WHERE mesh_host = ?
                ORDER BY trace_id DESC
                LIMIT ?
                """,
                (mesh_host, trace_limit),
            ).fetchall()

        nodes: list[dict[str, Any]] = []
        for row in node_rows:
            nodes.append(
                {
                    "num": int(row["node_num"]),
                    "id": row["node_id"],
                    "long_name": row["long_name"],
                    "short_name": row["short_name"],
                    "lat": self._to_float(row["lat"]),
                    "lon": self._to_float(row["lon"]),
                    "last_heard": self._to_float(row["last_heard"]),
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
