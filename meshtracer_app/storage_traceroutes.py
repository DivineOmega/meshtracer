from __future__ import annotations

import json
import sqlite3
from typing import Any

from .common import utc_now
from .meshtastic_helpers import extract_route_nums
from .storage_repo_base import StoreRepositoryBase


class TracerouteRepository(StoreRepositoryBase):
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
