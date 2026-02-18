from __future__ import annotations

import json
from typing import Any

from .common import utc_now
from .storage_repo_base import StoreRepositoryBase


class NodeRepository(StoreRepositoryBase):
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
