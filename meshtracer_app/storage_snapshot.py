from __future__ import annotations

from typing import Any

from .storage_repo_base import StoreRepositoryBase


class SnapshotRepository(StoreRepositoryBase):
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
                  tp.telemetry_json AS power_telemetry_json,
                  tp.updated_at_utc AS power_telemetry_updated_at_utc,
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
                LEFT JOIN node_telemetry AS tp
                  ON tp.mesh_host = n.mesh_host
                 AND tp.node_num = n.node_num
                 AND tp.telemetry_type = 'power'
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
            power_telemetry_raw = self._json_loads(row["power_telemetry_json"], {})
            position_raw = self._json_loads(row["position_json"], {})
            device_telemetry = device_telemetry_raw if isinstance(device_telemetry_raw, dict) else {}
            environment_telemetry = (
                environment_telemetry_raw if isinstance(environment_telemetry_raw, dict) else {}
            )
            power_telemetry = power_telemetry_raw if isinstance(power_telemetry_raw, dict) else {}
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
                    "power_telemetry": power_telemetry,
                    "power_telemetry_updated_at_utc": str(
                        row["power_telemetry_updated_at_utc"] or ""
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
