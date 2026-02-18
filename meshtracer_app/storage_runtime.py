from __future__ import annotations

import json
from typing import Any

from .common import utc_now
from .storage_repo_base import StoreRepositoryBase


class RuntimeConfigRepository(StoreRepositoryBase):
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
