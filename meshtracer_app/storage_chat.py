from __future__ import annotations

import json
import sqlite3
from typing import Any

from .common import utc_now
from .storage_repo_base import StoreRepositoryBase


class ChatRepository(StoreRepositoryBase):
    def _chat_message_from_row(self, row: Any) -> dict[str, Any]:
        packet_raw = self._json_loads(row["packet_json"], {})
        packet_value = packet_raw if isinstance(packet_raw, (dict, list)) else {}
        return {
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
            messages.append(self._chat_message_from_row(row))
        return messages

    def list_incoming_chat_messages_since(
        self,
        mesh_host: str,
        *,
        since_chat_id: Any,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        host = str(mesh_host or "").strip()
        if not host:
            return []

        since_int = self._to_int(since_chat_id)
        if since_int is None:
            since_int = 0
        since_int = max(0, since_int)

        try:
            limit_int = int(limit)
        except (TypeError, ValueError):
            limit_int = 200
        limit_int = max(1, min(1000, limit_int))

        with self._lock:
            rows = self._conn.execute(
                """
                SELECT
                  chat_id, direction, message_type, channel_index, peer_node_num,
                  from_node_num, to_node_num, packet_id, rx_time, text, packet_json, created_at_utc
                FROM chat_messages
                WHERE mesh_host = ?
                  AND direction = 'incoming'
                  AND chat_id > ?
                ORDER BY chat_id ASC
                LIMIT ?
                """,
                (host, since_int, limit_int),
            ).fetchall()

        messages: list[dict[str, Any]] = []
        for row in rows:
            messages.append(self._chat_message_from_row(row))
        return messages
