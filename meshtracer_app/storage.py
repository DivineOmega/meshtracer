from __future__ import annotations

import os
import sqlite3
import threading
from typing import Any

from .storage_chat import ChatRepository
from .storage_nodes import NodeRepository
from .storage_runtime import RuntimeConfigRepository
from .storage_snapshot import SnapshotRepository
from .storage_traceroutes import TracerouteRepository


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

        self._runtime_repo = RuntimeConfigRepository(self._conn, self._lock)
        self._node_repo = NodeRepository(self._conn, self._lock)
        self._traceroute_repo = TracerouteRepository(self._conn, self._lock)
        self._chat_repo = ChatRepository(self._conn, self._lock)
        self._snapshot_repo = SnapshotRepository(self._conn, self._lock)

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

    def get_runtime_config(self, config_key: str = "global") -> dict[str, Any] | None:
        return self._runtime_repo.get_runtime_config(config_key)

    def set_runtime_config(self, config: dict[str, Any], config_key: str = "global") -> None:
        self._runtime_repo.set_runtime_config(config, config_key)

    def reset_all_data(self) -> None:
        self._runtime_repo.reset_all_data()

    def upsert_nodes(self, mesh_host: str, nodes: list[dict[str, Any]]) -> None:
        self._node_repo.upsert_nodes(mesh_host, nodes)

    def upsert_node(self, mesh_host: str, node: dict[str, Any]) -> None:
        self._node_repo.upsert_node(mesh_host, node)

    def upsert_node_telemetry(
        self,
        mesh_host: str,
        node_num: int,
        telemetry_type: str,
        telemetry: dict[str, Any],
    ) -> bool:
        return self._node_repo.upsert_node_telemetry(mesh_host, node_num, telemetry_type, telemetry)

    def get_node_telemetry(self, mesh_host: str, node_num: int, telemetry_type: str) -> dict[str, Any] | None:
        return self._node_repo.get_node_telemetry(mesh_host, node_num, telemetry_type)

    def upsert_node_position(
        self,
        mesh_host: str,
        node_num: int,
        position: dict[str, Any],
    ) -> bool:
        return self._node_repo.upsert_node_position(mesh_host, node_num, position)

    def get_node_position(self, mesh_host: str, node_num: int) -> dict[str, Any] | None:
        return self._node_repo.get_node_position(mesh_host, node_num)

    def add_traceroute(self, mesh_host: str, result: dict[str, Any]) -> int:
        return self._traceroute_repo.add_traceroute(mesh_host, result)

    def prune_traceroutes_older_than(self, mesh_host: str, retention_hours: Any) -> int:
        return self._traceroute_repo.prune_traceroutes_older_than(mesh_host, retention_hours)

    def list_traceroute_queue(self, mesh_host: str) -> list[dict[str, Any]]:
        return self._traceroute_repo.list_traceroute_queue(mesh_host)

    def get_traceroute_queue_entry(self, mesh_host: str, queue_id: int) -> dict[str, Any] | None:
        return self._traceroute_repo.get_traceroute_queue_entry(mesh_host, queue_id)

    def find_traceroute_queue_entry_by_node(self, mesh_host: str, node_num: int) -> dict[str, Any] | None:
        return self._traceroute_repo.find_traceroute_queue_entry_by_node(mesh_host, node_num)

    def enqueue_traceroute_target(self, mesh_host: str, node_num: int) -> dict[str, Any] | None:
        return self._traceroute_repo.enqueue_traceroute_target(mesh_host, node_num)

    def queued_position_for_entry(self, mesh_host: str, queue_id: int) -> int:
        return self._traceroute_repo.queued_position_for_entry(mesh_host, queue_id)

    def pop_next_queued_traceroute(self, mesh_host: str) -> dict[str, Any] | None:
        return self._traceroute_repo.pop_next_queued_traceroute(mesh_host)

    def remove_traceroute_queue_entry(self, mesh_host: str, queue_id: int) -> bool:
        return self._traceroute_repo.remove_traceroute_queue_entry(mesh_host, queue_id)

    def requeue_running_traceroutes(self, mesh_host: str) -> int:
        return self._traceroute_repo.requeue_running_traceroutes(mesh_host)

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
        return self._chat_repo.add_chat_message(
            mesh_host,
            text=text,
            message_type=message_type,
            direction=direction,
            channel_index=channel_index,
            peer_node_num=peer_node_num,
            from_node_num=from_node_num,
            to_node_num=to_node_num,
            packet_id=packet_id,
            rx_time=rx_time,
            packet=packet,
            dedupe_key=dedupe_key,
            created_at_utc=created_at_utc,
        )

    def latest_chat_revision(self, mesh_host: str) -> int:
        return self._chat_repo.latest_chat_revision(mesh_host)

    def list_chat_channels(self, mesh_host: str) -> list[int]:
        return self._chat_repo.list_chat_channels(mesh_host)

    def list_recent_direct_nodes(self, mesh_host: str, limit: int = 30) -> list[int]:
        return self._chat_repo.list_recent_direct_nodes(mesh_host, limit=limit)

    def list_chat_messages(
        self,
        mesh_host: str,
        *,
        recipient_kind: Any,
        recipient_id: Any,
        limit: int = 300,
    ) -> list[dict[str, Any]]:
        return self._chat_repo.list_chat_messages(
            mesh_host,
            recipient_kind=recipient_kind,
            recipient_id=recipient_id,
            limit=limit,
        )

    def snapshot(
        self,
        mesh_host: str,
        max_traces: int | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        return self._snapshot_repo.snapshot(mesh_host, max_traces=max_traces)

    def close(self) -> None:
        with self._lock:
            self._conn.close()
