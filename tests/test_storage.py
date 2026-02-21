from __future__ import annotations

import tempfile
import unittest
import sqlite3
from pathlib import Path

from meshtracer_app.storage import SQLiteStore


def _result(node_num: int) -> dict:
    return {
        "captured_at_utc": "2026-01-01 00:00:00 UTC",
        "packet": {
            "from": {"num": node_num},
            "to": {"num": 1},
        },
        "route_towards_destination": [
            {"node": {"num": 1}},
            {"node": {"num": node_num}},
        ],
        "route_back_to_origin": [
            {"node": {"num": node_num}},
            {"node": {"num": 1}},
        ],
        "raw": {},
    }


class _UnserializableValue:
    def __str__(self) -> str:
        return "unserializable-value"


class SQLiteStoreTests(unittest.TestCase):
    def test_runtime_config_persists_across_store_instances(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                store.set_runtime_config(
                    {
                        "interval": 9,
                        "heard_window": 42,
                        "hop_limit": 3,
                        "webhook_url": "https://example.com/hook",
                        "webhook_api_token": "secret",
                        "traceroute_retention_hours": 12,
                    }
                )
            finally:
                store.close()

            store2 = SQLiteStore(str(db_path))
            try:
                loaded = store2.get_runtime_config()
                self.assertIsInstance(loaded, dict)
                self.assertEqual(loaded.get("interval"), 9)
                self.assertEqual(loaded.get("heard_window"), 42)
                self.assertEqual(loaded.get("hop_limit"), 3)
                self.assertEqual(loaded.get("webhook_url"), "https://example.com/hook")
                self.assertEqual(loaded.get("webhook_api_token"), "secret")
                self.assertEqual(loaded.get("traceroute_retention_hours"), 12)
            finally:
                store2.close()

    def test_prune_traceroutes_older_than_deletes_expired_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                older = _result(1)
                older["captured_at_utc"] = "2025-01-01 00:00:00 UTC"
                newer = _result(2)
                newer["captured_at_utc"] = "2099-12-01 00:00:00 UTC"
                store.add_traceroute("node:1", older)
                store.add_traceroute("node:1", newer)
                deleted = store.prune_traceroutes_older_than("node:1", 24)

                _nodes, traces = store.snapshot("node:1", max_traces=100)

                self.assertEqual(deleted, 1)
                self.assertEqual(len(traces), 1)
                self.assertEqual(traces[0].get("packet", {}).get("from", {}).get("num"), 2)
            finally:
                store.close()

    def test_upsert_nodes_preserves_existing_fields_when_update_is_partial(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                store.upsert_node(
                    "node:1",
                    {
                        "num": 123,
                        "id": "!abc",
                        "long_name": "Full Node",
                        "short_name": "FN",
                        "hw_model": "HELTEC_V3",
                        "role": "ROUTER",
                        "is_licensed": True,
                        "is_unmessagable": False,
                        "public_key": "abcdef1234567890abcdef1234567890",
                        "snr": 7.25,
                        "hops_away": 2,
                        "channel": 1,
                        "via_mqtt": True,
                        "is_favorite": True,
                        "is_ignored": False,
                        "is_muted": False,
                        "is_key_manually_verified": True,
                        "lat": 45.0,
                        "lon": -122.0,
                        "last_heard": 1000,
                    },
                )
                store.upsert_node("node:1", {"num": 123})
                store.upsert_node("node:1", {"num": 123, "last_heard": 999})

                nodes, _traces = store.snapshot("node:1", max_traces=10)
                self.assertEqual(len(nodes), 1)
                node = nodes[0]
                self.assertEqual(node.get("id"), "!abc")
                self.assertEqual(node.get("long_name"), "Full Node")
                self.assertEqual(node.get("short_name"), "FN")
                self.assertEqual(node.get("hw_model"), "HELTEC_V3")
                self.assertEqual(node.get("role"), "ROUTER")
                self.assertTrue(node.get("is_licensed"))
                self.assertFalse(node.get("is_unmessagable"))
                self.assertEqual(node.get("public_key"), "abcdef1234567890abcdef1234567890")
                self.assertEqual(node.get("snr"), 7.25)
                self.assertEqual(node.get("hops_away"), 2)
                self.assertEqual(node.get("channel"), 1)
                self.assertTrue(node.get("via_mqtt"))
                self.assertTrue(node.get("is_favorite"))
                self.assertFalse(node.get("is_ignored"))
                self.assertFalse(node.get("is_muted"))
                self.assertTrue(node.get("is_key_manually_verified"))
                self.assertEqual(node.get("lat"), 45.0)
                self.assertEqual(node.get("lon"), -122.0)
                self.assertEqual(node.get("last_heard"), 1000.0)
            finally:
                store.close()

    def test_list_nodes_for_traceroute_returns_interface_like_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                store.upsert_node(
                    "node:shape",
                    {
                        "num": 22,
                        "id": "!node22",
                        "long_name": "Node Twenty Two",
                        "short_name": "N22",
                        "last_heard": 12345,
                    },
                )

                rows = store.list_nodes_for_traceroute("node:shape")
                self.assertEqual(len(rows), 1)
                row = rows[0]
                self.assertEqual(row.get("num"), 22)
                self.assertEqual(row.get("id"), "!node22")
                self.assertEqual(row.get("lastHeard"), 12345.0)
                user = row.get("user") or {}
                self.assertEqual(user.get("id"), "!node22")
                self.assertEqual(user.get("longName"), "Node Twenty Two")
                self.assertEqual(user.get("shortName"), "N22")
            finally:
                store.close()

    def test_nodes_table_hw_model_column_migrates_for_existing_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"

            conn = sqlite3.connect(str(db_path))
            try:
                conn.executescript(
                    """
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
                    """
                )
                conn.commit()
            finally:
                conn.close()

            store = SQLiteStore(str(db_path))
            try:
                store.upsert_node(
                    "node:migrate",
                    {
                        "num": 55,
                        "short_name": "MIG",
                        "hw_model": "T_DECK",
                        "role": "CLIENT",
                        "via_mqtt": True,
                    },
                )
                nodes, _traces = store.snapshot("node:migrate", max_traces=10)
                self.assertEqual(len(nodes), 1)
                self.assertEqual(nodes[0].get("hw_model"), "T_DECK")
                self.assertEqual(nodes[0].get("role"), "CLIENT")
                self.assertTrue(nodes[0].get("via_mqtt"))
            finally:
                store.close()

    def test_snapshot_respects_mesh_host_partition(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                store.add_traceroute("node:A", _result(10))
                store.add_traceroute("node:B", _result(20))

                _nodes_a, traces_a = store.snapshot("node:A", max_traces=10)
                _nodes_b, traces_b = store.snapshot("node:B", max_traces=10)

                self.assertEqual(len(traces_a), 1)
                self.assertEqual(len(traces_b), 1)
                self.assertNotEqual(traces_a[0]["trace_id"], traces_b[0]["trace_id"])
            finally:
                store.close()

    def test_snapshot_without_limit_returns_all_traces(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                store.add_traceroute("node:all", _result(10))
                store.add_traceroute("node:all", _result(11))
                store.add_traceroute("node:all", _result(12))

                _nodes, traces = store.snapshot("node:all")
                self.assertEqual(len(traces), 3)
            finally:
                store.close()

    def test_node_telemetry_persists_and_is_exposed_in_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                store.upsert_node(
                    "node:telemetry",
                    {
                        "num": 321,
                        "id": "!telemetry",
                        "long_name": "Telemetry Node",
                        "short_name": "TEL",
                    },
                )
                self.assertTrue(
                    store.upsert_node_telemetry(
                        "node:telemetry",
                        321,
                        "device",
                        {"batteryLevel": 85},
                    )
                )
                self.assertTrue(
                    store.upsert_node_telemetry(
                        "node:telemetry",
                        321,
                        "device",
                        {"voltage": 3.93},
                    )
                )
                self.assertTrue(
                    store.upsert_node_telemetry(
                        "node:telemetry",
                        321,
                        "environment",
                        {"temperature": 22.5},
                    )
                )
                self.assertTrue(
                    store.upsert_node_telemetry(
                        "node:telemetry",
                        321,
                        "power",
                        {"ch1Voltage": 13.2},
                    )
                )

                device = store.get_node_telemetry("node:telemetry", 321, "device")
                environment = store.get_node_telemetry("node:telemetry", 321, "environment")
                power = store.get_node_telemetry("node:telemetry", 321, "power")
                self.assertIsNotNone(device)
                self.assertIsNotNone(environment)
                self.assertIsNotNone(power)
                self.assertEqual((device or {}).get("telemetry", {}).get("batteryLevel"), 85)
                self.assertEqual((device or {}).get("telemetry", {}).get("voltage"), 3.93)
                self.assertEqual((environment or {}).get("telemetry", {}).get("temperature"), 22.5)
                self.assertEqual((power or {}).get("telemetry", {}).get("ch1Voltage"), 13.2)
                self.assertTrue(bool((device or {}).get("updated_at_utc")))
                self.assertTrue(bool((environment or {}).get("updated_at_utc")))
                self.assertTrue(bool((power or {}).get("updated_at_utc")))

                nodes, _traces = store.snapshot("node:telemetry", max_traces=10)
                self.assertEqual(len(nodes), 1)
                node = nodes[0]
                self.assertEqual(node.get("device_telemetry", {}).get("batteryLevel"), 85)
                self.assertEqual(node.get("device_telemetry", {}).get("voltage"), 3.93)
                self.assertEqual(node.get("environment_telemetry", {}).get("temperature"), 22.5)
                self.assertEqual(node.get("power_telemetry", {}).get("ch1Voltage"), 13.2)
                self.assertTrue(bool(node.get("device_telemetry_updated_at_utc")))
                self.assertTrue(bool(node.get("environment_telemetry_updated_at_utc")))
                self.assertTrue(bool(node.get("power_telemetry_updated_at_utc")))
            finally:
                store.close()

    def test_node_position_persists_and_is_exposed_in_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                store.upsert_node(
                    "node:position",
                    {
                        "num": 987,
                        "id": "!position",
                        "long_name": "Position Node",
                        "short_name": "POS",
                        "lat": 52.67784,
                        "lon": -2.5559,
                    },
                )
                self.assertTrue(
                    store.upsert_node_position(
                        "node:position",
                        987,
                        {"latitudeI": 526778400, "longitudeI": -25559000},
                    )
                )
                self.assertTrue(
                    store.upsert_node_position(
                        "node:position",
                        987,
                        {"altitude": 165, "satsInView": 7},
                    )
                )

                position = store.get_node_position("node:position", 987)
                self.assertIsNotNone(position)
                self.assertEqual((position or {}).get("position", {}).get("latitudeI"), 526778400)
                self.assertEqual((position or {}).get("position", {}).get("longitudeI"), -25559000)
                self.assertEqual((position or {}).get("position", {}).get("altitude"), 165)
                self.assertEqual((position or {}).get("position", {}).get("satsInView"), 7)
                self.assertTrue(bool((position or {}).get("updated_at_utc")))

                nodes, _traces = store.snapshot("node:position", max_traces=10)
                self.assertEqual(len(nodes), 1)
                node = nodes[0]
                self.assertEqual(node.get("position", {}).get("latitudeI"), 526778400)
                self.assertEqual(node.get("position", {}).get("longitudeI"), -25559000)
                self.assertEqual(node.get("position", {}).get("altitude"), 165)
                self.assertEqual(node.get("position", {}).get("satsInView"), 7)
                self.assertTrue(bool(node.get("position_updated_at_utc")))
            finally:
                store.close()

    def test_upsert_node_position_coerces_non_json_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                self.assertTrue(
                    store.upsert_node_position(
                        "node:position-safe",
                        999,
                        {
                            "latitudeI": 515001234,
                            "longitudeI": -1234567,
                            "rawProto": _UnserializableValue(),
                            "nested": {"value": _UnserializableValue()},
                        },
                    )
                )

                position = store.get_node_position("node:position-safe", 999)
                self.assertIsNotNone(position)
                saved = (position or {}).get("position", {})
                self.assertEqual(saved.get("latitudeI"), 515001234)
                self.assertEqual(saved.get("longitudeI"), -1234567)
                self.assertEqual(saved.get("rawProto"), "unserializable-value")
                self.assertEqual((saved.get("nested") or {}).get("value"), "unserializable-value")
            finally:
                store.close()

    def test_traceroute_queue_flow_and_requeue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                first = store.enqueue_traceroute_target("node:q", 100)
                second = store.enqueue_traceroute_target("node:q", 200)
                self.assertIsNotNone(first)
                self.assertIsNotNone(second)
                self.assertEqual(store.queued_position_for_entry("node:q", int(first["queue_id"])), 1)
                self.assertEqual(store.queued_position_for_entry("node:q", int(second["queue_id"])), 2)

                running = store.pop_next_queued_traceroute("node:q")
                self.assertIsNotNone(running)
                self.assertEqual(running.get("status"), "running")
                self.assertEqual(running.get("node_num"), 100)

                entries = store.list_traceroute_queue("node:q")
                self.assertEqual([entry.get("status") for entry in entries], ["running", "queued"])

                requeued_count = store.requeue_running_traceroutes("node:q")
                self.assertEqual(requeued_count, 1)
                entries = store.list_traceroute_queue("node:q")
                self.assertEqual([entry.get("status") for entry in entries], ["queued", "queued"])

                removed = store.remove_traceroute_queue_entry("node:q", int(first["queue_id"]))
                self.assertTrue(removed)
                entries = store.list_traceroute_queue("node:q")
                self.assertEqual(len(entries), 1)
                self.assertEqual(entries[0].get("node_num"), 200)
            finally:
                store.close()

    def test_traceroute_queue_persists_across_store_instances(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                entry = store.enqueue_traceroute_target("node:persist", 4242)
                self.assertIsNotNone(entry)
            finally:
                store.close()

            reopened = SQLiteStore(str(db_path))
            try:
                entries = reopened.list_traceroute_queue("node:persist")
                self.assertEqual(len(entries), 1)
                self.assertEqual(entries[0].get("node_num"), 4242)
                self.assertEqual(entries[0].get("status"), "queued")
            finally:
                reopened.close()

    def test_chat_messages_store_and_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                channel_chat_id = store.add_chat_message(
                    "node:chat",
                    text="hello channel",
                    message_type="channel",
                    direction="incoming",
                    channel_index=2,
                    from_node_num=42,
                    to_node_num=0xFFFFFFFF,
                    packet_id=1001,
                    dedupe_key="pkt:1001",
                    packet={"id": 1001},
                )
                self.assertIsNotNone(channel_chat_id)
                duplicate_chat_id = store.add_chat_message(
                    "node:chat",
                    text="hello channel",
                    message_type="channel",
                    direction="incoming",
                    channel_index=2,
                    from_node_num=42,
                    to_node_num=0xFFFFFFFF,
                    packet_id=1001,
                    dedupe_key="pkt:1001",
                    packet={"id": 1001},
                )
                self.assertEqual(duplicate_chat_id, channel_chat_id)

                direct_1 = store.add_chat_message(
                    "node:chat",
                    text="hello node 99",
                    message_type="direct",
                    direction="outgoing",
                    peer_node_num=99,
                    from_node_num=10,
                    to_node_num=99,
                    packet_id=1002,
                    dedupe_key="pkt:1002",
                    packet={"id": 1002},
                )
                direct_2 = store.add_chat_message(
                    "node:chat",
                    text="reply from node 88",
                    message_type="direct",
                    direction="incoming",
                    peer_node_num=88,
                    from_node_num=88,
                    to_node_num=10,
                    packet_id=1003,
                    dedupe_key="pkt:1003",
                    packet={"id": 1003},
                )
                self.assertIsNotNone(direct_1)
                self.assertIsNotNone(direct_2)

                self.assertEqual(store.latest_chat_revision("node:chat"), int(direct_2 or 0))
                self.assertEqual(store.list_chat_channels("node:chat"), [2])
                self.assertEqual(store.list_recent_direct_nodes("node:chat"), [88, 99])

                channel_messages = store.list_chat_messages(
                    "node:chat",
                    recipient_kind="channel",
                    recipient_id=2,
                    limit=20,
                )
                self.assertEqual(len(channel_messages), 1)
                self.assertEqual(channel_messages[0].get("text"), "hello channel")
                self.assertEqual(channel_messages[0].get("channel_index"), 2)

                direct_messages = store.list_chat_messages(
                    "node:chat",
                    recipient_kind="direct",
                    recipient_id=99,
                    limit=20,
                )
                self.assertEqual(len(direct_messages), 1)
                self.assertEqual(direct_messages[0].get("text"), "hello node 99")
                self.assertEqual(direct_messages[0].get("peer_node_num"), 99)

                incoming_since_zero = store.list_incoming_chat_messages_since(
                    "node:chat",
                    since_chat_id=0,
                    limit=20,
                )
                self.assertEqual(
                    [message.get("chat_id") for message in incoming_since_zero],
                    [int(channel_chat_id or 0), int(direct_2 or 0)],
                )
                self.assertEqual(
                    [message.get("direction") for message in incoming_since_zero],
                    ["incoming", "incoming"],
                )

                incoming_after_direct_1 = store.list_incoming_chat_messages_since(
                    "node:chat",
                    since_chat_id=int(direct_1 or 0),
                    limit=20,
                )
                self.assertEqual(len(incoming_after_direct_1), 1)
                self.assertEqual(incoming_after_direct_1[0].get("text"), "reply from node 88")
            finally:
                store.close()

    def test_reset_all_data_clears_everything(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                store.set_runtime_config(
                    {
                        "interval": 5,
                        "heard_window": 120,
                        "hop_limit": 7,
                    }
                )
                store.upsert_node(
                    "node:reset",
                    {
                        "num": 99,
                        "id": "!reset",
                        "long_name": "Reset Node",
                        "short_name": "RST",
                        "lat": 1.23,
                        "lon": 4.56,
                    },
                )
                store.upsert_node_telemetry("node:reset", 99, "device", {"batteryLevel": 88})
                store.upsert_node_position("node:reset", 99, {"latitudeI": 123000000, "longitudeI": 456000000})
                store.add_traceroute("node:reset", _result(99))
                store.enqueue_traceroute_target("node:reset", 99)
                store.add_chat_message(
                    "node:reset",
                    text="chat before reset",
                    message_type="channel",
                    direction="incoming",
                    channel_index=0,
                    dedupe_key="chat:1",
                )

                store.reset_all_data()

                self.assertIsNone(store.get_runtime_config())
                nodes, traces = store.snapshot("node:reset", max_traces=10)
                self.assertEqual(nodes, [])
                self.assertEqual(traces, [])
                self.assertEqual(store.list_traceroute_queue("node:reset"), [])
                self.assertIsNone(store.get_node_telemetry("node:reset", 99, "device"))
                self.assertIsNone(store.get_node_position("node:reset", 99))
                self.assertEqual(
                    store.list_chat_messages(
                        "node:reset",
                        recipient_kind="channel",
                        recipient_id=0,
                    ),
                    [],
                )
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
