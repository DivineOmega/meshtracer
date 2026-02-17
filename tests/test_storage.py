from __future__ import annotations

import tempfile
import unittest
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
                        "max_map_traces": 12,
                        "max_stored_traces": 1234,
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
                self.assertEqual(loaded.get("max_map_traces"), 12)
                self.assertEqual(loaded.get("max_stored_traces"), 1234)
            finally:
                store2.close()

    def test_add_traceroute_prunes_to_max_keep(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                for i in range(1, 6):
                    store.add_traceroute("node:1", _result(i), max_keep=2)

                _nodes, traces = store.snapshot("node:1", max_traces=100)

                self.assertEqual(len(traces), 2)
                self.assertEqual([t["trace_id"] for t in traces], sorted([t["trace_id"] for t in traces]))
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
                self.assertEqual(node.get("lat"), 45.0)
                self.assertEqual(node.get("lon"), -122.0)
                self.assertEqual(node.get("last_heard"), 1000.0)
            finally:
                store.close()

    def test_snapshot_respects_mesh_host_partition(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                store.add_traceroute("node:A", _result(10), max_keep=None)
                store.add_traceroute("node:B", _result(20), max_keep=None)

                _nodes_a, traces_a = store.snapshot("node:A", max_traces=10)
                _nodes_b, traces_b = store.snapshot("node:B", max_traces=10)

                self.assertEqual(len(traces_a), 1)
                self.assertEqual(len(traces_b), 1)
                self.assertNotEqual(traces_a[0]["trace_id"], traces_b[0]["trace_id"])
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


if __name__ == "__main__":
    unittest.main()
