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


if __name__ == "__main__":
    unittest.main()
