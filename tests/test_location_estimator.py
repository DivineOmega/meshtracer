from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from meshtracer_app.location_estimator import estimate_node_positions
from meshtracer_app.common import utc_now
from meshtracer_app.state import MapState
from meshtracer_app.storage import SQLiteStore


class LocationEstimatorTests(unittest.TestCase):
    def test_estimates_unknown_node_between_two_anchors(self) -> None:
        nodes = [
            {"num": 1, "lat": 0.0, "lon": 0.0},
            {"num": 3, "lat": 0.0, "lon": 0.02},
        ]
        traces = [{"towards_nums": [1, 2, 3], "back_nums": []}]

        estimated_nodes = estimate_node_positions(nodes, traces)
        by_num = {int(node["num"]): node for node in estimated_nodes}

        self.assertIn(2, by_num)
        self.assertFalse(bool(by_num[1].get("estimated")))
        self.assertAlmostEqual(float(by_num[1].get("lat") or 0.0), 0.0, places=7)
        self.assertAlmostEqual(float(by_num[1].get("lon") or 0.0), 0.0, places=7)

        estimated = by_num[2]
        self.assertTrue(bool(estimated.get("estimated")))
        self.assertIsNotNone(estimated.get("lat"))
        self.assertIsNotNone(estimated.get("lon"))
        self.assertGreater(float(estimated.get("lon") or 0.0), 0.0)
        self.assertLess(float(estimated.get("lon") or 0.0), 0.02)

    def test_includes_trace_only_nodes_when_not_present_in_node_cache(self) -> None:
        estimated_nodes = estimate_node_positions(
            nodes=[],
            traces=[{"towards_nums": [99, 100], "back_nums": []}],
        )
        by_num = {int(node["num"]): node for node in estimated_nodes}

        self.assertEqual(set(by_num.keys()), {99, 100})
        self.assertEqual(by_num[99].get("long_name"), "Unknown 0063")
        self.assertEqual(by_num[100].get("long_name"), "Unknown 0064")
        self.assertTrue(bool(by_num[99].get("trace_only")))
        self.assertTrue(bool(by_num[100].get("trace_only")))
        self.assertFalse(bool(by_num[99].get("estimated")))
        self.assertFalse(bool(by_num[100].get("estimated")))
        self.assertIsNone(by_num[99].get("lat"))
        self.assertIsNone(by_num[99].get("lon"))

    def test_map_state_snapshot_applies_backend_estimates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                store.upsert_node(
                    "test:estimate",
                    {
                        "num": 1,
                        "id": "!00000001",
                        "long_name": "Anchor A",
                        "short_name": "A",
                        "lat": 37.00000,
                        "lon": -122.00000,
                    },
                )
                store.upsert_node(
                    "test:estimate",
                    {
                        "num": 3,
                        "id": "!00000003",
                        "long_name": "Anchor B",
                        "short_name": "B",
                        "lat": 37.00000,
                        "lon": -121.98000,
                    },
                )
                store.add_traceroute(
                    "test:estimate",
                    {
                        "captured_at_utc": utc_now(),
                        "packet": {"from": {"num": 1}, "to": {"num": 3}},
                        "route_towards_destination": [
                            {"node": {"num": 1}},
                            {"node": {"num": 2}},
                            {"node": {"num": 3}},
                        ],
                        "route_back_to_origin": [],
                        "raw": {},
                    },
                )

                map_state = MapState(store=store, mesh_host="test:estimate")
                snapshot = map_state.snapshot()
                nodes = list(snapshot.get("nodes") or [])
                by_num = {int(node["num"]): node for node in nodes if node.get("num") is not None}

                self.assertEqual(int(snapshot.get("node_count") or 0), 3)
                self.assertIn(2, by_num)
                self.assertTrue(bool(by_num[2].get("estimated")))
                self.assertTrue(bool(by_num[2].get("trace_only")))
                self.assertIsNotNone(by_num[2].get("lat"))
                self.assertIsNotNone(by_num[2].get("lon"))
                self.assertFalse(bool(by_num[1].get("estimated")))
                self.assertFalse(bool(by_num[3].get("estimated")))

                edges = list(snapshot.get("edges") or [])
                edge_pairs = {(int(edge["from_num"]), int(edge["to_num"])) for edge in edges}
                self.assertIn((1, 2), edge_pairs)
                self.assertIn((2, 3), edge_pairs)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
