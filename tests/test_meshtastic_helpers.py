from __future__ import annotations

import time
import unittest

from meshtracer_app.meshtastic_helpers import pick_recent_node, resolve_mesh_partition_key


class _DummyLocalNode:
    def __init__(self, node_num: int | None) -> None:
        self.nodeNum = node_num


class _DummyInterface:
    def __init__(self, nodes_by_num: dict, local_num: int | None = None, node_id_map: dict[int, str] | None = None) -> None:
        self.nodesByNum = nodes_by_num
        self.localNode = _DummyLocalNode(local_num)
        self._node_id_map = node_id_map or {}

    def _nodeNumToId(self, node_num: int, _allow_unknown: bool) -> str | None:
        return self._node_id_map.get(node_num)


class MeshtasticHelpersTests(unittest.TestCase):
    def test_pick_recent_node_ignores_bad_last_heard_values(self) -> None:
        now = time.time()
        interface = _DummyInterface(
            {
                1: {"num": 1, "lastHeard": "not-a-number"},
                2: {"num": 2, "lastHeard": None},
                3: {"num": 3, "lastHeard": now - 60},
            },
            local_num=99,
        )

        node, age, count = pick_recent_node(interface, heard_window_seconds=3600)

        self.assertIsNotNone(node)
        self.assertEqual(node.get("num"), 3)
        self.assertIsNotNone(age)
        self.assertEqual(count, 1)

    def test_pick_recent_node_excludes_local_node(self) -> None:
        now_like = time.time()
        interface = _DummyInterface(
            {
                10: {"num": 10, "lastHeard": now_like},
                20: {"num": 20, "lastHeard": now_like},
            },
            local_num=10,
        )

        node, _age, count = pick_recent_node(interface, heard_window_seconds=3600)

        self.assertEqual(count, 1)
        self.assertIsNotNone(node)
        self.assertEqual(node.get("num"), 20)

    def test_pick_recent_node_ignores_far_future_and_clamps_small_future_skew(self) -> None:
        now = time.time()
        interface = _DummyInterface(
            {
                1: {"num": 1, "lastHeard": now + 7200},
                2: {"num": 2, "lastHeard": now + 120},
            },
            local_num=99,
        )

        node, age, count = pick_recent_node(interface, heard_window_seconds=3600)

        self.assertEqual(count, 1)
        self.assertIsNotNone(node)
        self.assertEqual(node.get("num"), 2)
        self.assertEqual(age, 0.0)

    def test_resolve_mesh_partition_key_prefers_local_node_id(self) -> None:
        interface = _DummyInterface({}, local_num=1234, node_id_map={1234: "!abcd1234"})
        key = resolve_mesh_partition_key(interface, fallback_host="192.168.1.9")
        self.assertEqual(key, "node:1234:!abcd1234")

    def test_resolve_mesh_partition_key_falls_back_to_host(self) -> None:
        interface = _DummyInterface({}, local_num=None)
        key = resolve_mesh_partition_key(interface, fallback_host="ExampleHost")
        self.assertEqual(key, "host:examplehost")


if __name__ == "__main__":
    unittest.main()
