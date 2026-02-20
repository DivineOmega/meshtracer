from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from meshtracer_app.app import MeshTracerController
from meshtracer_app.state import MapState, RuntimeLogBuffer
from meshtracer_app.storage import SQLiteStore

from controller_test_utils import _DummyChatInterface, _DummyChatNoIdInterface, _DummyWorker, _args


class ControllerChatTests(unittest.TestCase):
    def test_send_chat_message_and_get_chat_messages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                map_state = MapState(store=store, mesh_host="test:chat")
                chat_iface = _DummyChatInterface(local_num=10)
                with controller._lock:
                    controller._interface = chat_iface
                    controller._worker_thread = _DummyWorker()
                    controller._map_state = map_state
                    controller._connection_state = "connected"

                ok, detail = controller.send_chat_message("channel", 0, "hello channel")
                self.assertTrue(ok, detail)
                ok, detail = controller.send_chat_message("direct", 42, "hello direct")
                self.assertTrue(ok, detail)
                self.assertEqual(len(chat_iface.sent_messages), 2)
                self.assertEqual(chat_iface.sent_messages[0].get("destinationId"), "^all")
                self.assertEqual(chat_iface.sent_messages[0].get("channelIndex"), 0)
                self.assertEqual(chat_iface.sent_messages[1].get("destinationId"), 42)

                ok, detail, channel_messages, channel_revision = controller.get_chat_messages("channel", 0, 100)
                self.assertTrue(ok, detail)
                self.assertGreaterEqual(channel_revision, 1)
                self.assertEqual(len(channel_messages), 1)
                self.assertEqual(channel_messages[0].get("text"), "hello channel")
                self.assertEqual(channel_messages[0].get("direction"), "outgoing")

                ok, detail, direct_messages, direct_revision = controller.get_chat_messages("direct", 42, 100)
                self.assertTrue(ok, detail)
                self.assertGreaterEqual(direct_revision, channel_revision)
                self.assertEqual(len(direct_messages), 1)
                self.assertEqual(direct_messages[0].get("text"), "hello direct")
                self.assertEqual(direct_messages[0].get("peer_node_num"), 42)

                incoming_chat_id = store.add_chat_message(
                    "test:chat",
                    text="incoming ping",
                    message_type="direct",
                    direction="incoming",
                    peer_node_num=42,
                    from_node_num=42,
                    to_node_num=10,
                    dedupe_key="incoming:1",
                )
                self.assertIsNotNone(incoming_chat_id)

                ok, detail, incoming_messages, incoming_revision = controller.get_incoming_chat_messages(0, 100)
                self.assertTrue(ok, detail)
                self.assertGreaterEqual(incoming_revision, int(incoming_chat_id or 0))
                self.assertEqual(len(incoming_messages), 1)
                self.assertEqual(incoming_messages[0].get("text"), "incoming ping")
                self.assertEqual(incoming_messages[0].get("direction"), "incoming")

                ok, detail, empty_messages, _incoming_revision = controller.get_incoming_chat_messages(
                    int(incoming_chat_id or 0),
                    100,
                )
                self.assertTrue(ok, detail)
                self.assertEqual(empty_messages, [])
            finally:
                store.close()

    def test_get_incoming_chat_messages_rejects_invalid_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                _ok, detail, _messages, _revision = controller.get_incoming_chat_messages(-1, 50)
                self.assertEqual(detail, "no active mesh partition")

                map_state = MapState(store=store, mesh_host="test:incoming-validation")
                with controller._lock:
                    controller._map_state = map_state

                ok, detail, _messages, _revision = controller.get_incoming_chat_messages(-1, 50)
                self.assertFalse(ok)
                self.assertEqual(detail, "invalid_since_chat_id")
            finally:
                store.close()

    def test_send_chat_message_allows_repeated_text_without_packet_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                map_state = MapState(store=store, mesh_host="test:chat-no-id")
                chat_iface = _DummyChatNoIdInterface(local_num=10)
                with controller._lock:
                    controller._interface = chat_iface
                    controller._worker_thread = _DummyWorker()
                    controller._map_state = map_state
                    controller._connection_state = "connected"

                ok, detail = controller.send_chat_message("channel", 0, "same text")
                self.assertTrue(ok, detail)
                ok, detail = controller.send_chat_message("channel", 0, "same text")
                self.assertTrue(ok, detail)

                ok, detail, channel_messages, _revision = controller.get_chat_messages("channel", 0, 100)
                self.assertTrue(ok, detail)
                self.assertEqual(len(channel_messages), 2)
                self.assertEqual(channel_messages[0].get("text"), "same text")
                self.assertEqual(channel_messages[1].get("text"), "same text")
            finally:
                store.close()

    def test_capture_chat_from_packet_handles_channel_and_direct(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.db"
            store = SQLiteStore(str(db_path))
            try:
                controller = MeshTracerController(
                    args=_args(db_path=str(db_path)),
                    store=store,
                    log_buffer=RuntimeLogBuffer(),
                    emit=lambda _message: None,
                    emit_error=lambda _message: None,
                )
                map_state = MapState(store=store, mesh_host="test:chat-packets")
                iface = _DummyChatInterface(local_num=10)

                packet_channel = {
                    "id": 3001,
                    "from": 42,
                    "to": 0xFFFFFFFF,
                    "channel": 1,
                    "hopStart": 3,
                    "hopLimit": 1,
                    "rxTime": 1739740001,
                    "decoded": {"portnum": "TEXT_MESSAGE_APP", "text": "channel hello"},
                }
                packet_direct = {
                    "id": 3002,
                    "from": 42,
                    "to": 10,
                    "hopStart": 1,
                    "hopLimit": 1,
                    "rxTime": 1739740002,
                    "decoded": {"portnum": "TEXT_MESSAGE_APP", "text": "direct hello"},
                }

                saved_channel = controller._capture_chat_from_packet(iface, map_state, packet_channel)
                saved_direct = controller._capture_chat_from_packet(iface, map_state, packet_direct)
                self.assertIsNotNone(saved_channel)
                self.assertIsNotNone(saved_direct)
                self.assertEqual((saved_channel or {}).get("message_type"), "channel")
                self.assertEqual((saved_direct or {}).get("message_type"), "direct")
                self.assertEqual((saved_direct or {}).get("peer_node_num"), 42)

                channel_messages = store.list_chat_messages(
                    "test:chat-packets",
                    recipient_kind="channel",
                    recipient_id=1,
                )
                direct_messages = store.list_chat_messages(
                    "test:chat-packets",
                    recipient_kind="direct",
                    recipient_id=42,
                )
                self.assertEqual(len(channel_messages), 1)
                self.assertEqual(len(direct_messages), 1)
                self.assertEqual(channel_messages[0].get("text"), "channel hello")
                self.assertEqual(direct_messages[0].get("text"), "direct hello")
                self.assertEqual(
                    ((channel_messages[0].get("packet") or {}).get("hopsAway")),
                    2,
                )
                self.assertEqual(
                    ((direct_messages[0].get("packet") or {}).get("hopsAway")),
                    0,
                )
            finally:
                store.close()
