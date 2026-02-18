from __future__ import annotations

import threading
from types import SimpleNamespace

from meshtracer_app.app import MeshTracerController
from meshtracer_app.state import MapState
from meshtracer_app.storage import SQLiteStore


class _DummyWorker:
    def is_alive(self) -> bool:
        return True


class _DummyLocalNode:
    def __init__(self, node_num: int | None) -> None:
        self.nodeNum = node_num


class _DummyInterface:
    def __init__(self, local_num: int | None = None) -> None:
        self.localNode = _DummyLocalNode(local_num)
        self.nodesByNum = {}


class _DummyTraceInterface(_DummyInterface):
    def __init__(self, local_num: int | None = None) -> None:
        super().__init__(local_num=local_num)
        self.trace_calls: list[tuple[int, int]] = []

    def sendTraceRoute(self, *, dest: int, hopLimit: int) -> None:
        self.trace_calls.append((int(dest), int(hopLimit)))


class _DummyChatInterface(_DummyInterface):
    def __init__(self, local_num: int | None = None) -> None:
        super().__init__(local_num=local_num)
        self.sent_messages: list[dict[str, object]] = []
        self._next_id = 2000

    def sendText(self, text: str, **kwargs: object) -> dict[str, int]:
        self.sent_messages.append({"text": text, **kwargs})
        self._next_id += 1
        return {"id": self._next_id}


class _DummyChatNoIdInterface(_DummyChatInterface):
    def sendText(self, text: str, **kwargs: object) -> dict[str, object]:
        self.sent_messages.append({"text": text, **kwargs})
        return {}


class _DummyTelemetryField:
    def __init__(self) -> None:
        self.last_copy = None

    def CopyFrom(self, value: object) -> None:
        self.last_copy = value


class _DummyTelemetryMessage:
    def __init__(self) -> None:
        self.device_metrics = _DummyTelemetryField()
        self.environment_metrics = _DummyTelemetryField()
        self.power_metrics = _DummyTelemetryField()


class _DummyTelemetryInterface(_DummyInterface):
    def __init__(self, local_num: int | None = None) -> None:
        super().__init__(local_num=local_num)
        self.sent_packets: list[dict[str, object]] = []
        self.onResponseTelemetry = lambda _packet=None: None
        self.isConnected = threading.Event()
        self.isConnected.set()

    def sendData(self, data: object, **kwargs: object) -> dict[str, int]:
        self.sent_packets.append({"data": data, **kwargs})
        return {"id": len(self.sent_packets)}


class _DummyPositionObject:
    def __init__(self, latitude_i: int, longitude_i: int) -> None:
        self.latitude_i = latitude_i
        self.longitude_i = longitude_i
        self.altitude = 100


def _args(**overrides: object) -> SimpleNamespace:
    base: dict[str, object] = {
        "traceroute_behavior": None,
        "interval": None,
        "heard_window": None,
        "fresh_window": None,
        "mid_window": None,
        "hop_limit": None,
        "traceroute_retention_hours": None,
        "webhook_url": None,
        "webhook_api_token": None,
        "web_ui": True,
        "db_path": "meshtracer.db",
        "map_host": "127.0.0.1",
        "map_port": 8090,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _set_connected_state(
    controller: MeshTracerController,
    store: SQLiteStore,
    *,
    local_num: int | None,
    mesh_host: str = "test:queue",
) -> threading.Event:
    wake_event = threading.Event()
    map_state = MapState(store=store, mesh_host=mesh_host)
    with controller._lock:
        controller._interface = _DummyInterface(local_num=local_num)
        controller._worker_thread = _DummyWorker()
        controller._worker_wake = wake_event
        controller._map_state = map_state
        controller._connection_state = "connected"
    return wake_event
