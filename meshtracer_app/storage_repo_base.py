from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Any


class StoreRepositoryBase:
    _TELEMETRY_TYPES = {"device", "environment", "power"}

    def __init__(self, conn: Any, lock: Any) -> None:
        self._conn = conn
        self._lock = lock

    @staticmethod
    def _queue_status_text(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in ("queued", "running"):
            return text
        return "queued"

    @staticmethod
    def _chat_message_type_text(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in ("channel", "direct"):
            return text
        return ""

    @staticmethod
    def _chat_direction_text(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in ("incoming", "outgoing", "unknown"):
            return text
        return "unknown"

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_bool(value: Any) -> bool | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(int(value))

        text = str(value).strip().lower()
        if not text:
            return None
        if text in ("1", "true", "yes", "y", "on"):
            return True
        if text in ("0", "false", "no", "n", "off"):
            return False
        return None

    @staticmethod
    def _json_loads(value: Any, fallback: Any) -> Any:
        if not value:
            return fallback
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(str(value))
        except json.JSONDecodeError:
            return fallback

    @classmethod
    def _json_safe_value(cls, value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, dict):
            safe: dict[str, Any] = {}
            for key, item in value.items():
                safe[str(key)] = cls._json_safe_value(item)
            return safe
        if isinstance(value, (list, tuple, set)):
            return [cls._json_safe_value(item) for item in value]
        if isinstance(value, (bytes, bytearray)):
            try:
                return bytes(value).decode("utf-8")
            except Exception:
                return bytes(value).hex()

        try:
            from google.protobuf.json_format import MessageToDict  # type: ignore

            as_dict = MessageToDict(value)
            return cls._json_safe_value(as_dict)
        except Exception:
            pass

        return str(value)

    @classmethod
    def _telemetry_type_text(cls, value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in cls._TELEMETRY_TYPES:
            return text
        return ""

    @staticmethod
    def _utc_cutoff_text_for_hours(hours: Any) -> str | None:
        try:
            hours_f = float(hours)
        except (TypeError, ValueError):
            return None
        if not (hours_f > 0):
            return None
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_f)
        return cutoff.strftime("%Y-%m-%d %H:%M:%S UTC")
