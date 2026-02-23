from __future__ import annotations

from typing import Any


DEFAULT_RUNTIME_CONFIG: dict[str, Any] = {
    "traceroute_behavior": "automatic",
    "traceroute_visual_style": "direction",
    "interval": 5,
    "heard_window": 120,
    "fresh_window": 120,
    "mid_window": 480,
    "hop_limit": 7,
    "webhook_url": None,
    "webhook_api_token": None,
    "traceroute_retention_hours": 720,
    "chat_notification_desktop": False,
    "chat_notification_sound": False,
    "chat_notification_notify_focused": False,
}
