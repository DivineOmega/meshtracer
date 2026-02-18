from __future__ import annotations

from typing import Any


DEFAULT_RUNTIME_CONFIG: dict[str, Any] = {
    "traceroute_behavior": "automatic",
    "interval": 5,
    "heard_window": 120,
    "fresh_window": 120,
    "mid_window": 480,
    "hop_limit": 7,
    "webhook_url": None,
    "webhook_api_token": None,
    "traceroute_retention_hours": 720,
}
