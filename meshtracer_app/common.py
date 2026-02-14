from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def age_str(seconds: float) -> str:
    total = int(max(seconds, 0))
    hours, rem = divmod(total, 3600)
    minutes, sec = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m {sec}s"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"
