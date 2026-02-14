from __future__ import annotations

import json
import urllib.error
import urllib.request


def post_webhook(url: str, api_token: str | None, payload: dict[str, object]) -> tuple[bool, str]:
    request_body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "meshtracer/1.0",
    }
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
        headers["X-API-Token"] = api_token

    req = urllib.request.Request(url, data=request_body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            status = response.getcode()
            body = response.read(200).decode("utf-8", errors="replace")
            if 200 <= status < 300:
                detail = f"HTTP {status}"
                if body.strip():
                    detail += f" ({body.strip()})"
                return True, detail
            return False, f"HTTP {status}"
    except urllib.error.HTTPError as exc:
        body = exc.read(200).decode("utf-8", errors="replace")
        detail = f"HTTP {exc.code}"
        if body.strip():
            detail += f" ({body.strip()})"
        return False, detail
    except Exception as exc:
        return False, str(exc)
