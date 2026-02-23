from __future__ import annotations

from copy import deepcopy
from typing import Any

from .common import utc_now
from .controller_defaults import DEFAULT_RUNTIME_CONFIG


class ControllerConfigMixin:
    def _bump_snapshot_revision_locked(self) -> int:
        self._snapshot_revision += 1
        try:
            self._snapshot_cv.notify_all()
        except Exception:
            pass
        return self._snapshot_revision

    def _bump_snapshot_revision(self) -> int:
        with self._lock:
            return self._bump_snapshot_revision_locked()

    def wait_for_snapshot_revision(self, since_revision: Any, timeout: float = 25.0) -> int:
        try:
            since_int = int(since_revision)
        except (TypeError, ValueError):
            since_int = 0
        try:
            wait_seconds = max(0.0, float(timeout))
        except (TypeError, ValueError):
            wait_seconds = 0.0

        with self._snapshot_cv:
            if self._snapshot_revision <= since_int:
                self._snapshot_cv.wait(timeout=wait_seconds)
            return self._snapshot_revision

    @staticmethod
    def _config_from_args(args: Any) -> dict[str, Any]:
        def pick_float(name: str) -> float:
            value = getattr(args, name, None)
            if value is None:
                return float(DEFAULT_RUNTIME_CONFIG[name])
            try:
                return float(value)
            except (TypeError, ValueError):
                return float(DEFAULT_RUNTIME_CONFIG[name])

        def pick_int(name: str) -> int:
            value = getattr(args, name, None)
            if value is None:
                return int(DEFAULT_RUNTIME_CONFIG[name])
            try:
                return int(value)
            except (TypeError, ValueError):
                return int(DEFAULT_RUNTIME_CONFIG[name])

        def pick_str(name: str) -> str | None:
            value = getattr(args, name, None)
            if value is None:
                return None
            text = str(value).strip()
            return text or None

        def pick_behavior(name: str) -> str:
            value = getattr(args, name, None)
            if value is None:
                return str(DEFAULT_RUNTIME_CONFIG["traceroute_behavior"])
            text = str(value).strip().lower()
            if text in ("automatic", "manual"):
                return text
            return str(DEFAULT_RUNTIME_CONFIG["traceroute_behavior"])

        config = deepcopy(DEFAULT_RUNTIME_CONFIG)
        config["traceroute_behavior"] = pick_behavior("traceroute_behavior")
        config["interval"] = max((1.0 / 60.0), pick_float("interval"))
        config["heard_window"] = max(1, pick_int("heard_window"))
        config["hop_limit"] = max(1, pick_int("hop_limit"))
        config["traceroute_retention_hours"] = max(1, pick_int("traceroute_retention_hours"))
        config["webhook_url"] = pick_str("webhook_url")
        config["webhook_api_token"] = pick_str("webhook_api_token")
        return config

    @staticmethod
    def _config_overrides_from_args(args: Any) -> dict[str, Any]:
        update: dict[str, Any] = {}

        def pick_float(name: str) -> float | None:
            value = getattr(args, name, None)
            if value is None:
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        def pick_int(name: str) -> int | None:
            value = getattr(args, name, None)
            if value is None:
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        def pick_any_str(name: str) -> str | None:
            value = getattr(args, name, None)
            if value is None:
                return None
            return str(value)

        interval = pick_float("interval")
        if interval is not None:
            update["interval"] = interval

        for key in [
            "heard_window",
            "fresh_window",
            "mid_window",
            "hop_limit",
            "traceroute_retention_hours",
        ]:
            value = pick_int(key)
            if value is None:
                continue
            update[key] = value

        webhook_url = pick_any_str("webhook_url")
        if webhook_url is not None:
            update["webhook_url"] = webhook_url

        webhook_api_token = pick_any_str("webhook_api_token")
        if webhook_api_token is not None:
            update["webhook_api_token"] = webhook_api_token

        traceroute_behavior = pick_any_str("traceroute_behavior")
        if traceroute_behavior is not None:
            update["traceroute_behavior"] = traceroute_behavior

        traceroute_visual_style = pick_any_str("traceroute_visual_style")
        if traceroute_visual_style is not None:
            update["traceroute_visual_style"] = traceroute_visual_style

        return update

    @staticmethod
    def _sanitize_config_for_public(config: dict[str, Any]) -> dict[str, Any]:
        sanitized = dict(config)
        token_raw = sanitized.get("webhook_api_token")
        token_set = False
        if token_raw is not None:
            try:
                token_set = bool(str(token_raw).strip())
            except Exception:
                token_set = False
        sanitized["webhook_api_token"] = None
        sanitized["webhook_api_token_set"] = token_set
        return sanitized

    @staticmethod
    def _merge_runtime_config(
        current: dict[str, Any], update: dict[str, Any]
    ) -> tuple[bool, str, dict[str, Any] | None]:
        if not isinstance(update, dict):
            return False, "expected an object", None

        def pick_float(name: str) -> float | None:
            if name not in update:
                return None
            value = update.get(name)
            if value is None:
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                raise ValueError(f"invalid {name}")

        def pick_int(name: str) -> int | None:
            if name not in update:
                return None
            value = update.get(name)
            if value is None:
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                raise ValueError(f"invalid {name}")

        def pick_str(name: str) -> str | None:
            if name not in update:
                return None
            value = update.get(name)
            if value is None:
                return None
            text = str(value).strip()
            return text or None

        def pick_behavior(name: str) -> str | None:
            if name not in update:
                return None
            value = update.get(name)
            if value is None:
                return None
            text = str(value).strip().lower()
            if not text:
                return None
            if text not in ("automatic", "manual"):
                raise ValueError("traceroute_behavior must be 'automatic' or 'manual'")
            return text

        def pick_visual_style(name: str) -> str | None:
            if name not in update:
                return None
            value = update.get(name)
            if value is None:
                return None
            text = str(value).strip().lower()
            if not text:
                return None
            if text not in ("direction", "signal"):
                raise ValueError("traceroute_visual_style must be 'direction' or 'signal'")
            return text

        def pick_bool(name: str) -> bool | None:
            if name not in update:
                return None
            value = update.get(name)
            if value is None:
                return None
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return bool(int(value))
            text = str(value).strip().lower()
            if text in ("1", "true", "yes", "y", "on"):
                return True
            if text in ("0", "false", "no", "n", "off"):
                return False
            raise ValueError(f"invalid {name}")

        try:
            traceroute_behavior = pick_behavior("traceroute_behavior")
            traceroute_visual_style = pick_visual_style("traceroute_visual_style")
            interval = pick_float("interval")
            heard_window = pick_int("heard_window")
            fresh_window = pick_int("fresh_window")
            mid_window = pick_int("mid_window")
            hop_limit = pick_int("hop_limit")
            traceroute_retention_hours = pick_int("traceroute_retention_hours")
            chat_notification_desktop = pick_bool("chat_notification_desktop")
            chat_notification_sound = pick_bool("chat_notification_sound")
            chat_notification_notify_focused = pick_bool("chat_notification_notify_focused")
        except ValueError as exc:
            return False, str(exc), None

        if interval is not None and interval <= 0:
            return False, "interval must be > 0", None
        if heard_window is not None and heard_window <= 0:
            return False, "heard_window must be > 0", None
        if fresh_window is not None and fresh_window <= 0:
            return False, "fresh_window must be > 0", None
        if mid_window is not None and mid_window <= 0:
            return False, "mid_window must be > 0", None

        candidate_fresh = (
            fresh_window
            if fresh_window is not None
            else int(current.get("fresh_window") or DEFAULT_RUNTIME_CONFIG["fresh_window"])
        )
        candidate_mid = (
            mid_window
            if mid_window is not None
            else int(current.get("mid_window") or DEFAULT_RUNTIME_CONFIG["mid_window"])
        )
        if candidate_mid < candidate_fresh:
            return False, "mid_window must be >= fresh_window", None
        if hop_limit is not None and hop_limit <= 0:
            return False, "hop_limit must be > 0", None
        if traceroute_retention_hours is not None and traceroute_retention_hours <= 0:
            return False, "traceroute_retention_hours must be > 0", None

        webhook_url = pick_str("webhook_url")
        webhook_api_token = pick_str("webhook_api_token")

        new_config = dict(current)
        if traceroute_behavior is not None:
            new_config["traceroute_behavior"] = traceroute_behavior
        if traceroute_visual_style is not None:
            new_config["traceroute_visual_style"] = traceroute_visual_style
        if interval is not None:
            new_config["interval"] = interval
        if heard_window is not None:
            new_config["heard_window"] = heard_window
        if fresh_window is not None:
            new_config["fresh_window"] = fresh_window
        if mid_window is not None:
            new_config["mid_window"] = mid_window
        if hop_limit is not None:
            new_config["hop_limit"] = hop_limit
        if traceroute_retention_hours is not None:
            new_config["traceroute_retention_hours"] = traceroute_retention_hours
        if chat_notification_desktop is not None:
            new_config["chat_notification_desktop"] = chat_notification_desktop
        if chat_notification_sound is not None:
            new_config["chat_notification_sound"] = chat_notification_sound
        if chat_notification_notify_focused is not None:
            new_config["chat_notification_notify_focused"] = chat_notification_notify_focused
        if webhook_url is not None or "webhook_url" in update:
            new_config["webhook_url"] = webhook_url
        if webhook_api_token is not None or "webhook_api_token" in update:
            new_config["webhook_api_token"] = webhook_api_token

        # Ensure keys exist even if older DB entries were partial.
        for key, value in DEFAULT_RUNTIME_CONFIG.items():
            if key not in new_config:
                new_config[key] = value
        for deprecated_key in ("max_map_traces", "max_stored_traces"):
            if deprecated_key in new_config:
                del new_config[deprecated_key]

        return True, "updated", new_config

    def get_config(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._config)

    def get_public_config(self) -> dict[str, Any]:
        return self._sanitize_config_for_public(self.get_config())

    def set_config(self, update: dict[str, Any]) -> tuple[bool, str]:
        with self._lock:
            current = dict(self._config)
            interface = self._interface
            map_state = self._map_state
            wake_event = self._worker_wake

        ok, detail, new_config = self._merge_runtime_config(current, update)
        if not ok or new_config is None:
            return False, detail

        try:
            self._store.set_runtime_config(new_config, "global")
        except Exception as exc:
            return False, f"failed to save config to SQLite: {exc}"

        with self._lock:
            self._config = new_config

        if map_state is not None:
            try:
                map_state.set_traceroute_retention_hours(
                    int(
                        new_config["traceroute_retention_hours"]
                        or DEFAULT_RUNTIME_CONFIG["traceroute_retention_hours"]
                    )
                )
            except Exception:
                pass

        if interface is not None:
            try:
                self._apply_interface_timeout(
                    interface,
                    interval_minutes=float(new_config["interval"]),
                    hop_limit=int(new_config["hop_limit"]),
                )
            except Exception:
                pass
        if wake_event is not None:
            wake_event.set()
        self._bump_snapshot_revision()

        webhook_on = "on" if new_config.get("webhook_url") else "off"
        traceroute_behavior = str(
            new_config.get("traceroute_behavior") or DEFAULT_RUNTIME_CONFIG["traceroute_behavior"]
        )
        traceroute_visual_style = str(
            new_config.get("traceroute_visual_style")
            or DEFAULT_RUNTIME_CONFIG["traceroute_visual_style"]
        )
        interval_minutes = float(new_config.get("interval") or DEFAULT_RUNTIME_CONFIG["interval"])
        traceroute_retention_hours = int(
            new_config.get("traceroute_retention_hours")
            or DEFAULT_RUNTIME_CONFIG["traceroute_retention_hours"]
        )
        self._emit(
            f"[{utc_now()}] Config updated: traceroute_behavior={traceroute_behavior} "
            f"traceroute_visual_style={traceroute_visual_style} "
            f"interval={interval_minutes:g}m "
            f"heard_window={new_config['heard_window']}m hop_limit={new_config['hop_limit']} "
            f"fresh_window={new_config['fresh_window']}m mid_window={new_config['mid_window']}m "
            f"traceroute_retention_hours={traceroute_retention_hours} "
            f"webhook={webhook_on}"
        )
        return True, "updated"

    def _apply_interface_timeout(self, interface: Any, *, interval_minutes: float, hop_limit: int) -> None:
        try:
            interval_seconds = max(1, int(float(interval_minutes) * 60.0))
        except (TypeError, ValueError):
            interval_seconds = max(1, int(float(DEFAULT_RUNTIME_CONFIG["interval"]) * 60.0))
        hop_limit_int = max(1, int(hop_limit))
        effective_timeout = max(1, (interval_seconds - 1) // hop_limit_int)
        if hasattr(interface, "_timeout") and hasattr(interface._timeout, "expireTimeout"):
            interface._timeout.expireTimeout = effective_timeout
            est_wait = effective_timeout * hop_limit_int
            self._emit(
                f"[{utc_now()}] Traceroute timeout base set to {effective_timeout}s "
                f"(~{est_wait}s max wait at hop-limit {hop_limit_int})."
            )
        else:
            self._emit_error(
                f"[{utc_now()}] Warning: unable to set Meshtastic internal timeout "
                "(private API changed?)."
            )

    def set_discovery_enabled(self, enabled: bool) -> None:
        self._discovery.set_enabled(enabled)
        self._bump_snapshot_revision()

    def rescan_discovery(self) -> tuple[bool, str]:
        self._discovery.trigger_scan()
        self._bump_snapshot_revision()
        return True, "scan_triggered"

    def _active_mesh_host(self) -> str | None:
        with self._lock:
            map_state = self._map_state
            if map_state is None:
                return None
            host = str(map_state.mesh_host or "").strip()
            return host or None
