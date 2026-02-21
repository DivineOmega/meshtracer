from __future__ import annotations

import threading
import time
from typing import Any

from .common import age_str, utc_now
from .controller_defaults import DEFAULT_RUNTIME_CONFIG
from .meshtastic_helpers import node_display, node_record_from_node, pick_recent_node_from_nodes
from .state import MapState
from .webhook import post_webhook


class ControllerWorkerMixin:
    @staticmethod
    def _target_from_num(interface: Any, node_num: int) -> dict[str, Any]:
        nodes_by_num = getattr(interface, "nodesByNum", {})
        if isinstance(nodes_by_num, dict):
            node = nodes_by_num.get(node_num)
            if isinstance(node, dict):
                target = dict(node)
                target["num"] = node_num
                return target
        return {"num": node_num}

    @staticmethod
    def _node_last_heard_age_seconds(node: dict[str, Any]) -> float | None:
        try:
            last_heard = node.get("lastHeard")
        except Exception:
            return None
        if last_heard is None:
            return None
        try:
            age = time.time() - float(last_heard)
        except (TypeError, ValueError):
            return None
        return max(0.0, age)

    def _traceroute_worker(
        self,
        interface: Any,
        map_state: MapState,
        traceroute_capture: dict[str, Any],
        stop_event: threading.Event,
        wake_event: threading.Event,
        connected_host: str,
    ) -> None:
        while not stop_event.is_set():
            config = self.get_config()
            traceroute_behavior = str(
                config.get("traceroute_behavior") or DEFAULT_RUNTIME_CONFIG["traceroute_behavior"]
            ).strip().lower()
            if traceroute_behavior not in ("automatic", "manual"):
                traceroute_behavior = str(DEFAULT_RUNTIME_CONFIG["traceroute_behavior"])
            manual_only_mode = traceroute_behavior == "manual"
            try:
                interval_minutes = float(config.get("interval") or DEFAULT_RUNTIME_CONFIG["interval"])
            except (TypeError, ValueError):
                interval_minutes = float(DEFAULT_RUNTIME_CONFIG["interval"])
            interval_seconds = max(1, int(interval_minutes * 60.0))
            heard_window_seconds = (
                int(config.get("heard_window") or DEFAULT_RUNTIME_CONFIG["heard_window"]) * 60
            )
            hop_limit = int(config.get("hop_limit") or DEFAULT_RUNTIME_CONFIG["hop_limit"])
            webhook_url = config.get("webhook_url")
            webhook_api_token = config.get("webhook_api_token")

            cycle_start = time.time()

            try:
                map_state.update_nodes_from_interface(interface)
                self._bump_snapshot_revision()
            except Exception as exc:
                self._emit_error(f"[{utc_now()}] Warning: failed to refresh nodes: {exc}")

            queue_mesh_host = str(map_state.mesh_host or "").strip()
            manual_entry = (
                self._store.pop_next_queued_traceroute(queue_mesh_host) if queue_mesh_host else None
            )
            manual_node_num = (
                int(manual_entry.get("node_num"))
                if isinstance(manual_entry, dict) and manual_entry.get("node_num") is not None
                else None
            )
            manual_queue_id = (
                int(manual_entry.get("queue_id"))
                if isinstance(manual_entry, dict) and manual_entry.get("queue_id") is not None
                else None
            )
            manual_triggered = manual_node_num is not None
            if manual_triggered:
                self._bump_snapshot_revision()
                target = self._target_from_num(interface, int(manual_node_num))
                last_heard_age = self._node_last_heard_age_seconds(target)
                candidate_count = 1
            elif manual_only_mode:
                if stop_event.is_set():
                    break
                if wake_event.wait():
                    wake_event.clear()
                continue
            else:
                local_node_num = getattr(getattr(interface, "localNode", None), "nodeNum", None)
                db_nodes = self._store.list_nodes_for_traceroute(queue_mesh_host) if queue_mesh_host else []
                target, last_heard_age, candidate_count = pick_recent_node_from_nodes(
                    db_nodes,
                    heard_window_seconds=heard_window_seconds,
                    local_node_num=local_node_num,
                )

            if target is None:
                self._emit_typed(
                    f"[{utc_now()}] No eligible nodes heard in the last "
                    f"{age_str(heard_window_seconds)}.",
                    log_type="traceroute",
                )
            else:
                if manual_triggered:
                    self._emit_typed(
                        f"\n[{utc_now()}] Manually selected {node_display(target)} "
                        f"(requested from UI).",
                        log_type="traceroute",
                    )
                else:
                    self._emit_typed(
                        f"\n[{utc_now()}] Selected {node_display(target)} "
                        f"(last heard {age_str(last_heard_age or 0)} ago, "
                        f"{candidate_count} eligible nodes).",
                        log_type="traceroute",
                    )
                self._emit_typed(f"[{utc_now()}] Starting traceroute...", log_type="traceroute")

                target_num: int | None = None
                try:
                    target_num = int(target.get("num"))
                    with self._lock:
                        self._current_traceroute_node_num = target_num
                        self._bump_snapshot_revision_locked()
                    traceroute_capture["result"] = None
                    interface.sendTraceRoute(
                        dest=target_num,
                        hopLimit=hop_limit,
                    )
                    self._emit_typed(
                        f"[{utc_now()}] Traceroute complete for "
                        f"{self._node_log_descriptor(interface, target_num)}.",
                        log_type="traceroute",
                    )

                    if webhook_url:
                        if traceroute_capture["result"] is None:
                            self._emit_error_typed(
                                f"[{utc_now()}] Webhook skipped: no parsed "
                                "traceroute response payload available.",
                                log_type="traceroute",
                            )
                        else:
                            webhook_payload = {
                                "event": "meshtastic_traceroute_complete",
                                "sent_at_utc": utc_now(),
                                "mesh_host": connected_host,
                                "interval_minutes": interval_minutes,
                                "interval_seconds": interval_seconds,
                                "hop_limit": hop_limit,
                                "selected_target": node_record_from_node(target),
                                "selected_target_last_heard_age_seconds": round(
                                    float(last_heard_age or 0), 3
                                ),
                                "eligible_candidate_count": candidate_count,
                                "trigger": "manual" if manual_triggered else "scheduled",
                                "traceroute": traceroute_capture["result"],
                            }
                            delivered, detail = post_webhook(
                                url=str(webhook_url),
                                api_token=str(webhook_api_token) if webhook_api_token else None,
                                payload=webhook_payload,
                            )
                            if delivered:
                                self._emit_typed(
                                    f"[{utc_now()}] Webhook delivered: {detail}",
                                    log_type="traceroute",
                                )
                            else:
                                self._emit_error_typed(
                                    f"[{utc_now()}] Webhook delivery failed: {detail}",
                                    log_type="traceroute",
                                )
                except Exception as exc:  # keep loop alive for unexpected runtime errors
                    if stop_event.is_set():
                        break
                    self._emit_error_typed(
                        f"[{utc_now()}] Traceroute failed: {exc}",
                        log_type="traceroute",
                    )
                finally:
                    if manual_triggered and manual_queue_id is not None and queue_mesh_host:
                        self._store.remove_traceroute_queue_entry(queue_mesh_host, manual_queue_id)
                    with self._lock:
                        if target_num is not None and self._current_traceroute_node_num == target_num:
                            self._current_traceroute_node_num = None
                            self._bump_snapshot_revision_locked()
                    if manual_triggered and manual_queue_id is not None:
                        self._bump_snapshot_revision()

            if manual_only_mode:
                continue

            elapsed = time.time() - cycle_start
            sleep_seconds = max(0.0, interval_seconds - elapsed)
            self._emit_typed(
                f"[{utc_now()}] Waiting {sleep_seconds:.1f}s for next run...",
                log_type="traceroute",
            )
            if stop_event.is_set():
                break
            if wake_event.wait(timeout=sleep_seconds):
                wake_event.clear()
