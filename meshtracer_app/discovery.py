from __future__ import annotations

import concurrent.futures
import ipaddress
import socket
import threading
import time
from collections.abc import Callable
from typing import Any

from .common import utc_now


DEFAULT_MESHTASTIC_TCP_PORT = 4403


def _primary_ipv4_address() -> str | None:
    # UDP connect trick: doesn't send packets, but asks the OS which interface
    # would be used. Works even without internet access.
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except OSError:
        return None
    try:
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
        if isinstance(ip, str) and ip:
            return ip
    except OSError:
        return None
    finally:
        try:
            sock.close()
        except Exception:
            pass
    return None


def _guess_private_ipv4_networks(max_networks: int = 3) -> list[ipaddress.IPv4Network]:
    ips = _guess_private_ipv4_addresses()
    networks: set[ipaddress.IPv4Network] = set()
    for ip_str in sorted(ips):
        try:
            addr = ipaddress.IPv4Address(ip_str)
        except ipaddress.AddressValueError:
            continue
        # Best-effort: assume /24 (typical home/office LAN).
        try:
            networks.add(ipaddress.ip_network(f"{addr}/24", strict=False))
        except ValueError:
            continue

    ordered = sorted(networks, key=lambda net: str(net))
    return ordered[: max(0, int(max_networks))]


def _guess_private_ipv4_addresses() -> set[str]:
    ips: set[str] = set()

    primary = _primary_ipv4_address()
    if primary:
        ips.add(primary)

    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if isinstance(ip, str) and ip:
                ips.add(ip)
    except OSError:
        pass

    cleaned: set[str] = set()
    for ip_str in ips:
        try:
            addr = ipaddress.IPv4Address(ip_str)
        except ipaddress.AddressValueError:
            continue
        if addr.is_loopback or addr.is_link_local:
            continue
        if not addr.is_private:
            continue
        cleaned.add(str(addr))
    return cleaned


def _check_tcp(host: str, port: int, timeout_seconds: float) -> tuple[bool, float]:
    start = time.perf_counter()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(timeout_seconds)
        result = sock.connect_ex((host, int(port)))
        ok = result == 0
        return ok, (time.perf_counter() - start)
    except OSError:
        return False, (time.perf_counter() - start)
    finally:
        try:
            sock.close()
        except Exception:
            pass


class LanDiscoverer:
    def __init__(
        self,
        *,
        port: int = DEFAULT_MESHTASTIC_TCP_PORT,
        scan_interval_seconds: float = 45.0,
        connect_timeout_seconds: float = 0.22,
        max_results: int = 30,
        on_change: Callable[[], None] | None = None,
        progress_notify_every: int = 8,
        progress_notify_min_interval_seconds: float = 0.2,
    ) -> None:
        self._port = int(port)
        self._scan_interval_seconds = float(scan_interval_seconds)
        self._connect_timeout_seconds = float(connect_timeout_seconds)
        self._max_results = int(max_results)
        self._on_change = on_change if callable(on_change) else None
        self._progress_notify_every = max(1, int(progress_notify_every))
        self._progress_notify_min_interval_seconds = max(
            0.01,
            float(progress_notify_min_interval_seconds),
        )

        self._lock = threading.Lock()
        self._enabled = True
        self._scanning = False
        self._progress_total = 0
        self._progress_done = 0
        self._last_scan_utc: str | None = None
        self._networks: list[str] = []
        self._found: dict[str, dict[str, Any]] = {}

        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name="meshtracer-discovery")
        self._thread.start()

    def _notify_change(self) -> None:
        callback = self._on_change
        if not callable(callback):
            return
        try:
            callback()
        except Exception:
            pass

    def set_enabled(self, enabled: bool) -> None:
        with self._lock:
            self._enabled = bool(enabled)
        if enabled:
            self.trigger_scan()

    def trigger_scan(self) -> None:
        self._wake_event.set()

    def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            candidates = list(self._found.values())
            candidates.sort(key=lambda item: float(item.get("last_seen_epoch", 0)), reverse=True)
            if self._max_results > 0:
                candidates = candidates[: self._max_results]
            # Strip internal fields for API.
            cleaned = [
                {
                    "host": str(item.get("host") or ""),
                    "port": int(item.get("port") or self._port),
                    "latency_ms": item.get("latency_ms"),
                    "last_seen_utc": item.get("last_seen_utc"),
                }
                for item in candidates
                if item.get("host")
            ]
            return {
                "enabled": self._enabled,
                "scanning": self._scanning,
                "progress_done": self._progress_done,
                "progress_total": self._progress_total,
                "port": self._port,
                "networks": list(self._networks),
                "last_scan_utc": self._last_scan_utc,
                "candidates": cleaned,
            }

    def _run(self) -> None:
        next_deadline = 0.0
        while not self._stop_event.is_set():
            now = time.time()
            timeout = max(0.5, next_deadline - now) if next_deadline else 0.5
            self._wake_event.wait(timeout=timeout)
            triggered = self._wake_event.is_set()
            self._wake_event.clear()

            if self._stop_event.is_set():
                break

            with self._lock:
                enabled = self._enabled

            if not enabled:
                # Poll occasionally so enabling discovery doesn't wait for a long sleep.
                next_deadline = time.time() + min(5.0, self._scan_interval_seconds)
                continue

            # Throttle scans.
            now = time.time()
            if next_deadline and now < next_deadline and not triggered:
                continue

            self._perform_scan()
            next_deadline = time.time() + self._scan_interval_seconds

    def _perform_scan(self) -> None:
        networks = _guess_private_ipv4_networks()
        network_strs = [str(net) for net in networks]
        local_ips = _guess_private_ipv4_addresses()

        hosts: list[str] = []
        for net in networks:
            # /24 scan only: 254 hosts.
            for ip in net.hosts():
                ip_str = str(ip)
                if ip_str in local_ips:
                    continue
                hosts.append(ip_str)

        with self._lock:
            self._scanning = True
            self._progress_total = len(hosts)
            self._progress_done = 0
            self._networks = network_strs
            self._last_scan_utc = utc_now()
        self._notify_change()

        if not hosts:
            with self._lock:
                self._scanning = False
            self._notify_change()
            return

        max_workers = min(96, max(24, len(hosts) // 4))
        found: dict[str, dict[str, Any]] = {}
        last_progress_notify = time.monotonic()

        def task(ip: str) -> tuple[str, bool, float]:
            ok, elapsed = _check_tcp(ip, self._port, self._connect_timeout_seconds)
            return ip, ok, elapsed

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(task, ip) for ip in hosts]
                for fut in concurrent.futures.as_completed(futures):
                    if self._stop_event.is_set():
                        break
                    ip, ok, elapsed = fut.result()
                    with self._lock:
                        self._progress_done += 1
                        progress_done = self._progress_done
                        progress_total = self._progress_total
                    now_monotonic = time.monotonic()
                    if (
                        progress_done >= progress_total
                        or (progress_done % self._progress_notify_every) == 0
                        or (now_monotonic - last_progress_notify)
                        >= self._progress_notify_min_interval_seconds
                    ):
                        last_progress_notify = now_monotonic
                        self._notify_change()
                    if not ok:
                        continue
                    found[ip] = {
                        "host": ip,
                        "port": self._port,
                        "latency_ms": round(float(elapsed) * 1000.0, 1),
                        "last_seen_utc": utc_now(),
                        "last_seen_epoch": time.time(),
                    }
        except Exception:
            # Best effort: discovery should never crash the app.
            found = {}

        with self._lock:
            # Merge to preserve "last seen" history across scans.
            for ip, item in found.items():
                self._found[ip] = item

            # Drop stale results after 10 minutes.
            cutoff = time.time() - 600.0
            self._found = {
                ip: item
                for ip, item in self._found.items()
                if float(item.get("last_seen_epoch") or 0) >= cutoff
            }
            self._scanning = False
        self._notify_change()
