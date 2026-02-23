from __future__ import annotations

import heapq
import math
from collections import deque
from dataclasses import dataclass
from typing import Any, Iterable


@dataclass
class _EdgeMeta:
    count: int = 0
    snr_sum: float = 0.0
    snr_count: int = 0


@dataclass
class _Point:
    x: float
    y: float


@dataclass
class _Constraint:
    anchor_num: int
    x: float
    y: float
    hop: int
    cost: float
    r: float
    w: float


def _is_real_number(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if not isinstance(value, (int, float)):
        return False
    return math.isfinite(float(value))


def _to_float(value: Any) -> float | None:
    if _is_real_number(value):
        return float(value)
    return None


def _to_node_num(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            number = float(text)
        elif isinstance(value, (int, float)):
            number = float(value)
        else:
            return None
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return int(number)


def _short_name_from_node_num(node_num: int) -> str:
    uint32 = int(node_num) & 0xFFFFFFFF
    return f"{uint32:08x}"[-4:]


def _has_coord(node: dict[str, Any]) -> bool:
    lat = _to_float(node.get("lat"))
    lon = _to_float(node.get("lon"))
    return lat is not None and lon is not None


def _meters_per_lon_degree(lat: float) -> float:
    return 111111.0 * max(0.2, math.cos((float(lat) * math.pi) / 180.0))


def _median(values: Iterable[float]) -> float:
    items = [float(v) for v in values if math.isfinite(float(v))]
    if not items:
        return float("nan")
    items.sort()
    mid = len(items) // 2
    if len(items) % 2:
        return items[mid]
    return (items[mid - 1] + items[mid]) / 2.0


def _haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371000.0
    rad = math.pi / 180.0
    phi1 = float(lat1) * rad
    phi2 = float(lat2) * rad
    d_phi = (float(lat2) - float(lat1)) * rad
    d_lam = (float(lon2) - float(lon1)) * rad
    s1 = math.sin(d_phi / 2.0)
    s2 = math.sin(d_lam / 2.0)
    a = s1 * s1 + math.cos(phi1) * math.cos(phi2) * s2 * s2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))
    return radius * c


def _edge_snr_from_route(route_nums: list[Any], snr_list: Any, index: int) -> float:
    if not isinstance(snr_list, list):
        return float("nan")
    if len(snr_list) == len(route_nums):
        raw = snr_list[index + 1] if index + 1 < len(snr_list) else snr_list[index]
    elif len(snr_list) == len(route_nums) - 1:
        raw = snr_list[index]
    else:
        return float("nan")
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return float("nan")
    return value if math.isfinite(value) else float("nan")


def _sorted_nodes(node_map: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    return [node_map[num] for num in sorted(node_map.keys())]


def estimate_node_positions(nodes: list[dict[str, Any]], traces: list[dict[str, Any]]) -> list[dict[str, Any]]:
    node_map: dict[int, dict[str, Any]] = {}

    for raw in nodes if isinstance(nodes, list) else []:
        if not isinstance(raw, dict):
            continue
        num = _to_node_num(raw.get("num"))
        if num is None:
            continue
        node = dict(raw)
        node["num"] = int(num)
        node["estimated"] = False
        if _has_coord(node):
            node["lat"] = float(node.get("lat"))
            node["lon"] = float(node.get("lon"))
        else:
            node["lat"] = None
            node["lon"] = None
        node_map[int(num)] = node

    def ensure_trace_node(raw_num: Any) -> None:
        num = _to_node_num(raw_num)
        if num is None or num in node_map:
            return
        short_name = _short_name_from_node_num(num)
        node_map[num] = {
            "num": num,
            "id": None,
            "long_name": f"Unknown {short_name}" if short_name else "Unknown",
            "short_name": short_name or None,
            "lat": None,
            "lon": None,
            "last_heard": None,
            "estimated": False,
            "trace_only": True,
        }

    adj: dict[int, dict[int, _EdgeMeta]] = {}

    def ensure_adj(num: int) -> None:
        if num not in adj:
            adj[num] = {}

    def add_edge(a_raw: Any, b_raw: Any, snr_db: float) -> None:
        a = _to_node_num(a_raw)
        b = _to_node_num(b_raw)
        if a is None or b is None or a == b:
            return
        if a not in node_map or b not in node_map:
            return
        ensure_adj(a)
        ensure_adj(b)

        a_meta = adj[a].get(b) or _EdgeMeta()
        a_meta.count += 1
        if math.isfinite(snr_db):
            a_meta.snr_sum += float(snr_db)
            a_meta.snr_count += 1
        adj[a][b] = a_meta

        b_meta = adj[b].get(a) or _EdgeMeta()
        b_meta.count += 1
        if math.isfinite(snr_db):
            b_meta.snr_sum += float(snr_db)
            b_meta.snr_count += 1
        adj[b][a] = b_meta

    for trace in traces if isinstance(traces, list) else []:
        if not isinstance(trace, dict):
            continue
        for key in ("towards_nums", "back_nums"):
            route = trace.get(key)
            route_values = route if isinstance(route, list) else []
            snr_key = "towards_snr_db" if key == "towards_nums" else "back_snr_db"
            snr_list = trace.get(snr_key) if isinstance(trace.get(snr_key), list) else None

            nums: list[int | None] = []
            for raw_num in route_values:
                ensure_trace_node(raw_num)
                nums.append(_to_node_num(raw_num))

            if len(nums) < 2:
                continue
            for idx in range(len(nums) - 1):
                snr_db = _edge_snr_from_route(nums, snr_list, idx)
                add_edge(nums[idx], nums[idx + 1], snr_db)

    anchors = [node for node in node_map.values() if _has_coord(node)]
    if not anchors:
        return _sorted_nodes(node_map)

    lat0 = 0.0
    lon0 = 0.0
    for anchor in anchors:
        lat0 += float(anchor.get("lat"))
        lon0 += float(anchor.get("lon"))
    lat0 /= len(anchors)
    lon0 /= len(anchors)
    meters_per_lat_degree = 111111.0
    meters_per_lon = _meters_per_lon_degree(lat0)

    def ll_to_xy(lat: float, lon: float) -> _Point:
        return _Point(
            x=(float(lon) - lon0) * meters_per_lon,
            y=(float(lat) - lat0) * meters_per_lat_degree,
        )

    def xy_to_ll(x: float, y: float) -> tuple[float, float]:
        return (
            lat0 + float(y) / meters_per_lat_degree,
            lon0 + float(x) / meters_per_lon,
        )

    anchor_pos: dict[int, _Point] = {}
    for anchor in anchors:
        anchor_num = int(anchor["num"])
        anchor_pos[anchor_num] = ll_to_xy(float(anchor["lat"]), float(anchor["lon"]))

    def edge_mean_snr_db(meta: _EdgeMeta | None) -> float:
        if meta is None or meta.snr_count <= 0:
            return float("nan")
        return float(meta.snr_sum) / float(meta.snr_count)

    def snr_quality_from_db(snr_db: float) -> float | None:
        value = float(snr_db)
        if not math.isfinite(value):
            return None
        clamped = max(-20.0, min(12.0, value))
        return (clamped + 20.0) / 32.0

    def edge_cost_units(meta: _EdgeMeta) -> float:
        quality = snr_quality_from_db(edge_mean_snr_db(meta))
        if quality is None:
            return 1.0
        mult = 0.85 + (1.0 - quality) * 1.25
        return max(0.7, min(2.4, mult))

    def edge_spring_weight(meta: _EdgeMeta) -> float:
        count = float(meta.count if isinstance(meta.count, int) else 1.0)
        quality = snr_quality_from_db(edge_mean_snr_db(meta))
        snr_factor = 0.85 if quality is None else 0.55 + 0.75 * quality
        return min(3.2, math.sqrt(max(1.0, count)) * snr_factor)

    def dijkstra(start_num: int, max_cost: float) -> dict[int, float]:
        start = int(start_num)
        dist: dict[int, float] = {}
        if start not in node_map:
            return dist
        dist[start] = 0.0

        heap: list[tuple[float, int]] = [(0.0, start)]
        while heap:
            cost, cur = heapq.heappop(heap)
            best = dist.get(cur)
            if best is None or cost > best + 1e-9:
                continue
            if cost > max_cost:
                continue
            neighbors = adj.get(cur)
            if not neighbors:
                continue
            for nxt, meta in neighbors.items():
                step = edge_cost_units(meta)
                next_cost = cost + step
                if not math.isfinite(next_cost) or next_cost > max_cost:
                    continue
                prev = dist.get(nxt)
                if prev is None or next_cost < prev - 1e-9:
                    dist[nxt] = next_cost
                    heapq.heappush(heap, (next_cost, nxt))
        return dist

    def bfs(start_num: int, max_depth: int) -> dict[int, int]:
        start = int(start_num)
        dist: dict[int, int] = {}
        if start not in node_map:
            return dist
        dist[start] = 0
        queue: deque[int] = deque([start])
        while queue:
            cur = queue.popleft()
            cur_dist = dist.get(cur, 0)
            if cur_dist >= max_depth:
                continue
            neighbors = adj.get(cur)
            if not neighbors:
                continue
            for nxt in neighbors.keys():
                if nxt in dist:
                    continue
                dist[nxt] = cur_dist + 1
                queue.append(nxt)
        return dist

    max_bfs_hops = 25
    max_dijkstra_cost = max_bfs_hops * 2.6
    hop_by_anchor: dict[int, dict[int, int]] = {}
    cost_by_anchor: dict[int, dict[int, float]] = {}
    for anchor in anchors:
        anchor_num = int(anchor["num"])
        hop_by_anchor[anchor_num] = bfs(anchor_num, max_bfs_hops)
        cost_by_anchor[anchor_num] = dijkstra(anchor_num, max_dijkstra_cost)

    global_ratios: list[float] = []
    per_anchor_ratios: dict[int, list[float]] = {}
    max_calib_hops = 12
    min_unit_meters = 10.0
    max_unit_meters = 20000.0

    for idx, anchor_a in enumerate(anchors):
        a_num = int(anchor_a["num"])
        hops_a = hop_by_anchor.get(a_num)
        costs_a = cost_by_anchor.get(a_num)
        if hops_a is None or costs_a is None:
            continue
        for jdx in range(idx + 1, len(anchors)):
            anchor_b = anchors[jdx]
            b_num = int(anchor_b["num"])
            hop = hops_a.get(b_num)
            if hop is None or hop <= 0 or hop > max_calib_hops:
                continue
            cost_units = costs_a.get(b_num)
            if cost_units is None or not math.isfinite(cost_units) or cost_units <= 0:
                continue
            meters = _haversine_meters(
                float(anchor_a["lat"]),
                float(anchor_a["lon"]),
                float(anchor_b["lat"]),
                float(anchor_b["lon"]),
            )
            if not math.isfinite(meters) or meters <= 0:
                continue
            ratio = meters / float(cost_units)
            if not math.isfinite(ratio) or ratio < min_unit_meters or ratio > max_unit_meters:
                continue
            global_ratios.append(ratio)
            per_anchor_ratios.setdefault(a_num, []).append(ratio)
            per_anchor_ratios.setdefault(b_num, []).append(ratio)

    global_meters_per_unit = _median(global_ratios)
    if not math.isfinite(global_meters_per_unit) or global_meters_per_unit <= 0:
        global_meters_per_unit = 400.0

    per_anchor_meters_per_unit: dict[int, float] = {}
    for anchor in anchors:
        anchor_num = int(anchor["num"])
        ratios = per_anchor_ratios.get(anchor_num, [])
        med = _median(ratios)
        if len(ratios) >= 3 and math.isfinite(med) and med > 0:
            per_anchor_meters_per_unit[anchor_num] = 0.7 * med + 0.3 * global_meters_per_unit

    def meters_per_unit_for_anchor(anchor_num: int) -> float:
        local = per_anchor_meters_per_unit.get(int(anchor_num))
        if local is not None and math.isfinite(local) and local > 0:
            return local
        return global_meters_per_unit

    constraints_by_node: dict[int, list[_Constraint]] = {}
    max_constraint_hops = 12
    max_anchors_per_node = 8

    for num, node in node_map.items():
        if _has_coord(node):
            continue
        constraints: list[_Constraint] = []
        for anchor in anchors:
            anchor_num = int(anchor["num"])
            hops_a = hop_by_anchor.get(anchor_num)
            costs_a = cost_by_anchor.get(anchor_num)
            if hops_a is None or costs_a is None:
                continue
            hop = hops_a.get(num)
            if hop is None or hop <= 0 or hop > max_constraint_hops:
                continue
            cost_units = costs_a.get(num)
            if cost_units is None or not math.isfinite(cost_units) or cost_units <= 0:
                continue
            pos = anchor_pos.get(anchor_num)
            if pos is None:
                continue
            unit_meters = meters_per_unit_for_anchor(anchor_num)
            radius = float(cost_units) * unit_meters
            constraints.append(
                _Constraint(
                    anchor_num=anchor_num,
                    x=pos.x,
                    y=pos.y,
                    hop=int(hop),
                    cost=float(cost_units),
                    r=radius,
                    w=1.0 / (float(hop) * float(hop)),
                )
            )
        constraints.sort(key=lambda item: (item.hop, item.anchor_num))
        if len(constraints) > max_anchors_per_node:
            constraints = constraints[:max_anchors_per_node]
        constraints_by_node[num] = constraints

    pos_by_num: dict[int, _Point] = {}
    fixed_nums: set[int] = set()
    for anchor in anchors:
        anchor_num = int(anchor["num"])
        pos = anchor_pos.get(anchor_num)
        if pos is None:
            continue
        pos_by_num[anchor_num] = _Point(pos.x, pos.y)
        fixed_nums.add(anchor_num)

    strong_nums: set[int] = set()

    def solve_multilateration(constraints: list[_Constraint]) -> _Point:
        x = 0.0
        y = 0.0
        w_sum = 0.0
        for constraint in constraints:
            x += constraint.x * constraint.w
            y += constraint.y * constraint.w
            w_sum += constraint.w
        if w_sum > 0:
            x /= w_sum
            y /= w_sum

        damping = 1.0
        for _ in range(22):
            a11 = 0.0
            a12 = 0.0
            a22 = 0.0
            b1 = 0.0
            b2 = 0.0
            for constraint in constraints:
                dx = x - constraint.x
                dy = y - constraint.y
                distance = math.hypot(dx, dy) or 1e-6
                jx = dx / distance
                jy = dy / distance
                resid = distance - constraint.r
                weight = constraint.w
                a11 += weight * jx * jx
                a12 += weight * jx * jy
                a22 += weight * jy * jy
                b1 += weight * jx * resid
                b2 += weight * jy * resid

            a11 += damping
            a22 += damping
            det = a11 * a22 - a12 * a12
            if not math.isfinite(det) or abs(det) < 1e-9:
                break
            dx_step = (-a22 * b1 + a12 * b2) / det
            dy_step = (a12 * b1 - a11 * b2) / det
            if not math.isfinite(dx_step) or not math.isfinite(dy_step):
                break

            step_mag = math.hypot(dx_step, dy_step)
            x += dx_step
            y += dy_step
            if step_mag < 0.2:
                break
        return _Point(x, y)

    for num, node in node_map.items():
        if _has_coord(node):
            continue
        constraints = constraints_by_node.get(num, [])
        if len(constraints) < 3:
            continue
        solved = solve_multilateration(constraints)
        pos_by_num[num] = solved
        strong_nums.add(num)

    def neighbor_hint(num: int) -> _Point | None:
        neighbors = adj.get(int(num))
        if not neighbors:
            return None
        x_sum = 0.0
        y_sum = 0.0
        count = 0
        for nxt in neighbors.keys():
            pos = pos_by_num.get(int(nxt))
            if pos is None:
                continue
            x_sum += pos.x
            y_sum += pos.y
            count += 1
        if count <= 0:
            return None
        return _Point(x_sum / count, y_sum / count)

    def circle_intersections(a: _Point, r1: float, b: _Point, r2: float) -> list[_Point]:
        dx = b.x - a.x
        dy = b.y - a.y
        distance = math.hypot(dx, dy)
        if not math.isfinite(distance) or distance < 1e-6:
            return []
        if distance > r1 + r2:
            return []
        if distance < abs(r1 - r2):
            return []
        t = (r1 * r1 - r2 * r2 + distance * distance) / (2.0 * distance)
        h2 = max(0.0, r1 * r1 - t * t)
        h = math.sqrt(h2)
        ux = dx / distance
        uy = dy / distance
        px = a.x + ux * t
        py = a.y + uy * t
        rx = -uy * h
        ry = ux * h
        return [_Point(px + rx, py + ry), _Point(px - rx, py - ry)]

    def place_with_two_anchors(
        num: int,
        c1: _Constraint,
        c2: _Constraint,
        hint: _Point | None,
    ) -> _Point:
        a = _Point(c1.x, c1.y)
        b = _Point(c2.x, c2.y)
        intersections = circle_intersections(a, c1.r, b, c2.r)
        if len(intersections) == 2:
            if hint is not None:
                d0 = math.hypot(intersections[0].x - hint.x, intersections[0].y - hint.y)
                d1 = math.hypot(intersections[1].x - hint.x, intersections[1].y - hint.y)
                return intersections[0] if d0 <= d1 else intersections[1]
            return intersections[0] if (abs(int(num)) % 2) == 0 else intersections[1]

        dx = b.x - a.x
        dy = b.y - a.y
        distance = math.hypot(dx, dy) or 1e-6
        t = max(0.0, min(1.0, c1.r / (c1.r + c2.r))) if (c1.r + c2.r) > 1e-6 else 0.5
        x = a.x + (dx / distance) * distance * t
        y = a.y + (dy / distance) * distance * t

        angle = ((abs(int(num)) % 360) * math.pi) / 180.0
        nudge = min(0.22 * global_meters_per_unit, 120.0)
        x += math.cos(angle) * nudge
        y += math.sin(angle) * nudge
        return _Point(x, y)

    def place_with_one_anchor(num: int, c1: _Constraint, hint: _Point | None) -> _Point:
        angle = ((abs(int(num)) % 360) * math.pi) / 180.0
        if hint is not None:
            angle = math.atan2(hint.y - c1.y, hint.x - c1.x)
        return _Point(
            x=c1.x + math.cos(angle) * c1.r,
            y=c1.y + math.sin(angle) * c1.r,
        )

    for _ in range(5):
        progressed = False
        for num, node in node_map.items():
            if _has_coord(node) or num in pos_by_num:
                continue
            constraints = constraints_by_node.get(num, [])
            if not constraints:
                continue
            hint = neighbor_hint(num)

            if len(constraints) >= 3:
                solved = solve_multilateration(constraints)
                pos_by_num[num] = solved
                strong_nums.add(num)
                progressed = True
                continue
            if len(constraints) == 2:
                pos_by_num[num] = place_with_two_anchors(num, constraints[0], constraints[1], hint)
                progressed = True
                continue
            if len(constraints) == 1:
                pos_by_num[num] = place_with_one_anchor(num, constraints[0], hint)
                progressed = True
                continue
        if not progressed:
            break

    def mobility(num: int) -> float:
        if num in fixed_nums:
            return 0.0
        if num in strong_nums:
            return 0.25
        return 1.0

    spring_iters = 28
    spring_alpha = 0.08
    for _ in range(spring_iters):
        for u, neighbors in adj.items():
            for v, meta in neighbors.items():
                if u >= v:
                    continue
                pos_u = pos_by_num.get(u)
                pos_v = pos_by_num.get(v)
                if pos_u is None or pos_v is None:
                    continue
                dx = pos_v.x - pos_u.x
                dy = pos_v.y - pos_u.y
                dist = math.hypot(dx, dy)
                if not math.isfinite(dist) or dist < 1e-6:
                    continue
                desired = edge_cost_units(meta) * global_meters_per_unit
                if not math.isfinite(desired) or desired <= 0:
                    continue
                weight = edge_spring_weight(meta)
                err = dist - desired
                max_step = 0.45 * desired
                step = spring_alpha * weight * err
                step = max(-max_step, min(max_step, step))

                ux = dx / dist
                uy = dy / dist
                mu = mobility(u)
                mv = mobility(v)
                total = mu + mv
                if total <= 0:
                    continue
                du = (mu / total) * step
                dv = (mv / total) * step
                if mu > 0:
                    pos_u.x += ux * du
                    pos_u.y += uy * du
                if mv > 0:
                    pos_v.x -= ux * dv
                    pos_v.y -= uy * dv

    for num, pos in pos_by_num.items():
        node = node_map.get(num)
        if node is None:
            continue
        if _has_coord(node):
            continue
        lat, lon = xy_to_ll(pos.x, pos.y)
        if not math.isfinite(lat) or not math.isfinite(lon):
            continue
        node["lat"] = float(lat)
        node["lon"] = float(lon)
        node["estimated"] = True

    return _sorted_nodes(node_map)
