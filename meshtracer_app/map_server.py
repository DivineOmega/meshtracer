from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .common import utc_now
from .state import MapState

MAP_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Meshtracer Map</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <style>
    :root {
      --bg: #0b1020;
      --panel: rgba(10, 18, 35, 0.9);
      --panel-border: rgba(148, 163, 184, 0.28);
      --text: #dbe4f6;
      --muted: #8ea0c2;
      --fresh: #6ff0b0;
      --fresh-border: #1d2f4f;
      --mid: #a8b3c8;
      --mid-border: #344560;
      --stale: #6f7f9c;
      --stale-border: #2a3850;
      --tab-active: #1a2847;
      --tab-border: #2b3c60;
      --item-bg: #0f1a33;
      --item-border: #243858;
      --item-hover: #132040;
      --item-active: #1a2b4f;
    }
    html, body {
      margin: 0;
      height: 100%;
      background: var(--bg);
      color: var(--text);
      font-family: "Segoe UI", "Helvetica Neue", Helvetica, Arial, sans-serif;
    }
    body.sidebar-resizing {
      cursor: ew-resize;
      user-select: none;
    }
    #map {
      height: 100%;
      width: 100%;
    }

    #sidebar {
      position: absolute;
      z-index: 1100;
      top: 12px;
      right: 12px;
      bottom: 12px;
      width: min(390px, calc(100vw - 24px));
      min-width: 300px;
      background: var(--panel);
      border: 1px solid var(--panel-border);
      border-radius: 14px;
      box-shadow: 0 12px 36px rgba(0, 0, 0, 0.42);
      backdrop-filter: blur(3px);
      overflow: hidden;
      transition: width 0.15s ease;
    }
    #sidebar.collapsed {
      width: 42px !important;
      min-width: 42px;
    }
    #sidebarResize {
      position: absolute;
      left: -4px;
      top: 0;
      bottom: 0;
      width: 8px;
      cursor: ew-resize;
    }
    #sidebar.collapsed #sidebarResize {
      display: none;
    }
    #sidebarToggle {
      position: absolute;
      top: 8px;
      right: 8px;
      z-index: 2;
      width: 26px;
      height: 26px;
      border: 1px solid #2d4065;
      border-radius: 8px;
      background: #122145;
      color: #d9e4fb;
      font-weight: 700;
      cursor: pointer;
      line-height: 1;
    }
    #sidebarToggle:hover {
      background: #19305f;
    }
    #sidebarShell {
      display: flex;
      flex-direction: column;
      height: 100%;
      min-height: 0;
    }
    #sidebar.collapsed #sidebarShell {
      display: none;
    }

    #sidebarHeader {
      padding: 10px 12px 8px 12px;
      border-bottom: 1px solid #273957;
    }
    .head-row {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 8px;
      padding-right: 28px;
    }
    .head-title {
      font-size: 14px;
      font-weight: 700;
      letter-spacing: 0.03em;
      text-transform: uppercase;
    }
    .head-updated {
      color: var(--muted);
      font-size: 11px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      max-width: 160px;
    }
    .mesh-host {
      color: #9cb0d6;
      font-size: 11px;
      margin-bottom: 8px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .stats {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 6px;
      margin-bottom: 8px;
    }
    .stat {
      border: 1px solid #2a3d61;
      background: #0e1730;
      border-radius: 8px;
      padding: 6px 8px;
    }
    .stat-k {
      display: block;
      color: var(--muted);
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    .stat-v {
      font-size: 14px;
      font-weight: 700;
      color: #e7efff;
    }
    .legend {
      display: inline-flex;
      align-items: center;
      gap: 10px;
      font-size: 11px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .dot {
      width: 9px;
      height: 9px;
      border-radius: 999px;
      display: inline-block;
      margin-right: 4px;
      vertical-align: middle;
      border: 1px solid #20304c;
    }
    .dot-fresh { background: var(--fresh); }
    .dot-mid { background: var(--mid); }
    .dot-stale { background: var(--stale); }

    #tabs {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 6px;
      padding: 8px 12px 8px 12px;
      border-bottom: 1px solid #273957;
    }
    .tab-btn {
      border: 1px solid var(--tab-border);
      background: #101a34;
      color: #c4d2ee;
      border-radius: 8px;
      padding: 7px 6px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      letter-spacing: 0.02em;
    }
    .tab-btn:hover {
      background: #142345;
    }
    .tab-btn.active {
      background: var(--tab-active);
      color: #f0f6ff;
      border-color: #3a5380;
    }

    #panels {
      flex: 1;
      min-height: 0;
      display: flex;
      flex-direction: column;
    }
    .panel {
      display: none;
      height: 100%;
      min-height: 0;
    }
    .panel.active {
      display: block;
    }

    .scroll-list {
      height: 100%;
      min-height: 0;
      overflow: auto;
      padding: 10px 12px 12px 12px;
      box-sizing: border-box;
    }
    .nodes-panel {
      flex-direction: column;
      min-height: 0;
      height: 100%;
    }
    .panel.nodes-panel.active {
      display: flex;
    }
    .nodes-search-wrap {
      border-bottom: 1px solid #253756;
      padding: 9px 12px 8px 12px;
      background: rgba(11, 20, 40, 0.6);
    }
    .nodes-controls {
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
    }
    .nodes-control {
      display: grid;
      grid-template-columns: auto 1fr;
      align-items: center;
      gap: 8px;
    }
    .nodes-control.search-only {
      grid-template-columns: 1fr;
    }
    .nodes-control-label {
      color: #9cb2da;
      font-size: 11px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      white-space: nowrap;
    }
    #nodeSearch {
      width: 100%;
      box-sizing: border-box;
      border: 1px solid #314a74;
      background: #0d1832;
      color: #d9e5fb;
      border-radius: 8px;
      padding: 7px 9px;
      font-size: 12px;
      outline: none;
    }
    #nodeSearch:focus {
      border-color: #4d79ba;
      box-shadow: 0 0 0 2px rgba(82, 137, 221, 0.24);
    }
    #nodeSort {
      width: 100%;
      box-sizing: border-box;
      border: 1px solid #314a74;
      background: #0d1832;
      color: #d9e5fb;
      border-radius: 8px;
      padding: 7px 9px;
      font-size: 12px;
      outline: none;
    }
    #nodeSort:focus {
      border-color: #4d79ba;
      box-shadow: 0 0 0 2px rgba(82, 137, 221, 0.24);
    }
    #nodeList {
      height: auto;
      flex: 1;
      min-height: 0;
      padding-top: 8px;
    }
    .empty {
      border: 1px dashed #2c4064;
      border-radius: 10px;
      padding: 14px 12px;
      color: #9fb0d1;
      font-size: 12px;
      background: rgba(13, 22, 44, 0.55);
      text-align: center;
    }
    .list-item {
      width: 100%;
      border: 1px solid var(--item-border);
      background: var(--item-bg);
      color: var(--text);
      border-radius: 9px;
      text-align: left;
      padding: 8px 10px;
      margin-bottom: 8px;
      cursor: pointer;
      box-sizing: border-box;
    }
    .list-item:hover {
      background: var(--item-hover);
    }
    .list-item.active {
      background: var(--item-active);
      border-color: #4e6a9d;
      box-shadow: inset 0 0 0 1px rgba(127, 177, 255, 0.35);
    }
    .list-item:disabled {
      opacity: 0.55;
      cursor: not-allowed;
    }
    .item-title {
      font-size: 13px;
      font-weight: 700;
      color: #f0f6ff;
      margin-bottom: 4px;
      display: block;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .item-meta {
      color: #98a9ca;
      font-size: 12px;
      display: block;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .log-entry {
      border: 1px solid #263a5e;
      background: #0d1731;
      border-radius: 8px;
      padding: 6px 8px;
      margin-bottom: 6px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 11px;
      line-height: 1.35;
      color: #c9d7f3;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }
    .log-entry.stderr {
      border-color: #6f3a4d;
      background: #2d1620;
      color: #f3c4d1;
    }

    .node-badge {
      display: inline-block;
      padding: 4px 8px;
      border-radius: 9px;
      border: 2px solid;
      text-align: center;
      font-size: 14px;
      font-weight: 700;
      line-height: 1.1;
      letter-spacing: 0.01em;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.32);
      white-space: nowrap;
      user-select: none;
      overflow: hidden;
      text-overflow: ellipsis;
      transition: box-shadow 0.12s ease;
    }
    .node-badge.node-selected {
      box-shadow:
        0 0 0 2px rgba(240, 248, 255, 0.95),
        0 0 0 5px rgba(85, 174, 255, 0.58),
        0 3px 10px rgba(0, 0, 0, 0.45);
    }
    .node-badge.node-fresh {
      background: var(--fresh);
      border-color: var(--fresh-border);
      color: #061a33;
    }
    .node-badge.node-mid {
      background: var(--mid);
      border-color: var(--mid-border);
      color: #16263f;
    }
    .node-badge.node-stale {
      background: var(--stale);
      border-color: var(--stale-border);
      color: #d9e4fb;
    }
    .node-badge.node-unknown {
      background: #8592a8;
      border-color: #35465f;
      color: #dce6fa;
    }

    #traceDetails {
      position: absolute;
      top: 12px;
      left: 12px;
      z-index: 1090;
      width: min(430px, calc(100vw - 84px));
      max-height: 46vh;
      overflow: auto;
      box-sizing: border-box;
      border: 1px solid #35507d;
      border-radius: 12px;
      background: rgba(12, 20, 40, 0.92);
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.4);
      backdrop-filter: blur(2px);
      padding: 10px 12px 12px 12px;
    }
    #traceDetails.hidden {
      display: none;
    }
    .trace-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 8px;
    }
    .trace-title {
      font-size: 14px;
      font-weight: 700;
      color: #e9f1ff;
      letter-spacing: 0.02em;
    }
    #traceDetailsClose {
      width: 24px;
      height: 24px;
      border: 1px solid #456196;
      border-radius: 7px;
      background: #13264b;
      color: #d9e6ff;
      font-size: 16px;
      line-height: 1;
      cursor: pointer;
      flex: 0 0 auto;
    }
    #traceDetailsClose:hover {
      background: #1b3362;
    }
    .trace-meta-row {
      display: block;
      margin-bottom: 4px;
      color: #b5c5e4;
      font-size: 12px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .trace-label {
      color: #8fa7d0;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-size: 10px;
      margin-right: 6px;
    }
    .trace-path {
      margin-top: 8px;
      border: 1px solid #2a3f64;
      border-radius: 9px;
      background: #0d1730;
      padding: 8px;
    }
    .trace-path-title {
      display: block;
      font-size: 11px;
      color: #9ab0d8;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-bottom: 4px;
    }
    .trace-path-value {
      color: #d8e3fb;
      font-size: 12px;
      line-height: 1.35;
      white-space: normal;
      word-break: break-word;
    }
    @media (max-width: 900px) {
      #sidebar {
        width: min(86vw, 390px);
        min-width: 250px;
      }
      .stats {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }
  </style>
</head>
<body>
  <div id="map"></div>
  <section id="traceDetails" class="hidden" aria-live="polite">
    <div class="trace-head">
      <div id="traceDetailsTitle" class="trace-title">Details</div>
      <button id="traceDetailsClose" type="button" aria-label="Clear selection">X</button>
    </div>
    <div id="traceDetailsBody"></div>
  </section>

  <aside id="sidebar">
    <div id="sidebarResize" aria-hidden="true"></div>
    <button id="sidebarToggle" type="button" aria-label="Collapse sidebar">></button>
    <div id="sidebarShell">
      <div id="sidebarHeader">
        <div class="head-row">
          <div class="head-title">Meshtracer</div>
          <div id="updated" class="head-updated">-</div>
        </div>
        <div id="meshHost" class="mesh-host">-</div>
        <div class="stats">
          <div class="stat"><span class="stat-k">Nodes</span><span id="nodeCount" class="stat-v">0</span></div>
          <div class="stat"><span class="stat-k">Traces</span><span id="traceCount" class="stat-v">0</span></div>
          <div class="stat"><span class="stat-k">Edges</span><span id="edgeCount" class="stat-v">0</span></div>
        </div>
        <div class="legend">
          <span><span class="dot dot-fresh"></span>Fresh</span>
          <span><span class="dot dot-mid"></span>Mid</span>
          <span><span class="dot dot-stale"></span>Stale</span>
        </div>
      </div>

      <div id="tabs">
        <button type="button" class="tab-btn active" data-tab="log">Log</button>
        <button type="button" class="tab-btn" data-tab="nodes">Nodes</button>
        <button type="button" class="tab-btn" data-tab="traces">Traces</button>
      </div>

      <div id="panels">
        <section class="panel active" data-panel="log">
          <div id="logList" class="scroll-list"></div>
        </section>
        <section class="panel nodes-panel" data-panel="nodes">
          <div class="nodes-search-wrap">
            <div class="nodes-controls">
              <div class="nodes-control search-only">
                <input id="nodeSearch" type="search" placeholder="Search short/long name..." autocomplete="off" spellcheck="false">
              </div>
              <div class="nodes-control">
                <label class="nodes-control-label" for="nodeSort">Sort</label>
                <select id="nodeSort">
                  <option value="last_heard">Last heard</option>
                  <option value="short_name">Short name</option>
                  <option value="long_name">Long name</option>
                </select>
              </div>
            </div>
          </div>
          <div id="nodeList" class="scroll-list"></div>
        </section>
        <section class="panel" data-panel="traces">
          <div id="traceList" class="scroll-list"></div>
        </section>
      </div>
    </div>
  </aside>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const map = L.map("map", { zoomControl: false }).setView([20, 0], 2);
    L.control.zoom({ position: "bottomleft" }).addTo(map);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution: "&copy; OpenStreetMap contributors",
    }).addTo(map);
    const markerLayer = L.layerGroup().addTo(map);
    const edgeLayer = L.layerGroup().addTo(map);

    const sidebar = document.getElementById("sidebar");
    const sidebarResize = document.getElementById("sidebarResize");
    const sidebarToggle = document.getElementById("sidebarToggle");
    const traceDetails = document.getElementById("traceDetails");
    const traceDetailsTitle = document.getElementById("traceDetailsTitle");
    const traceDetailsBody = document.getElementById("traceDetailsBody");
    const traceDetailsClose = document.getElementById("traceDetailsClose");
    const nodeSearchInput = document.getElementById("nodeSearch");
    const nodeSortSelect = document.getElementById("nodeSort");
    const tabButtons = Array.from(document.querySelectorAll(".tab-btn"));
    const tabPanels = Array.from(document.querySelectorAll(".panel"));

    const state = {
      fitted: false,
      lastServerData: null,
      lastData: null,
      markerByNum: new Map(),
      edgePolylinesByTrace: new Map(),
      nodeByNum: new Map(),
      traceById: new Map(),
      selectedNodeNum: null,
      selectedTraceId: null,
      activeTab: "log",
      nodeSearchQuery: "",
      nodeSortMode: "last_heard",
    };
    const ROUTE_COLORS = {
      towards: "#f59e0b",
      back: "#3b82f6",
    };
    const ROUTE_SELECTED_COLORS = {
      towards: "#ffd27a",
      back: "#7fb5ff",
    };
    const ROUTE_OFFSET_MIN_METERS = 20;
    const ROUTE_OFFSET_MAX_METERS = 320;
    const ROUTE_OFFSET_SCALE = 0.055;
    const ROUTE_OFFSET_JITTER_SCALE = 0.06;
    const ROUTE_OFFSET_TAPER_MIN_METERS = 35;
    const ROUTE_OFFSET_TAPER_MAX_METERS = 240;
    const ROUTE_OFFSET_TAPER_RATIO = 0.22;
    const NODE_FOCUS_MIN_ZOOM = 16;
    const TRACE_FOCUS_MAX_ZOOM = 16;

    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"]/g, (c) => {
        if (c === "&") return "&amp;";
        if (c === "<") return "&lt;";
        if (c === ">") return "&gt;";
        return "&quot;";
      });
    }

    function setActiveTab(tabName) {
      state.activeTab = tabName;
      for (const btn of tabButtons) {
        btn.classList.toggle("active", btn.dataset.tab === tabName);
      }
      for (const panel of tabPanels) {
        panel.classList.toggle("active", panel.dataset.panel === tabName);
      }
    }

    for (const btn of tabButtons) {
      btn.addEventListener("click", () => setActiveTab(btn.dataset.tab || "log"));
    }

    sidebarToggle.addEventListener("click", () => {
      const collapsed = sidebar.classList.toggle("collapsed");
      sidebarToggle.textContent = collapsed ? "<" : ">";
      sidebarToggle.setAttribute("aria-label", collapsed ? "Expand sidebar" : "Collapse sidebar");
      setTimeout(() => map.invalidateSize(), 180);
    });

    let resizing = false;
    let resizeStartX = 0;
    let resizeStartWidth = 0;
    sidebarResize.addEventListener("mousedown", (event) => {
      if (sidebar.classList.contains("collapsed")) return;
      resizing = true;
      resizeStartX = event.clientX;
      resizeStartWidth = sidebar.getBoundingClientRect().width;
      document.body.classList.add("sidebar-resizing");
      event.preventDefault();
    });
    window.addEventListener("mousemove", (event) => {
      if (!resizing) return;
      const delta = resizeStartX - event.clientX;
      const maxWidth = Math.max(320, window.innerWidth - 60);
      const width = Math.max(280, Math.min(maxWidth, resizeStartWidth + delta));
      sidebar.style.width = `${width}px`;
      map.invalidateSize(false);
    });
    window.addEventListener("mouseup", () => {
      if (!resizing) return;
      resizing = false;
      document.body.classList.remove("sidebar-resizing");
    });

    traceDetailsClose.addEventListener("click", () => {
      clearSelection();
    });
    nodeSearchInput.addEventListener("input", () => {
      state.nodeSearchQuery = String(nodeSearchInput.value || "");
      if (state.lastData) {
        renderNodeList(state.lastData.nodes || []);
      }
    });
    nodeSortSelect.addEventListener("change", () => {
      state.nodeSortMode = String(nodeSortSelect.value || "last_heard");
      if (state.lastData) {
        renderNodeList(state.lastData.nodes || []);
      }
    });

    function nodeLabel(node) {
      function trimLabel(value) {
        const text = String(value || "").trim();
        if (text.length <= 8) return text;
        return text.slice(0, 8);
      }
      if (node.short_name && String(node.short_name).trim()) {
        return trimLabel(node.short_name);
      }
      if (node.id && String(node.id).trim()) {
        return trimLabel(String(node.id).replace("!", ""));
      }
      const shortFallback = shortNameFromNodeNum(node.num);
      if (shortFallback) return trimLabel(shortFallback);
      return trimLabel(String(node.num || "NODE"));
    }

    function nodeClass(node, nowSec) {
      const heard = Number(node.last_heard || 0);
      if (!heard) return "node-unknown";
      const ageMinutes = (nowSec - heard) / 60;
      if (!Number.isFinite(ageMinutes)) return "node-unknown";
      if (ageMinutes <= 120) return "node-fresh";
      if (ageMinutes <= 480) return "node-mid";
      return "node-stale";
    }

    function prettyAge(node, nowSec) {
      const heard = Number(node.last_heard || 0);
      if (!heard) return "last heard unknown";
      const ageSec = Math.max(0, Math.floor(nowSec - heard));
      const hours = Math.floor(ageSec / 3600);
      const mins = Math.floor((ageSec % 3600) / 60);
      if (hours > 0) return `${hours}h ${mins}m ago`;
      return `${mins}m ago`;
    }

    function formatEpochUtc(epochSec) {
      const value = Number(epochSec || 0);
      if (!Number.isFinite(value) || value <= 0) return "-";
      return new Date(value * 1000).toISOString().replace("T", " ").replace(".000Z", " UTC");
    }

    function shortNameFromNodeNum(rawNum) {
      const num = Number(rawNum);
      if (!Number.isFinite(num)) return "";
      const uint32 = Math.trunc(num) >>> 0;
      return uint32.toString(16).padStart(8, "0").slice(-4);
    }

    function nodeFromRecord(record) {
      if (!record || typeof record !== "object") return "?";
      if (record.short_name && String(record.short_name).trim()) return String(record.short_name).trim();
      if (record.id && String(record.id).trim()) return String(record.id).replace("!", "");
      if (record.num !== undefined && record.num !== null) {
        const shortFallback = shortNameFromNodeNum(record.num);
        if (shortFallback) return shortFallback;
        return String(record.num);
      }
      return "?";
    }

    function hasCoord(node) {
      return (
        node &&
        typeof node.lat === "number" &&
        Number.isFinite(node.lat) &&
        typeof node.lon === "number" &&
        Number.isFinite(node.lon)
      );
    }

    function metersPerLonDegree(lat) {
      return 111111 * Math.max(0.2, Math.cos((lat * Math.PI) / 180));
    }

    function offsetLatLon(lat, lon, eastMeters, northMeters) {
      const newLat = lat + northMeters / 111111;
      const newLon = lon + eastMeters / metersPerLonDegree(lat);
      return [newLat, newLon];
    }

    function offsetSegment(lat1, lon1, lat2, lon2, offsetMeters) {
      const midLat = (lat1 + lat2) / 2;
      const mLat = 111111;
      const mLon = metersPerLonDegree(midLat);

      const x1 = lon1 * mLon;
      const y1 = lat1 * mLat;
      const x2 = lon2 * mLon;
      const y2 = lat2 * mLat;

      // Use a canonical orientation for the normal vector so the same
      // undirected segment gets a stable left/right offset even when traversed
      // in opposite directions.
      let ax = x1;
      let ay = y1;
      let bx = x2;
      let by = y2;
      if (ax > bx || (Math.abs(ax - bx) < 1e-9 && ay > by)) {
        ax = x2;
        ay = y2;
        bx = x1;
        by = y1;
      }

      const dx = bx - ax;
      const dy = by - ay;
      const length = Math.hypot(dx, dy);
      if (length < 1e-6) {
        return [[lat1, lon1], [lat2, lon2]];
      }

      const nx = -dy / length;
      const ny = dx / length;
      const ox = nx * offsetMeters;
      const oy = ny * offsetMeters;

      const segDx = x2 - x1;
      const segDy = y2 - y1;
      const segLength = Math.hypot(segDx, segDy);
      if (segLength < 1e-6) {
        return [[lat1, lon1], [lat2, lon2]];
      }
      const ux = segDx / segLength;
      const uy = segDy / segLength;

      // Taper into the offset path so lines attach exactly to node markers.
      let taper = Math.max(
        ROUTE_OFFSET_TAPER_MIN_METERS,
        Math.min(ROUTE_OFFSET_TAPER_MAX_METERS, segLength * ROUTE_OFFSET_TAPER_RATIO)
      );
      taper = Math.min(taper, segLength * 0.45);

      if (taper < 1e-3) {
        return [[lat1, lon1], [lat2, lon2]];
      }

      const inner1x = x1 + ux * taper + ox;
      const inner1y = y1 + uy * taper + oy;
      const inner2x = x2 - ux * taper + ox;
      const inner2y = y2 - uy * taper + oy;

      return [
        [lat1, lon1],
        [inner1y / mLat, inner1x / mLon],
        [inner2y / mLat, inner2x / mLon],
        [lat2, lon2],
      ];
    }

    function segmentDistanceMeters(lat1, lon1, lat2, lon2) {
      const midLat = (lat1 + lat2) / 2;
      const mLat = 111111;
      const mLon = metersPerLonDegree(midLat);
      const dx = (lon2 - lon1) * mLon;
      const dy = (lat2 - lat1) * mLat;
      return Math.hypot(dx, dy);
    }

    function estimateNodePositions(nodes, traces) {
      const nodeMap = new Map();
      for (const raw of Array.isArray(nodes) ? nodes : []) {
        const num = Number(raw?.num);
        if (!Number.isFinite(num)) continue;
        const node = { ...(raw || {}), num, estimated: false };
        if (!hasCoord(node)) {
          node.lat = null;
          node.lon = null;
        }
        nodeMap.set(num, node);
      }

      function ensureTraceNode(rawNum) {
        const num = Number(rawNum);
        if (!Number.isFinite(num) || nodeMap.has(num)) return;
        const shortName = shortNameFromNodeNum(num) || null;
        nodeMap.set(num, {
          num,
          id: null,
          long_name: shortName ? `Unknown ${shortName}` : "Unknown",
          short_name: shortName,
          lat: null,
          lon: null,
          last_heard: null,
          estimated: false,
          trace_only: true,
        });
      }

      for (const trace of Array.isArray(traces) ? traces : []) {
        for (const key of ["towards_nums", "back_nums"]) {
          const route = Array.isArray(trace?.[key]) ? trace[key] : [];
          for (const rawNum of route) {
            ensureTraceNode(rawNum);
          }
        }
      }

      const candidates = new Map();
      function addCandidate(num, lat, lon) {
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
        if (!candidates.has(num)) {
          candidates.set(num, []);
        }
        candidates.get(num).push([lat, lon]);
      }

      const routeLists = [];
      for (const trace of Array.isArray(traces) ? traces : []) {
        for (const key of ["towards_nums", "back_nums"]) {
          const route = Array.isArray(trace?.[key]) ? trace[key] : [];
          const cleaned = route
            .map((value) => Number(value))
            .filter((value) => Number.isFinite(value) && nodeMap.has(value));
          if (cleaned.length >= 2) {
            routeLists.push(cleaned);
          }
        }
      }

      for (const route of routeLists) {
        const knownIndices = [];
        for (let i = 0; i < route.length; i += 1) {
          const node = nodeMap.get(route[i]);
          if (hasCoord(node)) knownIndices.push(i);
        }

        for (let k = 0; k < knownIndices.length - 1; k += 1) {
          const aIndex = knownIndices[k];
          const bIndex = knownIndices[k + 1];
          if (bIndex - aIndex <= 1) continue;
          const a = nodeMap.get(route[aIndex]);
          const b = nodeMap.get(route[bIndex]);
          if (!hasCoord(a) || !hasCoord(b)) continue;

          for (let i = aIndex + 1; i < bIndex; i += 1) {
            const num = route[i];
            const node = nodeMap.get(num);
            if (!node || hasCoord(node)) continue;
            const t = (i - aIndex) / (bIndex - aIndex);
            addCandidate(num, a.lat + (b.lat - a.lat) * t, a.lon + (b.lon - a.lon) * t);
          }
        }

        for (let i = 0; i < route.length; i += 1) {
          const num = route[i];
          const node = nodeMap.get(num);
          if (!node || hasCoord(node)) continue;

          let nearestIndex = -1;
          let nearestDistance = Number.POSITIVE_INFINITY;
          for (let j = 0; j < route.length; j += 1) {
            if (i === j) continue;
            const ref = nodeMap.get(route[j]);
            if (!hasCoord(ref)) continue;
            const distance = Math.abs(i - j);
            if (distance < nearestDistance) {
              nearestDistance = distance;
              nearestIndex = j;
            }
          }
          if (nearestIndex < 0) continue;

          const anchor = nodeMap.get(route[nearestIndex]);
          if (!hasCoord(anchor)) continue;

          const angle = ((Math.abs(num) % 360) * Math.PI) / 180;
          const radius = 90 + Math.min(8, nearestDistance) * 55;
          const side = i < nearestIndex ? -1 : 1;
          const east = Math.cos(angle) * radius * side;
          const north = Math.sin(angle) * radius;
          const [lat, lon] = offsetLatLon(anchor.lat, anchor.lon, east, north);
          addCandidate(num, lat, lon);
        }
      }

      for (const [num, points] of candidates.entries()) {
        const node = nodeMap.get(num);
        if (!node || hasCoord(node) || !points.length) continue;
        let latTotal = 0;
        let lonTotal = 0;
        for (const [lat, lon] of points) {
          latTotal += lat;
          lonTotal += lon;
        }
        node.lat = latTotal / points.length;
        node.lon = lonTotal / points.length;
        node.estimated = true;
      }

      return nodeMap;
    }

    function renderLogs(logs) {
      const container = document.getElementById("logList");
      const entries = Array.isArray(logs) ? logs : [];
      const stickToBottom = container.scrollTop + container.clientHeight >= container.scrollHeight - 24;
      if (!entries.length) {
        container.innerHTML = '<div class="empty">No runtime logs yet.</div>';
        return;
      }
      container.innerHTML = entries.map((entry) => {
        const streamClass = entry && entry.stream === "stderr" ? "stderr" : "";
        return `<div class="log-entry ${streamClass}">${escapeHtml(entry.message || "")}</div>`;
      }).join("");
      if (stickToBottom) {
        container.scrollTop = container.scrollHeight;
      }
    }

    function renderNodeList(nodes) {
      const container = document.getElementById("nodeList");
      const nowSec = Date.now() / 1000;
      if (nodeSearchInput.value !== state.nodeSearchQuery) {
        nodeSearchInput.value = state.nodeSearchQuery;
      }
      if (nodeSortSelect.value !== state.nodeSortMode) {
        nodeSortSelect.value = state.nodeSortMode;
      }
      const query = String(state.nodeSearchQuery || "").trim().toLowerCase();
      const filtered = Array.isArray(nodes) ? nodes.filter((node) => {
        if (!query) return true;
        const shortName = String(node.short_name || "").toLowerCase();
        const longName = String(node.long_name || "").toLowerCase();
        return shortName.includes(query) || longName.includes(query);
      }) : [];

      const sorted = [...filtered].sort((a, b) => {
        function compareNames(nameA, nameB) {
          const aText = String(nameA || "").trim().toLowerCase();
          const bText = String(nameB || "").trim().toLowerCase();
          if (!aText && bText) return 1;
          if (aText && !bText) return -1;
          if (aText < bText) return -1;
          if (aText > bText) return 1;
          return 0;
        }

        if (state.nodeSortMode === "last_heard") {
          const aHeard = Number(a.last_heard || 0);
          const bHeard = Number(b.last_heard || 0);
          const aHas = Number.isFinite(aHeard) && aHeard > 0;
          const bHas = Number.isFinite(bHeard) && bHeard > 0;
          if (aHas && !bHas) return -1;
          if (!aHas && bHas) return 1;
          if (aHeard !== bHeard) return bHeard - aHeard;
        } else if (state.nodeSortMode === "short_name") {
          const cmpShort = compareNames(a.short_name, b.short_name);
          if (cmpShort !== 0) return cmpShort;
        } else if (state.nodeSortMode === "long_name") {
          const cmpLong = compareNames(a.long_name, b.long_name);
          if (cmpLong !== 0) return cmpLong;
        }

        const aLabel = nodeLabel(a).toLowerCase();
        const bLabel = nodeLabel(b).toLowerCase();
        if (aLabel < bLabel) return -1;
        if (aLabel > bLabel) return 1;
        return Number(a.num || 0) - Number(b.num || 0);
      });
      if (!sorted.length) {
        container.innerHTML = query
          ? '<div class="empty">No nodes match that search.</div>'
          : '<div class="empty">No known nodes.</div>';
        return;
      }
      container.innerHTML = sorted.map((node) => {
        const num = Number(node.num);
        const hasPos = hasCoord(node);
        const inferred = Boolean(node.estimated);
        const active = num === state.selectedNodeNum ? "active" : "";
        const disabled = hasPos ? "" : "disabled";
        const locationText = inferred ? "estimated from traceroute" : hasPos ? "GPS" : "no GPS";
        return `
          <button class="list-item ${active}" type="button" data-node-num="${num}" ${disabled}>
            <span class="item-title">${escapeHtml(nodeLabel(node))} ${hasPos ? "" : "(no GPS)"}</span>
            <span class="item-meta">${escapeHtml(node.long_name || "Unknown")} | ${escapeHtml(prettyAge(node, nowSec))}</span>
            <span class="item-meta">#${escapeHtml(node.num)} | ${escapeHtml(node.id || "-")} | ${escapeHtml(locationText)}</span>
          </button>
        `;
      }).join("");

      for (const btn of container.querySelectorAll("button[data-node-num]")) {
        btn.addEventListener("click", () => {
          const nodeNum = Number(btn.dataset.nodeNum);
          focusNode(nodeNum);
        });
      }
    }

    function scrollSelectedNodeListItemIntoView(options = {}) {
      const clearFilterIfHidden = Boolean(options.clearFilterIfHidden);
      let container = document.getElementById("nodeList");
      if (!container) return;

      let activeItem = container.querySelector("button[data-node-num].active");
      if (!activeItem && clearFilterIfHidden && state.nodeSearchQuery) {
        state.nodeSearchQuery = "";
        if (nodeSearchInput.value) {
          nodeSearchInput.value = "";
        }
        if (state.lastData) {
          renderNodeList(state.lastData.nodes || []);
          container = document.getElementById("nodeList");
          if (!container) return;
          activeItem = container.querySelector("button[data-node-num].active");
        }
      }

      if (!activeItem) return;
      activeItem.scrollIntoView({ block: "nearest", inline: "nearest" });
    }

    function traceNodesWithCoords(trace) {
      const coords = [];
      for (const key of ["towards_nums", "back_nums"]) {
        const route = Array.isArray(trace?.[key]) ? trace[key] : [];
        for (const num of route) {
          const node = state.nodeByNum.get(Number(num));
          if (!node) continue;
          if (typeof node.lat !== "number" || typeof node.lon !== "number") continue;
          coords.push([node.lat, node.lon]);
        }
      }
      return coords;
    }

    function routeToLabelPath(routeNums) {
      const route = Array.isArray(routeNums) ? routeNums : [];
      if (!route.length) return "-";
      return route.map((rawNum) => {
        const num = Number(rawNum);
        const node = state.nodeByNum.get(num);
        if (node) return nodeLabel(node);
        if (Number.isFinite(num)) {
          const shortFallback = shortNameFromNodeNum(num);
          if (shortFallback) return shortFallback;
          return String(num);
        }
        return "?";
      }).join(" -> ");
    }

    function drawableSegmentCount(routeNums) {
      const route = Array.isArray(routeNums) ? routeNums : [];
      let count = 0;
      for (let i = 0; i < route.length - 1; i += 1) {
        const src = state.nodeByNum.get(Number(route[i]));
        const dst = state.nodeByNum.get(Number(route[i + 1]));
        if (hasCoord(src) && hasCoord(dst)) count += 1;
      }
      return count;
    }

    function selectedTraceNodeNums() {
      if (state.selectedTraceId === null) return null;
      const trace = state.traceById.get(state.selectedTraceId);
      if (!trace) return null;
      const nodeNums = new Set();
      for (const key of ["towards_nums", "back_nums"]) {
        const route = Array.isArray(trace?.[key]) ? trace[key] : [];
        for (const rawNum of route) {
          const num = Number(rawNum);
          if (Number.isFinite(num)) nodeNums.add(num);
        }
      }
      for (const packetKey of ["from", "to"]) {
        const num = Number(trace?.packet?.[packetKey]?.num);
        if (Number.isFinite(num)) nodeNums.add(num);
      }
      return nodeNums;
    }

    function redrawFromLastServerData() {
      if (!state.lastServerData) return false;
      draw(state.lastServerData);
      return true;
    }

    function renderSelectionDetails() {
      if (state.selectedTraceId !== null) {
        const trace = state.traceById.get(state.selectedTraceId);
        if (!trace) {
          traceDetails.classList.add("hidden");
          traceDetailsBody.innerHTML = "";
          return;
        }

        const originLabel = nodeFromRecord(trace?.packet?.to);
        const targetLabel = nodeFromRecord(trace?.packet?.from);
        const towardsNums = Array.isArray(trace?.towards_nums) ? trace.towards_nums : [];
        const backNums = Array.isArray(trace?.back_nums) ? trace.back_nums : [];
        const towardsHops = Math.max(0, towardsNums.length - 1);
        const backHops = Math.max(0, backNums.length - 1);
        const towardsDrawable = drawableSegmentCount(towardsNums);
        const backDrawable = drawableSegmentCount(backNums);
        const towardsSummary =
          towardsDrawable < towardsHops
            ? `${towardsHops} hops, ${towardsDrawable} drawable segments`
            : `${towardsHops} hops`;
        const backSummary =
          backDrawable < backHops
            ? `${backHops} hops, ${backDrawable} drawable segments`
            : `${backHops} hops`;

        traceDetailsTitle.textContent = "Traceroute Details";
        traceDetailsBody.innerHTML = `
          <span class="trace-meta-row"><span class="trace-label">Trace</span>#${escapeHtml(trace.trace_id)}</span>
          <span class="trace-meta-row"><span class="trace-label">Time</span>${escapeHtml(trace.captured_at_utc || "-")}</span>
          <span class="trace-meta-row"><span class="trace-label">Route</span>${escapeHtml(originLabel)} -> ${escapeHtml(targetLabel)}</span>
          <span class="trace-meta-row"><span class="trace-label">Towards</span>${escapeHtml(towardsSummary)}</span>
          <span class="trace-meta-row"><span class="trace-label">Return</span>${escapeHtml(backSummary)}</span>
          <div class="trace-path">
            <span class="trace-path-title">Outgoing Path (orange)</span>
            <div class="trace-path-value">${escapeHtml(routeToLabelPath(towardsNums))}</div>
          </div>
          <div class="trace-path">
            <span class="trace-path-title">Return Path (blue)</span>
            <div class="trace-path-value">${escapeHtml(routeToLabelPath(backNums))}</div>
          </div>
        `;
        traceDetails.classList.remove("hidden");
        return;
      }

      if (state.selectedNodeNum !== null) {
        const node = state.nodeByNum.get(state.selectedNodeNum);
        if (!node) {
          traceDetails.classList.add("hidden");
          traceDetailsBody.innerHTML = "";
          return;
        }

        const nowSec = Date.now() / 1000;
        const hasPos = hasCoord(node);
        const locKind = node.estimated ? "estimated from traceroute" : hasPos ? "reported GPS" : "unknown";
        const lat = hasPos ? node.lat.toFixed(5) : "-";
        const lon = hasPos ? node.lon.toFixed(5) : "-";
        const longName = node.long_name && String(node.long_name).trim() ? node.long_name : "Unknown node";

        traceDetailsTitle.textContent = "Node Details";
        traceDetailsBody.innerHTML = `
          <span class="trace-meta-row"><span class="trace-label">Name</span>${escapeHtml(nodeLabel(node))}</span>
          <span class="trace-meta-row"><span class="trace-label">Long Name</span>${escapeHtml(longName)}</span>
          <span class="trace-meta-row"><span class="trace-label">Node</span>#${escapeHtml(node.num || "?")}</span>
          <span class="trace-meta-row"><span class="trace-label">ID</span>${escapeHtml(node.id || "-")}</span>
          <span class="trace-meta-row"><span class="trace-label">Last Heard</span>${escapeHtml(prettyAge(node, nowSec))}</span>
          <span class="trace-meta-row"><span class="trace-label">Last Heard UTC</span>${escapeHtml(formatEpochUtc(node.last_heard))}</span>
          <span class="trace-meta-row"><span class="trace-label">Location</span>${escapeHtml(locKind)}</span>
          <span class="trace-meta-row"><span class="trace-label">Lat/Lon</span>${escapeHtml(lat)}, ${escapeHtml(lon)}</span>
        `;
        traceDetails.classList.remove("hidden");
        return;
      }

      traceDetails.classList.add("hidden");
      traceDetailsBody.innerHTML = "";
    }

    function clearSelection() {
      if (state.selectedTraceId === null && state.selectedNodeNum === null) return;
      state.selectedTraceId = null;
      state.selectedNodeNum = null;
      if (redrawFromLastServerData()) return;
      applyNodeSelectionVisual();
      applyTraceSelectionVisual();
      if (state.lastData) {
        renderNodeList(state.lastData.nodes || []);
        renderTraceList(state.lastData.traces || []);
      }
      renderSelectionDetails();
    }

    function renderTraceList(traces) {
      const container = document.getElementById("traceList");
      const recent = Array.isArray(traces) ? traces.slice(-50).reverse() : [];
      if (!recent.length) {
        container.innerHTML = '<div class="empty">No completed traceroutes yet.</div>';
        return;
      }
      container.innerHTML = recent.map((trace) => {
        const traceId = Number(trace.trace_id);
        const active = traceId === state.selectedTraceId ? "active" : "";
        const originLabel = nodeFromRecord(trace?.packet?.to);
        const targetLabel = nodeFromRecord(trace?.packet?.from);
        const fwdHops = Math.max(0, (trace.towards_nums || []).length - 1);
        const backHops = Math.max(0, (trace.back_nums || []).length - 1);
        return `
          <button class="list-item ${active}" type="button" data-trace-id="${traceId}">
            <span class="item-title">#${escapeHtml(traceId)} ${escapeHtml(originLabel)} -> ${escapeHtml(targetLabel)}</span>
            <span class="item-meta">${escapeHtml(trace.captured_at_utc || "-")}</span>
            <span class="item-meta">towards: ${escapeHtml(fwdHops)} hops | back: ${escapeHtml(backHops)} hops</span>
          </button>
        `;
      }).join("");

      for (const btn of container.querySelectorAll("button[data-trace-id]")) {
        btn.addEventListener("click", () => {
          const traceId = Number(btn.dataset.traceId);
          focusTrace(traceId);
        });
      }
    }

    function applyNodeSelectionVisual() {
      for (const [num, marker] of state.markerByNum.entries()) {
        const element = marker.getElement();
        if (!element) continue;
        const badge = element.querySelector(".node-badge");
        if (!badge) continue;
        badge.classList.toggle("node-selected", num === state.selectedNodeNum);
      }
    }

    function applyTraceSelectionVisual() {
      const hasSelection = state.selectedTraceId !== null;
      for (const [traceId, polylines] of state.edgePolylinesByTrace.entries()) {
        for (const line of polylines) {
          const direction = line.options.meshDirection === "back" ? "back" : "towards";
          const baseColor = ROUTE_COLORS[direction];
          if (!hasSelection) {
            line.setStyle({ color: baseColor, weight: 3, opacity: 0.5 });
            continue;
          }
          if (traceId === state.selectedTraceId) {
            const selectedColor = ROUTE_SELECTED_COLORS[direction];
            line.setStyle({ color: selectedColor, weight: 6, opacity: 0.98 });
          } else {
            line.setStyle({ color: baseColor, weight: 2, opacity: 0.18 });
          }
        }
      }
    }

    function focusNode(nodeNum, options = {}) {
      if (!Number.isFinite(nodeNum)) return;
      const shouldScrollNodeList = Boolean(options.scrollNodeListIntoView || options.switchToNodesTab);
      const shouldPanZoom = options.panZoom !== false;
      state.selectedNodeNum = nodeNum;
      state.selectedTraceId = null;
      if (options.switchToNodesTab) {
        setActiveTab("nodes");
      }
      if (!redrawFromLastServerData()) {
        applyNodeSelectionVisual();
        applyTraceSelectionVisual();
        if (state.lastData) {
          renderNodeList(state.lastData.nodes || []);
          renderTraceList(state.lastData.traces || []);
        }
        renderSelectionDetails();
      }
      if (shouldScrollNodeList) {
        requestAnimationFrame(() => {
          scrollSelectedNodeListItemIntoView({ clearFilterIfHidden: true });
        });
      }
      if (!shouldPanZoom) return;
      const marker = state.markerByNum.get(nodeNum);
      if (!marker) return;
      const ll = marker.getLatLng();
      const zoom = Math.max(map.getZoom(), NODE_FOCUS_MIN_ZOOM);
      map.flyTo(ll, zoom, { animate: true, duration: 0.35 });
    }

    function focusTrace(traceId) {
      if (!Number.isFinite(traceId)) return;
      state.selectedTraceId = traceId;
      state.selectedNodeNum = null;
      if (!redrawFromLastServerData()) {
        applyNodeSelectionVisual();
        applyTraceSelectionVisual();
        if (state.lastData) {
          renderNodeList(state.lastData.nodes || []);
          renderTraceList(state.lastData.traces || []);
        }
        renderSelectionDetails();
      }

      const selectedLines = state.edgePolylinesByTrace.get(traceId) || [];
      const bounds = [];
      for (const line of selectedLines) {
        for (const ll of line.getLatLngs()) {
          bounds.push(ll);
        }
      }
      if (!bounds.length) {
        const trace = state.traceById.get(traceId);
        if (trace) {
          for (const coord of traceNodesWithCoords(trace)) {
            bounds.push(L.latLng(coord[0], coord[1]));
          }
        }
      }
      if (bounds.length) {
        map.fitBounds(L.latLngBounds(bounds), { padding: [40, 40], maxZoom: TRACE_FOCUS_MAX_ZOOM });
      }
    }

    function addTracePathSegments(traceId, routeNums, direction) {
      let count = 0;
      const route = Array.isArray(routeNums) ? routeNums : [];
      if (route.length < 2) return count;
      const directionSign = direction === "back" ? -1 : 1;

      for (let i = 0; i < route.length - 1; i += 1) {
        const src = state.nodeByNum.get(Number(route[i]));
        const dst = state.nodeByNum.get(Number(route[i + 1]));
        if (!hasCoord(src) || !hasCoord(dst)) continue;

        const segmentMeters = segmentDistanceMeters(src.lat, src.lon, dst.lat, dst.lon);
        const baseOffset = Math.max(
          ROUTE_OFFSET_MIN_METERS,
          Math.min(ROUTE_OFFSET_MAX_METERS, segmentMeters * ROUTE_OFFSET_SCALE)
        );
        const jitter = ((Math.abs(traceId) % 7) - 3) * Math.max(1, baseOffset * ROUTE_OFFSET_JITTER_SCALE);
        const segmentOffset = directionSign * (baseOffset + jitter);

        const pathPoints = offsetSegment(src.lat, src.lon, dst.lat, dst.lon, segmentOffset);
        const color = ROUTE_COLORS[direction];
        const polyline = L.polyline(pathPoints, {
          color,
          weight: 3,
          opacity: 0.55,
          meshDirection: direction,
        }).addTo(edgeLayer);
        if (!state.edgePolylinesByTrace.has(traceId)) {
          state.edgePolylinesByTrace.set(traceId, []);
        }
        state.edgePolylinesByTrace.get(traceId).push(polyline);
        count += 1;
      }

      return count;
    }

    function draw(data) {
      state.lastServerData = data;
      state.nodeByNum = estimateNodePositions(data.nodes || [], data.traces || []);
      state.traceById.clear();
      for (const trace of data.traces || []) {
        state.traceById.set(Number(trace.trace_id), trace);
      }
      if (state.selectedTraceId !== null && !state.traceById.has(state.selectedTraceId)) {
        state.selectedTraceId = null;
      }
      if (state.selectedNodeNum !== null && !state.nodeByNum.has(state.selectedNodeNum)) {
        state.selectedNodeNum = null;
      }

      markerLayer.clearLayers();
      edgeLayer.clearLayers();
      state.markerByNum.clear();
      state.edgePolylinesByTrace.clear();

      const bounds = [];
      const nowSec = Date.now() / 1000;
      const displayNodes = Array.from(state.nodeByNum.values());
      const visibleTraceNodes = selectedTraceNodeNums();

      for (const node of displayNodes) {
        if (!hasCoord(node)) continue;
        const nodeNum = Number(node.num);
        if (visibleTraceNodes !== null && !visibleTraceNodes.has(nodeNum)) continue;
        const ll = [node.lat, node.lon];
        bounds.push(ll);
        const labelText = nodeLabel(node);
        const label = escapeHtml(labelText);
        const cssClass = nodeClass(node, nowSec);
        const width = Math.max(30, Math.min(92, 18 + labelText.length * 9));
        const height = 30;
        const icon = L.divIcon({
          className: "",
          html: `<div class="node-badge ${cssClass}">${label}</div>`,
          iconSize: [width, height],
          iconAnchor: [width / 2, height / 2],
          popupAnchor: [0, -14],
        });
        const marker = L.marker(ll, { icon, riseOnHover: true, keyboard: false }).addTo(markerLayer);
        marker.on("click", () => {
          focusNode(nodeNum, { switchToNodesTab: true, scrollNodeListIntoView: true, panZoom: false });
        });
        state.markerByNum.set(nodeNum, marker);
      }

      let drawnEdgeCount = 0;
      for (const trace of data.traces || []) {
        const traceId = Number(trace.trace_id);
        if (!Number.isFinite(traceId)) continue;
        drawnEdgeCount += addTracePathSegments(traceId, trace.towards_nums || [], "towards");
        drawnEdgeCount += addTracePathSegments(traceId, trace.back_nums || [], "back");
      }

      const viewData = {
        ...data,
        nodes: displayNodes,
        drawn_edge_count: drawnEdgeCount,
      };
      state.lastData = viewData;

      document.getElementById("meshHost").textContent = data.mesh_host || "-";
      document.getElementById("nodeCount").textContent = String(data.node_count || 0);
      document.getElementById("traceCount").textContent = String(data.trace_count || 0);
      document.getElementById("edgeCount").textContent = String(drawnEdgeCount);
      document.getElementById("updated").textContent = data.generated_at_utc || "-";

      renderLogs(data.logs || []);
      renderNodeList(displayNodes);
      renderTraceList(data.traces || []);
      applyNodeSelectionVisual();
      applyTraceSelectionVisual();
      renderSelectionDetails();

      if (!state.fitted && bounds.length > 0) {
        map.fitBounds(bounds, { padding: [24, 24] });
        state.fitted = true;
      }
    }

    async function refresh() {
      try {
        const response = await fetch("/api/map", { cache: "no-store" });
        if (!response.ok) return;
        const data = await response.json();
        draw(data);
      } catch (_e) {
      }
    }

    refresh();
    setInterval(refresh, 1000);
  </script>
</body>
</html>
"""


def start_map_server(state: MapState, host: str, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, html: str, status: int = 200) -> None:
            body = html.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            path = self.path.split("?", 1)[0]
            if path in ("/", "/map"):
                self._send_html(MAP_HTML)
                return
            if path == "/api/map":
                self._send_json(state.snapshot())
                return
            if path == "/healthz":
                self._send_json({"ok": True, "at_utc": utc_now()})
                return
            self._send_json({"error": "not_found"}, status=404)

    server = ThreadingHTTPServer((host, port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
