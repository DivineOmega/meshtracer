from __future__ import annotations

import json
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from .common import utc_now

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
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 8px;
      padding-right: 28px;
    }
    .head-actions {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 8px;
      min-width: 0;
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
    .icon-btn {
      width: 28px;
      height: 28px;
      border: 1px solid #2d4065;
      border-radius: 10px;
      background: rgba(16, 26, 52, 0.92);
      color: #cfe0ff;
      cursor: pointer;
      line-height: 1;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      flex: 0 0 auto;
    }
    .icon-btn:hover {
      background: rgba(20, 35, 69, 0.98);
    }
    .icon-btn:focus-visible {
      outline: 2px solid rgba(82, 137, 221, 0.72);
      outline-offset: 2px;
    }
    .icon-btn svg {
      width: 16px;
      height: 16px;
    }
    .mesh-host-row {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 8px;
    }
    .mesh-host {
      color: #9cb0d6;
      font-size: 11px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      flex: 1;
      min-width: 0;
    }
    .disconnect-btn {
      border: 1px solid #2d4065;
      border-radius: 10px;
      background: rgba(18, 33, 69, 0.75);
      color: #d9e4fb;
      font-weight: 700;
      cursor: pointer;
      padding: 6px 9px;
      font-size: 11px;
      white-space: nowrap;
    }
    .disconnect-btn:hover {
      background: rgba(25, 48, 95, 0.85);
    }
    .onboarding {
      position: fixed;
      inset: 0;
      z-index: 2500;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 20px;
      background:
        radial-gradient(1200px 800px at 20% 15%, rgba(81, 160, 255, 0.22), rgba(11, 16, 32, 0.92)),
        radial-gradient(900px 700px at 85% 80%, rgba(111, 240, 176, 0.12), rgba(11, 16, 32, 0.92)),
        rgba(11, 16, 32, 0.96);
      box-sizing: border-box;
    }
    .onboarding.hidden {
      display: none;
    }
    .onboarding-card {
      width: min(720px, 100%);
      border: 1px solid rgba(148, 163, 184, 0.28);
      background: rgba(10, 18, 35, 0.88);
      border-radius: 18px;
      padding: 22px 22px 18px 22px;
      box-shadow: 0 18px 60px rgba(0, 0, 0, 0.55);
      backdrop-filter: blur(4px);
    }
    .onboarding-brand {
      font-size: 20px;
      font-weight: 800;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      color: #eef5ff;
    }
    .onboarding-tagline {
      margin-top: 6px;
      color: #b7c7e6;
      font-size: 13px;
      line-height: 1.4;
    }
    .onboarding-form {
      margin-top: 16px;
    }
    .onboarding-label {
      display: block;
      color: #9cb2da;
      font-size: 11px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      margin-bottom: 7px;
    }
    .onboarding-row {
      display: flex;
      gap: 8px;
      align-items: center;
    }
    #connectHost {
      flex: 1;
      min-width: 0;
      box-sizing: border-box;
      border: 1px solid #314a74;
      background: #0d1832;
      color: #d9e5fb;
      border-radius: 12px;
      padding: 11px 12px;
      font-size: 14px;
      outline: none;
    }
    #connectHost:focus {
      border-color: #4d79ba;
      box-shadow: 0 0 0 2px rgba(82, 137, 221, 0.24);
    }
    .connect-btn {
      border: 1px solid #2d4065;
      border-radius: 12px;
      background: #122145;
      color: #d9e4fb;
      font-weight: 800;
      cursor: pointer;
      padding: 11px 12px;
      font-size: 13px;
      white-space: nowrap;
    }
    .connect-btn:hover {
      background: #19305f;
    }
    .connect-btn:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    .connect-error {
      display: none;
      border: 1px solid rgba(255, 85, 125, 0.42);
      background: rgba(70, 15, 25, 0.55);
      color: #ffd0dc;
      border-radius: 12px;
      padding: 9px 10px;
      font-size: 12px;
      margin-top: 10px;
      white-space: normal;
      overflow-wrap: anywhere;
    }
    .connect-error.visible {
      display: block;
    }
    .onboarding-help {
      margin-top: 10px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.4;
    }
    .onboarding-help code {
      display: inline-block;
      padding: 1px 7px;
      margin-right: 6px;
      border-radius: 999px;
      border: 1px solid rgba(36, 56, 88, 0.9);
      background: rgba(15, 26, 51, 0.8);
      color: #d8e3fb;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 12px;
    }
    .connect-status {
      margin-top: 12px;
      color: #9cb0d6;
      font-size: 11px;
      line-height: 1.35;
    }
    .discovery {
      margin-top: 16px;
      padding-top: 14px;
      border-top: 1px solid rgba(39, 57, 87, 0.75);
    }
    .discovery-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 8px;
    }
    .discovery-title {
      font-size: 12px;
      font-weight: 800;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      color: #e7efff;
    }
    .discovery-meta {
      color: var(--muted);
      font-size: 11px;
      line-height: 1.35;
      margin-bottom: 8px;
      white-space: normal;
      overflow-wrap: anywhere;
    }
    .discovery-list {
      display: grid;
      gap: 8px;
    }
    .discovery-rescan {
      border: 1px solid rgba(45, 64, 101, 0.9);
      border-radius: 10px;
      background: rgba(16, 26, 52, 0.92);
      color: #cfe0ff;
      font-weight: 800;
      cursor: pointer;
      padding: 6px 9px;
      font-size: 11px;
      white-space: nowrap;
    }
    .discovery-rescan:hover {
      background: rgba(20, 35, 69, 0.98);
    }
    .discovery-rescan:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    .discovery-item {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      border: 1px solid rgba(36, 56, 88, 0.95);
      background: rgba(15, 26, 51, 0.86);
      border-radius: 14px;
      padding: 10px 10px;
    }
    .discovery-item-main {
      min-width: 0;
    }
    .discovery-item-host {
      display: block;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 13px;
      font-weight: 800;
      color: #eef5ff;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .discovery-item-meta {
      display: block;
      margin-top: 2px;
      color: var(--muted);
      font-size: 11px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .discovery-item-btn {
      border: 1px solid #2d4065;
      border-radius: 12px;
      background: rgba(18, 33, 69, 0.85);
      color: #d9e4fb;
      font-weight: 800;
      cursor: pointer;
      padding: 8px 10px;
      font-size: 12px;
      white-space: nowrap;
      flex: 0 0 auto;
    }
    .discovery-item-btn:hover {
      background: rgba(25, 48, 95, 0.92);
    }
    .discovery-empty {
      border: 1px dashed rgba(44, 64, 100, 0.9);
      border-radius: 14px;
      padding: 10px 10px;
      color: #9fb0d1;
      font-size: 12px;
      background: rgba(13, 22, 44, 0.55);
      text-align: left;
      line-height: 1.35;
    }
    @media (max-width: 520px) {
      .onboarding-row {
        flex-direction: column;
        align-items: stretch;
      }
      .connect-btn {
        width: 100%;
      }
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
      grid-template-columns: repeat(auto-fit, minmax(0, 1fr));
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
    .traces-panel {
      flex-direction: column;
      min-height: 0;
      height: 100%;
    }
    .panel.traces-panel.active {
      display: flex;
    }
    .traces-toolbar {
      border-bottom: 1px solid #253756;
      padding: 8px 12px 8px 12px;
      background: rgba(11, 20, 40, 0.6);
      display: flex;
      justify-content: flex-start;
    }
    .trace-manage-btn {
      padding: 6px 10px;
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
    .cfg-section {
      border: 1px solid #263a5e;
      background: rgba(13, 22, 44, 0.55);
      border-radius: 12px;
      padding: 10px 10px 10px 10px;
      margin-bottom: 10px;
    }
    .cfg-title {
      font-size: 11px;
      font-weight: 800;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      color: #e7efff;
      margin-bottom: 8px;
    }
    .cfg-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
    }
    .cfg-field {
      display: grid;
      grid-template-columns: 1fr;
      gap: 6px;
    }
    .cfg-label-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
    }
    .cfg-label {
      color: #9cb2da;
      font-size: 11px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      white-space: nowrap;
    }
    .cfg-help {
      width: 20px;
      height: 20px;
      border-radius: 999px;
      border: 1px solid rgba(45, 64, 101, 0.92);
      background: rgba(16, 26, 52, 0.92);
      color: #cfe0ff;
      font-weight: 900;
      font-size: 12px;
      line-height: 1;
      cursor: pointer;
      flex: 0 0 auto;
    }
    .cfg-help:hover {
      background: rgba(20, 35, 69, 0.98);
    }
    .cfg-input {
      width: 100%;
      box-sizing: border-box;
      border: 1px solid #314a74;
      background: #0d1832;
      color: #d9e5fb;
      border-radius: 10px;
      padding: 8px 10px;
      font-size: 12px;
      outline: none;
    }
    .cfg-input:focus {
      border-color: #4d79ba;
      box-shadow: 0 0 0 2px rgba(82, 137, 221, 0.24);
    }
    .cfg-actions {
      display: flex;
      gap: 8px;
      margin-top: 10px;
    }
    .cfg-btn {
      border: 1px solid #2d4065;
      border-radius: 10px;
      background: #122145;
      color: #d9e4fb;
      font-weight: 800;
      cursor: pointer;
      padding: 8px 10px;
      font-size: 12px;
      white-space: nowrap;
      flex: 1;
    }
    .cfg-btn:hover {
      background: #19305f;
    }
    .cfg-btn.secondary {
      background: rgba(16, 26, 52, 0.92);
      color: #cfe0ff;
    }
    .cfg-btn.secondary:hover {
      background: rgba(20, 35, 69, 0.98);
    }
    .cfg-status {
      margin-top: 10px;
      border: 1px solid rgba(148, 163, 184, 0.28);
      background: rgba(10, 18, 35, 0.55);
      color: #c9d7f3;
      border-radius: 10px;
      padding: 8px 10px;
      font-size: 12px;
      line-height: 1.35;
      display: none;
      white-space: normal;
      overflow-wrap: anywhere;
    }
    .cfg-status.visible {
      display: block;
    }
    .cfg-status.error {
      border-color: rgba(255, 85, 125, 0.42);
      background: rgba(70, 15, 25, 0.55);
      color: #ffd0dc;
    }
    .cfg-readonly {
      margin-top: 10px;
      border: 1px dashed rgba(44, 64, 100, 0.9);
      border-radius: 12px;
      padding: 10px 10px;
      color: #9fb0d1;
      font-size: 12px;
      background: rgba(13, 22, 44, 0.55);
      line-height: 1.35;
    }
    .cfg-readonly code {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      color: #d8e3fb;
    }
    .config-modal {
      position: fixed;
      inset: 0;
      z-index: 2500;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 20px;
      box-sizing: border-box;
    }
    .config-modal.hidden {
      display: none;
    }
    .config-overlay {
      position: absolute;
      inset: 0;
      background: rgba(7, 10, 18, 0.62);
      backdrop-filter: blur(2px);
    }
    .config-card {
      position: relative;
      width: min(560px, 100%);
      max-height: min(760px, calc(100vh - 40px));
      border: 1px solid rgba(148, 163, 184, 0.28);
      background: rgba(10, 18, 35, 0.92);
      border-radius: 16px;
      box-shadow: 0 18px 60px rgba(0, 0, 0, 0.55);
      padding: 14px 14px 12px 14px;
      backdrop-filter: blur(4px);
      display: flex;
      flex-direction: column;
      box-sizing: border-box;
    }
    .config-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 8px;
    }
    .config-title {
      font-size: 13px;
      font-weight: 900;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: #eef5ff;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .config-close {
      width: 26px;
      height: 26px;
      border: 1px solid #2d4065;
      border-radius: 10px;
      background: rgba(16, 26, 52, 0.92);
      color: #cfe0ff;
      font-weight: 900;
      cursor: pointer;
      line-height: 1;
      flex: 0 0 auto;
    }
    .config-close:hover {
      background: rgba(20, 35, 69, 0.98);
    }
    .config-body {
      flex: 1;
      min-height: 0;
      overflow: auto;
      padding: 10px 12px 12px 12px;
      box-sizing: border-box;
    }
    .help-modal {
      position: fixed;
      inset: 0;
      z-index: 2600;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 20px;
      box-sizing: border-box;
    }
    .help-modal.hidden {
      display: none;
    }
    .help-overlay {
      position: absolute;
      inset: 0;
      background: rgba(7, 10, 18, 0.62);
      backdrop-filter: blur(2px);
    }
    .help-card {
      position: relative;
      width: min(520px, 100%);
      border: 1px solid rgba(148, 163, 184, 0.28);
      background: rgba(10, 18, 35, 0.92);
      border-radius: 16px;
      box-shadow: 0 18px 60px rgba(0, 0, 0, 0.55);
      padding: 14px 14px 12px 14px;
      backdrop-filter: blur(4px);
    }
    .help-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 8px;
    }
    .help-title {
      font-size: 13px;
      font-weight: 900;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: #eef5ff;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .help-close {
      width: 26px;
      height: 26px;
      border: 1px solid #2d4065;
      border-radius: 10px;
      background: rgba(16, 26, 52, 0.92);
      color: #cfe0ff;
      font-weight: 900;
      cursor: pointer;
      line-height: 1;
      flex: 0 0 auto;
    }
    .help-close:hover {
      background: rgba(20, 35, 69, 0.98);
    }
    .help-body {
      color: #c9d7f3;
      font-size: 12px;
      line-height: 1.45;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }
    .queue-modal {
      position: fixed;
      inset: 0;
      z-index: 2650;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 20px;
      box-sizing: border-box;
    }
    .queue-modal.hidden {
      display: none;
    }
    .queue-overlay {
      position: absolute;
      inset: 0;
      background: rgba(7, 10, 18, 0.62);
      backdrop-filter: blur(2px);
    }
    .queue-card {
      position: relative;
      width: min(620px, 100%);
      max-height: min(760px, calc(100vh - 40px));
      border: 1px solid rgba(148, 163, 184, 0.28);
      background: rgba(10, 18, 35, 0.92);
      border-radius: 16px;
      box-shadow: 0 18px 60px rgba(0, 0, 0, 0.55);
      padding: 14px 14px 12px 14px;
      backdrop-filter: blur(4px);
      display: flex;
      flex-direction: column;
      box-sizing: border-box;
    }
    .queue-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 8px;
    }
    .queue-title {
      font-size: 13px;
      font-weight: 900;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: #eef5ff;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .queue-close {
      width: 26px;
      height: 26px;
      border: 1px solid #2d4065;
      border-radius: 10px;
      background: rgba(16, 26, 52, 0.92);
      color: #cfe0ff;
      font-weight: 900;
      cursor: pointer;
      line-height: 1;
      flex: 0 0 auto;
    }
    .queue-close:hover {
      background: rgba(20, 35, 69, 0.98);
    }
    .queue-body {
      flex: 1;
      min-height: 0;
      overflow: auto;
      padding: 10px 12px 12px 12px;
      box-sizing: border-box;
    }
    .queue-summary {
      color: #b5c5e4;
      font-size: 12px;
      margin-bottom: 10px;
    }
    .queue-status {
      margin-bottom: 10px;
      border: 1px solid rgba(148, 163, 184, 0.28);
      background: rgba(10, 18, 35, 0.55);
      color: #c9d7f3;
      border-radius: 10px;
      padding: 8px 10px;
      font-size: 12px;
      line-height: 1.35;
      display: none;
      white-space: normal;
      overflow-wrap: anywhere;
    }
    .queue-status.visible {
      display: block;
    }
    .queue-status.error {
      border-color: rgba(255, 85, 125, 0.42);
      background: rgba(70, 15, 25, 0.55);
      color: #ffd0dc;
    }
    .queue-list {
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .queue-item {
      border: 1px solid #2c4064;
      border-radius: 10px;
      background: rgba(13, 22, 44, 0.55);
      padding: 8px 10px;
    }
    .queue-item-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 6px;
    }
    .queue-item-title {
      color: #eef5ff;
      font-size: 12px;
      font-weight: 700;
      line-height: 1.3;
      white-space: normal;
      overflow-wrap: anywhere;
    }
    .queue-item-meta {
      color: #9fb0d1;
      font-size: 11px;
      line-height: 1.35;
      white-space: normal;
      overflow-wrap: anywhere;
    }
    .queue-item-actions {
      display: flex;
      align-items: center;
      gap: 8px;
      justify-content: space-between;
      margin-top: 8px;
    }
    .queue-status-pill {
      border-radius: 999px;
      border: 1px solid #344c75;
      background: #122145;
      color: #d9e4fb;
      font-size: 10px;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      padding: 3px 8px;
      font-weight: 700;
    }
    .queue-status-pill.running {
      border-color: #7f6b2f;
      background: rgba(114, 92, 28, 0.35);
      color: #ffe2a8;
    }
    .queue-status-pill.queued {
      border-color: #2c5b95;
      background: rgba(24, 49, 84, 0.45);
      color: #cae2ff;
    }
    .queue-remove-btn {
      border: 1px solid #2d4065;
      border-radius: 8px;
      background: #122145;
      color: #d9e4fb;
      font-weight: 700;
      cursor: pointer;
      padding: 6px 9px;
      font-size: 11px;
      white-space: nowrap;
    }
    .queue-remove-btn:hover {
      background: #19305f;
    }
    .queue-remove-btn:disabled {
      opacity: 0.62;
      cursor: not-allowed;
    }
    .client-error {
      display: none;
      margin-top: 8px;
      border: 1px solid rgba(255, 85, 125, 0.42);
      background: rgba(70, 15, 25, 0.55);
      color: #ffd0dc;
      border-radius: 12px;
      padding: 8px 10px;
      font-size: 12px;
      line-height: 1.35;
      white-space: normal;
      overflow-wrap: anywhere;
    }
    .client-error.visible {
      display: block;
    }
    #nodeList {
      height: auto;
      flex: 1;
      min-height: 0;
      padding-top: 8px;
    }
    #traceList {
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
    .trace-actions {
      margin-top: 10px;
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }
    .trace-action-btn {
      border: 1px solid #2d4065;
      border-radius: 10px;
      background: #122145;
      color: #d9e4fb;
      font-weight: 700;
      cursor: pointer;
      padding: 7px 10px;
      font-size: 12px;
      white-space: nowrap;
    }
    .trace-action-btn:hover {
      background: #19305f;
    }
    .trace-action-btn:disabled {
      opacity: 0.62;
      cursor: not-allowed;
    }
    .trace-action-status {
      color: #b5c5e4;
      font-size: 11px;
      line-height: 1.35;
      white-space: normal;
      overflow-wrap: anywhere;
    }
    .trace-action-status.error {
      color: #ffd0dc;
    }
    .node-recent-traces {
      margin-top: 10px;
      border: 1px solid #2a3f64;
      border-radius: 9px;
      background: #0d1730;
      padding: 8px;
    }
    .node-recent-title {
      display: block;
      font-size: 11px;
      color: #9ab0d8;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-bottom: 6px;
    }
    .node-recent-empty {
      color: #b5c5e4;
      font-size: 12px;
      line-height: 1.35;
    }
    .node-recent-item {
      width: 100%;
      text-align: left;
      border: 1px solid #344c75;
      border-radius: 8px;
      background: #122145;
      color: #d9e4fb;
      cursor: pointer;
      padding: 7px 8px;
      margin-bottom: 6px;
      box-sizing: border-box;
    }
    .node-recent-item:last-child {
      margin-bottom: 0;
    }
    .node-recent-item:hover {
      background: #19305f;
    }
    .node-recent-main {
      display: block;
      font-size: 12px;
      color: #eef5ff;
      line-height: 1.3;
    }
    .node-recent-meta {
      display: block;
      font-size: 11px;
      color: #b5c5e4;
      line-height: 1.3;
      margin-top: 2px;
    }
    @media (max-width: 900px) {
      #sidebar {
        width: min(86vw, 390px);
        min-width: 250px;
      }
    }
  </style>
</head>
<body>
  <div id="map"></div>
  <section id="onboarding" class="onboarding hidden" aria-label="Connect to Meshtastic node">
    <div class="onboarding-card">
      <div class="onboarding-brand">Meshtracer</div>
      <div class="onboarding-tagline">
        Enter the IP address or hostname of your WiFi-connected Meshtastic node to start tracing and populating the map.
      </div>
      <div class="onboarding-form">
        <label class="onboarding-label" for="connectHost">Node IP or hostname</label>
        <div class="onboarding-row">
          <input id="connectHost" type="text" inputmode="decimal" placeholder="192.168.1.50" autocomplete="off" spellcheck="false">
          <button id="connectBtn" class="connect-btn" type="button">Connect</button>
        </div>
        <div id="connectError" class="connect-error"></div>
        <div class="onboarding-help">
          Examples:
          <code>192.168.1.50</code>
          <code>meshtastic.local</code>
        </div>
        <div id="connectStatus" class="connect-status"></div>
        <div id="discoverySection" class="discovery">
          <div class="discovery-head">
            <div class="discovery-title">Discovered Nodes</div>
            <button id="discoveryRescan" class="discovery-rescan" type="button">Rescan</button>
          </div>
          <div id="discoveryMeta" class="discovery-meta"></div>
          <div id="discoveryList" class="discovery-list"></div>
        </div>
      </div>
    </div>
  </section>
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
          <div class="head-actions">
            <div id="updated" class="head-updated">-</div>
            <button id="configOpen" class="icon-btn" type="button" aria-label="Open config">
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
                <circle cx="12" cy="12" r="3"></circle>
                <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06A1.65 1.65 0 0 0 4.6 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06A1.65 1.65 0 0 0 9 4.6a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"></path>
              </svg>
            </button>
          </div>
        </div>
        <div class="mesh-host-row">
          <div id="meshHost" class="mesh-host">-</div>
          <button id="disconnectBtn" class="disconnect-btn" type="button" style="display:none">Disconnect</button>
        </div>
        <div id="clientError" class="client-error"></div>
        <div class="legend">
          <span><span class="dot dot-fresh"></span><span id="legendFreshText">Fresh</span></span>
          <span><span class="dot dot-mid"></span><span id="legendMidText">Mid</span></span>
          <span><span class="dot dot-stale"></span><span id="legendStaleText">Stale</span></span>
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
        <section class="panel traces-panel" data-panel="traces">
          <div class="traces-toolbar">
            <button id="manageTraceQueueBtn" class="trace-action-btn trace-manage-btn" type="button">Manage traceroute queue</button>
          </div>
          <div id="traceList" class="scroll-list"></div>
        </section>
      </div>
    </div>
  </aside>

  <section id="configModal" class="config-modal hidden" aria-label="Config" role="dialog" aria-modal="true">
    <div id="configOverlay" class="config-overlay" aria-hidden="true"></div>
    <div class="config-card" role="document">
      <div class="config-head">
        <div class="config-title">Config</div>
        <button id="configClose" class="config-close" type="button" aria-label="Close config">X</button>
      </div>
      <div class="config-body">
        <div class="cfg-section">
          <div class="cfg-title">Traceroute</div>
          <div class="cfg-grid">
            <div class="cfg-field">
              <div class="cfg-label-row">
                <label class="cfg-label" for="cfgTracerouteBehavior">Traceroute Behaviour</label>
                <button type="button" class="cfg-help" data-help="traceroute_behavior" aria-label="Help for traceroute behaviour">?</button>
              </div>
              <select id="cfgTracerouteBehavior" class="cfg-input">
                <option value="automatic">Automatic</option>
                <option value="manual">Manual</option>
              </select>
            </div>
            <div class="cfg-field">
              <div class="cfg-label-row">
                <label class="cfg-label" for="cfgInterval">Interval / Timeout Basis</label>
                <button type="button" class="cfg-help" data-help="interval" aria-label="Help for interval">?</button>
              </div>
              <select id="cfgInterval" class="cfg-input">
                <option value="5">5 minutes</option>
                <option value="0.5">30 seconds</option>
                <option value="1">1 minute</option>
                <option value="2">2 minutes</option>
                <option value="10">10 minutes</option>
                <option value="15">15 minutes</option>
                <option value="30">30 minutes</option>
              </select>
            </div>
            <div class="cfg-field">
              <div class="cfg-label-row">
                <label class="cfg-label" for="cfgHeardWindow">Heard Window (minutes)</label>
                <button type="button" class="cfg-help" data-help="heard_window" aria-label="Help for heard window">?</button>
              </div>
              <input id="cfgHeardWindow" class="cfg-input" type="number" min="1" step="1">
            </div>
            <div class="cfg-field">
              <div class="cfg-label-row">
                <label class="cfg-label" for="cfgHopLimit">Hop Limit</label>
                <button type="button" class="cfg-help" data-help="hop_limit" aria-label="Help for hop limit">?</button>
              </div>
              <input id="cfgHopLimit" class="cfg-input" type="number" min="1" step="1">
            </div>
          </div>
        </div>
        <div class="cfg-section">
          <div class="cfg-title">UI</div>
          <div class="cfg-grid">
            <div class="cfg-field">
              <div class="cfg-label-row">
                <label class="cfg-label" for="cfgFreshWindow">Fresh Window (minutes)</label>
                <button type="button" class="cfg-help" data-help="fresh_window" aria-label="Help for fresh window">?</button>
              </div>
              <input id="cfgFreshWindow" class="cfg-input" type="number" min="1" step="1">
            </div>
            <div class="cfg-field">
              <div class="cfg-label-row">
                <label class="cfg-label" for="cfgMidWindow">Mid Window (minutes)</label>
                <button type="button" class="cfg-help" data-help="mid_window" aria-label="Help for mid window">?</button>
              </div>
              <input id="cfgMidWindow" class="cfg-input" type="number" min="1" step="1">
            </div>
          </div>
        </div>
        <div class="cfg-section">
          <div class="cfg-title">Storage</div>
          <div class="cfg-grid">
            <div class="cfg-field">
              <div class="cfg-label-row">
                <label class="cfg-label" for="cfgMaxMapTraces">Max Map Traces</label>
                <button type="button" class="cfg-help" data-help="max_map_traces" aria-label="Help for max map traces">?</button>
              </div>
              <input id="cfgMaxMapTraces" class="cfg-input" type="number" min="1" step="1">
            </div>
            <div class="cfg-field">
              <div class="cfg-label-row">
                <label class="cfg-label" for="cfgMaxStoredTraces">Max Stored Traces (0 disables pruning)</label>
                <button type="button" class="cfg-help" data-help="max_stored_traces" aria-label="Help for max stored traces">?</button>
              </div>
              <input id="cfgMaxStoredTraces" class="cfg-input" type="number" min="0" step="1">
            </div>
          </div>
        </div>
        <div class="cfg-section">
          <div class="cfg-title">Webhook</div>
          <div class="cfg-grid">
            <div class="cfg-field">
              <div class="cfg-label-row">
                <label class="cfg-label" for="cfgWebhookUrl">Webhook URL</label>
                <button type="button" class="cfg-help" data-help="webhook_url" aria-label="Help for webhook url">?</button>
              </div>
              <input id="cfgWebhookUrl" class="cfg-input" type="text" placeholder="https://example.com/hook">
            </div>
            <div class="cfg-field">
              <div class="cfg-label-row">
                <label class="cfg-label" for="cfgWebhookToken">Webhook API Token</label>
                <button type="button" class="cfg-help" data-help="webhook_api_token" aria-label="Help for webhook api token">?</button>
              </div>
              <input id="cfgWebhookToken" class="cfg-input" type="password" placeholder="(optional)">
            </div>
          </div>
        </div>

        <div class="cfg-actions">
          <button id="cfgApply" class="cfg-btn" type="button">Apply</button>
          <button id="cfgReset" class="cfg-btn secondary" type="button">Reset</button>
        </div>
        <div id="cfgStatus" class="cfg-status"></div>
        <div class="cfg-readonly">
          <div>Server settings are controlled by CLI args and require restart.</div>
          <div>DB: <code id="cfgDbPath">-</code></div>
          <div>UI bind: <code id="cfgUiBind">-</code></div>
        </div>
      </div>
    </div>
  </section>

  <section id="helpModal" class="help-modal hidden" aria-label="Help" role="dialog" aria-modal="true">
    <div id="helpOverlay" class="help-overlay" aria-hidden="true"></div>
    <div class="help-card" role="document">
      <div class="help-head">
        <div id="helpTitle" class="help-title">Help</div>
        <button id="helpClose" class="help-close" type="button" aria-label="Close help">X</button>
      </div>
      <div id="helpBody" class="help-body"></div>
    </div>
  </section>

  <section id="queueModal" class="queue-modal hidden" aria-label="Traceroute Queue" role="dialog" aria-modal="true">
    <div id="queueOverlay" class="queue-overlay" aria-hidden="true"></div>
    <div class="queue-card" role="document">
      <div class="queue-head">
        <div class="queue-title">Traceroute Queue</div>
        <button id="queueClose" class="queue-close" type="button" aria-label="Close traceroute queue">X</button>
      </div>
      <div class="queue-body">
        <div id="queueSummary" class="queue-summary">Queue is empty.</div>
        <div id="queueStatus" class="queue-status"></div>
        <div id="queueList" class="queue-list"></div>
      </div>
    </div>
  </section>

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
    const manageTraceQueueBtn = document.getElementById("manageTraceQueueBtn");
    const nodeSearchInput = document.getElementById("nodeSearch");
    const nodeSortSelect = document.getElementById("nodeSort");
    const legendFreshText = document.getElementById("legendFreshText");
    const legendMidText = document.getElementById("legendMidText");
    const legendStaleText = document.getElementById("legendStaleText");
    const tabButtons = Array.from(document.querySelectorAll(".tab-btn"));
    const tabPanels = Array.from(document.querySelectorAll(".panel"));
    const onboarding = document.getElementById("onboarding");
    const connectHostInput = document.getElementById("connectHost");
    const connectBtn = document.getElementById("connectBtn");
    const disconnectBtn = document.getElementById("disconnectBtn");
    const connectError = document.getElementById("connectError");
    const connectStatus = document.getElementById("connectStatus");
    const discoverySection = document.getElementById("discoverySection");
    const discoveryRescan = document.getElementById("discoveryRescan");
    const discoveryMeta = document.getElementById("discoveryMeta");
    const discoveryList = document.getElementById("discoveryList");
    const cfgTracerouteBehavior = document.getElementById("cfgTracerouteBehavior");
    const cfgInterval = document.getElementById("cfgInterval");
    const cfgHeardWindow = document.getElementById("cfgHeardWindow");
    const cfgFreshWindow = document.getElementById("cfgFreshWindow");
    const cfgMidWindow = document.getElementById("cfgMidWindow");
    const cfgHopLimit = document.getElementById("cfgHopLimit");
    const cfgMaxMapTraces = document.getElementById("cfgMaxMapTraces");
    const cfgMaxStoredTraces = document.getElementById("cfgMaxStoredTraces");
    const cfgWebhookUrl = document.getElementById("cfgWebhookUrl");
    const cfgWebhookToken = document.getElementById("cfgWebhookToken");
    const cfgApply = document.getElementById("cfgApply");
    const cfgReset = document.getElementById("cfgReset");
    const cfgStatus = document.getElementById("cfgStatus");
    const cfgDbPath = document.getElementById("cfgDbPath");
    const cfgUiBind = document.getElementById("cfgUiBind");
    const configModal = document.getElementById("configModal");
    const configOverlay = document.getElementById("configOverlay");
    const configClose = document.getElementById("configClose");
    const configOpen = document.getElementById("configOpen");
    const helpModal = document.getElementById("helpModal");
    const helpOverlay = document.getElementById("helpOverlay");
    const helpClose = document.getElementById("helpClose");
    const helpTitle = document.getElementById("helpTitle");
    const helpBody = document.getElementById("helpBody");
    const queueModal = document.getElementById("queueModal");
    const queueOverlay = document.getElementById("queueOverlay");
    const queueClose = document.getElementById("queueClose");
    const queueSummary = document.getElementById("queueSummary");
    const queueStatus = document.getElementById("queueStatus");
    const queueList = document.getElementById("queueList");
    const clientError = document.getElementById("clientError");

    const state = {
      fitted: false,
      lastServerData: null,
      lastData: null,
      lastSnapshotRevision: 0,
      lastMapRevision: 0,
      lastMapStyleSignature: "",
      lastLogRevision: 0,
      refreshInFlight: false,
      sseConnected: false,
      lastSseEventAtMs: 0,
      sseSource: null,
      markerByNum: new Map(),
      edgePolylinesByTrace: new Map(),
      nodeByNum: new Map(),
      traceById: new Map(),
      selectedNodeNum: null,
      selectedTraceId: null,
      lastDrawSelectedTraceId: null,
      activeTab: "log",
      nodeSearchQuery: "",
      nodeSortMode: "last_heard",
      promptedConnect: false,
      configLoaded: false,
      configDirty: false,
      configDefaults: null,
      configTokenSet: false,
      configTokenTouched: false,
      queueRemoveBusyIds: new Set(),
    };
    const FALLBACK_POLL_MS_CONNECTED = 30000;
    const FALLBACK_POLL_MS_DISCONNECTED = 3000;

    function reportClientError(message, options = {}) {
      const text = String(message || "").trim();
      if (!text) return;
      const prefix = options.prefix ? String(options.prefix) : "UI error";
      const line = `${prefix}: ${text}`;
      try {
        console.error(line);
      } catch (_e) {
      }
      if (!clientError) return;
      clientError.textContent = line;
      clientError.classList.add("visible");
    }

    window.addEventListener("error", (event) => {
      const msg = event && (event.message || (event.error && event.error.message)) ? String(event.message || event.error.message) : "unknown error";
      reportClientError(msg, { prefix: "UI error" });
    });
    window.addEventListener("unhandledrejection", (event) => {
      const reason = event && event.reason ? event.reason : null;
      const msg = reason && reason.message ? String(reason.message) : String(reason || "unhandled promise rejection");
      reportClientError(msg, { prefix: "UI error" });
    });
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

    function clampFreshnessThresholds(config) {
      const cfg = config && typeof config === "object" ? config : {};
      const defaults = state.configDefaults && typeof state.configDefaults === "object"
        ? state.configDefaults
        : { fresh_window: 120, mid_window: 480 };

      function asInt(value, fallback) {
        const n = Number(value);
        if (!Number.isFinite(n)) return fallback;
        return Math.trunc(n);
      }

      let fresh = asInt(cfg.fresh_window ?? defaults.fresh_window, 120);
      let mid = asInt(cfg.mid_window ?? defaults.mid_window, 480);
      fresh = Math.max(1, fresh);
      mid = Math.max(fresh, mid);
      return { fresh, mid };
    }

    function formatMinutesCompact(minutes) {
      const total = Math.max(0, Math.trunc(Number(minutes || 0)));
      const hours = Math.floor(total / 60);
      const mins = total % 60;
      if (hours <= 0) return `${total}m`;
      if (mins <= 0) return `${hours}h`;
      return `${hours}h${mins}m`;
    }

    function updateFreshnessLegend(config) {
      if (!legendFreshText || !legendMidText || !legendStaleText) return;
      const { fresh, mid } = clampFreshnessThresholds(config);
      const freshText = formatMinutesCompact(fresh);
      const midText = formatMinutesCompact(mid);
      legendFreshText.textContent = `Fresh (<= ${freshText})`;
      legendMidText.textContent = mid > fresh ? `Mid (${freshText}-${midText})` : `Mid (<= ${midText})`;
      legendStaleText.textContent = `Stale (> ${midText})`;
    }

    function nodeClass(node, nowSec, config) {
      const heard = Number(node.last_heard || 0);
      if (!heard) return "node-unknown";
      const ageMinutes = (nowSec - heard) / 60;
      if (!Number.isFinite(ageMinutes)) return "node-unknown";
      const { fresh, mid } = clampFreshnessThresholds(config);
      if (ageMinutes <= fresh) return "node-fresh";
      if (ageMinutes <= mid) return "node-mid";
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

    function numericRevision(value, fallback = 0) {
      const parsed = Number(value);
      if (!Number.isFinite(parsed) || parsed < 0) return Math.max(0, Number(fallback) || 0);
      return Math.trunc(parsed);
    }

    function snapshotRevisionOf(data) {
      return numericRevision(data && data.snapshot_revision, state.lastSnapshotRevision);
    }

    function mapRevisionOf(data) {
      return numericRevision(data && data.map_revision, state.lastMapRevision);
    }

    function logRevisionOf(data) {
      const direct = numericRevision(data && data.log_revision, -1);
      if (direct >= 0) return direct;
      const logs = Array.isArray(data && data.logs) ? data.logs : [];
      let maxSeq = 0;
      for (const entry of logs) {
        const seq = numericRevision(entry && entry.seq, 0);
        if (seq > maxSeq) maxSeq = seq;
      }
      return maxSeq;
    }

    function mapStyleSignatureOf(data) {
      const config = data && typeof data.config === "object" ? data.config : {};
      const fresh = numericRevision(config.fresh_window, 0);
      const mid = numericRevision(config.mid_window, 0);
      return `${fresh}:${mid}`;
    }

    function tracerouteControlFromData(data) {
      if (!data || typeof data !== "object") return {};
      const control = data.traceroute_control;
      if (!control || typeof control !== "object") return {};
      return control;
    }

    function tracerouteQueueEntriesFromData(data) {
      const control = tracerouteControlFromData(data);
      const rawEntries = Array.isArray(control.queue_entries) ? control.queue_entries : [];
      const entries = [];
      for (const rawEntry of rawEntries) {
        if (!rawEntry || typeof rawEntry !== "object") continue;
        const queueId = Number(rawEntry.queue_id);
        const nodeNum = Number(rawEntry.node_num);
        if (!Number.isFinite(queueId) || queueId <= 0) continue;
        if (!Number.isFinite(nodeNum)) continue;
        const statusRaw = String(rawEntry.status || "").trim().toLowerCase();
        const status = statusRaw === "running" ? "running" : "queued";
        entries.push({
          queue_id: Math.trunc(queueId),
          node_num: Math.trunc(nodeNum),
          status,
          created_at_utc: String(rawEntry.created_at_utc || ""),
          updated_at_utc: String(rawEntry.updated_at_utc || ""),
        });
      }
      return entries;
    }

    function updateQueueManageButton(data) {
      if (!manageTraceQueueBtn) return;
      const count = tracerouteQueueEntriesFromData(data).length;
      if (count > 0) {
        manageTraceQueueBtn.textContent = `Manage traceroute queue (${count})`;
      } else {
        manageTraceQueueBtn.textContent = "Manage traceroute queue";
      }
    }

    function queueNodeLabel(nodeNum) {
      const nodeNumInt = Math.trunc(Number(nodeNum));
      const fallback = `Node #${nodeNumInt}`;
      const node = state.nodeByNum.get(nodeNumInt);
      if (!node) return fallback;
      const short = nodeLabel(node);
      const longName = node.long_name && String(node.long_name).trim()
        ? String(node.long_name).trim()
        : "";
      if (!longName || longName.toLowerCase() === short.toLowerCase()) {
        return `${short} (#${nodeNumInt})`;
      }
      return `${short} / ${longName} (#${nodeNumInt})`;
    }

    function queueStatusText(status) {
      return status === "running" ? "Running" : "Queued";
    }

    function setQueueStatus(message, options = {}) {
      if (!queueStatus) return;
      const text = String(message || "").trim();
      const isError = Boolean(options.error);
      queueStatus.textContent = text;
      queueStatus.classList.toggle("visible", Boolean(text));
      queueStatus.classList.toggle("error", isError);
    }

    function renderQueueModal(data = state.lastServerData) {
      if (!queueSummary || !queueList) return;
      const entries = tracerouteQueueEntriesFromData(data);
      const queuedCount = entries.filter((entry) => entry.status === "queued").length;
      const runningCount = entries.length - queuedCount;
      if (!entries.length) {
        queueSummary.textContent = "Queue is empty.";
        queueList.innerHTML = '<div class="empty">No traceroutes are queued.</div>';
        return;
      }
      queueSummary.textContent = `${entries.length} total (${runningCount} running, ${queuedCount} queued)`;
      queueList.innerHTML = entries.map((entry) => {
        const queueId = Number(entry.queue_id);
        const status = entry.status === "running" ? "running" : "queued";
        const busy = state.queueRemoveBusyIds.has(queueId);
        const removeDisabled = status === "running" || busy;
        const removeText = status === "running"
          ? "Running"
          : busy
            ? "Removing..."
            : "Remove";
        return `
          <div class="queue-item">
            <div class="queue-item-head">
              <div class="queue-item-title">#${escapeHtml(queueId)} ${escapeHtml(queueNodeLabel(entry.node_num))}</div>
            </div>
            <div class="queue-item-meta">Queued at: ${escapeHtml(entry.created_at_utc || "-")}</div>
            <div class="queue-item-meta">Updated at: ${escapeHtml(entry.updated_at_utc || "-")}</div>
            <div class="queue-item-actions">
              <span class="queue-status-pill ${status}">${escapeHtml(queueStatusText(status))}</span>
              <button class="queue-remove-btn" type="button" data-queue-id="${escapeHtml(queueId)}" ${removeDisabled ? "disabled" : ""}>${escapeHtml(removeText)}</button>
            </div>
          </div>
        `;
      }).join("");

      for (const btn of queueList.querySelectorAll("button[data-queue-id]")) {
        btn.addEventListener("click", () => {
          const queueId = Number(btn.dataset.queueId);
          if (!Number.isFinite(queueId)) return;
          removeQueueEntry(Math.trunc(queueId));
        });
      }
    }

    let lastQueueFocus = null;
    function openQueueModal() {
      if (!queueModal) return;
      lastQueueFocus = document.activeElement;
      setQueueStatus("", { error: false });
      renderQueueModal(state.lastServerData);
      queueModal.classList.remove("hidden");
      try {
        if (queueClose) queueClose.focus();
      } catch (_e) {
      }
    }

    function closeQueueModal() {
      if (!queueModal) return;
      queueModal.classList.add("hidden");
      try {
        if (manageTraceQueueBtn) {
          manageTraceQueueBtn.focus();
        } else if (lastQueueFocus && typeof lastQueueFocus.focus === "function") {
          lastQueueFocus.focus();
        }
      } catch (_e) {
      }
      lastQueueFocus = null;
    }

    async function removeQueueEntry(queueId) {
      const queueIdInt = Math.trunc(Number(queueId));
      if (!Number.isFinite(queueIdInt) || queueIdInt <= 0) return;
      if (state.queueRemoveBusyIds.has(queueIdInt)) return;
      state.queueRemoveBusyIds.add(queueIdInt);
      renderQueueModal(state.lastServerData);
      setQueueStatus(`Removing queue entry #${queueIdInt}...`, { error: false });

      try {
        const { ok, body } = await apiPost("/api/traceroute/queue/remove", { queue_id: queueIdInt });
        if (!ok) {
          const detail = body && (body.detail || body.error)
            ? String(body.detail || body.error)
            : "failed to remove queue entry";
          setQueueStatus(detail, { error: true });
          return;
        }
        const detail = body && body.detail
          ? String(body.detail)
          : `removed queued traceroute #${queueIdInt}`;
        setQueueStatus(detail, { error: false });
        if (body && body.snapshot && typeof body.snapshot === "object") {
          applySnapshot(body.snapshot, { force: true });
        } else {
          await refresh({ force: true });
        }
      } catch (e) {
        setQueueStatus(String(e || "failed to remove queue entry"), { error: true });
      } finally {
        state.queueRemoveBusyIds.delete(queueIdInt);
        renderQueueModal(state.lastServerData);
      }
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

      // Build graph edges from all observed traceroute hop pairs.
      const adj = new Map(); // num -> Map(neighborNum -> { count, snr_sum, snr_count })
      function ensureAdj(num) {
        if (!adj.has(num)) adj.set(num, new Map());
      }
      function addEdge(aRaw, bRaw, snrDb) {
        const a = Number(aRaw);
        const b = Number(bRaw);
        if (!Number.isFinite(a) || !Number.isFinite(b) || a === b) return;
        if (!nodeMap.has(a) || !nodeMap.has(b)) return;
        ensureAdj(a);
        ensureAdj(b);
        const mA = adj.get(a);
        const mB = adj.get(b);
        const aMeta = mA.get(b) || { count: 0, snr_sum: 0, snr_count: 0 };
        aMeta.count += 1;
        if (Number.isFinite(snrDb)) {
          aMeta.snr_sum += snrDb;
          aMeta.snr_count += 1;
        }
        mA.set(b, aMeta);

        const bMeta = mB.get(a) || { count: 0, snr_sum: 0, snr_count: 0 };
        bMeta.count += 1;
        if (Number.isFinite(snrDb)) {
          bMeta.snr_sum += snrDb;
          bMeta.snr_count += 1;
        }
        mB.set(a, bMeta);
      }

      function edgeSnrFromRoute(routeNums, snrList, index) {
        if (!Array.isArray(snrList)) return NaN;
        if (snrList.length === routeNums.length) {
          const raw = index + 1 < snrList.length ? snrList[index + 1] : snrList[index];
          const value = Number(raw);
          return Number.isFinite(value) ? value : NaN;
        }
        if (snrList.length === routeNums.length - 1) {
          const value = Number(snrList[index]);
          return Number.isFinite(value) ? value : NaN;
        }
        return NaN;
      }

      for (const trace of Array.isArray(traces) ? traces : []) {
        for (const key of ["towards_nums", "back_nums"]) {
          const route = Array.isArray(trace?.[key]) ? trace[key] : [];
          const snrKey = key === "towards_nums" ? "towards_snr_db" : "back_snr_db";
          const snrList = Array.isArray(trace?.[snrKey]) ? trace[snrKey] : null;
          for (const rawNum of route) {
            ensureTraceNode(rawNum);
          }
          const nums = route.map((value) => Number(value));
          if (nums.length < 2) continue;
          for (let i = 0; i < nums.length - 1; i += 1) {
            addEdge(nums[i], nums[i + 1], edgeSnrFromRoute(nums, snrList, i));
          }
        }
      }

      const anchors = [];
      for (const node of nodeMap.values()) {
        if (hasCoord(node)) anchors.push(node);
      }
      if (!anchors.length) return nodeMap;

      function median(values) {
        const items = Array.isArray(values) ? values.filter((v) => Number.isFinite(v)) : [];
        if (!items.length) return NaN;
        items.sort((a, b) => a - b);
        const mid = Math.floor(items.length / 2);
        if (items.length % 2) return items[mid];
        return (items[mid - 1] + items[mid]) / 2;
      }

      function haversineMeters(lat1, lon1, lat2, lon2) {
        const R = 6371000;
        const rad = Math.PI / 180;
        const phi1 = Number(lat1) * rad;
        const phi2 = Number(lat2) * rad;
        const dPhi = (Number(lat2) - Number(lat1)) * rad;
        const dLam = (Number(lon2) - Number(lon1)) * rad;
        const s1 = Math.sin(dPhi / 2);
        const s2 = Math.sin(dLam / 2);
        const a = s1 * s1 + Math.cos(phi1) * Math.cos(phi2) * s2 * s2;
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(Math.max(0, 1 - a)));
        return R * c;
      }

      // Local equirectangular projection around the mean anchor position.
      let lat0 = 0;
      let lon0 = 0;
      for (const a of anchors) {
        lat0 += Number(a.lat);
        lon0 += Number(a.lon);
      }
      lat0 /= anchors.length;
      lon0 /= anchors.length;
      const mLat = 111111;
      const mLon = metersPerLonDegree(lat0);
      function llToXY(lat, lon) {
        return {
          x: (Number(lon) - lon0) * mLon,
          y: (Number(lat) - lat0) * mLat,
        };
      }
      function xyToLL(x, y) {
        return {
          lat: lat0 + Number(y) / mLat,
          lon: lon0 + Number(x) / mLon,
        };
      }

      const anchorPos = new Map(); // num -> {x,y}
      for (const a of anchors) {
        anchorPos.set(Number(a.num), llToXY(a.lat, a.lon));
      }

      function edgeMeanSnrDb(meta) {
        if (!meta || !meta.snr_count) return NaN;
        return meta.snr_sum / meta.snr_count;
      }

      function snrQualityFromDb(snrDb) {
        const value = Number(snrDb);
        if (!Number.isFinite(value)) return null;
        // Typical LoRa SNR values fall roughly between [-20, +12].
        const clamped = Math.max(-20, Math.min(12, value));
        return (clamped + 20) / 32;
      }

      function edgeCostUnits(meta) {
        const q = snrQualityFromDb(edgeMeanSnrDb(meta));
        if (q === null) return 1;
        // High SNR => shorter, low SNR => longer (bounded multiplier).
        const mult = 0.85 + (1 - q) * 1.25;
        return Math.max(0.7, Math.min(2.4, mult));
      }

      function edgeSpringWeight(meta) {
        const count = meta && Number.isFinite(meta.count) ? Number(meta.count) : 1;
        const q = snrQualityFromDb(edgeMeanSnrDb(meta));
        const snrFactor = q === null ? 0.85 : 0.55 + 0.75 * q;
        return Math.min(3.2, Math.sqrt(Math.max(1, count)) * snrFactor);
      }

      function dijkstra(startNum, maxCost) {
        const start = Number(startNum);
        const dist = new Map();
        if (!Number.isFinite(start) || !nodeMap.has(start)) return dist;
        dist.set(start, 0);

        const heap = []; // [cost, num]
        function heapPush(item) {
          heap.push(item);
          let i = heap.length - 1;
          while (i > 0) {
            const p = (i - 1) >> 1;
            if (heap[p][0] <= item[0]) break;
            heap[i] = heap[p];
            i = p;
          }
          heap[i] = item;
        }
        function heapPop() {
          const top = heap[0];
          const last = heap.pop();
          if (heap.length) {
            let i = 0;
            while (true) {
              const left = i * 2 + 1;
              const right = left + 1;
              if (left >= heap.length) break;
              let child = left;
              if (right < heap.length && heap[right][0] < heap[left][0]) child = right;
              if (heap[child][0] >= last[0]) break;
              heap[i] = heap[child];
              i = child;
            }
            heap[i] = last;
          }
          return top;
        }

        heapPush([0, start]);
        while (heap.length) {
          const [cost, cur] = heapPop();
          const best = dist.get(cur);
          if (best === undefined || cost > best + 1e-9) continue;
          if (cost > maxCost) continue;
          const neighbors = adj.get(cur);
          if (!neighbors) continue;
          for (const [nxt, meta] of neighbors.entries()) {
            const step = edgeCostUnits(meta);
            const nextCost = cost + step;
            if (!Number.isFinite(nextCost) || nextCost > maxCost) continue;
            const prev = dist.get(nxt);
            if (prev === undefined || nextCost < prev - 1e-9) {
              dist.set(nxt, nextCost);
              heapPush([nextCost, nxt]);
            }
          }
        }
        return dist;
      }

      function bfs(startNum, maxDepth) {
        const start = Number(startNum);
        const dist = new Map();
        if (!Number.isFinite(start) || !nodeMap.has(start)) return dist;
        dist.set(start, 0);
        const queue = [start];
        for (let qi = 0; qi < queue.length; qi += 1) {
          const cur = queue[qi];
          const curDist = dist.get(cur) || 0;
          if (curDist >= maxDepth) continue;
          const neighbors = adj.get(cur);
          if (!neighbors) continue;
          for (const nxt of neighbors.keys()) {
            if (dist.has(nxt)) continue;
            dist.set(nxt, curDist + 1);
            queue.push(nxt);
          }
        }
        return dist;
      }

      const MAX_BFS_HOPS = 25;
      const MAX_DIJKSTRA_COST = MAX_BFS_HOPS * 2.6;
      const hopByAnchor = new Map(); // anchorNum -> Map(nodeNum -> hops)
      const costByAnchor = new Map(); // anchorNum -> Map(nodeNum -> costUnits)
      for (const a of anchors) {
        const aNum = Number(a.num);
        hopByAnchor.set(aNum, bfs(aNum, MAX_BFS_HOPS));
        costByAnchor.set(aNum, dijkstra(aNum, MAX_DIJKSTRA_COST));
      }

      // Calibrate meters-per-unit from anchor pairs. Units are the sum of
      // edgeCostUnits(...) along the best path.
      const globalRatios = [];
      const perAnchorRatios = new Map(); // anchorNum -> [metersPerUnit]
      const MAX_CALIB_HOPS = 12;
      const MIN_UNIT_METERS = 10;
      const MAX_UNIT_METERS = 20000;
      for (let i = 0; i < anchors.length; i += 1) {
        const a = anchors[i];
        const hopsA = hopByAnchor.get(Number(a.num));
        const costsA = costByAnchor.get(Number(a.num));
        if (!hopsA || !costsA) continue;
        for (let j = i + 1; j < anchors.length; j += 1) {
          const b = anchors[j];
          const hop = hopsA.get(Number(b.num));
          if (!hop || hop <= 0 || hop > MAX_CALIB_HOPS) continue;
          const costUnits = costsA.get(Number(b.num));
          if (!Number.isFinite(costUnits) || costUnits <= 0) continue;
          const meters = haversineMeters(a.lat, a.lon, b.lat, b.lon);
          if (!Number.isFinite(meters) || meters <= 0) continue;
          const ratio = meters / costUnits;
          if (!Number.isFinite(ratio) || ratio < MIN_UNIT_METERS || ratio > MAX_UNIT_METERS) continue;
          globalRatios.push(ratio);
          if (!perAnchorRatios.has(Number(a.num))) perAnchorRatios.set(Number(a.num), []);
          if (!perAnchorRatios.has(Number(b.num))) perAnchorRatios.set(Number(b.num), []);
          perAnchorRatios.get(Number(a.num)).push(ratio);
          perAnchorRatios.get(Number(b.num)).push(ratio);
        }
      }
      let globalMetersPerUnit = median(globalRatios);
      if (!Number.isFinite(globalMetersPerUnit) || globalMetersPerUnit <= 0) {
        globalMetersPerUnit = 400; // safe default when we can't calibrate from anchors.
      }

      const perAnchorMetersPerUnit = new Map();
      for (const a of anchors) {
        const ratios = perAnchorRatios.get(Number(a.num)) || [];
        const med = median(ratios);
        if (ratios.length >= 3 && Number.isFinite(med) && med > 0) {
          // Blend with global so tiny anchor sets don't overfit.
          perAnchorMetersPerUnit.set(Number(a.num), 0.7 * med + 0.3 * globalMetersPerUnit);
        }
      }

      function metersPerUnitForAnchor(anchorNum) {
        const local = perAnchorMetersPerUnit.get(Number(anchorNum));
        if (Number.isFinite(local) && local > 0) return local;
        return globalMetersPerUnit;
      }

      const constraintsByNode = new Map(); // num -> [{anchor_num,x,y,hop,cost,r,w}]
      const MAX_CONSTRAINT_HOPS = 12;
      const MAX_ANCHORS_PER_NODE = 8;
      for (const [num, node] of nodeMap.entries()) {
        if (hasCoord(node)) continue;
        const constraints = [];
        for (const a of anchors) {
          const hopsA = hopByAnchor.get(Number(a.num));
          const costsA = costByAnchor.get(Number(a.num));
          if (!hopsA || !costsA) continue;
          const hop = hopsA.get(Number(num));
          if (!hop || hop <= 0 || hop > MAX_CONSTRAINT_HOPS) continue;
          const costUnits = costsA.get(Number(num));
          if (!Number.isFinite(costUnits) || costUnits <= 0) continue;
          const p = anchorPos.get(Number(a.num));
          if (!p) continue;
          const unitMeters = metersPerUnitForAnchor(Number(a.num));
          const r = costUnits * unitMeters;
          constraints.push({
            anchor_num: Number(a.num),
            x: p.x,
            y: p.y,
            hop,
            cost: costUnits,
            r,
            w: 1 / (hop * hop),
          });
        }
        constraints.sort((c1, c2) => c1.hop - c2.hop || c1.anchor_num - c2.anchor_num);
        if (constraints.length > MAX_ANCHORS_PER_NODE) constraints.length = MAX_ANCHORS_PER_NODE;
        constraintsByNode.set(Number(num), constraints);
      }

      const posByNum = new Map(); // num -> {x,y}
      const fixedNums = new Set();
      for (const a of anchors) {
        const p = anchorPos.get(Number(a.num));
        if (!p) continue;
        posByNum.set(Number(a.num), { x: p.x, y: p.y });
        fixedNums.add(Number(a.num));
      }

      const strongNums = new Set();
      function solveMultilateration(constraints) {
        let x = 0;
        let y = 0;
        let wSum = 0;
        for (const c of constraints) {
          x += c.x * c.w;
          y += c.y * c.w;
          wSum += c.w;
        }
        if (wSum > 0) {
          x /= wSum;
          y /= wSum;
        }

        let lambda = 1;
        for (let iter = 0; iter < 22; iter += 1) {
          let A11 = 0;
          let A12 = 0;
          let A22 = 0;
          let b1 = 0;
          let b2 = 0;
          for (const c of constraints) {
            const dx = x - c.x;
            const dy = y - c.y;
            const d = Math.hypot(dx, dy) || 1e-6;
            const jx = dx / d;
            const jy = dy / d;
            const resid = d - c.r;
            const w = c.w;
            A11 += w * jx * jx;
            A12 += w * jx * jy;
            A22 += w * jy * jy;
            b1 += w * jx * resid;
            b2 += w * jy * resid;
          }

          A11 += lambda;
          A22 += lambda;
          const det = A11 * A22 - A12 * A12;
          if (!Number.isFinite(det) || Math.abs(det) < 1e-9) break;
          const dxStep = (-A22 * b1 + A12 * b2) / det;
          const dyStep = (A12 * b1 - A11 * b2) / det;
          if (!Number.isFinite(dxStep) || !Number.isFinite(dyStep)) break;

          const stepMag = Math.hypot(dxStep, dyStep);
          x += dxStep;
          y += dyStep;
          if (stepMag < 0.2) break;
        }
        return { x, y };
      }

      for (const [num, node] of nodeMap.entries()) {
        if (hasCoord(node)) continue;
        const constraints = constraintsByNode.get(Number(num)) || [];
        if (constraints.length < 3) continue;
        const solved = solveMultilateration(constraints);
        posByNum.set(Number(num), solved);
        strongNums.add(Number(num));
      }

      function neighborHint(num) {
        const neighbors = adj.get(Number(num));
        if (!neighbors) return null;
        let x = 0;
        let y = 0;
        let count = 0;
        for (const nxt of neighbors.keys()) {
          const p = posByNum.get(Number(nxt));
          if (!p) continue;
          x += p.x;
          y += p.y;
          count += 1;
        }
        if (!count) return null;
        return { x: x / count, y: y / count };
      }

      function circleIntersections(a, r1, b, r2) {
        const dx = b.x - a.x;
        const dy = b.y - a.y;
        const d = Math.hypot(dx, dy);
        if (!Number.isFinite(d) || d < 1e-6) return [];
        if (d > r1 + r2) return [];
        if (d < Math.abs(r1 - r2)) return [];
        const t = (r1 * r1 - r2 * r2 + d * d) / (2 * d);
        const h2 = Math.max(0, r1 * r1 - t * t);
        const h = Math.sqrt(h2);
        const ux = dx / d;
        const uy = dy / d;
        const px = a.x + ux * t;
        const py = a.y + uy * t;
        const rx = -uy * h;
        const ry = ux * h;
        return [
          { x: px + rx, y: py + ry },
          { x: px - rx, y: py - ry },
        ];
      }

      function placeWithTwoAnchors(num, c1, c2, hint) {
        const a = { x: c1.x, y: c1.y };
        const b = { x: c2.x, y: c2.y };
        const r1 = c1.r;
        const r2 = c2.r;
        const ints = circleIntersections(a, r1, b, r2);
        if (ints.length === 2) {
          if (hint) {
            const d0 = Math.hypot(ints[0].x - hint.x, ints[0].y - hint.y);
            const d1 = Math.hypot(ints[1].x - hint.x, ints[1].y - hint.y);
            return d0 <= d1 ? ints[0] : ints[1];
          }
          // Deterministic tie-break.
          const pick = (Math.abs(Number(num)) % 2) === 0 ? ints[0] : ints[1];
          return pick;
        }

        const dx = b.x - a.x;
        const dy = b.y - a.y;
        const d = Math.hypot(dx, dy) || 1e-6;
        const t = r1 + r2 > 1e-6 ? Math.max(0, Math.min(1, r1 / (r1 + r2))) : 0.5;
        let x = a.x + (dx / d) * d * t;
        let y = a.y + (dy / d) * d * t;

        // Nudge off the line so multiple nodes don't stack exactly.
        const angle = ((Math.abs(Number(num)) % 360) * Math.PI) / 180;
        const nudge = Math.min(0.22 * globalMetersPerUnit, 120);
        x += Math.cos(angle) * nudge;
        y += Math.sin(angle) * nudge;
        return { x, y };
      }

      function placeWithOneAnchor(num, c1, hint) {
        const base = { x: c1.x, y: c1.y };
        const r = c1.r;
        let angle = ((Math.abs(Number(num)) % 360) * Math.PI) / 180;
        if (hint) {
          angle = Math.atan2(hint.y - base.y, hint.x - base.x);
        }
        return {
          x: base.x + Math.cos(angle) * r,
          y: base.y + Math.sin(angle) * r,
        };
      }

      // Fill remaining nodes using whatever constraints exist, in a few passes so
      // neighbor hints become available.
      for (let pass = 0; pass < 5; pass += 1) {
        let progressed = false;
        for (const [num, node] of nodeMap.entries()) {
          if (hasCoord(node) || posByNum.has(Number(num))) continue;
          const constraints = constraintsByNode.get(Number(num)) || [];
          if (!constraints.length) continue;
          const hint = neighborHint(Number(num));

          if (constraints.length >= 3) {
            const solved = solveMultilateration(constraints);
            posByNum.set(Number(num), solved);
            strongNums.add(Number(num));
            progressed = true;
            continue;
          }
          if (constraints.length === 2) {
            const placed = placeWithTwoAnchors(Number(num), constraints[0], constraints[1], hint);
            posByNum.set(Number(num), placed);
            progressed = true;
            continue;
          }
          if (constraints.length === 1) {
            const placed = placeWithOneAnchor(Number(num), constraints[0], hint);
            posByNum.set(Number(num), placed);
            progressed = true;
            continue;
          }
        }
        if (!progressed) break;
      }

      function mobility(num) {
        if (fixedNums.has(Number(num))) return 0;
        if (strongNums.has(Number(num))) return 0.25;
        return 1;
      }

      // Small spring relaxation pass to reduce local edge distortion while keeping
      // well-anchored nodes mostly stable.
      const SPRING_ITERS = 28;
      const SPRING_ALPHA = 0.08;
      for (let iter = 0; iter < SPRING_ITERS; iter += 1) {
        for (const [uRaw, neighbors] of adj.entries()) {
          const u = Number(uRaw);
          for (const [vRaw, meta] of neighbors.entries()) {
            const v = Number(vRaw);
            if (u >= v) continue; // handle each undirected edge once
            const pu = posByNum.get(u);
            const pv = posByNum.get(v);
            if (!pu || !pv) continue;
            const dx = pv.x - pu.x;
            const dy = pv.y - pu.y;
            const dist = Math.hypot(dx, dy);
            if (!Number.isFinite(dist) || dist < 1e-6) continue;
            const desired = edgeCostUnits(meta) * globalMetersPerUnit;
            if (!Number.isFinite(desired) || desired <= 0) continue;
            const weight = edgeSpringWeight(meta);
            const err = dist - desired;
            const maxStep = 0.45 * desired;
            let step = SPRING_ALPHA * weight * err;
            step = Math.max(-maxStep, Math.min(maxStep, step));

            const ux = dx / dist;
            const uy = dy / dist;
            const mu = mobility(u);
            const mv = mobility(v);
            const total = mu + mv;
            if (total <= 0) continue;
            const du = (mu / total) * step;
            const dv = (mv / total) * step;
            if (mu > 0) {
              pu.x += ux * du;
              pu.y += uy * du;
            }
            if (mv > 0) {
              pv.x -= ux * dv;
              pv.y -= uy * dv;
            }
          }
        }
      }

      for (const [num, p] of posByNum.entries()) {
        const node = nodeMap.get(Number(num));
        if (!node) continue;
        if (hasCoord(node)) continue; // never overwrite GPS
        const ll = xyToLL(p.x, p.y);
        if (!Number.isFinite(ll.lat) || !Number.isFinite(ll.lon)) continue;
        node.lat = ll.lat;
        node.lon = ll.lon;
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

    function traceTouchesNode(trace, nodeNum) {
      const target = Number(nodeNum);
      if (!Number.isFinite(target)) return false;
      for (const key of ["towards_nums", "back_nums"]) {
        const route = Array.isArray(trace?.[key]) ? trace[key] : [];
        for (const rawNum of route) {
          if (Number(rawNum) === target) return true;
        }
      }
      for (const packetKey of ["from", "to"]) {
        const num = Number(trace?.packet?.[packetKey]?.num);
        if (Number.isFinite(num) && num === target) return true;
      }
      return false;
    }

    function recentTracesForNode(nodeNum, limit = 6) {
      const maxItems = Math.max(1, Number(limit) || 6);
      const traces = Array.isArray(state.lastData?.traces) ? state.lastData.traces : [];
      const matches = [];
      for (let i = traces.length - 1; i >= 0; i -= 1) {
        const trace = traces[i];
        if (!traceTouchesNode(trace, nodeNum)) continue;
        matches.push(trace);
        if (matches.length >= maxItems) break;
      }
      return matches;
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

    function redrawFromLastServerData(options = {}) {
      if (!state.lastServerData) return false;
      const forceMap = Boolean(options.forceMap);
      const traceFilterChanged = state.selectedTraceId !== state.lastDrawSelectedTraceId;
      if (forceMap || traceFilterChanged) {
        draw(state.lastServerData);
        return true;
      }
      applyNodeSelectionVisual();
      applyTraceSelectionVisual();
      if (state.lastData) {
        renderNodeList(state.lastData.nodes || []);
        renderTraceList(state.lastData.traces || []);
      }
      renderSelectionDetails();
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
        const canTraceNow = Boolean(state.lastServerData && state.lastServerData.connected);
        const selectedNodeNum = Number(node.num);
        const tracerouteControl = state.lastServerData && typeof state.lastServerData.traceroute_control === "object"
          ? state.lastServerData.traceroute_control
          : {};
        const runningNodeNumRaw = tracerouteControl.running_node_num;
        const runningNodeNum = runningNodeNumRaw === null || runningNodeNumRaw === undefined
          ? NaN
          : Number(runningNodeNumRaw);
        const queuedNodeNums = Array.isArray(tracerouteControl.queued_node_nums)
          ? tracerouteControl.queued_node_nums
              .filter((value) => value !== null && value !== undefined)
              .map((value) => Number(value))
              .filter((value) => Number.isFinite(value))
          : [];
        const queueIndex = Number.isFinite(selectedNodeNum) ? queuedNodeNums.indexOf(selectedNodeNum) : -1;
        const isRunningForNode = Number.isFinite(runningNodeNum) && runningNodeNum === selectedNodeNum;
        const isQueuedForNode = queueIndex >= 0;
        const isBusyForNode = isRunningForNode || isQueuedForNode;
        const traceDisabled = (!canTraceNow || isBusyForNode) ? "disabled" : "";
        const nodeRecentTraces = recentTracesForNode(selectedNodeNum, 8);
        const recentTraceSectionHtml = nodeRecentTraces.length
          ? nodeRecentTraces.map((trace) => {
              const traceId = Number(trace.trace_id);
              const originLabel = nodeFromRecord(trace?.packet?.to);
              const targetLabel = nodeFromRecord(trace?.packet?.from);
              const fwdHops = Math.max(0, (trace.towards_nums || []).length - 1);
              const backHops = Math.max(0, (trace.back_nums || []).length - 1);
              return `
                <button class="node-recent-item" type="button" data-recent-trace-id="${traceId}">
                  <span class="node-recent-main">#${escapeHtml(traceId)} ${escapeHtml(originLabel)} -> ${escapeHtml(targetLabel)}</span>
                  <span class="node-recent-meta">${escapeHtml(trace.captured_at_utc || "-")} | towards ${escapeHtml(fwdHops)} hops | back ${escapeHtml(backHops)} hops</span>
                </button>
              `;
            }).join("")
          : '<div class="node-recent-empty">No completed traceroutes for this node yet.</div>';
        let traceHint = "";
        if (!canTraceNow) {
          traceHint = "Connect to a node to run traceroute.";
        } else if (isRunningForNode) {
          traceHint = "Traceroute running for this node...";
        } else if (isQueuedForNode) {
          traceHint = queueIndex === 0
            ? "Traceroute queued for this node."
            : `Traceroute queued for this node (position ${queueIndex + 1}).`;
        }

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
          <div class="trace-actions">
            <button id="traceNowBtn" class="trace-action-btn" type="button" ${traceDisabled}>Run traceroute</button>
            <span id="traceNowStatus" class="trace-action-status">${escapeHtml(traceHint)}</span>
          </div>
          <div class="node-recent-traces">
            <span class="node-recent-title">Recent Traceroutes</span>
            ${recentTraceSectionHtml}
          </div>
        `;
        const traceNowBtn = document.getElementById("traceNowBtn");
        const traceNowStatus = document.getElementById("traceNowStatus");
        for (const btn of traceDetailsBody.querySelectorAll("button[data-recent-trace-id]")) {
          btn.addEventListener("click", () => {
            const traceId = Number(btn.dataset.recentTraceId);
            if (!Number.isFinite(traceId)) return;
            focusTrace(traceId);
          });
        }
        if (traceNowBtn) {
          traceNowBtn.addEventListener("click", async () => {
            const nodeNum = Number(node.num);
            if (!Number.isFinite(nodeNum)) return;
            traceNowBtn.disabled = true;
            let started = false;
            if (traceNowStatus) {
              traceNowStatus.textContent = "Starting traceroute...";
              traceNowStatus.classList.remove("error");
            }
            try {
              const { ok, body } = await apiPost("/api/traceroute", { node_num: nodeNum });
              if (!ok) {
                const detail = body && (body.detail || body.error)
                  ? String(body.detail || body.error)
                  : "failed to start traceroute";
                if (traceNowStatus) {
                  traceNowStatus.textContent = detail;
                  traceNowStatus.classList.add("error");
                }
                return;
              }
              started = true;
              const detail = body && body.detail ? String(body.detail) : "traceroute queued";
              if (traceNowStatus) {
                traceNowStatus.textContent = detail;
                traceNowStatus.classList.remove("error");
              }
            } catch (e) {
              const detail = String(e || "failed to start traceroute");
              if (traceNowStatus) {
                traceNowStatus.textContent = detail;
                traceNowStatus.classList.add("error");
              }
            } finally {
              traceNowBtn.disabled = started || !canTraceNow;
            }
            refresh();
          });
        }
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
            // Hide non-selected traceroute lines entirely while a trace is selected.
            line.setStyle({ color: baseColor, weight: 0, opacity: 0.0 });
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
        const cssClass = nodeClass(node, nowSec, data && data.config);
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

      for (const trace of data.traces || []) {
        const traceId = Number(trace.trace_id);
        if (!Number.isFinite(traceId)) continue;
        addTracePathSegments(traceId, trace.towards_nums || [], "towards");
        addTracePathSegments(traceId, trace.back_nums || [], "back");
      }

      const viewData = {
        ...data,
        nodes: displayNodes,
      };
      state.lastData = viewData;

      updateConnectionUi(data);
      updateConfigUi(data);
      updateFreshnessLegend(data && data.config);
      updateQueueManageButton(data);
      const nodeTab = tabButtons.find((btn) => (btn.dataset.tab || "") === "nodes");
      if (nodeTab) nodeTab.textContent = `Nodes (${String(displayNodes.length)})`;
      const traceTab = tabButtons.find((btn) => (btn.dataset.tab || "") === "traces");
      if (traceTab) traceTab.textContent = `Traces (${String(data.trace_count || 0)})`;
      document.getElementById("updated").textContent = data.generated_at_utc || "-";

      renderLogs(data.logs || []);
      renderNodeList(displayNodes);
      renderTraceList(data.traces || []);
      applyNodeSelectionVisual();
      applyTraceSelectionVisual();
      renderSelectionDetails();
      state.lastDrawSelectedTraceId = state.selectedTraceId;

      if (!state.fitted && bounds.length > 0) {
        map.fitBounds(bounds, { padding: [24, 24] });
        state.fitted = true;
      }
    }

    function applySnapshot(data, options = {}) {
      const force = Boolean(options.force);
      const snapshotRevision = snapshotRevisionOf(data);
      if (!force && state.lastSnapshotRevision > 0 && snapshotRevision <= state.lastSnapshotRevision) {
        return;
      }

      const mapRevision = mapRevisionOf(data);
      const styleSignature = mapStyleSignatureOf(data);
      const mapChanged = (
        force
        || !state.lastData
        || mapRevision !== state.lastMapRevision
        || styleSignature !== state.lastMapStyleSignature
      );
      const logsRevision = logRevisionOf(data);
      state.lastServerData = data;

      if (mapChanged) {
        draw(data);
        state.lastMapRevision = mapRevision;
        state.lastMapStyleSignature = styleSignature;
        state.lastLogRevision = logsRevision;
      } else {
        const displayNodes = Array.isArray(state.lastData?.nodes) ? state.lastData.nodes : [];
        state.lastData = { ...data, nodes: displayNodes };

        updateConnectionUi(data);
        updateConfigUi(data);
        updateFreshnessLegend(data && data.config);
        updateQueueManageButton(data);
        const nodeTab = tabButtons.find((btn) => (btn.dataset.tab || "") === "nodes");
        if (nodeTab) nodeTab.textContent = `Nodes (${String(displayNodes.length)})`;
        const traceTab = tabButtons.find((btn) => (btn.dataset.tab || "") === "traces");
        if (traceTab) traceTab.textContent = `Traces (${String(data.trace_count || 0)})`;
        document.getElementById("updated").textContent = data.generated_at_utc || "-";

        if (logsRevision !== state.lastLogRevision) {
          renderLogs(data.logs || []);
          state.lastLogRevision = logsRevision;
        }
        renderSelectionDetails();
      }

      if (queueModal && !queueModal.classList.contains("hidden")) {
        renderQueueModal(data);
      }
      state.lastSnapshotRevision = Math.max(state.lastSnapshotRevision, snapshotRevision);
    }

    function updateConnectionUi(data) {
      const connected = Boolean(data && data.connected);
      const connState = String((data && data.connection_state) || "");
      const host = (data && data.connected_host) ? String(data.connected_host) : "";
      const partition = (data && data.mesh_host) ? String(data.mesh_host) : "";
      const error = (data && data.connection_error) ? String(data.connection_error) : "";

      let label = "Not connected";
      if (connected) {
        label = host ? `Connected to ${host}` : "Connected";
      } else if (connState === "connecting") {
        label = host ? `Connecting to ${host}...` : "Connecting...";
      } else if (connState === "error") {
        label = "Connection error";
      }
      if (partition && partition !== "-" && partition !== "disconnected") {
        label += ` (${partition})`;
      }
      document.getElementById("meshHost").textContent = label;

      onboarding.classList.toggle("hidden", connected);
      disconnectBtn.style.display = connected ? "inline-block" : "none";

      const isConnecting = connState === "connecting";
      connectBtn.disabled = isConnecting;
      connectHostInput.disabled = isConnecting;
      connectBtn.textContent = isConnecting ? "Connecting..." : "Connect";

      if (!connected && error) {
        connectError.textContent = error;
        connectError.classList.add("visible");
      } else {
        connectError.textContent = "";
        connectError.classList.remove("visible");
      }

      renderDiscovery(data && data.discovery);

      if (connected) {
        connectStatus.textContent = "";
        state.promptedConnect = false;
        return;
      }

      if (isConnecting) {
        connectStatus.textContent = host ? `Connecting to ${host}...` : "Connecting...";
      } else {
        connectStatus.textContent = "Meshtracer runs locally and connects to your node over TCP on your LAN.";
      }

      if (!connected && !state.promptedConnect) {
        state.promptedConnect = true;
        try {
          connectHostInput.focus();
          connectHostInput.select();
        } catch (_e) {
        }
      }
    }

    function renderDiscovery(discovery) {
      if (!discoverySection || !discoveryMeta || !discoveryList || !discoveryRescan) return;

      const enabled = Boolean(discovery && discovery.enabled);
      const scanning = Boolean(discovery && discovery.scanning);
      const networks = Array.isArray(discovery && discovery.networks) ? discovery.networks : [];
      const port = Number((discovery && discovery.port) || 4403);
      const candidates = Array.isArray(discovery && discovery.candidates) ? discovery.candidates : [];
      const done = Number((discovery && discovery.progress_done) || 0);
      const total = Number((discovery && discovery.progress_total) || 0);
      const lastScanUtc = String((discovery && discovery.last_scan_utc) || "");

      discoveryRescan.disabled = !enabled || scanning;

      if (!enabled) {
        discoveryMeta.textContent = "Auto-discovery is disabled.";
        discoveryList.innerHTML = '<div class="discovery-empty">Enter a node IP/hostname above, or start Meshtracer with discovery enabled.</div>';
        return;
      }

      const metaParts = [];
      if (scanning) {
        metaParts.push(total > 0 ? `Scanning ${done}/${total}...` : "Scanning...");
      } else if (lastScanUtc) {
        metaParts.push(`Last scan: ${lastScanUtc}`);
      }
      if (networks.length) metaParts.push(`Networks: ${networks.join(", ")}`);
      if (Number.isFinite(port) && port > 0) metaParts.push(`Port: ${port}`);
      discoveryMeta.textContent = metaParts.join(" | ") || "Searching your LAN...";

      if (!candidates.length) {
        const hint = scanning
          ? "No nodes found yet."
          : "No nodes found. Make sure your computer and node are on the same network, and that the node's TCP interface is reachable.";
        discoveryList.innerHTML = `<div class="discovery-empty">${escapeHtml(hint)}</div>`;
        return;
      }

      discoveryList.innerHTML = candidates.map((item) => {
        const host = String((item && item.host) || "").trim();
        if (!host) return "";
        const itemPort = Number((item && item.port) || port);
        const latency = item && item.latency_ms !== undefined && item.latency_ms !== null
          ? `${item.latency_ms}ms`
          : "";
        const seen = item && item.last_seen_utc ? `seen ${item.last_seen_utc}` : "";
        const meta = [seen, latency].filter(Boolean).join(" | ") || "reachable";
        return `
          <div class="discovery-item">
            <div class="discovery-item-main">
              <span class="discovery-item-host">${escapeHtml(host)}${itemPort ? ":" + escapeHtml(itemPort) : ""}</span>
              <span class="discovery-item-meta">${escapeHtml(meta)}</span>
            </div>
            <button class="discovery-item-btn" type="button" data-host="${escapeHtml(host)}">Connect</button>
          </div>
        `;
      }).join("");

      for (const btn of discoveryList.querySelectorAll("button[data-host]")) {
        btn.addEventListener("click", () => {
          const host = String(btn.dataset.host || "").trim();
          if (!host) return;
          connectToHost(host);
        });
      }
    }

    function setCfgStatus(message, options = {}) {
      const isError = Boolean(options.error);
      const text = String(message || "").trim();
      if (!cfgStatus) return;
      cfgStatus.textContent = text;
      cfgStatus.classList.toggle("visible", Boolean(text));
      cfgStatus.classList.toggle("error", isError);
    }

    function markConfigDirty() {
      state.configDirty = true;
      setCfgStatus("Unsaved changes.", { error: false });
    }

    function normalizeIntervalMinutes(raw, fallback = 5) {
      const parsed = Number(raw);
      if (!Number.isFinite(parsed) || parsed <= 0) return Number(fallback);
      return Math.round(parsed * 1000) / 1000;
    }

    function intervalOptionLabel(minutesRaw) {
      const minutes = normalizeIntervalMinutes(minutesRaw, 5);
      if (minutes < 1) {
        const seconds = Math.max(1, Math.round(minutes * 60));
        return `${seconds} seconds`;
      }
      const rounded = Math.round(minutes * 1000) / 1000;
      const text = Number.isInteger(rounded) ? String(Math.trunc(rounded)) : String(rounded);
      return `${text} minute${rounded === 1 ? "" : "s"}`;
    }

    function setIntervalSelectValue(rawMinutes) {
      if (!cfgInterval) return;
      const normalized = normalizeIntervalMinutes(rawMinutes, 5);
      const value = String(normalized);
      let hasOption = false;
      for (const opt of Array.from(cfgInterval.options || [])) {
        if (String(opt.value || "").trim() === value) {
          hasOption = true;
          break;
        }
      }
      if (!hasOption) {
        const opt = document.createElement("option");
        opt.value = value;
        opt.textContent = intervalOptionLabel(normalized);
        cfgInterval.appendChild(opt);
      }
      cfgInterval.value = value;
    }

    function applyConfigToForm(config) {
      if (!config) return;
      if (cfgTracerouteBehavior) {
        const behavior = String(config.traceroute_behavior ?? "automatic").trim().toLowerCase();
        cfgTracerouteBehavior.value = behavior === "automatic" ? "automatic" : "manual";
      }
      setIntervalSelectValue(config.interval ?? 5);
      if (cfgHeardWindow) cfgHeardWindow.value = String(config.heard_window ?? "");
      if (cfgFreshWindow) cfgFreshWindow.value = String(config.fresh_window ?? "");
      if (cfgMidWindow) cfgMidWindow.value = String(config.mid_window ?? "");
      if (cfgHopLimit) cfgHopLimit.value = String(config.hop_limit ?? "");
      if (cfgMaxMapTraces) cfgMaxMapTraces.value = String(config.max_map_traces ?? "");
      if (cfgMaxStoredTraces) cfgMaxStoredTraces.value = String(config.max_stored_traces ?? "");
      if (cfgWebhookUrl) cfgWebhookUrl.value = String(config.webhook_url ?? "");
      state.configTokenSet = Boolean(config.webhook_api_token_set);
      state.configTokenTouched = false;
      if (cfgWebhookToken) {
        cfgWebhookToken.value = "";
        cfgWebhookToken.placeholder = state.configTokenSet
          ? "Saved token is hidden. Type a new value to replace; leave blank to keep."
          : "Optional token";
      }
    }

    function updateConfigUi(data) {
      const config = data && typeof data.config === "object" ? data.config : null;
      const defaults = data && typeof data.config_defaults === "object" ? data.config_defaults : null;
      const server = data && typeof data.server === "object" ? data.server : null;

      if (defaults && !state.configDefaults) {
        state.configDefaults = defaults;
      }
      if (server) {
        if (cfgDbPath) cfgDbPath.textContent = String(server.db_path || "-");
        if (cfgUiBind) cfgUiBind.textContent = `${String(server.map_host || "-")}:${String(server.map_port || "-")}`;
      }

      if (!config) return;
      if (!state.configLoaded || !state.configDirty) {
        applyConfigToForm(config);
        state.configLoaded = true;
        if (!state.configDirty) {
          setCfgStatus("", { error: false });
        }
      }
    }

    async function apiPost(path, payload) {
      const response = await fetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload || {}),
      });
      let body = null;
      try {
        body = await response.json();
      } catch (_e) {
      }
      return { ok: response.ok, status: response.status, body };
    }

    async function applyConfig() {
      function asInt(value, fallback) {
        const n = Number(String(value || "").trim());
        if (!Number.isFinite(n)) return fallback;
        return Math.trunc(n);
      }

      function asFloat(value, fallback) {
        const n = Number(String(value || "").trim());
        if (!Number.isFinite(n) || n <= 0) return fallback;
        return n;
      }

      const payload = {
        traceroute_behavior: String(cfgTracerouteBehavior?.value || "automatic").trim().toLowerCase(),
        interval: asFloat(cfgInterval?.value, 5),
        heard_window: asInt(cfgHeardWindow?.value, 120),
        fresh_window: asInt(cfgFreshWindow?.value, 120),
        mid_window: asInt(cfgMidWindow?.value, 480),
        hop_limit: asInt(cfgHopLimit?.value, 7),
        max_map_traces: asInt(cfgMaxMapTraces?.value, 800),
        max_stored_traces: asInt(cfgMaxStoredTraces?.value, 50000),
        webhook_url: String(cfgWebhookUrl?.value || "").trim() || null,
      };
      if (state.configTokenTouched) {
        payload.webhook_api_token = String(cfgWebhookToken?.value || "").trim() || null;
      }

      setCfgStatus("Applying...", { error: false });
      if (cfgApply) cfgApply.disabled = true;
      try {
        const { ok, body } = await apiPost("/api/config", payload);
        if (!ok) {
          const detail = body && (body.detail || body.error) ? String(body.detail || body.error) : "config update failed";
          setCfgStatus(detail, { error: true });
          return;
        }
        state.configDirty = false;
        setCfgStatus("Applied.", { error: false });
        if (body && body.config) {
          applyConfigToForm(body.config);
        }
      } catch (e) {
        setCfgStatus(String(e || "config update failed"), { error: true });
      } finally {
        if (cfgApply) cfgApply.disabled = false;
      }
      refresh();
    }

    function resetConfig() {
      const defaults = state.configDefaults || {
        traceroute_behavior: "automatic",
        interval: 5,
        heard_window: 120,
        fresh_window: 120,
        mid_window: 480,
        hop_limit: 7,
        webhook_url: null,
        webhook_api_token: null,
        max_map_traces: 800,
        max_stored_traces: 50000,
      };
      applyConfigToForm(defaults);
      state.configTokenTouched = true;
      markConfigDirty();
      setCfgStatus("Reset to defaults (not applied).", { error: false });
    }

    const HELP_COPY = {
      traceroute_behavior: {
        title: "Traceroute Behaviour",
        body: `Manual: Meshtracer only runs traceroutes you queue from the node details panel.

Automatic: Meshtracer continuously selects eligible recent nodes and runs traceroutes on an interval.`,
      },
      interval: {
        title: "Interval / Timeout Basis",
        body: `How often Meshtracer attempts a traceroute in Automatic mode.

This is also the basis for Meshtastic per-hop timeout tuning. A lower interval (like 30 seconds) gives faster cadence but tighter timeout windows.`,
      },
      heard_window: {
        title: "Heard Window (minutes)",
        body: `Only nodes heard within this many minutes are eligible as traceroute targets.

Larger values include more nodes (including stale ones). Smaller values focus on recently-active nodes.`,
      },
      fresh_window: {
        title: "Fresh Window (minutes)",
        body: `Nodes heard within this many minutes are shown as Fresh (green) in the map legend.`,
      },
      mid_window: {
        title: "Mid Window (minutes)",
        body: `Nodes heard within this many minutes are shown as Mid (yellow). Older nodes are Stale (red).

Must be >= Fresh Window.`,
      },
      hop_limit: {
        title: "Hop Limit",
        body: `The maximum hop count used for Meshtastic traceroute.

Higher values can discover longer routes but may take longer. Meshtracer derives an internal per-hop timeout from (interval / hop_limit).`,
      },
      max_map_traces: {
        title: "Max Map Traces",
        body: `Maximum number of completed traceroutes included in the map/API snapshot.

This only affects what the UI loads and renders; it does not delete history from the database.`,
      },
      max_stored_traces: {
        title: "Max Stored Traces",
        body: `Maximum number of completed traceroutes kept in SQLite for the connected node partition.

Set to 0 to disable pruning (database can grow without bound).`,
      },
      webhook_url: {
        title: "Webhook URL",
        body: `Optional URL to POST structured JSON when a traceroute completes.

Leave blank to disable webhooks.`,
      },
      webhook_api_token: {
        title: "Webhook API Token",
        body: `Optional token added to webhook requests.

Sent as both an Authorization: Bearer token and X-API-Token header. Leave blank for no auth.`,
      },
    };

    let lastConfigFocus = null;
    function openConfig() {
      if (!configModal) return;
      lastConfigFocus = document.activeElement;
      configModal.classList.remove("hidden");
      try {
        if (configClose) configClose.focus();
      } catch (_e) {
      }
    }

    function closeConfig() {
      if (!configModal) return;
      configModal.classList.add("hidden");
      try {
        if (configOpen) {
          configOpen.focus();
        } else if (lastConfigFocus && typeof lastConfigFocus.focus === "function") {
          lastConfigFocus.focus();
        }
      } catch (_e) {
      }
      lastConfigFocus = null;
    }

    function openHelp(helpId) {
      const key = String(helpId || "").trim();
      const entry = HELP_COPY[key];
      if (!entry || !helpModal || !helpTitle || !helpBody) return;
      helpTitle.textContent = String(entry.title || "Help");
      helpBody.textContent = String(entry.body || "");
      helpModal.classList.remove("hidden");
      try {
        helpClose.focus();
      } catch (_e) {
      }
    }

    function closeHelp() {
      if (!helpModal) return;
      helpModal.classList.add("hidden");
    }

    async function connectToHost(hostOverride) {
      const host = String((hostOverride !== undefined ? hostOverride : connectHostInput.value) || "").trim();
      if (!host) {
        connectError.textContent = "Enter a node IP or hostname.";
        connectError.classList.add("visible");
        return;
      }
      if (hostOverride !== undefined) {
        connectHostInput.value = host;
      }
      try {
        localStorage.setItem("meshtracer.lastHost", host);
      } catch (_e) {
      }
      connectError.textContent = "";
      connectError.classList.remove("visible");
      connectBtn.disabled = true;
      connectHostInput.disabled = true;
      try {
        const { ok, body } = await apiPost("/api/connect", { host });
        if (!ok) {
          const detail = body && (body.detail || body.error) ? String(body.detail || body.error) : "connect failed";
          connectError.textContent = detail;
          connectError.classList.add("visible");
        }
      } catch (e) {
        connectError.textContent = String(e || "connect failed");
        connectError.classList.add("visible");
      } finally {
        connectBtn.disabled = false;
        connectHostInput.disabled = false;
      }
      refresh();
    }

    async function disconnectFromHost() {
      try {
        await apiPost("/api/disconnect", {});
      } catch (_e) {
      }
      refresh();
    }

    async function rescanDiscovery() {
      discoveryRescan.disabled = true;
      try {
        await apiPost("/api/discovery/rescan", {});
      } catch (_e) {
      }
      refresh();
    }

    try {
      const savedHost = localStorage.getItem("meshtracer.lastHost");
      if (savedHost && !connectHostInput.value) {
        connectHostInput.value = String(savedHost);
      }
    } catch (_e) {
    }

    connectBtn.addEventListener("click", () => connectToHost());
    disconnectBtn.addEventListener("click", () => disconnectFromHost());
    discoveryRescan.addEventListener("click", () => rescanDiscovery());
    if (manageTraceQueueBtn) manageTraceQueueBtn.addEventListener("click", () => openQueueModal());
    if (cfgApply) cfgApply.addEventListener("click", () => applyConfig());
    if (cfgReset) cfgReset.addEventListener("click", () => resetConfig());
    if (configOpen) configOpen.addEventListener("click", () => openConfig());
    if (configOverlay) configOverlay.addEventListener("click", () => closeConfig());
    if (configClose) configClose.addEventListener("click", () => closeConfig());
    if (helpOverlay) helpOverlay.addEventListener("click", () => closeHelp());
    if (helpClose) helpClose.addEventListener("click", () => closeHelp());
    if (queueOverlay) queueOverlay.addEventListener("click", () => closeQueueModal());
    if (queueClose) queueClose.addEventListener("click", () => closeQueueModal());
    window.addEventListener("keydown", (event) => {
      if (event.key !== "Escape") return;
      if (helpModal && !helpModal.classList.contains("hidden")) {
        closeHelp();
        return;
      }
      if (queueModal && !queueModal.classList.contains("hidden")) {
        closeQueueModal();
        return;
      }
      if (configModal && !configModal.classList.contains("hidden")) {
        closeConfig();
      }
    });
    for (const btn of document.querySelectorAll("button[data-help]")) {
      btn.addEventListener("click", () => openHelp(btn.dataset.help));
    }
    for (const el of [
      cfgTracerouteBehavior,
      cfgInterval,
      cfgHeardWindow,
      cfgFreshWindow,
      cfgMidWindow,
      cfgHopLimit,
      cfgMaxMapTraces,
      cfgMaxStoredTraces,
      cfgWebhookUrl,
    ]) {
      if (!el) continue;
      el.addEventListener("input", () => markConfigDirty());
      el.addEventListener("change", () => markConfigDirty());
      el.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          applyConfig();
        }
      });
    }
    if (cfgWebhookToken) {
      cfgWebhookToken.addEventListener("input", () => {
        state.configTokenTouched = true;
        markConfigDirty();
      });
      cfgWebhookToken.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          applyConfig();
        }
      });
    }
    connectHostInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        connectToHost();
      }
    });

    async function refresh(options = {}) {
      if (state.refreshInFlight) return;
      state.refreshInFlight = true;
      try {
        const response = await fetch("/api/map", { cache: "no-store" });
        if (!response.ok) {
          reportClientError(`GET /api/map failed (HTTP ${response.status})`, { prefix: "Network" });
          return;
        }
        const data = await response.json();
        applySnapshot(data, options);
      } catch (e) {
        reportClientError(String(e || "refresh failed"), { prefix: "UI error" });
      } finally {
        state.refreshInFlight = false;
      }
    }

    function startEventStream() {
      if (typeof EventSource !== "function") {
        reportClientError("EventSource is not available in this browser; using polling fallback.", { prefix: "Realtime" });
        return;
      }
      if (state.sseSource) {
        try {
          state.sseSource.close();
        } catch (_e) {
        }
        state.sseSource = null;
      }
      const since = Math.max(0, Number(state.lastSnapshotRevision) || 0);
      const streamUrl = `/api/events?since=${encodeURIComponent(String(since))}`;
      const stream = new EventSource(streamUrl);
      state.sseSource = stream;

      stream.addEventListener("open", () => {
        state.sseConnected = true;
        state.lastSseEventAtMs = Date.now();
      });

      stream.addEventListener("heartbeat", () => {
        state.sseConnected = true;
        state.lastSseEventAtMs = Date.now();
      });

      stream.addEventListener("snapshot", (event) => {
        state.sseConnected = true;
        state.lastSseEventAtMs = Date.now();
        let data = null;
        try {
          data = JSON.parse(String(event && event.data ? event.data : "{}"));
        } catch (e) {
          reportClientError(String(e || "invalid snapshot event"), { prefix: "Realtime" });
          return;
        }
        if (!data || typeof data !== "object") return;
        applySnapshot(data);
      });

      stream.onerror = () => {
        const staleMs = Date.now() - state.lastSseEventAtMs;
        if (!Number.isFinite(staleMs) || staleMs > FALLBACK_POLL_MS_DISCONNECTED) {
          state.sseConnected = false;
        }
      };
    }

    refresh({ force: true });
    startEventStream();
    setInterval(() => {
      if (state.sseConnected) return;
      refresh();
    }, FALLBACK_POLL_MS_DISCONNECTED);
    setInterval(() => {
      refresh();
    }, FALLBACK_POLL_MS_CONNECTED);
  </script>
</body>
</html>
"""


def start_map_server(
    snapshot: Callable[[], dict[str, Any]],
    wait_for_snapshot_revision: Callable[[int, float], int],
    connect: Callable[[str], tuple[bool, str]],
    disconnect: Callable[[], tuple[bool, str]],
    run_traceroute: Callable[[int], tuple[bool, str]],
    remove_traceroute_queue_entry: Callable[[int], tuple[bool, str]],
    rescan_discovery: Callable[[], tuple[bool, str]],
    get_config: Callable[[], dict[str, Any]],
    set_config: Callable[[dict[str, Any]], tuple[bool, str]],
    host: str,
    port: int,
) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def _read_json_body(self) -> tuple[dict[str, Any] | None, str | None]:
            length_raw = self.headers.get("Content-Length", "0")
            try:
                length = int(length_raw) if length_raw else 0
            except (TypeError, ValueError):
                length = 0
            if length <= 0:
                return None, "missing_body"
            try:
                raw = self.rfile.read(length)
            except Exception:
                return None, "read_failed"
            try:
                value = json.loads(raw.decode("utf-8", errors="replace"))
            except json.JSONDecodeError:
                return None, "invalid_json"
            if not isinstance(value, dict):
                return None, "expected_object"
            return value, None

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

        def _send_sse(self, *, event: str, payload: dict[str, Any], event_id: int | None = None) -> None:
            if event_id is not None:
                self.wfile.write(f"id: {int(event_id)}\n".encode("utf-8"))
            self.wfile.write(f"event: {event}\n".encode("utf-8"))
            data = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
            self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
            self.wfile.flush()

        def _serve_events(self, since_revision: int) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()

            latest = snapshot()
            latest_revision = int(latest.get("snapshot_revision") or 0)
            self._send_sse(event="snapshot", payload=latest, event_id=latest_revision)

            cursor = latest_revision
            heartbeat_seconds = 20.0
            while True:
                next_revision = wait_for_snapshot_revision(cursor, heartbeat_seconds)
                if int(next_revision) <= cursor:
                    self._send_sse(
                        event="heartbeat",
                        payload={"at_utc": utc_now(), "snapshot_revision": cursor},
                    )
                    continue

                payload = snapshot()
                payload_revision = int(payload.get("snapshot_revision") or next_revision)
                cursor = max(cursor, payload_revision)
                self._send_sse(event="snapshot", payload=payload, event_id=cursor)

        def do_GET(self) -> None:
            url = urlsplit(self.path)
            path = url.path
            if path in ("/", "/map"):
                self._send_html(MAP_HTML)
                return
            if path == "/api/map":
                self._send_json(snapshot())
                return
            if path == "/api/events":
                query = parse_qs(url.query, keep_blank_values=False)
                since_raw = query.get("since", [0])[0]
                try:
                    since = int(since_raw)
                except (TypeError, ValueError):
                    since = 0
                try:
                    self._serve_events(since)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    return
                return
            if path == "/api/config":
                self._send_json({"ok": True, "config": get_config()})
                return
            if path == "/healthz":
                self._send_json({"ok": True, "at_utc": utc_now()})
                return
            self._send_json({"error": "not_found"}, status=404)

        def do_POST(self) -> None:
            path = self.path.split("?", 1)[0]
            if path == "/api/config":
                body, err = self._read_json_body()
                if err is not None or body is None:
                    self._send_json({"ok": False, "error": err or "bad_request"}, status=400)
                    return
                ok, detail = set_config(body)
                status = 200 if ok else 400
                self._send_json(
                    {
                        "ok": ok,
                        "detail": detail,
                        "config": get_config(),
                        "snapshot": snapshot(),
                    },
                    status=status,
                )
                return
            if path == "/api/connect":
                body, err = self._read_json_body()
                if err is not None or body is None:
                    self._send_json({"ok": False, "error": err or "bad_request"}, status=400)
                    return
                host_value = body.get("host")
                host = str(host_value or "").strip()
                if not host:
                    self._send_json({"ok": False, "error": "missing_host"}, status=400)
                    return
                ok, detail = connect(host)
                status = 200 if ok else 500
                self._send_json({"ok": ok, "detail": detail, "snapshot": snapshot()}, status=status)
                return
            if path == "/api/disconnect":
                ok, detail = disconnect()
                status = 200 if ok else 500
                self._send_json({"ok": ok, "detail": detail, "snapshot": snapshot()}, status=status)
                return
            if path == "/api/traceroute":
                body, err = self._read_json_body()
                if err is not None or body is None:
                    self._send_json({"ok": False, "error": err or "bad_request"}, status=400)
                    return
                node_num_raw = body.get("node_num")
                try:
                    node_num = int(node_num_raw)
                except (TypeError, ValueError):
                    self._send_json({"ok": False, "error": "invalid_node_num"}, status=400)
                    return
                ok, detail = run_traceroute(node_num)
                status = 200 if ok else 400
                self._send_json({"ok": ok, "detail": detail, "snapshot": snapshot()}, status=status)
                return
            if path == "/api/traceroute/queue/remove":
                body, err = self._read_json_body()
                if err is not None or body is None:
                    self._send_json({"ok": False, "error": err or "bad_request"}, status=400)
                    return
                queue_id_raw = body.get("queue_id")
                try:
                    queue_id = int(queue_id_raw)
                except (TypeError, ValueError):
                    self._send_json({"ok": False, "error": "invalid_queue_id"}, status=400)
                    return
                ok, detail = remove_traceroute_queue_entry(queue_id)
                status = 200 if ok else 400
                self._send_json({"ok": ok, "detail": detail, "snapshot": snapshot()}, status=status)
                return
            if path == "/api/discovery/rescan":
                ok, detail = rescan_discovery()
                status = 200 if ok else 500
                self._send_json({"ok": ok, "detail": detail, "snapshot": snapshot()}, status=status)
                return
            self._send_json({"error": "not_found"}, status=404)

    server = ThreadingHTTPServer((host, port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
