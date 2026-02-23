    const map = L.map("map", { zoomControl: false }).setView([20, 0], 2);
    L.control.zoom({ position: "bottomleft" }).addTo(map);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution: "&copy; OpenStreetMap contributors",
    }).addTo(map);
    const markerLayer = L.layerGroup().addTo(map);
    const edgeLayer = L.layerGroup().addTo(map);
    const spiderLayer = L.layerGroup().addTo(map);

    const sidebar = document.getElementById("sidebar");
    const sidebarResize = document.getElementById("sidebarResize");
    const sidebarToggle = document.getElementById("sidebarToggle");
    const traceDetails = document.getElementById("traceDetails");
    const traceDetailsTitle = document.getElementById("traceDetailsTitle");
    const traceDetailsBody = document.getElementById("traceDetailsBody");
    const traceDetailsNodeChat = document.getElementById("traceDetailsNodeChat");
    const traceDetailsClose = document.getElementById("traceDetailsClose");
    const chatModal = document.getElementById("chatModal");
    const chatOpen = document.getElementById("chatOpen");
    const chatUnreadBadge = document.getElementById("chatUnreadBadge");
    const chatClose = document.getElementById("chatClose");
    const chatRecipient = document.getElementById("chatRecipient");
    const chatRecipientWarning = document.getElementById("chatRecipientWarning");
    const chatMessages = document.getElementById("chatMessages");
    const chatStatus = document.getElementById("chatStatus");
    const chatInput = document.getElementById("chatInput");
    const chatSend = document.getElementById("chatSend");
    const manageTraceQueueBtn = document.getElementById("manageTraceQueueBtn");
    const nodeSearchInput = document.getElementById("nodeSearch");
    const nodeSortSelect = document.getElementById("nodeSort");
    const legendFreshText = document.getElementById("legendFreshText");
    const legendMidText = document.getElementById("legendMidText");
    const legendStaleText = document.getElementById("legendStaleText");
    const tabButtons = Array.from(document.querySelectorAll(".tab-btn"));
    const tabPanels = Array.from(document.querySelectorAll(".panel"));
    const logList = document.getElementById("logList");
    const logFilterTraceroute = document.getElementById("logFilterTraceroute");
    const logFilterTelemetry = document.getElementById("logFilterTelemetry");
    const logFilterMessaging = document.getElementById("logFilterMessaging");
    const logFilterPosition = document.getElementById("logFilterPosition");
    const logFilterNodeInfo = document.getElementById("logFilterNodeInfo");
    const logFilterOther = document.getElementById("logFilterOther");
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
    const cfgTracerouteRetentionHours = document.getElementById("cfgTracerouteRetentionHours");
    const cfgWebhookUrl = document.getElementById("cfgWebhookUrl");
    const cfgWebhookToken = document.getElementById("cfgWebhookToken");
    const cfgChatNotifDesktop = document.getElementById("cfgChatNotifDesktop");
    const cfgChatNotifSound = document.getElementById("cfgChatNotifSound");
    const cfgChatNotifFocused = document.getElementById("cfgChatNotifFocused");
    const cfgChatNotifHint = document.getElementById("cfgChatNotifHint");
    const cfgApply = document.getElementById("cfgApply");
    const cfgReset = document.getElementById("cfgReset");
    const cfgResetDatabase = document.getElementById("cfgResetDatabase");
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
      selectedNodeDetailsTab: "node_info",
      selectedNodeTelemetryTab: "device",
      lastDrawSelectedTraceId: null,
      spiderGroups: new Map(),
      activeSpiderGroupKey: null,
      activeTab: "log",
      logTypeFilters: {
        traceroute: true,
        telemetry: true,
        messaging: true,
        position: true,
        node_info: true,
        other: true,
      },
      nodeSearchQuery: "",
      nodeSortMode: "last_heard",
      promptedConnect: false,
      configLoaded: false,
      configDirty: false,
      configDefaults: null,
      configTokenSet: false,
      configTokenTouched: false,
      queueRemoveBusyIds: new Set(),
      telemetryRequestState: {},
      chatOpen: false,
      chatRecipientKind: "channel",
      chatRecipientId: 0,
      chatMessages: [],
      chatLoading: false,
      chatSendBusy: false,
      chatLoadedKey: "",
      lastChatRevision: 0,
      chatStatusMessage: "",
      chatStatusError: false,
      chatUnreadCount: 0,
      chatNotifyMeshHost: "",
      chatNotifyCursor: 0,
      chatIncomingBusy: false,
      chatIncomingPending: false,
      chatNotificationSettings: {
        desktop: false,
        sound: false,
        notifyFocused: false,
      },
      chatAudioCtx: null,
    };
    const FALLBACK_POLL_MS_CONNECTED = 30000;
    const FALLBACK_POLL_MS_DISCONNECTED = 3000;
    const LOG_FILTER_STORAGE_KEY = "meshtracer.logTypeFilters";
    const CHAT_NOTIFICATION_CURSOR_STORAGE_PREFIX = "meshtracer.chatNotificationCursor:";

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
    const SPIDERFY_RADIUS_MIN_PX = 20;
    const SPIDERFY_RADIUS_STEP_PX = 2.6;
    const SPIDERFY_RADIUS_MAX_PX = 46;

    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"]/g, (c) => {
        if (c === "&") return "&amp;";
        if (c === "<") return "&lt;";
        if (c === ">") return "&gt;";
        return "&quot;";
      });
    }

    function chatRevisionFromData(data) {
      return numericRevision(data && data.chat && data.chat.revision, 0);
    }

    function chatChannelsFromData(data) {
      const channelsRaw = Array.isArray(data && data.chat && data.chat.channels)
        ? data.chat.channels
        : [];
      const channels = [];
      for (const raw of channelsRaw) {
        const value = Number(raw);
        if (!Number.isFinite(value)) continue;
        const idx = Math.trunc(value);
        if (idx < 0) continue;
        if (!channels.includes(idx)) channels.push(idx);
      }
      if (!channels.length) channels.push(0);
      channels.sort((a, b) => a - b);
      return channels;
    }

    function chatChannelNamesFromData(data) {
      const namesRaw = data && data.chat && data.chat.channel_names;
      if (!namesRaw || typeof namesRaw !== "object") {
        return {};
      }
      const names = {};
      for (const [rawIndex, rawName] of Object.entries(namesRaw)) {
        const indexValue = Number(rawIndex);
        if (!Number.isFinite(indexValue)) continue;
        const channelIndex = Math.trunc(indexValue);
        if (channelIndex < 0) continue;
        const channelName = String(rawName || "").trim();
        if (!channelName) continue;
        names[channelIndex] = channelName;
      }
      return names;
    }

    function chatChannelLabel(channelIndex, channelNames = {}) {
      const idx = Math.trunc(Number(channelIndex) || 0);
      const name = channelNames && typeof channelNames === "object"
        ? String(channelNames[idx] || "").trim()
        : "";
      if (name) return name;
      if (idx === 0) return "Primary";
      return `Channel ${idx}`;
    }

    function chatRecentDirectNodesFromData(data) {
      const recentRaw = Array.isArray(data && data.chat && data.chat.recent_direct_node_nums)
        ? data.chat.recent_direct_node_nums
        : [];
      const recent = [];
      for (const raw of recentRaw) {
        const value = Number(raw);
        if (!Number.isFinite(value)) continue;
        const nodeNum = Math.trunc(value);
        if (recent.includes(nodeNum)) continue;
        recent.push(nodeNum);
      }
      return recent;
    }

    function chatRecipientKey(kind, id) {
      return `${String(kind || "").trim().toLowerCase()}:${Math.trunc(Number(id) || 0)}`;
    }

    function chatNotificationCursorStorageKey(meshHost) {
      const host = String(meshHost || "").trim();
      if (!host) return "";
      return `${CHAT_NOTIFICATION_CURSOR_STORAGE_PREFIX}${host}`;
    }

    function chatNotificationApiSupported() {
      return typeof Notification === "function";
    }

    function normalizeChatNotificationSettings(raw) {
      const value = raw && typeof raw === "object" ? raw : {};
      return {
        desktop: flagIsTrue(value.desktop),
        sound: flagIsTrue(value.sound),
        notifyFocused: flagIsTrue(value.notifyFocused),
      };
    }

    function chatNotificationSettingsFromConfig(config) {
      const source = config && typeof config === "object" ? config : {};
      return normalizeChatNotificationSettings({
        desktop: source.chat_notification_desktop,
        sound: source.chat_notification_sound,
        notifyFocused: source.chat_notification_notify_focused,
      });
    }

    function loadChatNotificationCursor(meshHost) {
      const key = chatNotificationCursorStorageKey(meshHost);
      if (!key) return null;
      try {
        const raw = localStorage.getItem(key);
        if (!raw) return null;
        const parsed = Number(raw);
        if (!Number.isFinite(parsed) || parsed < 0) return null;
        return Math.trunc(parsed);
      } catch (_e) {
        return null;
      }
    }

    function saveChatNotificationCursor(meshHost, chatId) {
      const key = chatNotificationCursorStorageKey(meshHost);
      if (!key) return;
      const chatIdInt = Math.max(0, Math.trunc(Number(chatId) || 0));
      try {
        localStorage.setItem(key, String(chatIdInt));
      } catch (_e) {
      }
    }

    function chatMessageId(message) {
      const value = Number(message && message.chat_id);
      if (!Number.isFinite(value) || value <= 0) return 0;
      return Math.trunc(value);
    }

    function chatRecipientFromMessage(message) {
      const kind = String(message && message.message_type || "").trim().toLowerCase();
      if (kind === "channel") {
        const channelIndex = Math.trunc(Number(message && message.channel_index) || 0);
        if (channelIndex < 0) return null;
        return { kind: "channel", id: channelIndex };
      }
      if (kind === "direct") {
        const peerNodeNum = Math.trunc(Number(message && message.peer_node_num) || 0);
        if (peerNodeNum <= 0) return null;
        return { kind: "direct", id: peerNodeNum };
      }
      return null;
    }

    function chatRecipientKeyFromMessage(message) {
      const recipient = chatRecipientFromMessage(message);
      if (!recipient) return "";
      return chatRecipientKey(recipient.kind, recipient.id);
    }

    function chatMessageMatchesActiveRecipient(message) {
      if (!state.chatOpen) return false;
      const activeKey = chatRecipientKey(state.chatRecipientKind, state.chatRecipientId);
      if (!activeKey) return false;
      return activeKey === chatRecipientKeyFromMessage(message);
    }

    function updateChatOpenButton() {
      if (!chatOpen || !chatUnreadBadge) return;
      const unreadCount = Math.max(0, Math.trunc(Number(state.chatUnreadCount) || 0));
      const hasUnread = unreadCount > 0;
      chatUnreadBadge.classList.toggle("visible", hasUnread);
      chatUnreadBadge.textContent = hasUnread
        ? (unreadCount > 99 ? "99+" : String(unreadCount))
        : "";
      chatOpen.classList.toggle("has-unread", hasUnread);
      chatOpen.setAttribute(
        "aria-label",
        hasUnread ? `Open chat (${unreadCount} unread)` : "Open chat"
      );
    }

    function allKnownNodeNums(data) {
      const nodes = Array.isArray(data && data.nodes) ? data.nodes : [];
      const nums = [];
      for (const node of nodes) {
        const value = Number(node && node.num);
        if (!Number.isFinite(value)) continue;
        const nodeNum = Math.trunc(value);
        if (!nums.includes(nodeNum)) nums.push(nodeNum);
      }
      return nums;
    }

    function normalizeChatRecipient(data, options = {}) {
      const channels = chatChannelsFromData(data);
      const recentDirect = chatRecentDirectNodesFromData(data);
      const nodeNums = allKnownNodeNums(data);

      const preferNodeNumValue = Number(options.nodeNum);
      if (Number.isFinite(preferNodeNumValue)) {
        state.chatRecipientKind = "direct";
        state.chatRecipientId = Math.trunc(preferNodeNumValue);
      }

      const currentKind = String(state.chatRecipientKind || "").trim().toLowerCase();
      const currentId = Math.trunc(Number(state.chatRecipientId) || 0);
      let nextKind = currentKind;
      let nextId = currentId;

      if (currentKind === "channel") {
        if (!channels.includes(currentId)) {
          nextId = channels[0];
        }
      } else if (currentKind === "direct") {
        const validDirect = recentDirect.includes(currentId) || nodeNums.includes(currentId);
        if (!validDirect) {
          nextKind = "channel";
          nextId = channels[0];
        }
      } else {
        nextKind = "channel";
        nextId = channels[0];
      }

      state.chatRecipientKind = nextKind;
      state.chatRecipientId = nextId;

      const recentSet = new Set(recentDirect);
      const otherDirect = nodeNums.filter((nodeNum) => !recentSet.has(nodeNum));

      if (state.chatRecipientKind === "direct" && !recentSet.has(state.chatRecipientId) && !otherDirect.includes(state.chatRecipientId)) {
        recentDirect.unshift(state.chatRecipientId);
      }

      return { channels, recentDirect, otherDirect };
    }

    function chatNodeLabel(nodeNum) {
      const nodeNumInt = Math.trunc(Number(nodeNum));
      const fallback = `Node #${nodeNumInt}`;
      if (!Number.isFinite(nodeNumInt)) return "Node";
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

    function intOrNull(value) {
      const parsed = Number(value);
      if (!Number.isFinite(parsed)) return null;
      return Math.trunc(parsed);
    }

    function flagIsTrue(value) {
      if (value === true) return true;
      if (value === false || value === null || value === undefined) return false;
      if (typeof value === "number") return Number.isFinite(value) && value !== 0;
      const text = String(value).trim().toLowerCase();
      return ["1", "true", "yes", "y", "on"].includes(text);
    }

    function chatPacketHopCount(message) {
      const packet = message && message.packet && typeof message.packet === "object"
        ? message.packet
        : null;
      if (!packet) return null;

      const hopsAway = intOrNull(packet.hopsAway ?? packet.hops_away);
      if (hopsAway !== null && hopsAway >= 0) return hopsAway;
      return null;
    }

    function chatMetaText(message) {
      const created = String(message && message.created_at_utc ? message.created_at_utc : "").trim();
      const hopCount = chatPacketHopCount(message);
      const hopText = hopCount === null
        ? ""
        : ` | ${hopCount} hop${hopCount === 1 ? "" : "s"}`;
      if (!created) {
        return hopText ? `-${hopText}` : "-";
      }
      return `${created}${hopText}`;
    }

    function setChatStatus(message, options = {}) {
      state.chatStatusMessage = String(message || "").trim();
      state.chatStatusError = Boolean(options.error);
      if (!chatStatus) return;
      chatStatus.textContent = state.chatStatusMessage;
      chatStatus.classList.toggle("error", state.chatStatusError);
    }

    function activeMeshHostFromData(data) {
      const host = String(data && data.mesh_host || "").trim();
      if (!host || host === "-" || host === "disconnected") return "";
      return host;
    }

    function syncChatNotificationPartition(data) {
      const nextMeshHost = activeMeshHostFromData(data);
      const chatRevision = chatRevisionFromData(data);
      if (nextMeshHost === state.chatNotifyMeshHost) {
        if (!nextMeshHost && state.chatNotifyCursor !== 0) {
          state.chatNotifyCursor = 0;
        }
        if (chatRevision < state.chatNotifyCursor) {
          state.chatNotifyCursor = Math.max(0, chatRevision);
          if (nextMeshHost) saveChatNotificationCursor(nextMeshHost, state.chatNotifyCursor);
        }
        return;
      }

      state.chatNotifyMeshHost = nextMeshHost;
      state.chatUnreadCount = 0;
      state.chatIncomingPending = false;
      if (!nextMeshHost) {
        state.chatNotifyCursor = 0;
        updateChatOpenButton();
        return;
      }

      const storedCursor = loadChatNotificationCursor(nextMeshHost);
      if (storedCursor === null) {
        state.chatNotifyCursor = Math.max(0, chatRevision);
        saveChatNotificationCursor(nextMeshHost, state.chatNotifyCursor);
      } else {
        state.chatNotifyCursor = Math.max(0, storedCursor);
      }
      updateChatOpenButton();
    }

    function chatNotificationPermission() {
      if (!chatNotificationApiSupported()) return "unsupported";
      return String(Notification.permission || "default");
    }

    function setChatNotificationHint(message) {
      if (!cfgChatNotifHint) return;
      cfgChatNotifHint.textContent = String(message || "").trim();
    }

    function updateChatNotificationHint() {
      const permission = chatNotificationPermission();
      if (permission === "unsupported") {
        setChatNotificationHint("Desktop notifications are not supported in this browser.");
        return;
      }
      if (permission === "denied") {
        setChatNotificationHint("Desktop notifications are blocked in browser permissions for this site.");
        return;
      }
      if (permission === "granted") {
        setChatNotificationHint("Desktop notifications are allowed for this site.");
        return;
      }
      setChatNotificationHint("Desktop notifications require browser permission the first time you enable them.");
    }

    function applyChatNotificationSettingsToForm() {
      const desktopSupported = chatNotificationApiSupported();
      if (cfgChatNotifDesktop) {
        cfgChatNotifDesktop.disabled = !desktopSupported;
        cfgChatNotifDesktop.checked = desktopSupported && Boolean(state.chatNotificationSettings.desktop);
      }
      if (cfgChatNotifSound) cfgChatNotifSound.checked = Boolean(state.chatNotificationSettings.sound);
      if (cfgChatNotifFocused) cfgChatNotifFocused.checked = Boolean(state.chatNotificationSettings.notifyFocused);
      updateChatNotificationHint();
    }

    function notificationWindowIsFocused() {
      if (document.visibilityState && document.visibilityState !== "visible") return false;
      if (typeof document.hasFocus !== "function") return true;
      try {
        return Boolean(document.hasFocus());
      } catch (_e) {
        return true;
      }
    }

    function shouldPlayNotificationWhileFocused() {
      return Boolean(state.chatNotificationSettings.notifyFocused) || !notificationWindowIsFocused();
    }

    function chatMessagePreviewText(message) {
      const text = String(message && message.text || "").trim();
      if (!text) return "New chat message";
      if (text.length <= 120) return text;
      return `${text.slice(0, 117)}...`;
    }

    function chatMessageSenderLabel(message) {
      const senderNodeNum = Number(message && message.from_node_num);
      if (!Number.isFinite(senderNodeNum)) return "Node";
      return chatNodeLabel(Math.trunc(senderNodeNum));
    }

    function chatNotificationTitleForMessage(message, data) {
      const recipient = chatRecipientFromMessage(message);
      const sender = chatMessageSenderLabel(message);
      if (!recipient) return "Incoming chat";
      if (recipient.kind === "channel") {
        const names = chatChannelNamesFromData(data);
        const label = chatChannelLabel(recipient.id, names);
        return `${sender} in ${label}`;
      }
      return `Direct message from ${sender}`;
    }

    function openChatForMessage(message) {
      const recipient = chatRecipientFromMessage(message);
      if (!recipient) return;
      openChat({
        recipientKind: recipient.kind,
        recipientId: recipient.id,
      });
    }

    function showDesktopNotification(title, body, message, data) {
      if (!chatNotificationApiSupported()) return;
      if (chatNotificationPermission() !== "granted") return;
      const recipientKey = chatRecipientKeyFromMessage(message) || "incoming";
      try {
        const note = new Notification(String(title || "Incoming chat"), {
          body: String(body || ""),
          tag: `meshtracer-chat-${recipientKey}`,
        });
        note.onclick = () => {
          try {
            window.focus();
          } catch (_e) {
          }
          openChatForMessage(message);
          try {
            note.close();
          } catch (_e) {
          }
        };
      } catch (_e) {
      }
    }

    function showDesktopNotificationSummary(messages, data) {
      const total = Array.isArray(messages) ? messages.length : 0;
      if (total <= 0) return;
      const first = messages[0];
      const sender = chatMessageSenderLabel(first);
      const preview = chatMessagePreviewText(first);
      const suffix = total > 1 ? `${total} new messages` : "1 new message";
      showDesktopNotification(suffix, `${sender}: ${preview}`, first, data);
    }

    function playChatNotificationSound() {
      const AudioCtx = window.AudioContext || window.webkitAudioContext;
      if (typeof AudioCtx !== "function") return;
      try {
        if (!state.chatAudioCtx) {
          state.chatAudioCtx = new AudioCtx();
        }
        const ctx = state.chatAudioCtx;
        if (ctx.state === "suspended") {
          ctx.resume().catch(() => {});
        }
        const now = ctx.currentTime;
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.type = "triangle";
        osc.frequency.setValueAtTime(880, now);
        gain.gain.setValueAtTime(0.0001, now);
        gain.gain.exponentialRampToValueAtTime(0.055, now + 0.01);
        gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.2);
        osc.connect(gain);
        gain.connect(ctx.destination);
        osc.start(now);
        osc.stop(now + 0.2);
      } catch (_e) {
      }
    }

    function shouldNotifyForMessage(message) {
      if (!message || typeof message !== "object") return false;
      if (chatMessageMatchesActiveRecipient(message)) return false;
      return shouldPlayNotificationWhileFocused();
    }

    async function enableDesktopNotifications(enabled) {
      const previousEnabled = Boolean(state.chatNotificationSettings.desktop);
      const nextEnabled = Boolean(enabled);
      if (!nextEnabled) {
        state.chatNotificationSettings.desktop = false;
        applyChatNotificationSettingsToForm();
        if (previousEnabled) markConfigDirty();
        return;
      }
      if (!chatNotificationApiSupported()) {
        state.chatNotificationSettings.desktop = false;
        applyChatNotificationSettingsToForm();
        return;
      }
      if (chatNotificationPermission() === "denied") {
        state.chatNotificationSettings.desktop = false;
        applyChatNotificationSettingsToForm();
        return;
      }
      if (chatNotificationPermission() !== "granted") {
        try {
          const permission = await Notification.requestPermission();
          if (String(permission || "").toLowerCase() !== "granted") {
            state.chatNotificationSettings.desktop = false;
            applyChatNotificationSettingsToForm();
            return;
          }
        } catch (_e) {
          state.chatNotificationSettings.desktop = false;
          applyChatNotificationSettingsToForm();
          return;
        }
      }
      state.chatNotificationSettings.desktop = true;
      applyChatNotificationSettingsToForm();
      if (!previousEnabled) markConfigDirty();
    }

    async function fetchIncomingChatDeltas(data, options = {}) {
      const force = Boolean(options.force);
      const meshHost = activeMeshHostFromData(data);
      if (!meshHost) return;
      if (meshHost !== state.chatNotifyMeshHost) return;
      const chatRevision = chatRevisionFromData(data);
      if (!force && chatRevision <= state.chatNotifyCursor) return;

      const sinceChatId = Math.max(0, Math.trunc(Number(state.chatNotifyCursor) || 0));
      const query = new URLSearchParams({
        since_chat_id: String(sinceChatId),
        limit: "500",
      });
      const response = await fetch(`/api/chat/incoming?${query.toString()}`, { cache: "no-store" });
      let body = null;
      try {
        body = await response.json();
      } catch (_e) {
      }
      if (!response.ok || !body || body.ok === false) {
        return;
      }

      const messages = Array.isArray(body.messages) ? body.messages : [];
      let highestSeenChatId = Math.max(
        sinceChatId,
        numericRevision(body.chat_revision, chatRevision),
      );
      const notifyCandidates = [];
      let unreadIncrement = 0;
      for (const message of messages) {
        const chatId = chatMessageId(message);
        if (chatId > highestSeenChatId) highestSeenChatId = chatId;
        const direction = String(message && message.direction || "").trim().toLowerCase();
        if (direction !== "incoming") continue;
        if (chatMessageMatchesActiveRecipient(message)) continue;
        unreadIncrement += 1;
        if (!shouldNotifyForMessage(message)) continue;
        notifyCandidates.push(message);
      }

      state.chatNotifyCursor = Math.max(state.chatNotifyCursor, highestSeenChatId);
      saveChatNotificationCursor(meshHost, state.chatNotifyCursor);

      if (unreadIncrement > 0) {
        state.chatUnreadCount = Math.max(0, state.chatUnreadCount + unreadIncrement);
        updateChatOpenButton();
      }

      if (notifyCandidates.length <= 0) return;
      if (state.chatNotificationSettings.sound) {
        playChatNotificationSound();
      }
      if (!state.chatNotificationSettings.desktop) return;
      if (chatNotificationPermission() !== "granted") return;
      if (notifyCandidates.length === 1) {
        const message = notifyCandidates[0];
        showDesktopNotification(
          chatNotificationTitleForMessage(message, data),
          chatMessagePreviewText(message),
          message,
          data,
        );
        return;
      }
      showDesktopNotificationSummary(notifyCandidates, data);
    }

    function scheduleIncomingChatDeltaFetch(data, options = {}) {
      if (state.chatIncomingBusy) {
        state.chatIncomingPending = true;
        return;
      }
      state.chatIncomingBusy = true;
      fetchIncomingChatDeltas(data, options)
        .catch((_e) => {})
        .finally(() => {
          state.chatIncomingBusy = false;
          if (!state.chatIncomingPending) return;
          state.chatIncomingPending = false;
          scheduleIncomingChatDeltaFetch(state.lastServerData || data, { force: false });
        });
    }

    function positionChatModal() {
      if (!chatModal || chatModal.classList.contains("hidden")) return;
      let top = 12;
      if (traceDetails && !traceDetails.classList.contains("hidden")) {
        const detailsRect = traceDetails.getBoundingClientRect();
        if (Number.isFinite(detailsRect.bottom) && detailsRect.bottom > 0) {
          top = Math.max(top, Math.round(detailsRect.bottom + 10));
        }
      }
      const maxTop = Math.max(12, window.innerHeight - 200);
      top = Math.min(top, maxTop);
      chatModal.style.top = `${top}px`;
      const maxHeight = Math.max(180, window.innerHeight - top - 12);
      chatModal.style.maxHeight = `${maxHeight}px`;
    }

    function renderChatPanel(data = state.lastServerData) {
      if (!chatModal || !chatRecipient || !chatMessages || !chatInput || !chatSend) return;
      if (!state.chatOpen) {
        chatModal.classList.add("hidden");
        return;
      }
      chatModal.classList.remove("hidden");
      positionChatModal();

      const recipients = normalizeChatRecipient(data);
      const selectedKey = chatRecipientKey(state.chatRecipientKind, state.chatRecipientId);
      const channelNames = chatChannelNamesFromData(data);

      const channelsOptionsHtml = recipients.channels.map((channelIndex) => {
        const value = chatRecipientKey("channel", channelIndex);
        const selectedAttr = selectedKey === value ? "selected" : "";
        return `<option value="${escapeHtml(value)}" ${selectedAttr}>${escapeHtml(chatChannelLabel(channelIndex, channelNames))}</option>`;
      }).join("");

      const recentOptionsHtml = recipients.recentDirect.map((nodeNum) => {
        const value = chatRecipientKey("direct", nodeNum);
        const selectedAttr = selectedKey === value ? "selected" : "";
        return `<option value="${escapeHtml(value)}" ${selectedAttr}>${escapeHtml(chatNodeLabel(nodeNum))}</option>`;
      }).join("");

      const otherOptionsHtml = recipients.otherDirect.map((nodeNum) => {
        const value = chatRecipientKey("direct", nodeNum);
        const selectedAttr = selectedKey === value ? "selected" : "";
        return `<option value="${escapeHtml(value)}" ${selectedAttr}>${escapeHtml(chatNodeLabel(nodeNum))}</option>`;
      }).join("");

      chatRecipient.innerHTML = `
        <optgroup label="Channels">
          ${channelsOptionsHtml}
        </optgroup>
        <optgroup label="Recently Messaged Nodes">
          ${recentOptionsHtml || `<option disabled>(none)</option>`}
        </optgroup>
        <optgroup label="Other Nodes">
          ${otherOptionsHtml || `<option disabled>(none)</option>`}
        </optgroup>
      `;
      chatRecipient.value = selectedKey;

      if (chatRecipientWarning) {
        let warningText = "";
        const recipientKind = String(state.chatRecipientKind || "").trim().toLowerCase();
        const recipientId = Math.trunc(Number(state.chatRecipientId) || 0);
        if (recipientKind === "direct" && recipientId > 0) {
          const node = state.nodeByNum.get(recipientId);
          if (node && flagIsTrue(node.is_unmessagable)) {
            warningText = `${chatNodeLabel(recipientId)} is marked unmessagable. Direct messages may fail.`;
          }
        }
        chatRecipientWarning.textContent = warningText;
        chatRecipientWarning.classList.toggle("visible", Boolean(warningText));
      }

      const messages = Array.isArray(state.chatMessages) ? state.chatMessages : [];
      if (!messages.length) {
        chatMessages.innerHTML = '<div class="chat-empty">No messages yet for this recipient.</div>';
      } else {
        chatMessages.innerHTML = messages.map((message) => {
          const direction = String(message && message.direction || "").trim().toLowerCase() === "outgoing"
            ? "outgoing"
            : "incoming";
          const text = String(message && message.text || "");
          const fromNodeNum = Number(message && message.from_node_num);
          const senderNodeNum = Number.isFinite(fromNodeNum) ? Math.trunc(fromNodeNum) : null;
          const senderLabel = senderNodeNum !== null
            ? chatNodeLabel(senderNodeNum)
            : "Node";
          const senderHtml = direction === "outgoing"
            ? "You"
            : senderNodeNum !== null
              ? `<button type="button" class="chat-node-link" data-chat-node-num="${senderNodeNum}" title="Open node details">${escapeHtml(senderLabel)}</button>`
              : escapeHtml(senderLabel);
          const createdAtText = escapeHtml(chatMetaText(message));
          return `
            <div class="chat-row ${direction}">
              <div class="chat-bubble">${escapeHtml(text)}</div>
              <div class="chat-meta">${senderHtml} | ${createdAtText}</div>
            </div>
          `;
        }).join("");
      }
      for (const nodeBtn of chatMessages.querySelectorAll("button[data-chat-node-num]")) {
        nodeBtn.addEventListener("click", () => {
          const nodeNum = Number(nodeBtn.dataset.chatNodeNum);
          if (!Number.isFinite(nodeNum)) return;
          focusNode(Math.trunc(nodeNum), {
            switchToNodesTab: true,
            scrollNodeListIntoView: true,
          });
        });
      }

      const connected = Boolean(data && data.connected);
      chatSend.disabled = state.chatSendBusy || !connected;
      chatInput.disabled = !connected;
      if (!connected && !state.chatStatusMessage) {
        setChatStatus("Connect to a node to send messages.", { error: false });
      }
      if (chatStatus) {
        chatStatus.textContent = state.chatStatusMessage;
        chatStatus.classList.toggle("error", state.chatStatusError);
      }

      if (state.chatLoading) {
        setChatStatus("Loading messages...", { error: false });
      }
    }

    async function loadChatMessages(options = {}) {
      if (!state.chatOpen) return;
      if (state.chatLoading) return;
      const force = Boolean(options.force);
      const recipientKind = String(state.chatRecipientKind || "").trim().toLowerCase();
      const recipientId = Math.trunc(Number(state.chatRecipientId) || 0);
      if (recipientKind !== "channel" && recipientKind !== "direct") return;

      const data = state.lastServerData;
      const currentRevision = chatRevisionFromData(data);
      const loadKey = chatRecipientKey(recipientKind, recipientId);
      if (!force && state.chatLoadedKey === loadKey && state.lastChatRevision === currentRevision) {
        return;
      }

      state.chatLoading = true;
      renderChatPanel(data);
      try {
        const query = new URLSearchParams({
          recipient_kind: recipientKind,
          recipient_id: String(recipientId),
          limit: "400",
        });
        const response = await fetch(`/api/chat/messages?${query.toString()}`, { cache: "no-store" });
        let body = null;
        try {
          body = await response.json();
        } catch (_e) {
        }
        if (!response.ok || !body || body.ok === false) {
          const detail = body && (body.detail || body.error)
            ? String(body.detail || body.error)
            : "failed to load messages";
          setChatStatus(detail, { error: true });
          return;
        }
        state.chatMessages = Array.isArray(body.messages) ? body.messages : [];
        state.chatLoadedKey = loadKey;
        setChatStatus("", { error: false });
      } catch (e) {
        setChatStatus(String(e || "failed to load messages"), { error: true });
      } finally {
        state.chatLoading = false;
        renderChatPanel(data);
        try {
          if (chatMessages) {
            chatMessages.scrollTop = chatMessages.scrollHeight;
          }
        } catch (_e) {
        }
      }
    }

    async function sendChatMessage() {
      if (state.chatSendBusy) return;
      const inputValue = String(chatInput && chatInput.value || "");
      const text = inputValue.trim();
      if (!text) return;
      const recipientKind = String(state.chatRecipientKind || "").trim().toLowerCase();
      const recipientId = Math.trunc(Number(state.chatRecipientId) || 0);
      if (recipientKind !== "channel" && recipientKind !== "direct") return;

      state.chatSendBusy = true;
      setChatStatus("Sending message...", { error: false });
      renderChatPanel(state.lastServerData);
      try {
        const { ok, body } = await apiPost("/api/chat/send", {
          recipient_kind: recipientKind,
          recipient_id: recipientId,
          text,
        });
        if (!ok) {
          const detail = body && (body.detail || body.error)
            ? String(body.detail || body.error)
            : "failed to send message";
          setChatStatus(detail, { error: true });
          return;
        }
        if (chatInput && String(chatInput.value || "") === inputValue) {
          chatInput.value = "";
        }
        setChatStatus("Message sent.", { error: false });
        if (body && body.snapshot && typeof body.snapshot === "object") {
          applySnapshot(body.snapshot, { force: true });
        } else {
          await refresh({ force: true });
        }
      } catch (e) {
        setChatStatus(String(e || "failed to send message"), { error: true });
      } finally {
        state.chatSendBusy = false;
        renderChatPanel(state.lastServerData);
      }
    }

    function openChat(options = {}) {
      const recipientKindRaw = String(options.recipientKind || "").trim().toLowerCase();
      const recipientIdValue = Number(options.recipientId);
      if ((recipientKindRaw === "channel" || recipientKindRaw === "direct") && Number.isFinite(recipientIdValue)) {
        state.chatRecipientKind = recipientKindRaw;
        state.chatRecipientId = Math.trunc(recipientIdValue);
      }
      const nodeNumValue = Number(options.nodeNum);
      if (Number.isFinite(nodeNumValue)) {
        state.chatRecipientKind = "direct";
        state.chatRecipientId = Math.trunc(nodeNumValue);
      }
      state.chatOpen = true;
      state.chatUnreadCount = 0;
      updateChatOpenButton();
      setChatStatus("", { error: false });
      renderChatPanel(state.lastServerData);
      loadChatMessages({ force: true });
      try {
        if (chatInput) chatInput.focus();
      } catch (_e) {
      }
    }

    function closeChat() {
      state.chatOpen = false;
      updateChatOpenButton();
      renderChatPanel(state.lastServerData);
      try {
        if (chatOpen) chatOpen.focus();
      } catch (_e) {
      }
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
    if (traceDetailsNodeChat) {
      traceDetailsNodeChat.addEventListener("click", () => {
        const nodeNum = Number(state.selectedNodeNum);
        if (!Number.isFinite(nodeNum)) return;
        openChat({ nodeNum });
      });
    }
    map.on("click", () => {
      collapseSpiderGroup();
    });
    map.on("zoomstart", () => {
      collapseSpiderGroup({ skipSelectionVisual: true });
    });
    map.on("movestart", () => {
      collapseSpiderGroup({ skipSelectionVisual: true });
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
    for (const [key, input] of [
      ["traceroute", logFilterTraceroute],
      ["telemetry", logFilterTelemetry],
      ["messaging", logFilterMessaging],
      ["position", logFilterPosition],
      ["node_info", logFilterNodeInfo],
      ["other", logFilterOther],
    ]) {
      if (!input) continue;
      input.addEventListener("change", () => {
        setLogTypeFilter(key, Boolean(input.checked));
      });
    }
    if (chatRecipient) {
      chatRecipient.addEventListener("change", () => {
        const value = String(chatRecipient.value || "");
        const parts = value.split(":");
        const kind = String(parts[0] || "").trim().toLowerCase();
        const idValue = Number(parts[1]);
        if ((kind !== "channel" && kind !== "direct") || !Number.isFinite(idValue)) {
          return;
        }
        state.chatRecipientKind = kind;
        state.chatRecipientId = Math.trunc(idValue);
        state.chatMessages = [];
        state.chatLoadedKey = "";
        renderChatPanel(state.lastServerData);
        loadChatMessages({ force: true });
      });
    }
    if (chatSend) {
      chatSend.addEventListener("click", () => {
        sendChatMessage();
      });
    }
    if (chatInput) {
      chatInput.addEventListener("keydown", (event) => {
        if (event.key !== "Enter") return;
        event.preventDefault();
        sendChatMessage();
      });
    }
    if (chatOpen) {
      chatOpen.addEventListener("click", () => {
        if (state.chatOpen) {
          closeChat();
        } else {
          openChat();
        }
      });
    }
    if (chatClose) {
      chatClose.addEventListener("click", () => closeChat());
    }
    window.addEventListener("resize", () => {
      positionChatModal();
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

    function formatTelemetryKey(keyRaw) {
      const key = String(keyRaw || "").trim();
      if (!key) return "";
      const spaced = key
        .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
        .replace(/_/g, " ")
        .trim();
      return spaced.charAt(0).toUpperCase() + spaced.slice(1);
    }

    function formatTelemetryValue(value) {
      if (value === null || value === undefined) return "-";
      if (typeof value === "number") {
        if (!Number.isFinite(value)) return "-";
        const rounded = Math.round(value * 1000) / 1000;
        return Number.isInteger(rounded) ? String(Math.trunc(rounded)) : String(rounded);
      }
      if (typeof value === "boolean") return value ? "true" : "false";
      if (typeof value === "object") {
        try {
          return JSON.stringify(value);
        } catch (_e) {
          return String(value);
        }
      }
      return String(value);
    }

    function formatNodeRole(value) {
      const text = String(value || "").trim();
      if (!text) return "-";
      return text.replace(/_/g, " ");
    }

    function formatNodeFlag(value, options = {}) {
      const unknownText = Object.prototype.hasOwnProperty.call(options || {}, "unknownText")
        ? String(options.unknownText)
        : "-";
      if (value === true) return "Yes";
      if (value === false) return "No";
      if (typeof value === "number") {
        if (!Number.isFinite(value)) return unknownText;
        return value !== 0 ? "Yes" : "No";
      }
      const text = String(value ?? "").trim().toLowerCase();
      if (!text) return unknownText;
      if (["1", "true", "yes", "y", "on"].includes(text)) return "Yes";
      if (["0", "false", "no", "n", "off"].includes(text)) return "No";
      return unknownText;
    }

    function formatNodePublicKey(value) {
      const text = String(value || "").trim();
      if (!text) return "-";
      if (text.length <= 26) return text;
      return `${text.slice(0, 12)}...${text.slice(-10)}`;
    }

    function telemetryStatusKey(nodeNum, telemetryType) {
      const nodeNumInt = Math.trunc(Number(nodeNum));
      const type = String(telemetryType || "").trim().toLowerCase();
      return `${nodeNumInt}:${type}`;
    }

    function telemetryRequestStatus(nodeNum, telemetryType) {
      const key = telemetryStatusKey(nodeNum, telemetryType);
      const status = state.telemetryRequestState && state.telemetryRequestState[key];
      if (!status || typeof status !== "object") return null;
      return status;
    }

    function setTelemetryRequestStatus(nodeNum, telemetryType, status) {
      const key = telemetryStatusKey(nodeNum, telemetryType);
      if (!state.telemetryRequestState || typeof state.telemetryRequestState !== "object") {
        state.telemetryRequestState = {};
      }
      if (!status) {
        delete state.telemetryRequestState[key];
        return;
      }
      state.telemetryRequestState[key] = {
        busy: Boolean(status.busy),
        error: Boolean(status.error),
        message: String(status.message || ""),
      };
    }

    function nodeDetailsTabValue(rawTab) {
      const value = String(rawTab || "").trim().toLowerCase();
      if (
        value === "telemetry"
        || value === "position"
        || value === "node_info"
        || value === "traceroutes"
      ) return value;
      if (value === "device" || value === "environment" || value === "power") return "telemetry";
      return "node_info";
    }

    function nodeDetailsTabLabel(tabValue) {
      if (tabValue === "telemetry") return "Telemetry";
      if (tabValue === "position") return "Position";
      if (tabValue === "traceroutes") return "Traceroutes";
      return "Node Info";
    }

    function nodeTelemetryTabValue(rawTab) {
      const value = String(rawTab || "").trim().toLowerCase();
      if (value === "power") return "power";
      if (value === "environment") return "environment";
      return "device";
    }

    function nodeTelemetryTabLabel(tabValue) {
      if (tabValue === "power") return "Power";
      if (tabValue === "environment") return "Environment";
      return "Device";
    }

    function renderInfoRows(rows, emptyMessage = "No data yet.") {
      const list = Array.isArray(rows)
        ? rows.filter((row) => row && String(row.key || "").trim())
        : [];
      if (!list.length) {
        return `<div class="telemetry-empty">${escapeHtml(emptyMessage)}</div>`;
      }
      return `
        <div class="node-info-grid">
          ${list.map((row) => {
            const key = String(row.key || "").trim();
            const value = row.value === undefined || row.value === null || String(row.value) === ""
              ? "-"
              : String(row.value);
            const title = row.title === undefined || row.title === null ? "" : String(row.title);
            const titleAttr = title ? ` title="${escapeHtml(title)}"` : "";
            return `
              <div class="node-info-key">${escapeHtml(key)}</div>
              <div class="node-info-value"${titleAttr}>${escapeHtml(value)}</div>
            `;
          }).join("")}
        </div>
      `;
    }

    function renderTelemetryRows(telemetry, options = {}) {
      const emptyMessage = options && options.emptyMessage
        ? String(options.emptyMessage)
        : "No telemetry received yet.";
      if (!telemetry || typeof telemetry !== "object") {
        return `<div class="telemetry-empty">${escapeHtml(emptyMessage)}</div>`;
      }
      const keys = Object.keys(telemetry).sort((a, b) => a.localeCompare(b));
      if (!keys.length) {
        return `<div class="telemetry-empty">${escapeHtml(emptyMessage)}</div>`;
      }
      return `
        <div class="telemetry-grid">
          ${keys.map((key) => `
            <div class="telemetry-key">${escapeHtml(formatTelemetryKey(key))}</div>
            <div class="telemetry-value">${escapeHtml(formatTelemetryValue(telemetry[key]))}</div>
          `).join("")}
        </div>
      `;
    }

    async function requestNodeTelemetry(nodeNum, telemetryType) {
      const nodeNumInt = Math.trunc(Number(nodeNum));
      const type = String(telemetryType || "").trim().toLowerCase();
      if (!Number.isFinite(nodeNumInt) || !type) return;
      if (state.selectedNodeNum !== nodeNumInt) return;
      const statusPrefix = type === "environment"
        ? "environment"
        : (type === "power" ? "power" : "device");

      setTelemetryRequestStatus(nodeNumInt, type, {
        busy: true,
        error: false,
        message: `Requesting ${statusPrefix} telemetry...`,
      });
      renderSelectionDetails();

      try {
        const { ok, body } = await apiPost("/api/telemetry/request", {
          node_num: nodeNumInt,
          telemetry_type: type,
        });
        if (!ok) {
          const detail = body && (body.detail || body.error)
            ? String(body.detail || body.error)
            : `failed to request ${statusPrefix} telemetry`;
          setTelemetryRequestStatus(nodeNumInt, type, {
            busy: false,
            error: true,
            message: detail,
          });
          renderSelectionDetails();
          return;
        }
        const detail = body && body.detail
          ? String(body.detail)
          : `requested ${statusPrefix} telemetry`;
        setTelemetryRequestStatus(nodeNumInt, type, {
          busy: false,
          error: false,
          message: detail,
        });
        renderSelectionDetails();
      } catch (e) {
        setTelemetryRequestStatus(nodeNumInt, type, {
          busy: false,
          error: true,
          message: String(e || `failed to request ${statusPrefix} telemetry`),
        });
        renderSelectionDetails();
      }
    }

    async function requestNodeInfo(nodeNum) {
      const nodeNumInt = Math.trunc(Number(nodeNum));
      if (!Number.isFinite(nodeNumInt)) return;
      if (state.selectedNodeNum !== nodeNumInt) return;

      setTelemetryRequestStatus(nodeNumInt, "node_info", {
        busy: true,
        error: false,
        message: "Requesting node info...",
      });
      renderSelectionDetails();

      try {
        const { ok, body } = await apiPost("/api/nodeinfo/request", {
          node_num: nodeNumInt,
        });
        if (!ok) {
          const detail = body && (body.detail || body.error)
            ? String(body.detail || body.error)
            : "failed to request node info";
          setTelemetryRequestStatus(nodeNumInt, "node_info", {
            busy: false,
            error: true,
            message: detail,
          });
          renderSelectionDetails();
          return;
        }
        const detail = body && body.detail
          ? String(body.detail)
          : "requested node info";
        setTelemetryRequestStatus(nodeNumInt, "node_info", {
          busy: false,
          error: false,
          message: detail,
        });
        renderSelectionDetails();
      } catch (e) {
        setTelemetryRequestStatus(nodeNumInt, "node_info", {
          busy: false,
          error: true,
          message: String(e || "failed to request node info"),
        });
        renderSelectionDetails();
      }
    }

    async function requestNodePosition(nodeNum) {
      const nodeNumInt = Math.trunc(Number(nodeNum));
      if (!Number.isFinite(nodeNumInt)) return;
      if (state.selectedNodeNum !== nodeNumInt) return;

      setTelemetryRequestStatus(nodeNumInt, "position", {
        busy: true,
        error: false,
        message: "Requesting position...",
      });
      renderSelectionDetails();

      try {
        const { ok, body } = await apiPost("/api/position/request", {
          node_num: nodeNumInt,
        });
        if (!ok) {
          const detail = body && (body.detail || body.error)
            ? String(body.detail || body.error)
            : "failed to request position";
          setTelemetryRequestStatus(nodeNumInt, "position", {
            busy: false,
            error: true,
            message: detail,
          });
          renderSelectionDetails();
          return;
        }
        const detail = body && body.detail
          ? String(body.detail)
          : "requested position";
        setTelemetryRequestStatus(nodeNumInt, "position", {
          busy: false,
          error: false,
          message: detail,
        });
        renderSelectionDetails();
      } catch (e) {
        setTelemetryRequestStatus(nodeNumInt, "position", {
          busy: false,
          error: true,
          message: String(e || "failed to request position"),
        });
        renderSelectionDetails();
      }
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

    function nodeCoordKey(node) {
      if (!hasCoord(node)) return "";
      return `${Number(node.lat).toFixed(7)},${Number(node.lon).toFixed(7)}`;
    }

    function spiderRadiusPx(count) {
      const n = Math.max(0, Math.trunc(Number(count) || 0));
      if (n <= 1) return SPIDERFY_RADIUS_MIN_PX;
      const radius = SPIDERFY_RADIUS_MIN_PX + (n - 1) * SPIDERFY_RADIUS_STEP_PX;
      return Math.min(SPIDERFY_RADIUS_MAX_PX, radius);
    }

    function spiderLayoutLatLngs(centerLatLng, count) {
      const n = Math.max(0, Math.trunc(Number(count) || 0));
      if (!centerLatLng || n <= 0) return [];
      const centerPoint = map.latLngToLayerPoint(centerLatLng);
      const angleStep = (Math.PI * 2) / n;
      const radius = spiderRadiusPx(n);
      const startAngle = -Math.PI / 2;
      const latLngs = [];
      for (let i = 0; i < n; i += 1) {
        const angle = startAngle + i * angleStep;
        const point = L.point(
          centerPoint.x + radius * Math.cos(angle),
          centerPoint.y + radius * Math.sin(angle)
        );
        latLngs.push(map.layerPointToLatLng(point));
      }
      return latLngs;
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

    function estimateNodePositions(nodes, _traces) {
      const nodeMap = new Map();
      for (const raw of Array.isArray(nodes) ? nodes : []) {
        const num = Number(raw?.num);
        if (!Number.isFinite(num)) continue;
        const node = {
          ...(raw || {}),
          num,
          estimated: Boolean(raw && raw.estimated),
        };
        if (!hasCoord(node)) {
          node.lat = null;
          node.lon = null;
        }
        nodeMap.set(num, node);
      }
      return nodeMap;
    }

    function normalizedLogTypeFilters(raw) {
      const source = raw && typeof raw === "object" ? raw : {};
      return {
        traceroute: source.traceroute !== false,
        telemetry: source.telemetry !== false,
        messaging: source.messaging !== false,
        position: source.position !== false,
        node_info: source.node_info !== false,
        other: source.other !== false,
      };
    }

    function logTypeFromEntry(entry) {
      const rawType = String(
        (entry && (entry.type || entry.log_type))
          ? (entry.type || entry.log_type)
          : ""
      )
        .trim()
        .toLowerCase()
        .replace(/[-\s]+/g, "_");
      if (rawType === "traceroute") return "traceroute";
      if (rawType === "telemetry") return "telemetry";
      if (rawType === "messaging") return "messaging";
      if (rawType === "position") return "position";
      if (rawType === "node_info") return "node_info";
      return "other";
    }

    function syncLogFilterControls() {
      if (logFilterTraceroute) logFilterTraceroute.checked = Boolean(state.logTypeFilters.traceroute);
      if (logFilterTelemetry) logFilterTelemetry.checked = Boolean(state.logTypeFilters.telemetry);
      if (logFilterMessaging) logFilterMessaging.checked = Boolean(state.logTypeFilters.messaging);
      if (logFilterPosition) logFilterPosition.checked = Boolean(state.logTypeFilters.position);
      if (logFilterNodeInfo) logFilterNodeInfo.checked = Boolean(state.logTypeFilters.node_info);
      if (logFilterOther) logFilterOther.checked = Boolean(state.logTypeFilters.other);
    }

    function persistLogTypeFilters() {
      try {
        localStorage.setItem(LOG_FILTER_STORAGE_KEY, JSON.stringify(state.logTypeFilters));
      } catch (_e) {
      }
    }

    function setLogTypeFilter(key, enabled) {
      const normalized = normalizedLogTypeFilters(state.logTypeFilters);
      if (!(key in normalized)) return;
      normalized[key] = Boolean(enabled);
      state.logTypeFilters = normalized;
      syncLogFilterControls();
      persistLogTypeFilters();
      const logs = Array.isArray(state.lastServerData && state.lastServerData.logs)
        ? state.lastServerData.logs
        : [];
      renderLogs(logs);
    }

    function renderLogs(logs) {
      const container = logList;
      if (!container) return;
      const entries = Array.isArray(logs) ? logs : [];
      const stickToBottom = container.scrollTop + container.clientHeight >= container.scrollHeight - 24;
      if (!entries.length) {
        container.innerHTML = '<div class="empty">No runtime logs yet.</div>';
        return;
      }
      const filtered = entries.filter((entry) => Boolean(state.logTypeFilters[logTypeFromEntry(entry)]));
      if (!filtered.length) {
        container.innerHTML = '<div class="empty">No logs match the selected type filters.</div>';
        return;
      }
      container.innerHTML = filtered.map((entry) => {
        const messageText = String(entry && entry.message ? entry.message : "");
        const streamClass = entry && entry.stream === "stderr" ? "stderr" : "";
        const logType = logTypeFromEntry(entry);
        const tracerouteSuccessClass = (
          logType === "traceroute"
          && messageText.toLowerCase().includes("traceroute complete")
        )
          ? " traceroute-success"
          : "";
        return `<div class="log-entry ${streamClass} log-${escapeHtml(logType)}${tracerouteSuccessClass}">${escapeHtml(messageText)}</div>`;
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
        const active = num === state.selectedNodeNum ? "active" : "";
        const locationText = hasPos ? "" : "No position";
        const locationSuffix = locationText ? ` | ${escapeHtml(locationText)}` : "";
        return `
          <button class="list-item ${active}" type="button" data-node-num="${num}">
            <span class="item-title">${escapeHtml(nodeLabel(node))}${hasPos ? "" : " (No position)"}</span>
            <span class="item-meta">${escapeHtml(node.long_name || "Unknown")} | ${escapeHtml(prettyAge(node, nowSec))}</span>
            <span class="item-meta">#${escapeHtml(node.num)} | ${escapeHtml(node.id || "-")}${locationSuffix}</span>
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
          if (traceDetailsNodeChat) traceDetailsNodeChat.classList.add("hidden");
          traceDetails.classList.add("hidden");
          traceDetailsBody.innerHTML = "";
          return;
        }
        if (traceDetailsNodeChat) traceDetailsNodeChat.classList.add("hidden");

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
        positionChatModal();
        return;
      }

      if (state.selectedNodeNum !== null) {
        const node = state.nodeByNum.get(state.selectedNodeNum);
        if (!node) {
          if (traceDetailsNodeChat) traceDetailsNodeChat.classList.add("hidden");
          traceDetails.classList.add("hidden");
          traceDetailsBody.innerHTML = "";
          return;
        }
        if (traceDetailsNodeChat) traceDetailsNodeChat.classList.remove("hidden");

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
        const rawSelectedNodeTab = String(state.selectedNodeDetailsTab || "").trim().toLowerCase();
        if (rawSelectedNodeTab === "device" || rawSelectedNodeTab === "environment" || rawSelectedNodeTab === "power") {
          state.selectedNodeTelemetryTab = rawSelectedNodeTab;
        }
        const selectedNodeTab = nodeDetailsTabValue(rawSelectedNodeTab);
        const selectedTelemetryTab = nodeTelemetryTabValue(state.selectedNodeTelemetryTab);
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

        const snrValue = Number(node.snr);
        const snrText = Number.isFinite(snrValue)
          ? `${Math.round(snrValue * 100) / 100} dB`
          : "-";
        const hopsAwayValue = Number(node.hops_away);
        const hopsAwayText = Number.isFinite(hopsAwayValue)
          ? String(Math.trunc(hopsAwayValue))
          : "-";
        const channelValue = Number(node.channel);
        const channelText = Number.isFinite(channelValue)
          ? String(Math.trunc(channelValue))
          : "-";
        const roleText = formatNodeRole(node.role);
        const viaMqttText = formatNodeFlag(node.via_mqtt, { unknownText: "No" });
        const favoriteText = formatNodeFlag(node.is_favorite, { unknownText: "No" });
        const ignoredText = formatNodeFlag(node.is_ignored, { unknownText: "No" });
        const mutedText = formatNodeFlag(node.is_muted, { unknownText: "No" });
        const keyVerifiedText = formatNodeFlag(node.is_key_manually_verified, { unknownText: "No" });
        const licensedText = formatNodeFlag(node.is_licensed, { unknownText: "No" });
        const unmessagableText = formatNodeFlag(node.is_unmessagable, { unknownText: "No" });
        const publicKeyText = formatNodePublicKey(node.public_key);
        const publicKeyTitle = String(node.public_key || "").trim();

        const tabItems = ["node_info", "traceroutes", "position", "telemetry"];
        const tabButtonsHtml = tabItems.map((tabValue) => {
          const activeClass = selectedNodeTab === tabValue ? "active" : "";
          return `
            <button
              class="node-detail-tab-btn ${activeClass}"
              type="button"
              data-node-tab="${escapeHtml(tabValue)}"
            >${escapeHtml(nodeDetailsTabLabel(tabValue))}</button>
          `;
        }).join("");

        let tabBodyHtml = "";
        if (selectedNodeTab === "node_info") {
          const nodeInfoRequestStatus = telemetryRequestStatus(selectedNodeNum, "node_info");
          const nodeInfoRequestBusy = Boolean(nodeInfoRequestStatus && nodeInfoRequestStatus.busy);
          const nodeInfoRequestMessage = nodeInfoRequestStatus
            ? String(nodeInfoRequestStatus.message || "")
            : "";
          const nodeInfoRequestError = Boolean(nodeInfoRequestStatus && nodeInfoRequestStatus.error);
          const nodeInfoRows = [
            { key: "Long Name", value: longName },
            { key: "ID", value: node.id || "-" },
            { key: "Hardware", value: node.hw_model || "-" },
            { key: "Role", value: roleText },
            { key: "Hops Away", value: hopsAwayText },
            { key: "SNR", value: snrText },
            { key: "Channel", value: channelText },
            { key: "Via MQTT", value: viaMqttText },
            { key: "Favorite", value: favoriteText },
            { key: "Ignored", value: ignoredText },
            { key: "Muted", value: mutedText },
            { key: "Key Verified", value: keyVerifiedText },
            { key: "Licensed", value: licensedText },
            { key: "Unmessagable", value: unmessagableText },
            { key: "Public Key", value: publicKeyText, title: publicKeyTitle },
          ];
          tabBodyHtml = `
            <div class="node-tab-panel">
              ${renderInfoRows(nodeInfoRows)}
            </div>
            <div class="trace-actions">
              <button id="requestNodeInfoBtn" class="trace-action-btn" type="button" ${nodeInfoRequestBusy || !canTraceNow ? "disabled" : ""}>
                ${escapeHtml(nodeInfoRequestBusy ? "Requesting Node Info..." : "Request Node Info")}
              </button>
              <span id="requestNodeInfoStatus" class="trace-action-status ${nodeInfoRequestError ? "error" : ""}">
                ${escapeHtml(nodeInfoRequestMessage || (canTraceNow ? "" : "Connect to a node to request node info."))}
              </span>
            </div>
          `;
        } else if (selectedNodeTab === "traceroutes") {
          tabBodyHtml = `
            <div class="node-recent-traces">
              <span class="node-recent-title">Recent Traceroutes</span>
              ${recentTraceSectionHtml}
            </div>
            <div class="trace-actions">
              <button id="traceNowBtn" class="trace-action-btn" type="button" ${traceDisabled}>Run Traceroute</button>
              <span id="traceNowStatus" class="trace-action-status">${escapeHtml(traceHint)}</span>
            </div>
          `;
        } else if (selectedNodeTab === "position") {
          const positionRequestStatus = telemetryRequestStatus(selectedNodeNum, "position");
          const positionRequestBusy = Boolean(positionRequestStatus && positionRequestStatus.busy);
          const positionRequestMessage = positionRequestStatus
            ? String(positionRequestStatus.message || "")
            : "";
          const positionRequestError = Boolean(positionRequestStatus && positionRequestStatus.error);
          const positionRaw = node.position && typeof node.position === "object"
            ? node.position
            : {};
          const positionData = {};
          const hiddenPositionKeys = new Set([
            "lat",
            "lon",
            "latitude",
            "longitude",
            "latitudei",
            "longitudei",
          ]);
          for (const [rawKey, rawValue] of Object.entries(positionRaw)) {
            const keyText = String(rawKey || "").trim();
            const keyLower = keyText.toLowerCase();
            const keyCompact = keyLower.replace(/_/g, "");
            if (keyLower === "raw") continue;
            if (hiddenPositionKeys.has(keyLower) || hiddenPositionKeys.has(keyCompact)) continue;
            positionData[rawKey] = rawValue;
          }
          const positionUpdatedAt = String(node.position_updated_at_utc || "");
          const positionSummaryRows = [
            { key: "Source", value: locKind },
            { key: "Latitude", value: lat },
            { key: "Longitude", value: lon },
            { key: "Last Heard UTC", value: formatEpochUtc(node.last_heard) },
          ];

          tabBodyHtml = `
            <div class="node-tab-panel">
              <div class="telemetry-updated">Last updated: ${escapeHtml(positionUpdatedAt || "-")}</div>
              ${renderInfoRows(positionSummaryRows)}
              ${renderTelemetryRows(positionData, { emptyMessage: "No position payload received yet." })}
            </div>
            <div class="trace-actions">
              <button
                id="requestPositionBtn"
                class="trace-action-btn"
                type="button"
                ${positionRequestBusy || !canTraceNow ? "disabled" : ""}
              >${escapeHtml(positionRequestBusy ? "Requesting Position..." : "Request Position")}</button>
              <span id="requestPositionStatus" class="trace-action-status ${positionRequestError ? "error" : ""}">
                ${escapeHtml(positionRequestMessage || (canTraceNow ? "" : "Connect to a node to request position."))}
              </span>
            </div>
          `;
        } else if (selectedNodeTab === "telemetry") {
          const telemetryType = selectedTelemetryTab === "environment"
            ? "environment"
            : (selectedTelemetryTab === "power" ? "power" : "device");
          const telemetryLabel = telemetryType === "environment"
            ? "Environment"
            : (telemetryType === "power" ? "Power" : "Device");
          const telemetryButtonLabel = telemetryType === "environment"
            ? "Request Environment Telemetry"
            : (telemetryType === "power" ? "Request Power Telemetry" : "Request Device Telemetry");
          const telemetryTabItems = ["device", "environment", "power"];
          const telemetryTabButtonsHtml = telemetryTabItems.map((tabValue) => {
            const activeClass = selectedTelemetryTab === tabValue ? "active" : "";
            return `
              <button
                class="node-detail-tab-btn ${activeClass}"
                type="button"
                data-node-telemetry-tab="${escapeHtml(tabValue)}"
              >${escapeHtml(nodeTelemetryTabLabel(tabValue))}</button>
            `;
          }).join("");
          const telemetryData = telemetryType === "environment"
            ? node.environment_telemetry
            : (telemetryType === "power" ? node.power_telemetry : node.device_telemetry);
          const telemetryUpdatedAt = telemetryType === "environment"
            ? String(node.environment_telemetry_updated_at_utc || "")
            : (telemetryType === "power"
              ? String(node.power_telemetry_updated_at_utc || "")
              : String(node.device_telemetry_updated_at_utc || ""));
          const requestStatus = telemetryRequestStatus(selectedNodeNum, telemetryType);
          const requestBusy = Boolean(requestStatus && requestStatus.busy);
          const requestMessage = requestStatus ? String(requestStatus.message || "") : "";
          const requestError = Boolean(requestStatus && requestStatus.error);

          tabBodyHtml = `
            <div class="node-detail-tabs">
              ${telemetryTabButtonsHtml}
            </div>
            <div class="node-tab-panel">
              <div class="telemetry-updated">Last updated: ${escapeHtml(telemetryUpdatedAt || "-")}</div>
              ${renderTelemetryRows(telemetryData)}
            </div>
            <div class="trace-actions">
              <button
                id="requestTelemetryBtn"
                class="trace-action-btn"
                type="button"
                data-telemetry-type="${escapeHtml(telemetryType)}"
                ${requestBusy || !canTraceNow ? "disabled" : ""}
              >${escapeHtml(requestBusy ? `Requesting ${telemetryLabel}...` : telemetryButtonLabel)}</button>
              <span id="requestTelemetryStatus" class="trace-action-status ${requestError ? "error" : ""}">
                ${escapeHtml(requestMessage || (canTraceNow ? "" : "Connect to a node to request telemetry."))}
              </span>
            </div>
          `;
        } else {
          tabBodyHtml = `
            <div class="node-tab-panel">
              <div class="telemetry-empty">No details available for this tab.</div>
            </div>
          `;
        }

        const nodeSummaryRows = [
          { key: "Name", value: nodeLabel(node) },
          { key: "Node", value: `#${node.num || "?"}` },
          { key: "Last Heard", value: prettyAge(node, nowSec) },
          { key: "Last Heard UTC", value: formatEpochUtc(node.last_heard) },
        ];
        traceDetailsTitle.textContent = "Node Details";
        traceDetailsBody.innerHTML = `
          <div class="node-summary-panel">
            ${renderInfoRows(nodeSummaryRows)}
          </div>
          <div class="node-detail-tabs">
            ${tabButtonsHtml}
          </div>
          ${tabBodyHtml}
        `;
        for (const tabBtn of traceDetailsBody.querySelectorAll("button[data-node-tab]")) {
          tabBtn.addEventListener("click", () => {
            const tabValue = nodeDetailsTabValue(tabBtn.dataset.nodeTab);
            if (state.selectedNodeDetailsTab === tabValue) return;
            state.selectedNodeDetailsTab = tabValue;
            renderSelectionDetails();
          });
        }
        for (const telemetryTabBtn of traceDetailsBody.querySelectorAll("button[data-node-telemetry-tab]")) {
          telemetryTabBtn.addEventListener("click", () => {
            const tabValue = nodeTelemetryTabValue(telemetryTabBtn.dataset.nodeTelemetryTab);
            if (state.selectedNodeTelemetryTab === tabValue && state.selectedNodeDetailsTab === "telemetry") return;
            state.selectedNodeDetailsTab = "telemetry";
            state.selectedNodeTelemetryTab = tabValue;
            renderSelectionDetails();
          });
        }

        if (selectedNodeTab === "node_info") {
          const requestNodeInfoBtn = document.getElementById("requestNodeInfoBtn");
          if (requestNodeInfoBtn) {
            requestNodeInfoBtn.addEventListener("click", () => {
              const nodeNum = Number(node.num);
              if (!Number.isFinite(nodeNum)) return;
              requestNodeInfo(nodeNum);
            });
          }
        } else if (selectedNodeTab === "traceroutes") {
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
        } else if (selectedNodeTab === "position") {
          const requestPositionBtn = document.getElementById("requestPositionBtn");
          if (requestPositionBtn) {
            requestPositionBtn.addEventListener("click", () => {
              const nodeNum = Number(node.num);
              if (!Number.isFinite(nodeNum)) return;
              requestNodePosition(nodeNum);
            });
          }
        } else if (selectedNodeTab === "telemetry") {
          const requestBtn = document.getElementById("requestTelemetryBtn");
          if (requestBtn) {
            requestBtn.addEventListener("click", () => {
              const nodeNum = Number(node.num);
              const type = String(requestBtn.dataset.telemetryType || "");
              if (!Number.isFinite(nodeNum) || !type) return;
              requestNodeTelemetry(nodeNum, type);
            });
          }
        }
        traceDetails.classList.remove("hidden");
        positionChatModal();
        return;
      }

      if (traceDetailsNodeChat) traceDetailsNodeChat.classList.add("hidden");
      traceDetails.classList.add("hidden");
      traceDetailsBody.innerHTML = "";
      positionChatModal();
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
      const recent = Array.isArray(traces) ? traces.slice().reverse() : [];
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

    function collapseSpiderGroup(options = {}) {
      const key = String(state.activeSpiderGroupKey || "");
      if (!key) {
        spiderLayer.clearLayers();
        return;
      }
      const group = state.spiderGroups.get(key);
      if (group && Array.isArray(group.markers)) {
        for (const item of group.markers) {
          if (!item || !item.marker) continue;
          item.marker.setLatLng(group.centerLatLng);
          item.marker.setZIndexOffset(0);
        }
      }
      spiderLayer.clearLayers();
      state.activeSpiderGroupKey = null;
      if (!options.skipSelectionVisual) {
        applyNodeSelectionVisual();
      }
    }

    function expandSpiderGroup(coordKey) {
      const key = String(coordKey || "");
      if (!key) return false;
      const group = state.spiderGroups.get(key);
      if (!group || !Array.isArray(group.markers) || group.markers.length <= 1) return false;
      if (state.activeSpiderGroupKey === key) return true;

      collapseSpiderGroup({ skipSelectionVisual: true });

      const targets = spiderLayoutLatLngs(group.centerLatLng, group.markers.length);
      spiderLayer.clearLayers();
      for (let i = 0; i < group.markers.length; i += 1) {
        const item = group.markers[i];
        const marker = item && item.marker ? item.marker : null;
        if (!marker) continue;
        const target = targets[i] || group.centerLatLng;
        marker.setLatLng(target);
        marker.setZIndexOffset(1000 + i);
        L.polyline([group.centerLatLng, target], {
          color: "#8fa7d0",
          weight: 1.25,
          opacity: 0.78,
          dashArray: "2 4",
          interactive: false,
        }).addTo(spiderLayer);
      }
      state.activeSpiderGroupKey = key;
      applyNodeSelectionVisual();
      return true;
    }

    function handleMapMarkerClick(nodeNum, coordKey) {
      const key = String(coordKey || "");
      const group = key ? state.spiderGroups.get(key) : null;
      const overlap = Boolean(group && Array.isArray(group.markers) && group.markers.length > 1);
      const isExpanded = overlap && state.activeSpiderGroupKey === key;

      if (overlap && !isExpanded) {
        expandSpiderGroup(key);
        return;
      }
      focusNode(nodeNum, { switchToNodesTab: true, scrollNodeListIntoView: true, panZoom: false });
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
      state.selectedNodeDetailsTab = "node_info";
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

      const previousSpiderKey = String(state.activeSpiderGroupKey || "");
      collapseSpiderGroup({ skipSelectionVisual: true });
      markerLayer.clearLayers();
      edgeLayer.clearLayers();
      spiderLayer.clearLayers();
      state.markerByNum.clear();
      state.edgePolylinesByTrace.clear();
      state.spiderGroups.clear();

      const bounds = [];
      const nowSec = Date.now() / 1000;
      const displayNodes = Array.from(state.nodeByNum.values());
      const visibleTraceNodes = selectedTraceNodeNums();
      const overlapGroups = new Map();

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
        const coordKey = nodeCoordKey(node);
        if (coordKey) {
          if (!overlapGroups.has(coordKey)) {
            overlapGroups.set(coordKey, {
              centerLatLng: L.latLng(node.lat, node.lon),
              markers: [],
            });
          }
          overlapGroups.get(coordKey).markers.push({
            nodeNum,
            marker,
          });
        }
        marker.on("click", () => {
          handleMapMarkerClick(nodeNum, coordKey);
        });
        state.markerByNum.set(nodeNum, marker);
      }

      for (const [coordKey, group] of overlapGroups.entries()) {
        if (!group || !Array.isArray(group.markers) || group.markers.length <= 1) continue;
        group.markers.sort((a, b) => Number(a.nodeNum) - Number(b.nodeNum));
        state.spiderGroups.set(coordKey, group);
      }
      if (previousSpiderKey && state.spiderGroups.has(previousSpiderKey)) {
        expandSpiderGroup(previousSpiderKey);
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
      renderChatPanel(data);
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
      const chatRevision = chatRevisionFromData(data);
      syncChatNotificationPartition(data);
      const chatChanged = force || chatRevision !== state.lastChatRevision;
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
        renderChatPanel(data);
      }

      if (queueModal && !queueModal.classList.contains("hidden")) {
        renderQueueModal(data);
      }

      if (chatChanged) {
        scheduleIncomingChatDeltaFetch(data, { force: force && state.chatNotifyCursor < chatRevision });
      }
      state.lastChatRevision = chatRevision;
      if (state.chatOpen && chatChanged) {
        loadChatMessages({ force: true });
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
        connectStatus.textContent = "Meshtracer runs locally and connects to your node over TCP (LAN) or BLE (Bluetooth).";
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
      const scanPhase = String((discovery && discovery.scan_phase) || "").trim().toLowerCase();
      const networks = Array.isArray(discovery && discovery.networks) ? discovery.networks : [];
      const port = Number((discovery && discovery.port) || 4403);
      const candidates = Array.isArray(discovery && discovery.candidates) ? discovery.candidates : [];
      const bleCandidates = Array.isArray(discovery && discovery.ble_candidates)
        ? discovery.ble_candidates
        : [];
      const done = Number((discovery && discovery.progress_done) || 0);
      const total = Number((discovery && discovery.progress_total) || 0);
      const lastScanUtc = String((discovery && discovery.last_scan_utc) || "");
      const bleLastScanUtc = String((discovery && discovery.ble_last_scan_utc) || "");

      discoveryRescan.disabled = !enabled || scanning;

      if (!enabled) {
        discoveryMeta.textContent = "Auto-discovery is disabled.";
        discoveryList.innerHTML = '<div class="discovery-empty">Enter a node target above (TCP host or ble://...), or start Meshtracer with discovery enabled.</div>';
        return;
      }

      const metaParts = [];
      if (scanning) {
        if (scanPhase === "ble") {
          metaParts.push("BLE scan...");
        } else {
          metaParts.push(total > 0 ? `TCP scan ${done}/${total}...` : "Scanning...");
        }
      } else {
        if (lastScanUtc) metaParts.push(`TCP scan: ${lastScanUtc}`);
        if (bleLastScanUtc) metaParts.push(`BLE scan: ${bleLastScanUtc}`);
      }
      if (networks.length) metaParts.push(`Networks: ${networks.join(", ")}`);
      if (Number.isFinite(port) && port > 0) metaParts.push(`Port: ${port}`);
      if (candidates.length || bleCandidates.length) {
        metaParts.push(`Found TCP ${candidates.length}, BLE ${bleCandidates.length}`);
      }
      discoveryMeta.textContent = metaParts.join(" | ") || "Searching your LAN and BLE...";

      if (!candidates.length && !bleCandidates.length) {
        const hint = scanning
          ? "No nodes found yet."
          : "No nodes found yet. Make sure TCP nodes are reachable on your LAN and BLE nodes are advertising nearby.";
        discoveryList.innerHTML = `<div class="discovery-empty">${escapeHtml(hint)}</div>`;
        return;
      }

      const sections = [];
      if (candidates.length) {
        const tcpItems = candidates.map((item) => {
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
              <button class="discovery-item-btn" type="button" data-target="${escapeHtml(host)}">Connect</button>
            </div>
          `;
        }).join("");
        sections.push(`
          <div class="discovery-section">
            <div class="discovery-section-title">TCP (LAN)</div>
            ${tcpItems}
          </div>
        `);
      }

      if (bleCandidates.length) {
        const bleItems = bleCandidates.map((item) => {
          const identifier = String((item && item.identifier) || "").trim();
          const name = String((item && item.name) || "").trim();
          const address = String((item && item.address) || "").trim();
          const connectTargetRaw = String((item && item.connect_target) || "").trim();
          const connectTarget = connectTargetRaw || (identifier ? `ble://${identifier}` : "");
          if (!connectTarget) return "";
          const title = (name && address)
            ? `${name} (${address})`
            : (name || address || identifier || "BLE node");
          const seen = item && item.last_seen_utc ? `seen ${item.last_seen_utc}` : "";
          const rssiRaw = Number((item && item.rssi));
          const rssiText = Number.isFinite(rssiRaw) ? `${Math.trunc(rssiRaw)} dBm` : "";
          const meta = [seen, rssiText].filter(Boolean).join(" | ") || "reachable";
          return `
            <div class="discovery-item">
              <div class="discovery-item-main">
                <span class="discovery-item-host">${escapeHtml(title)}</span>
                <span class="discovery-item-meta">${escapeHtml(meta)}</span>
              </div>
              <button class="discovery-item-btn" type="button" data-target="${escapeHtml(connectTarget)}">Connect</button>
            </div>
          `;
        }).join("");
        sections.push(`
          <div class="discovery-section">
            <div class="discovery-section-title">Bluetooth (BLE)</div>
            ${bleItems}
          </div>
        `);
      }

      discoveryList.innerHTML = sections.join("");

      for (const btn of discoveryList.querySelectorAll("button[data-target]")) {
        btn.addEventListener("click", () => {
          const target = String(btn.dataset.target || "").trim();
          if (!target) return;
          connectToHost(target);
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

    function normalizeRetentionHours(raw, fallback = 720) {
      const parsed = Number(raw);
      if (!Number.isFinite(parsed) || parsed <= 0) return Math.trunc(Number(fallback));
      return Math.trunc(parsed);
    }

    function retentionOptionLabel(hoursRaw) {
      const hours = normalizeRetentionHours(hoursRaw, 720);
      if (hours % 24 === 0) {
        const days = Math.trunc(hours / 24);
        return `${days} day${days === 1 ? "" : "s"}`;
      }
      return `${hours} hour${hours === 1 ? "" : "s"}`;
    }

    function setRetentionSelectValue(rawHours) {
      if (!cfgTracerouteRetentionHours) return;
      const normalized = normalizeRetentionHours(rawHours, 720);
      const value = String(normalized);
      let hasOption = false;
      for (const opt of Array.from(cfgTracerouteRetentionHours.options || [])) {
        if (String(opt.value || "").trim() === value) {
          hasOption = true;
          break;
        }
      }
      if (!hasOption) {
        const opt = document.createElement("option");
        opt.value = value;
        opt.textContent = retentionOptionLabel(normalized);
        cfgTracerouteRetentionHours.appendChild(opt);
      }
      cfgTracerouteRetentionHours.value = value;
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
      setRetentionSelectValue(config.traceroute_retention_hours ?? 720);
      if (cfgWebhookUrl) cfgWebhookUrl.value = String(config.webhook_url ?? "");
      state.configTokenSet = Boolean(config.webhook_api_token_set);
      state.configTokenTouched = false;
      if (cfgWebhookToken) {
        cfgWebhookToken.value = "";
        cfgWebhookToken.placeholder = state.configTokenSet
          ? "Saved token is hidden. Type a new value to replace; leave blank to keep."
          : "Optional token";
      }
      state.chatNotificationSettings = chatNotificationSettingsFromConfig(config);
      applyChatNotificationSettingsToForm();
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
        traceroute_retention_hours: asInt(cfgTracerouteRetentionHours?.value, 720),
        webhook_url: String(cfgWebhookUrl?.value || "").trim() || null,
        chat_notification_desktop: Boolean(state.chatNotificationSettings.desktop),
        chat_notification_sound: Boolean(state.chatNotificationSettings.sound),
        chat_notification_notify_focused: Boolean(state.chatNotificationSettings.notifyFocused),
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
        traceroute_retention_hours: 720,
        webhook_url: null,
        webhook_api_token: null,
        chat_notification_desktop: false,
        chat_notification_sound: false,
        chat_notification_notify_focused: false,
      };
      applyConfigToForm(defaults);
      state.configTokenTouched = true;
      markConfigDirty();
      setCfgStatus("Reset to defaults (not applied).", { error: false });
    }

    async function resetDatabase() {
      const confirmed = window.confirm(
        "This will permanently delete ALL data in the Meshtracer database and disconnect from the current node. Continue?"
      );
      if (!confirmed) return;

      setCfgStatus("Resetting database...", { error: false });
      if (cfgResetDatabase) cfgResetDatabase.disabled = true;
      if (cfgApply) cfgApply.disabled = true;
      if (cfgReset) cfgReset.disabled = true;

      try {
        const { ok, body } = await apiPost("/api/database/reset", {});
        if (!ok) {
          const detail = body && (body.detail || body.error)
            ? String(body.detail || body.error)
            : "database reset failed";
          setCfgStatus(detail, { error: true });
          return;
        }
        state.configDirty = false;
        state.configLoaded = false;
        state.telemetryRequestState = {};
        setCfgStatus("Database reset complete. Disconnected.", { error: false });
      } catch (e) {
        setCfgStatus(String(e || "database reset failed"), { error: true });
      } finally {
        if (cfgResetDatabase) cfgResetDatabase.disabled = false;
        if (cfgApply) cfgApply.disabled = false;
        if (cfgReset) cfgReset.disabled = false;
      }

      refresh({ force: true });
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
      traceroute_retention_hours: {
        title: "Delete traceroutes older than",
        body: `Completed traceroutes older than this age are deleted from SQLite.

Meshtracer displays all stored traceroutes from the database; this setting controls how long history is retained.`,
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
        connectError.textContent = "Enter a node target (TCP host or ble://...).";
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
    try {
      const rawLogFilters = localStorage.getItem(LOG_FILTER_STORAGE_KEY);
      if (rawLogFilters) {
        state.logTypeFilters = normalizedLogTypeFilters(JSON.parse(String(rawLogFilters)));
      } else {
        state.logTypeFilters = normalizedLogTypeFilters(state.logTypeFilters);
      }
    } catch (_e) {
      state.logTypeFilters = normalizedLogTypeFilters(state.logTypeFilters);
    }
    syncLogFilterControls();
    applyChatNotificationSettingsToForm();
    updateChatOpenButton();

    connectBtn.addEventListener("click", () => connectToHost());
    disconnectBtn.addEventListener("click", () => disconnectFromHost());
    discoveryRescan.addEventListener("click", () => rescanDiscovery());
    if (manageTraceQueueBtn) manageTraceQueueBtn.addEventListener("click", () => openQueueModal());
    if (cfgApply) cfgApply.addEventListener("click", () => applyConfig());
    if (cfgReset) cfgReset.addEventListener("click", () => resetConfig());
    if (cfgResetDatabase) cfgResetDatabase.addEventListener("click", () => resetDatabase());
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
      if (chatModal && !chatModal.classList.contains("hidden")) {
        closeChat();
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
      cfgTracerouteRetentionHours,
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
    if (cfgChatNotifDesktop) {
      cfgChatNotifDesktop.addEventListener("change", async () => {
        await enableDesktopNotifications(Boolean(cfgChatNotifDesktop.checked));
      });
    }
    if (cfgChatNotifSound) {
      cfgChatNotifSound.addEventListener("change", () => {
        state.chatNotificationSettings.sound = Boolean(cfgChatNotifSound.checked);
        markConfigDirty();
      });
    }
    if (cfgChatNotifFocused) {
      cfgChatNotifFocused.addEventListener("change", () => {
        state.chatNotificationSettings.notifyFocused = Boolean(cfgChatNotifFocused.checked);
        markConfigDirty();
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
