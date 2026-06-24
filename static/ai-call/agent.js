const state = {
  handoffs: [],
  selectedHandoff: null,
  seatToken: null,
  room: null,
  localTrack: null,
  localMuted: false,
  remoteAudioElements: [],
  pollTimer: null,
  agentStatus: null,
};

const MIN_ACCEPT_REMAINING_MS = 3000;

const el = {
  statusPill: document.querySelector("#status-pill"),
  refreshList: document.querySelector("#refresh-list"),
  agentPresence: document.querySelector("#agent-presence"),
  agentStatusText: document.querySelector("#agent-status-text"),
  saveAgentStatus: document.querySelector("#save-agent-status"),
  handoffList: document.querySelector("#handoff-list"),
  agentIdentity: document.querySelector("#agent-identity"),
  activeHandoff: document.querySelector("#active-handoff"),
  joinHandoff: document.querySelector("#join-handoff"),
  muteAgent: document.querySelector("#mute-agent"),
  disconnectAgent: document.querySelector("#disconnect-agent"),
  log: document.querySelector("#log"),
};

function log(message) {
  const at = new Date().toLocaleTimeString();
  el.log.textContent = `[${at}] ${message}\n${el.log.textContent}`;
}

function notify(message, mode = "success") {
  let stack = document.querySelector(".toast-stack");
  if (!stack) {
    stack = document.createElement("div");
    stack.className = "toast-stack";
    stack.setAttribute("aria-live", "polite");
    document.body.appendChild(stack);
  }
  const toast = document.createElement("div");
  toast.className = `toast is-${mode}`;
  toast.textContent = message;
  stack.appendChild(toast);
  window.setTimeout(() => toast.remove(), mode === "error" ? 4200 : 2600);
}

function apiPath(path) {
  const marker = "/static/";
  const staticIndex = window.location.pathname.indexOf(marker);
  const basePath = staticIndex >= 0 ? window.location.pathname.slice(0, staticIndex) : "";
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${basePath}${normalizedPath}`;
}

async function api(path, options = {}) {
  const response = await fetch(apiPath(path), {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok || body.code !== 200) {
    throw new Error(body.msg || `HTTP ${response.status}`);
  }
  return body.data;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatDateTime(value) {
  return value ? new Date(value).toLocaleTimeString() : "-";
}

function formatRemaining(expiresAt) {
  if (!expiresAt) return "-";
  const remaining = Math.max(0, new Date(expiresAt).getTime() - Date.now());
  return `${Math.ceil(remaining / 1000)}s`;
}

function handoffHasAcceptWindow(handoff) {
  if (!handoff?.expiresAt) return true;
  return new Date(handoff.expiresAt).getTime() - Date.now() > MIN_ACCEPT_REMAINING_MS;
}

async function ensureAgentMediaPreflight() {
  if (!window.isSecureContext) {
    throw new Error(
      "当前页面无法使用麦克风：请使用 HTTPS，或在接听电脑上把当前地址加入浏览器安全来源白名单。",
    );
  }
  if (
    !navigator.mediaDevices ||
    typeof navigator.mediaDevices.getUserMedia !== "function"
  ) {
    throw new Error("当前页面无法使用麦克风：浏览器未开放 getUserMedia。");
  }
}

const HANDOFF_STATUS_LABELS = {
  requested: "等待坐席接入",
  accepted: "坐席已接管",
  connected: "人工通话中",
  completed: "人工已结束",
  canceled: "已取消",
  failed: "转人工失败",
  expired: "接入超时",
};

const AGENT_STATUS_LABELS = {
  online: "在线",
  busy: "通话中",
  offline: "离线",
};

function formatLabel(value, labels = {}) {
  const key = String(value ?? "");
  if (!key) return "-";
  return labels[key] || key;
}

function titleAttr(rawValue, displayValue) {
  const raw = String(rawValue ?? "");
  if (!raw || raw === displayValue) return "";
  return ` title="${escapeHtml(raw)}"`;
}

function setStatus(text, mode = "") {
  el.statusPill.textContent = text;
  el.statusPill.className = `status-pill ${mode}`.trim();
}

function currentAgentIdentity() {
  return el.agentIdentity.value.trim() || "agent-debug-001";
}

function renderAgentStatus() {
  const status = state.agentStatus?.status || "offline";
  const label = formatLabel(status, AGENT_STATUS_LABELS);
  const active = state.agentStatus?.activeHandoffId;
  el.agentStatusText.textContent = active ? `${label} · ${active}` : label;
  if (status !== "busy") {
    el.agentPresence.value = status === "online" ? "online" : "offline";
  }
  el.agentPresence.disabled = status === "busy";
  el.saveAgentStatus.disabled = status === "busy";
}

async function fetchAgentStatus() {
  const identity = encodeURIComponent(currentAgentIdentity());
  state.agentStatus = await api(`/ai-call/handoff-agents/${identity}`);
  renderAgentStatus();
  renderActiveHandoff();
  return state.agentStatus;
}

async function setAgentPresence(status) {
  const identity = encodeURIComponent(currentAgentIdentity());
  state.agentStatus = await api(`/ai-call/handoff-agents/${identity}/status`, {
    method: "POST",
    body: JSON.stringify({ status, skillGroup: "default" }),
  });
  renderAgentStatus();
  renderActiveHandoff();
  return state.agentStatus;
}

function renderList() {
  if (!state.handoffs.length) {
    el.handoffList.innerHTML = '<div class="subtle">暂无可接入请求</div>';
    return;
  }

  el.handoffList.innerHTML = state.handoffs
    .map((handoff) => {
      const selected = state.selectedHandoff?.handoffId === handoff.handoffId;
      const statusLabel = formatLabel(handoff.status, HANDOFF_STATUS_LABELS);
      return `
        <button
          type="button"
          class="agent-item ${selected ? "is-selected" : ""}"
          data-handoff-id="${escapeHtml(handoff.handoffId)}"
        >
          <div class="agent-item-head">
            <div class="agent-item-title mono">${escapeHtml(handoff.callId)}</div>
            <div class="agent-item-status"${titleAttr(handoff.status, statusLabel)}>${escapeHtml(statusLabel)}</div>
          </div>
          <div class="agent-item-meta">
            <span>转人工</span>
            <span class="mono">${escapeHtml(handoff.handoffId)}</span>
            <span>房间</span>
            <span class="mono">${escapeHtml(handoff.roomName)}</span>
            <span>请求时间</span>
            <span>${formatDateTime(handoff.requestedAt)}</span>
            <span>剩余</span>
            <span>${formatRemaining(handoff.expiresAt)}</span>
          </div>
        </button>
      `;
    })
    .join("");

  for (const item of el.handoffList.querySelectorAll(".agent-item")) {
    item.addEventListener("click", () => {
      const handoff = state.handoffs.find(
        (row) => row.handoffId === item.dataset.handoffId,
      );
      selectHandoff(handoff || null);
    });
  }
}

function renderActiveHandoff() {
  const handoff = state.selectedHandoff;
  if (!handoff) {
    el.activeHandoff.innerHTML = '<div class="subtle">暂无接入</div>';
    el.joinHandoff.disabled = true;
    updateMicButton();
    el.disconnectAgent.disabled = true;
    return;
  }

  const rows = [
    ["通话ID", handoff.callId],
    ["转人工ID", handoff.handoffId],
    ["房间", handoff.roomName],
    ["状态", formatLabel(handoff.status, HANDOFF_STATUS_LABELS)],
    ["人工", handoff.humanAgentIdentity || "-"],
    ["超时", formatDateTime(handoff.expiresAt)],
  ];
  el.activeHandoff.innerHTML = rows
    .map(
      ([label, value]) => `
        <div class="handoff-row">
          <span>${escapeHtml(label)}</span>
          <span class="mono">${escapeHtml(value)}</span>
        </div>
      `,
    )
    .join("");

  el.joinHandoff.disabled =
    Boolean(state.room) ||
    handoff.status !== "requested" ||
    !handoffHasAcceptWindow(handoff) ||
    state.agentStatus?.status !== "online";
  el.disconnectAgent.disabled = !state.room;
  updateMicButton();
}

function selectHandoff(handoff) {
  state.selectedHandoff = handoff;
  renderList();
  renderActiveHandoff();
}

function selectFirstJoinableHandoffWhenIdle() {
  if (!state.room && !state.selectedHandoff && state.handoffs.length) {
    state.selectedHandoff = state.handoffs[0];
  }
}

async function refreshJoinableHandoffs() {
  const data = await api("/ai-call/handoffs/joinable?limit=50");
  state.handoffs = data.rows || [];
  if (state.selectedHandoff) {
    const refreshed = state.handoffs.find(
      (row) => row.handoffId === state.selectedHandoff.handoffId,
    );
    if (refreshed) {
      state.selectedHandoff = refreshed;
    } else if (!state.room) {
      state.selectedHandoff = null;
    }
  }
  selectFirstJoinableHandoffWhenIdle();
  renderList();
  renderActiveHandoff();
}

async function refreshAgentAndJoinableHandoffs() {
  await fetchAgentStatus();
  await refreshJoinableHandoffs();
}

async function acceptHandoff(handoff) {
  const humanAgentIdentity = currentAgentIdentity();
  const result = await api(`/ai-call/handoffs/${handoff.handoffId}/accept`, {
    method: "POST",
    body: JSON.stringify({ humanAgentIdentity }),
  });
  state.selectedHandoff = result.handoff;
  state.seatToken = result.seatToken;
  await fetchAgentStatus();
  renderActiveHandoff();
  log("坐席令牌已签发");
  return result.handoff;
}

async function joinSelectedHandoff() {
  if (!state.selectedHandoff) return;
  if (!window.LivekitClient) {
    throw new Error("LiveKit Web SDK 未加载");
  }
  const agentStatus = await fetchAgentStatus();
  if (agentStatus.status !== "online") {
    throw new Error("坐席不在线，不能接入");
  }
  if (!handoffHasAcceptWindow(state.selectedHandoff)) {
    throw new Error("转人工请求即将超时，请刷新后重新发起");
  }
  await ensureAgentMediaPreflight();
  const requestedHandoff = state.selectedHandoff;
  let acceptedHandoff = null;
  try {
    acceptedHandoff = await acceptHandoff(requestedHandoff);
    const seatToken = state.seatToken;
    await connectRoom(seatToken);
    acceptedHandoff = await markConnected(acceptedHandoff);
    setStatus("通话中", "is-ready");
    log("坐席已接入房间，可开始通话");
    await refreshJoinableHandoffs().catch((refreshError) => {
      log(`刷新可接入列表失败：${refreshError.message}`);
    });
  } catch (error) {
    await failAcceptedHandoff(acceptedHandoff, "agent_connect", error.message);
    cleanupLocalRoomState();
    await refreshAgentAndJoinableHandoffs().catch((refreshError) => {
      log(`刷新坐席状态失败：${refreshError.message}`);
    });
    throw error;
  }
}

async function connectRoom(seatToken = state.seatToken) {
  if (!seatToken) {
    throw new Error("缺少坐席 Token");
  }
  const { Room, RoomEvent, createLocalAudioTrack } = window.LivekitClient;
  const room = new Room({ adaptiveStream: true, dynacast: true });
  let audioTrack = null;
  room.on(RoomEvent.TrackSubscribed, (track) => {
    if (track.kind !== "audio") return;
    const media = track.attach();
    media.autoplay = true;
    media.muted = false;
    media.playsInline = true;
    document.body.appendChild(media);
    media.play().catch((error) => log(`远端音频播放失败：${error.message}`));
    state.remoteAudioElements.push(media);
  });
  room.on(RoomEvent.Disconnected, () => {
    setStatus("已断开");
    log("坐席房间已断开");
  });

  try {
    audioTrack = await createLocalAudioTrack({
      echoCancellation: true,
      noiseSuppression: true,
      autoGainControl: true,
    });
    await room.connect(seatToken.livekitUrl, seatToken.participantToken);
    await room.localParticipant.publishTrack(audioTrack);
    state.room = room;
    state.localTrack = audioTrack;
    state.localMuted = false;
    renderActiveHandoff();
  } catch (error) {
    if (audioTrack) {
      audioTrack.stop();
    }
    room.disconnect();
    throw error;
  }
}

async function markConnected(handoff = state.selectedHandoff) {
  const handoffId = handoff?.handoffId || state.selectedHandoff?.handoffId;
  if (!handoffId) {
    throw new Error("缺少转人工 ID，无法标记已接入");
  }
  const connectedHandoff = await api(`/ai-call/handoffs/${handoffId}/connected`, {
    method: "POST",
  });
  state.selectedHandoff = connectedHandoff;
  await fetchAgentStatus();
  renderActiveHandoff();
  return connectedHandoff;
}

async function completeHandoff() {
  const handoff = await api(`/ai-call/handoffs/${state.selectedHandoff.handoffId}/complete`, {
    method: "POST",
    body: JSON.stringify({ reason: "agent_completed" }),
  });
  state.selectedHandoff = handoff;
  await fetchAgentStatus();
  renderActiveHandoff();
}

async function failAcceptedHandoff(handoff, failureStage, failureMessage) {
  const targetHandoff = handoff || state.selectedHandoff;
  const handoffId = targetHandoff?.handoffId;
  if (!handoffId) return;
  const handoffStatus = targetHandoff?.status || state.selectedHandoff?.status;
  const reportableStatuses = ["accepted", "connected"];
  if (!reportableStatuses.includes(handoffStatus)) return;
  try {
    state.selectedHandoff = await api(`/ai-call/handoffs/${handoffId}/fail`, {
      method: "POST",
      body: JSON.stringify({ failureStage, failureMessage }),
    });
    await fetchAgentStatus();
  } catch (error) {
    log(`标记转人工失败失败：${error.message}`);
  }
}

function cleanupLocalRoomState() {
  if (state.localTrack) {
    state.localTrack.stop();
  }
  if (state.room) {
    state.room.disconnect();
  }
  for (const media of state.remoteAudioElements) {
    media.pause();
    media.srcObject = null;
    media.remove();
  }
  state.localTrack = null;
  state.localMuted = false;
  state.room = null;
  state.seatToken = null;
  state.remoteAudioElements = [];
  updateMicButton();
}

function updateMicButton() {
  el.muteAgent.disabled = !state.localTrack;
  el.muteAgent.textContent = state.localMuted ? "麦克风开" : "麦克风关";
}

async function setAgentMuted(muted) {
  if (!state.localTrack) return;
  if (muted && typeof state.localTrack.mute === "function") {
    await state.localTrack.mute();
  } else if (!muted && typeof state.localTrack.unmute === "function") {
    await state.localTrack.unmute();
  } else if (state.localTrack.mediaStreamTrack) {
    state.localTrack.mediaStreamTrack.enabled = !muted;
  }
  state.localMuted = muted;
  updateMicButton();
}

async function toggleAgentMute() {
  await setAgentMuted(!state.localMuted);
  log(state.localMuted ? "坐席麦克风已关闭" : "坐席麦克风已开启");
  notify(state.localMuted ? "坐席麦克风已关闭" : "坐席麦克风已开启", "info");
}

async function completeConnectedHandoff() {
  if (state.selectedHandoff?.status !== "connected") return;
  await completeHandoff();
}

async function disconnectAgent({ complete = false } = {}) {
  cleanupLocalRoomState();
  if (complete) {
    await completeConnectedHandoff();
  }
  await fetchAgentStatus().catch((error) => log(error.message));
  renderActiveHandoff();
}

function bindActions() {
  el.agentIdentity.addEventListener("change", () => {
    fetchAgentStatus().catch((error) => log(error.message));
  });
  el.saveAgentStatus.addEventListener("click", () => {
    setAgentPresence(el.agentPresence.value)
      .then(() => notify("坐席状态已保存"))
      .catch((error) => {
        log(error.message);
        notify(error.message || "保存失败", "error");
      });
  });
  el.refreshList.addEventListener("click", () => {
    refreshAgentAndJoinableHandoffs()
      .then(() => notify("可接入列表已刷新"))
      .catch((error) => {
        log(error.message);
        notify(error.message || "刷新失败", "error");
      });
  });
  el.joinHandoff.addEventListener("click", () => {
    el.joinHandoff.disabled = true;
    joinSelectedHandoff()
      .then(() => notify("坐席已接入"))
      .catch((error) => {
        setStatus("接入失败", "is-error");
        log(error.message);
        notify(error.message || "接入失败", "error");
      })
      .finally(() => renderActiveHandoff());
  });
  el.muteAgent.addEventListener("click", () => {
    toggleAgentMute().catch((error) => {
      log(error.message);
      notify(error.message || "麦克风操作失败", "error");
    });
  });
  el.disconnectAgent.addEventListener("click", () => {
    disconnectAgent({ complete: true })
      .then(() => notify("坐席已断开"))
      .catch((error) => {
        log(error.message);
        notify(error.message || "断开失败", "error");
      });
  });
}

function startPolling() {
  state.pollTimer = window.setInterval(() => {
    refreshAgentAndJoinableHandoffs().catch((error) => log(error.message));
  }, 2000);
}

window.addEventListener("pagehide", () => {
  disconnectAgent();
});

document.documentElement.dataset.livekitReady = String(Boolean(window.LivekitClient));
bindActions();
refreshAgentAndJoinableHandoffs()
  .catch((error) => log(error.message));
startPolling();
