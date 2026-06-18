const state = {
  handoffs: [],
  selectedHandoff: null,
  seatToken: null,
  room: null,
  localTrack: null,
  localMuted: false,
  remoteAudioElements: [],
  pollTimer: null,
};

const el = {
  statusPill: document.querySelector("#status-pill"),
  refreshList: document.querySelector("#refresh-list"),
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

const HANDOFF_STATUS_LABELS = {
  requested: "等待坐席接入",
  accepted: "坐席已接管",
  connected: "人工通话中",
  completed: "人工已结束",
  canceled: "已取消",
  failed: "转人工失败",
  expired: "接入超时",
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

  el.joinHandoff.disabled = Boolean(state.room) || handoff.status !== "requested";
  el.disconnectAgent.disabled = !state.room;
  updateMicButton();
}

function selectHandoff(handoff) {
  state.selectedHandoff = handoff;
  renderList();
  renderActiveHandoff();
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
    }
  }
  renderList();
  renderActiveHandoff();
}

async function acceptHandoff(handoff) {
  const humanAgentIdentity = el.agentIdentity.value.trim() || "agent-debug-001";
  const result = await api(`/ai-call/handoffs/${handoff.handoffId}/accept`, {
    method: "POST",
    body: JSON.stringify({ humanAgentIdentity }),
  });
  state.selectedHandoff = result.handoff;
  state.seatToken = result.seatToken;
  renderActiveHandoff();
  log("坐席令牌已签发");
}

async function joinSelectedHandoff() {
  if (!state.selectedHandoff) return;
  if (!window.LivekitClient) {
    throw new Error("LiveKit Web SDK 未加载");
  }
  await acceptHandoff(state.selectedHandoff);
  await connectRoom();
  await markConnected();
  setStatus("通话中", "is-ready");
  log("坐席已接入房间，可开始通话");
  await refreshJoinableHandoffs();
}

async function connectRoom() {
  if (!state.seatToken) {
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
    await room.connect(state.seatToken.livekitUrl, state.seatToken.participantToken);
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

async function markConnected() {
  const handoff = await api(`/ai-call/handoffs/${state.selectedHandoff.handoffId}/connected`, {
    method: "POST",
  });
  state.selectedHandoff = handoff;
  renderActiveHandoff();
}

async function completeHandoff() {
  const handoff = await api(`/ai-call/handoffs/${state.selectedHandoff.handoffId}/complete`, {
    method: "POST",
    body: JSON.stringify({ reason: "agent_completed" }),
  });
  state.selectedHandoff = handoff;
  renderActiveHandoff();
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
  if (complete) {
    await completeConnectedHandoff();
  }
  renderActiveHandoff();
}

function bindActions() {
  el.refreshList.addEventListener("click", () => {
    refreshJoinableHandoffs()
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
    refreshJoinableHandoffs().catch((error) => log(error.message));
  }, 2000);
}

window.addEventListener("pagehide", () => {
  disconnectAgent();
});

document.documentElement.dataset.livekitReady = String(Boolean(window.LivekitClient));
bindActions();
refreshJoinableHandoffs().catch((error) => log(error.message));
startPolling();
