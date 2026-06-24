const state = {
  session: null,
  room: null,
  handoff: null,
  handoffRequesting: false,
  localTrack: null,
  localMuted: false,
  pollTimer: null,
  events: [],
  eventIds: new Set(),
  lastEventId: null,
  pendingBrowserFirstAudioTurnId: null,
  reportedBrowserFirstAudioTurnId: null,
  reportedBrowserReadyFor: null,
  reportedAudioInputDiagnosticsFor: null,
  remoteAudioContext: null,
  remoteAudioMonitorTimer: null,
  remoteAudioAnalyser: null,
  remoteAudioSamples: null,
  remoteAudioActive: false,
  remoteAudioHotTicks: 0,
  remoteAudioQuietTicks: 0,
  localAudioContext: null,
  localAudioMonitorTimer: null,
  localAudioAnalyser: null,
  localAudioSamples: null,
  localSpeechHotTicks: 0,
  localSpeechQuietTicks: 0,
  localSpeechBaselineRms: 0.006,
  localSpeechReportInFlight: false,
  localSpeechSegmentId: null,
  localSpeechSegmentStartedAt: 0,
  localSpeechSegmentHotFrameCount: 0,
  localSpeechSegmentPeakRms: 0,
  localSpeechSegmentLastReportAt: 0,
  remoteAudioElements: [],
  remoteAudioRms: 0,
  // 程序主动断开时不写 browser_disconnect，避免把正常重置误判成用户离开。
  suppressDisconnectReport: false,
  disconnectReportedFor: null,
  uiLoading: false,
  voiceProfiles: [],
  promptProfiles: [],
  asrActive: false,
};

const EVENT_RENDER_LIMIT = 300;
const REMOTE_AUDIO_POLL_MS = 40;
const REMOTE_AUDIO_START_RMS = 0.015;
const REMOTE_AUDIO_RELEASE_RMS = 0.006;
const REMOTE_AUDIO_START_TICKS = 2;
const REMOTE_AUDIO_RELEASE_TICKS = 6;
const LOCAL_SPEECH_POLL_MS = 40;
const LOCAL_SPEECH_MIN_RMS = 0.012;
const LOCAL_SPEECH_DELTA_RMS = 0.012;
const LOCAL_SPEECH_BASELINE_MULTIPLIER = 2.2;
const LOCAL_SPEECH_START_TICKS = 3;
const LOCAL_SPEECH_BASELINE_ALPHA = 0.06;
const LOCAL_SPEECH_END_TICKS = 4;
const LOCAL_SPEECH_SEGMENT_UPDATE_MS = 160;
const MIN_RMS_FOR_DBFS = 0.000001;

const el = {
  statusPill: document.querySelector("#status-pill"),
  createSession: document.querySelector("#create-session"),
  connectRoom: document.querySelector("#connect-room"),
  muteRoom: document.querySelector("#mute-room"),
  endSession: document.querySelector("#end-session"),
  refreshStatus: document.querySelector("#refresh-status"),
  refreshEvents: document.querySelector("#refresh-events"),
  refreshRecording: document.querySelector("#refresh-recording"),
  refreshDialogue: document.querySelector("#refresh-dialogue"),
  refreshHandoff: document.querySelector("#refresh-handoff"),
  requestHandoff: document.querySelector("#request-handoff"),
  voiceSelect: document.querySelector("#voice-select"),
  sceneCodeInput: document.querySelector("#scene-code-input"),
  businessIdInput: document.querySelector("#business-id-input"),
  businessParamsInput: document.querySelector("#business-params-input"),
  callId: document.querySelector("#call-id"),
  roomName: document.querySelector("#room-name"),
  modelName: document.querySelector("#model-name"),
  sessionStatus: document.querySelector("#session-status"),
  micState: document.querySelector("#mic-state"),
  recordingState: document.querySelector("#recording-state"),
  handoffState: document.querySelector("#handoff-state"),
  dialogueList: document.querySelector("#dialogue-list"),
  eventList: document.querySelector("#event-list"),
  log: document.querySelector("#log"),
  metricModelFirst: document.querySelector("#metric-model-first"),
  metricModelStats: document.querySelector("#metric-model-stats"),
  metricBrowserFirst: document.querySelector("#metric-browser-first"),
  metricBrowserStats: document.querySelector("#metric-browser-stats"),
  metricInterrupt: document.querySelector("#metric-interrupt"),
  metricQueue: document.querySelector("#metric-queue"),
  loadingMask: document.querySelector("#loading-mask"),
  loadingTitle: document.querySelector("#loading-title"),
  loadingDesc: document.querySelector("#loading-desc"),
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

function errorMessage(error) {
  if (error instanceof Error && error.message) {
    return error.message;
  }
  return String(error || "未知错误");
}

function wrapError(message, error) {
  return new Error(`${message}：${errorMessage(error)}`);
}

function microphoneErrorMessage(error) {
  const name = error?.name || "";
  const message = errorMessage(error);
  if (name === "NotAllowedError" || name === "SecurityError") {
    return "申请麦克风失败：浏览器麦克风权限被拒绝，请在地址栏允许麦克风权限后重试";
  }
  if (name === "NotFoundError" || name === "DevicesNotFoundError") {
    return "申请麦克风失败：没有检测到可用麦克风，请检查输入设备";
  }
  if (name === "NotReadableError" || name === "TrackStartError") {
    return "申请麦克风失败：麦克风可能被其他应用占用，或被系统隐私设置阻止";
  }
  if (name === "OverconstrainedError" || name === "ConstraintNotSatisfiedError") {
    return "申请麦克风失败：当前设备不满足麦克风采集要求，请更换麦克风或调整系统输入设备";
  }
  if (name === "AbortError") {
    return "申请麦克风失败：浏览器启动音频输入被中断，请重新尝试";
  }
  return `申请麦克风失败：${message}`;
}

function actionErrorMessage(error, prefix) {
  const message = errorMessage(error);
  return message.startsWith(`${prefix}：`) ? message : `${prefix}：${message}`;
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
  return Object.prototype.hasOwnProperty.call(body, "data") ? body.data : body;
}

function setStatus(text, mode = "") {
  if (!el.statusPill) return;
  el.statusPill.textContent = formatLabel(text, SESSION_STATUS_LABELS);
  el.statusPill.className = `status-pill ${mode}`.trim();
}

function setPageLoading(active, title = "处理中", desc = "请稍候...") {
  state.uiLoading = active;
  document.body.classList.toggle("is-loading", active);
  el.loadingMask.classList.toggle("is-active", active);
  el.loadingMask.setAttribute("aria-hidden", String(!active));
  if (active) {
    el.loadingTitle.textContent = title;
    el.loadingDesc.textContent = desc;
  }
}

function setButtonLoading(button, active, text) {
  if (!button.dataset.defaultText) {
    button.dataset.defaultText = button.textContent;
  }
  button.classList.toggle("is-loading", active);
  button.setAttribute("aria-busy", String(active));
  button.textContent = active ? text : button.dataset.defaultText;
  button.disabled = active;
}

function restoreLoadingButtonState(button) {
  if (button === el.createSession) {
    el.createSession.disabled = false;
    return;
  }
  if (button === el.connectRoom) {
    el.connectRoom.disabled =
      !state.session || Boolean(state.localTrack) || Boolean(state.room);
  }
}

async function runWithLoading(options, task) {
  const { button, loadingText, title, desc } = options;
  setButtonLoading(button, true, loadingText);
  setPageLoading(true, title, desc);
  try {
    await task();
  } finally {
    setPageLoading(false);
    setButtonLoading(button, false);
    restoreLoadingButtonState(button);
  }
}

function formatMetric(value) {
  return value === null || value === undefined ? "-" : `${value}ms`;
}

function formatMetricStats(count, p50, p90, max) {
  if (!count) return "-";
  return `P50 ${formatMetric(p50)} / P90 ${formatMetric(p90)} / Max ${formatMetric(max)}`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function renderVoiceOptions(rows) {
  const selectedVoice = el.voiceSelect.value || "Tina";
  const profiles = rows.length
    ? rows
    : [{ voice: "Tina", displayName: "甜甜 Tina", gender: "女声", voiceType: "内置" }];
  el.voiceSelect.innerHTML = profiles
    .map((profile) => {
      const labelParts = [
        profile.displayName || profile.voice,
        profile.gender,
        profile.voiceType === "自定义复刻" ? "自定义" : "",
      ].filter(Boolean);
      return `
        <option value="${escapeHtml(profile.voice)}">
          ${escapeHtml(labelParts.join(" · "))}
        </option>
      `;
    })
    .join("");
  if (profiles.some((profile) => profile.voice === selectedVoice)) {
    el.voiceSelect.value = selectedVoice;
  }
}

async function loadVoiceProfiles() {
  const result = await api("/ai-call/voice-profiles?pageSize=200");
  state.voiceProfiles = result.rows || [];
  renderVoiceOptions(state.voiceProfiles);
  log(`音色列表已加载：${state.voiceProfiles.length} 个`);
}

function renderSceneOptions(rows) {
  const selectedSceneCode = el.sceneCodeInput.value || "";
  if (!rows.length) {
    el.sceneCodeInput.innerHTML = '<option value="">暂无可用业务场景</option>';
    return;
  }
  const options = rows.map((profile) => {
    const label = [profile.name, profile.sceneCode].filter(Boolean).join(" · ");
    return `<option value="${escapeHtml(profile.sceneCode)}">${escapeHtml(label)}</option>`;
  });
  el.sceneCodeInput.innerHTML = options.join("");
  if (rows.some((profile) => profile.sceneCode === selectedSceneCode)) {
    el.sceneCodeInput.value = selectedSceneCode;
  } else {
    el.sceneCodeInput.value = rows[0].sceneCode;
  }
}

async function loadPromptProfiles() {
  const result = await api("/ai-call/prompt-profiles?pageSize=200");
  state.promptProfiles = result.rows || [];
  renderSceneOptions(state.promptProfiles);
  log(`业务场景已加载：${state.promptProfiles.length} 个`);
}

const SESSION_STATUS_LABELS = {
  created: "已创建",
  preparing: "准备中",
  ready: "已就绪",
  connected: "通话中",
  user_speaking: "用户说话中",
  ai_thinking: "AI 思考中",
  ai_speaking: "AI 说话中",
  interrupted: "已打断",
  completed: "已完成",
  failed: "失败",
};

const RECORDING_STATUS_LABELS = {
  starting: "启动中",
  recording: "录音中",
  stopping: "停止中",
  verifying: "确认中",
  completed: "已完成",
  failed: "失败",
  skipped: "未开启",
};

const ASR_STATUS_LABELS = {
  pending: "等待转写",
  running: "转写中",
  completed: "转写完成",
  failed: "转写失败",
  skipped: "已跳过",
};

const RECORDING_TRACK_ROLE_LABELS = {
  customer: "客户分轨",
  ai: "AI分轨",
  human_agent: "坐席分轨",
};

const HANDOFF_STATUS_LABELS = {
  requested: "等待坐席接入",
  accepted: "坐席已接管",
  connected: "人工通话中",
  completed: "人工已结束",
  canceled: "已取消",
  failed: "转人工失败",
  expired: "接入超时",
};

const SOURCE_LABELS = {
  browser: "浏览器",
  orchestrator: "编排器",
  provider: "模型服务",
  livekit: "LiveKit",
  system: "系统",
  operator: "操作员",
  customer: "用户",
  ai: "AI",
  agent: "AI 坐席",
  human_agent: "人工坐席",
  handoff: "转人工",
  qwen_realtime: "实时文本",
  offline_asr: "离线ASR",
};

const REASON_LABELS = {
  customer_request: "用户要求转人工",
  operator_cancelled: "操作员取消",
  web_user_end: "页面手动结束",
  browser_disconnect: "浏览器断开",
  agent_completed: "人工结束",
  handoff_timeout: "转人工超时",
  handoff_failed: "转人工失败",
  model_error: "模型异常",
  agent_start_failed: "AI 启动失败",
};

const EVENT_TYPE_LABELS = {
  session_created: "会话已创建",
  session_preparing: "会话准备中",
  room_created: "房间已创建",
  browser_token_issued: "浏览器令牌已签发",
  model_session_started: "模型会话已启动",
  model_session_updated: "模型会话已更新",
  agent_started: "AI 已启动",
  session_ready: "会话已就绪",
  browser_ready: "浏览器已就绪",
  opening_started: "开场白开始",
  model_response_started: "AI 开始回复",
  model_audio_delta: "AI 音频片段",
  ai_audio_published: "AI 音频已发布",
  model_audio_done: "AI 音频完成",
  model_response_done: "AI 回复完成",
  model_error: "模型异常",
  session_failed: "会话失败",
  session_ending: "会话结束中",
  session_completed: "会话已完成",
  browser_first_audio: "浏览器收到首包音频",
  browser_disconnect: "浏览器断开",
  browser_audio_input_diagnostics: "浏览器音频输入诊断",
  browser_user_speech_segment: "浏览器检测到用户语音段",
  browser_user_speech_started: "浏览器检测到用户说话",
  user_speech_started: "用户开始说话",
  user_speech_stopped: "用户停止说话",
  input_audio_committed: "用户音频已提交",
  input_audio_cleared: "用户音频已清空",
  user_transcript_delta: "用户文本增量",
  user_transcript_done: "用户文本完成",
  user_transcript_failed: "用户识别失败",
  ai_transcript_delta: "AI 文本增量",
  ai_transcript_done: "AI 文本完成",
  conversation_item_created: "对话项已创建",
  provider_event_unmapped: "模型事件未映射",
  interrupt_candidate: "打断候选",
  interrupt_confirmed: "已确认打断",
  interrupt_cleanup_failed: "打断清理失败",
  handoff_requested: "已发起转人工",
  handoff_accepted: "坐席已接管",
  handoff_connected: "人工已接入",
  handoff_completed: "人工已结束",
  handoff_canceled: "转人工已取消",
  handoff_failed: "转人工失败",
  handoff_expired: "转人工超时",
  handoff_timeout_task_started: "转人工超时计时开始",
  handoff_timeout_task_canceled: "转人工超时计时取消",
  handoff_prompt_started: "转人工提示开始",
  handoff_prompt_done: "转人工提示完成",
  handoff_prompt_failed: "转人工提示失败",
  handoff_waiting_tone_started: "等待声开始",
  handoff_waiting_tone_stopped: "等待声停止",
  handoff_waiting_tone_failed: "等待声失败",
  handoff_unavailable_prompt_started: "无坐席提示开始",
  handoff_unavailable_prompt_done: "无坐席提示完成",
  handoff_unavailable_prompt_failed: "无坐席提示失败",
  handoff_auto_ended: "转人工自动结束通话",
  agent_suspended_for_handoff: "AI 已暂停等待人工",
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

function getEventType(event) {
  return event.type || event.eventType || "-";
}

function isTerminalStatus(status) {
  return status === "completed" || status === "failed";
}

function renderSession(session) {
  state.session = session;
  el.callId.textContent = session.callId;
  el.roomName.textContent = session.roomName;
  el.modelName.textContent = session.effectiveConfig.model;
  renderSessionStatus(session.status);
  el.connectRoom.disabled = false;
  updateLocalMuteButton();
  el.endSession.disabled = false;
  el.refreshStatus.disabled = false;
  el.refreshEvents.disabled = false;
  el.refreshRecording.disabled = false;
  el.refreshDialogue.disabled = false;
  el.refreshHandoff.disabled = false;
  updateHandoffControls();
  setStatus(session.status, "is-ready");
}

function renderSessionStatus(status) {
  el.sessionStatus.textContent = formatLabel(status, SESSION_STATUS_LABELS);
}

function renderMetrics(metrics = {}) {
  el.metricModelFirst.textContent = formatMetric(metrics.lastModelFirstAudioMs);
  el.metricModelStats.textContent = formatMetricStats(
    metrics.modelFirstAudioCount,
    metrics.modelFirstAudioP50Ms,
    metrics.modelFirstAudioP90Ms,
    metrics.modelFirstAudioMaxMs,
  );
  el.metricBrowserFirst.textContent = formatMetric(metrics.lastBrowserFirstAudioMs);
  el.metricBrowserStats.textContent = formatMetricStats(
    metrics.browserFirstAudioCount,
    metrics.browserFirstAudioP50Ms,
    metrics.browserFirstAudioP90Ms,
    metrics.browserFirstAudioMaxMs,
  );
  el.metricInterrupt.textContent = formatMetric(metrics.lastInterruptStopMs);
  el.metricQueue.textContent = metrics.audioQueueDepth ?? "-";
}

function renderEvents(events) {
  if (!events.length) {
    el.eventList.innerHTML = '<div class="subtle">暂无事件</div>';
    return;
  }

  el.eventList.innerHTML = events
    .slice()
    .reverse()
    .map((event) => {
      const eventType = getEventType(event);
      const eventTypeLabel = formatLabel(eventType, EVENT_TYPE_LABELS);
      const source = event.source || "-";
      const sourceLabel = formatLabel(source, SOURCE_LABELS);
      const eventTime = event.timestamp || event.eventTime;
      const time = eventTime ? new Date(eventTime).toLocaleTimeString() : "-";
      return `
        <div class="event">
          <div>
            <div class="event-type"${titleAttr(eventType, eventTypeLabel)}>${escapeHtml(eventTypeLabel)}</div>
            <div class="event-meta"${titleAttr(source, sourceLabel)}>${escapeHtml(sourceLabel)}</div>
          </div>
          <div class="event-meta">
            <div>${time}</div>
            <div class="mono">${escapeHtml(event.eventId || "-")}</div>
          </div>
        </div>
      `;
    })
    .join("");
}

function renderRecording(recording) {
  if (!recording) {
    state.asrActive = false;
    el.recordingState.innerHTML = '<div class="subtle">暂无录音记录</div>';
    return;
  }

  const rows = [
    ["状态", formatLabel(recording.status, RECORDING_STATUS_LABELS)],
    ["录制任务", recording.egressId || "-"],
    ["文件ID", recording.ossId || "-"],
    ["时长", formatMetric(recording.durationMs)],
  ];
  const facts = rows
    .map(
      ([label, value]) => `
        <div class="recording-row">
          <span>${escapeHtml(label)}</span>
          <span class="mono">${escapeHtml(value)}</span>
        </div>
      `,
    )
    .join("");
  const audio = recording.playUrl
    ? `<audio class="recording-audio" controls src="${escapeHtml(recording.playUrl)}"></audio>`
    : "";
  const failure = recording.failureMessage
    ? `<div class="recording-failure">${escapeHtml(recording.failureMessage)}</div>`
    : "";
  const tracks = renderRecordingTracks(recording.tracks || []);
  const asrJobs = recording.asrJobs || [];
  state.asrActive = hasActiveAsrJobs(asrJobs);
  const asr = renderAsrJobs(asrJobs);
  el.recordingState.innerHTML = `${facts}${audio}${failure}${tracks}${asr}`;
}

function renderRecordingTracks(tracks) {
  if (!Array.isArray(tracks) || tracks.length === 0) {
    return "";
  }
  return `
    <div class="subtle" style="margin-top: 12px;">分轨录音</div>
    ${tracks
      .map((track) => {
        const label = formatLabel(track.trackRole, RECORDING_TRACK_ROLE_LABELS);
        const status = formatLabel(track.status, RECORDING_STATUS_LABELS);
        const audio = track.playUrl
          ? `<audio class="recording-audio" controls src="${escapeHtml(track.playUrl)}"></audio>`
          : "";
        const failure = track.failureMessage
          ? `<div class="recording-failure">${escapeHtml(track.failureMessage)}</div>`
          : "";
        return `
          <div class="recording-row">
            <span>${escapeHtml(label)}</span>
            <span class="mono">${escapeHtml(status)}</span>
          </div>
          <div class="recording-row">
            <span>参与方</span>
            <span class="mono">${escapeHtml(track.participantIdentity || "-")}</span>
          </div>
          ${audio}
          ${failure}
        `;
      })
      .join("")}
  `;
}

function hasActiveAsrJobs(jobs) {
  if (!Array.isArray(jobs)) return false;
  return jobs.some((job) => ["pending", "running"].includes(job.status));
}

function renderAsrJobs(jobs) {
  if (!Array.isArray(jobs) || jobs.length === 0) {
    return "";
  }
  return `
    <div class="subtle" style="margin-top: 12px;">离线ASR</div>
    ${jobs
      .map((job) => {
        const role = formatLabel(job.trackRole, RECORDING_TRACK_ROLE_LABELS);
        const status = formatLabel(job.status, ASR_STATUS_LABELS);
        const model = [job.provider, job.model].filter(Boolean).join(" / ") || "-";
        const count =
          job.segmentCount === null || job.segmentCount === undefined
            ? "-"
            : `${job.segmentCount} 段`;
        const failure = job.failureMessage
          ? `<div class="recording-failure">${escapeHtml(job.failureMessage)}</div>`
          : "";
        return `
          <div class="recording-row">
            <span>${escapeHtml(role)}</span>
            <span class="mono"${titleAttr(job.status, status)}>${escapeHtml(status)}</span>
          </div>
          <div class="recording-row">
            <span>模型</span>
            <span class="mono">${escapeHtml(model)}</span>
          </div>
          <div class="recording-row">
            <span>文本</span>
            <span class="mono">${escapeHtml(count)}</span>
          </div>
          ${failure}
        `;
      })
      .join("")}
  `;
}

function renderHandoff(handoff) {
  state.handoff = handoff;
  if (!handoff) {
    const emptyText =
      state.session && isTerminalStatus(state.session.status)
        ? "暂无转人工记录"
        : "暂无转人工请求";
    el.handoffState.innerHTML = `<div class="subtle">${emptyText}</div>`;
    updateHandoffControls();
    return;
  }

  const rows = [
    ["转人工ID", handoff.handoffId],
    ["状态", formatLabel(handoff.status, HANDOFF_STATUS_LABELS)],
    ["等待", formatHandoffWaitState(handoff)],
    ["来源", formatLabel(handoff.requestSource, SOURCE_LABELS)],
    ["原因", formatLabel(handoff.requestReason, REASON_LABELS)],
    ["人工", handoff.humanAgentIdentity || "-"],
    ["请求时间", formatDateTime(handoff.requestedAt)],
    ["接管时间", formatDateTime(handoff.acceptedAt)],
    ["连接时间", formatDateTime(handoff.connectedAt)],
    ["结束时间", formatDateTime(handoff.endedAt)],
    ["超时时间", formatDateTime(handoff.expiresAt)],
    ["失败", handoff.failureMessage || "-"],
  ];
  el.handoffState.innerHTML = rows
    .map(
      ([label, value]) => `
        <div class="handoff-row">
          <span>${escapeHtml(label)}</span>
          <span class="mono">${escapeHtml(value)}</span>
        </div>
      `,
    )
    .join("");
  updateHandoffControls();
}

function formatDateTime(value) {
  if (!value) return "-";
  return new Date(value).toLocaleTimeString();
}

function formatHandoffWaitState(handoff) {
  if (!handoff || !["requested", "accepted"].includes(handoff.status)) {
    if (handoff?.status === "expired" || handoff?.status === "failed") {
      return "人工暂未接入，即将结束通话";
    }
    return "-";
  }
  if (!handoff.expiresAt) return "等待人工接入";
  const remainingMs = new Date(handoff.expiresAt).getTime() - Date.now();
  const remainingSeconds = Math.max(0, Math.ceil(remainingMs / 1000));
  return `等待人工接入，剩余 ${remainingSeconds}s`;
}

function isHandoffTerminal(status) {
  return ["completed", "canceled", "failed", "expired"].includes(status);
}

function updateHandoffControls() {
  const hasSession = Boolean(state.session) && !isTerminalStatus(state.session.status);
  const handoff = state.handoff;
  const handoffStatus = handoff?.status;
  const active = handoff && !isHandoffTerminal(handoffStatus);
  const requesting = Boolean(state.handoffRequesting);

  el.requestHandoff.disabled = !hasSession || Boolean(active) || requesting;
  if (!state.session) {
    el.requestHandoff.textContent = "主动转人工";
    el.requestHandoff.title = "请先创建会话";
  } else if (isTerminalStatus(state.session.status)) {
    el.requestHandoff.textContent = "会话已结束";
    el.requestHandoff.title = "会话已结束，不能再发起转人工";
  } else if (active) {
    el.requestHandoff.textContent = "转人工处理中";
    el.requestHandoff.title = "已有进行中的转人工请求，不能重复发起";
  } else if (requesting) {
    el.requestHandoff.textContent = "转人工发起中...";
    el.requestHandoff.title = "正在发起转人工请求";
  } else {
    el.requestHandoff.textContent = "主动转人工";
    el.requestHandoff.title = "发起人工接管请求";
  }
}

function renderDialogue(rows) {
  if (!rows.length) {
    el.dialogueList.innerHTML = '<div class="subtle">暂无文本</div>';
    return;
  }

  el.dialogueList.innerHTML = rows
    .slice()
    .sort(compareDialogueSegments)
    .map((segment) => {
      const side = segment.speakerType === "customer" ? "is-customer" : "is-agent";
      const speakerName = dialogueSpeakerName(segment);
      return `
        <div class="dialogue-row ${side}">
          <div class="dialogue-bubble">
            <div class="dialogue-meta">
              <span>${escapeHtml(speakerName)}</span>
            </div>
            <div class="dialogue-text">${escapeHtml(segment.text || "")}</div>
          </div>
        </div>
      `;
    })
    .join("");
}

function dialogueSpeakerName(segment) {
  const speakerName = {
    customer: "用户",
    ai: "AI",
    human_agent: "人工",
  }[segment.speakerType] || segment.speakerType;
  if (segment.speakerType === "ai" && segment.segmentStatus === "interrupted") {
    return "AI（已打断）";
  }
  return speakerName;
}

function compareDialogueSegments(left, right) {
  const leftTime = left.startedAt ? new Date(left.startedAt).getTime() : Number.MAX_SAFE_INTEGER;
  const rightTime = right.startedAt ? new Date(right.startedAt).getTime() : Number.MAX_SAFE_INTEGER;
  if (leftTime !== rightTime) {
    return leftTime - rightTime;
  }
  return (left.segmentNo || 0) - (right.segmentNo || 0);
}

function resetEventState() {
  state.events = [];
  state.eventIds = new Set();
  state.lastEventId = null;
  state.pendingBrowserFirstAudioTurnId = null;
  state.reportedBrowserFirstAudioTurnId = null;
  renderEvents([]);
  renderRecording(null);
  renderHandoff(null);
  renderDialogue([]);
}

function appendEvents(events) {
  for (const event of events) {
    if (state.eventIds.has(event.eventId)) continue;
    state.events.push(event);
    state.eventIds.add(event.eventId);
    state.lastEventId = event.eventId;
    observeEvent(event);
  }
  while (state.events.length > EVENT_RENDER_LIMIT) {
    const removed = state.events.shift();
    if (removed) state.eventIds.delete(removed.eventId);
  }
  renderEvents(state.events);
}

function observeEvent(event) {
  // 每个模型响应轮次都重新武装首包检测，开场白也按同一口径统计。
  const eventType = getEventType(event);
  if (eventType === "opening_started" || eventType === "user_speech_stopped") {
    state.pendingBrowserFirstAudioTurnId = event.eventId;
    state.reportedBrowserFirstAudioTurnId = null;
    state.remoteAudioActive = false;
    state.remoteAudioHotTicks = 0;
    state.remoteAudioQuietTicks = 0;
  }
}

function stopPolling() {
  if (state.pollTimer) {
    window.clearInterval(state.pollTimer);
  }
  state.pollTimer = null;
}

function stopClientAudioRuntime() {
  stopRemoteAudioMonitor();
  stopLocalSpeechMonitor();
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
  state.remoteAudioElements = [];
  updateLocalMuteButton();
}

function resetClientRuntime() {
  state.suppressDisconnectReport = true;
  stopPolling();
  stopClientAudioRuntime();
  resetEventState();
  state.session = null;
  state.handoff = null;
  state.handoffRequesting = false;
  state.reportedBrowserReadyFor = null;
  state.reportedAudioInputDiagnosticsFor = null;
  state.disconnectReportedFor = null;
  el.sessionStatus.textContent = "未创建";
  el.micState.textContent = "未连接";
  disableSessionControls();
  state.suppressDisconnectReport = false;
}

function disableSessionControls() {
  el.connectRoom.disabled = true;
  el.muteRoom.disabled = true;
  el.endSession.disabled = true;
  el.refreshStatus.disabled = true;
  el.refreshEvents.disabled = true;
  el.refreshRecording.disabled = true;
  el.refreshDialogue.disabled = true;
  el.refreshHandoff.disabled = true;
  el.requestHandoff.disabled = true;
}

function updateLocalMuteButton() {
  el.muteRoom.disabled = !state.localTrack;
  el.muteRoom.textContent = state.localMuted ? "麦克风：开" : "麦克风：关";
}

async function setLocalTrackMuted(muted) {
  if (!state.localTrack) return;
  if (muted && typeof state.localTrack.mute === "function") {
    await state.localTrack.mute();
  } else if (!muted && typeof state.localTrack.unmute === "function") {
    await state.localTrack.unmute();
  } else if (state.localTrack.mediaStreamTrack) {
    state.localTrack.mediaStreamTrack.enabled = !muted;
  }
  state.localMuted = muted;
  el.micState.textContent = muted ? "已静音" : "已发布";
  updateLocalMuteButton();
}

async function toggleLocalMute() {
  await setLocalTrackMuted(!state.localMuted);
  log(state.localMuted ? "客户麦克风已关闭" : "客户麦克风已开启");
  notify(state.localMuted ? "客户麦克风已关闭" : "客户麦克风已开启", "info");
}

async function createSession() {
  resetClientRuntime();
  const payload = { voice: el.voiceSelect.value };
  const sceneCode = el.sceneCodeInput.value.trim();
  const businessId = el.businessIdInput.value.trim();
  const businessParamsText = el.businessParamsInput.value.trim();
  if (!sceneCode) {
    throw new Error("请先配置并选择业务场景");
  }
  payload.sceneCode = sceneCode;
  if (businessId) {
    payload.businessId = businessId;
  }
  if (businessParamsText) {
    let businessParams = {};
    try {
      businessParams = JSON.parse(businessParamsText);
    } catch (error) {
      throw new Error(`业务参数不是合法 JSON：${error.message}`);
    }
    if (!businessParams || Array.isArray(businessParams) || typeof businessParams !== "object") {
      throw new Error("业务参数必须是 JSON object");
    }
    payload.businessParams = businessParams;
  }
  const session = await api("/ai-call/sessions", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  renderSession(session);
  await refreshAll();
  startPolling();
  log(`会话已创建：${session.callId}`);
}

async function connectRoom() {
  if (!state.session) return;
  if (!window.LivekitClient) {
    throw new Error("LiveKit Web SDK 未加载");
  }

  const { Room, RoomEvent, createLocalAudioTrack } = window.LivekitClient;
  const room = new Room({ adaptiveStream: true, dynacast: true });
  let audioTrack = null;
  state.room = room;
  room.on(RoomEvent.TrackSubscribed, (track) => {
    if (track.kind !== "audio") return;
    const media = track.attach();
    media.autoplay = true;
    media.muted = false;
    media.playsInline = true;
    startRemoteAudioMonitor(track);
    document.body.appendChild(media);
    media
      .play()
      .then(() => log("远端音频播放已启动"))
      .catch((error) => log(`远端音频播放失败：${error.message}`));
    state.remoteAudioElements.push(media);
    log("已订阅远端音频");
  });
  room.on(RoomEvent.Disconnected, () => {
    el.micState.textContent = "已断开";
    reportBrowserDisconnect().catch((error) => log(error.message));
    log("LiveKit 房间已断开");
  });

  try {
    try {
      audioTrack = await createLocalAudioTrack({
        echoCancellation: state.session.webAudioConstraints.echoCancellation,
        noiseSuppression: state.session.webAudioConstraints.noiseSuppression,
        autoGainControl: state.session.webAudioConstraints.autoGainControl,
      });
    } catch (error) {
      throw new Error(microphoneErrorMessage(error));
    }
    try {
      await room.connect(state.session.livekitUrl, state.session.participantToken);
    } catch (error) {
      throw wrapError("连接 LiveKit 房间失败", error);
    }
    try {
      await room.localParticipant.publishTrack(audioTrack);
    } catch (error) {
      throw wrapError("发布麦克风到 LiveKit 房间失败", error);
    }
    state.localTrack = audioTrack;
    state.localMuted = false;
    startLocalSpeechMonitor(audioTrack);
    await reportBrowserAudioInputDiagnostics(audioTrack);
    try {
      await reportBrowserReady();
    } catch (error) {
      throw wrapError("麦克风已连接，但上报浏览器就绪失败", error);
    }
    el.micState.textContent = "已发布";
    el.connectRoom.disabled = true;
    updateLocalMuteButton();
    log("麦克风已发布到 LiveKit 房间");
  } catch (error) {
    if (audioTrack) {
      audioTrack.stop();
    }
    stopLocalSpeechMonitor();
    room.disconnect();
    state.room = null;
    state.localTrack = null;
    state.localMuted = false;
    updateLocalMuteButton();
    throw error;
  }
}

function startLocalSpeechMonitor(track) {
  stopLocalSpeechMonitor();
  const mediaStreamTrack = track.mediaStreamTrack;
  if (!mediaStreamTrack || !window.AudioContext) return;

  const audioContext = new AudioContext();
  const stream = new MediaStream([mediaStreamTrack]);
  const source = audioContext.createMediaStreamSource(stream);
  const analyser = audioContext.createAnalyser();
  analyser.fftSize = 512;
  source.connect(analyser);
  audioContext.resume().catch(() => {});

  state.localAudioContext = audioContext;
  state.localAudioAnalyser = analyser;
  state.localAudioSamples = new Uint8Array(analyser.fftSize);
  state.localSpeechHotTicks = 0;
  state.localSpeechQuietTicks = 0;
  state.localSpeechBaselineRms = 0.006;
  state.localSpeechReportInFlight = false;
  clearLocalSpeechSegment();
  state.localAudioMonitorTimer = window.setInterval(
    checkLocalSpeechLevel,
    LOCAL_SPEECH_POLL_MS,
  );
}

function stopLocalSpeechMonitor() {
  if (state.localAudioMonitorTimer) {
    window.clearInterval(state.localAudioMonitorTimer);
  }
  if (state.localAudioContext) {
    state.localAudioContext.close().catch(() => {});
  }
  state.localAudioMonitorTimer = null;
  state.localAudioContext = null;
  state.localAudioAnalyser = null;
  state.localAudioSamples = null;
  state.localSpeechHotTicks = 0;
  state.localSpeechQuietTicks = 0;
  state.localSpeechReportInFlight = false;
  clearLocalSpeechSegment();
}

function checkLocalSpeechLevel() {
  if (!state.localAudioAnalyser || !state.localAudioSamples || !state.session) return;
  if (state.localMuted) return;

  state.localAudioAnalyser.getByteTimeDomainData(state.localAudioSamples);
  let sumSquares = 0;
  for (const sample of state.localAudioSamples) {
    const centered = (sample - 128) / 128;
    sumSquares += centered * centered;
  }
  const rms = Math.sqrt(sumSquares / state.localAudioSamples.length);

  const threshold = Math.max(
    LOCAL_SPEECH_MIN_RMS,
    state.localSpeechBaselineRms * LOCAL_SPEECH_BASELINE_MULTIPLIER +
      LOCAL_SPEECH_DELTA_RMS,
  );
  if (rms < threshold) {
    if (!state.remoteAudioActive && !state.localSpeechSegmentId) {
      state.localSpeechBaselineRms =
        state.localSpeechBaselineRms * (1 - LOCAL_SPEECH_BASELINE_ALPHA) +
        rms * LOCAL_SPEECH_BASELINE_ALPHA;
    }
    state.localSpeechHotTicks = 0;
    if (state.localSpeechSegmentId) {
      state.localSpeechQuietTicks += 1;
      if (state.localSpeechQuietTicks >= LOCAL_SPEECH_END_TICKS) {
        reportBrowserSpeechSegment("ended", rms).catch((error) => log(error.message));
        clearLocalSpeechSegment();
      }
    }
    return;
  }

  state.localSpeechHotTicks += 1;
  state.localSpeechQuietTicks = 0;
  if (!state.localSpeechSegmentId) {
    if (state.localSpeechHotTicks < LOCAL_SPEECH_START_TICKS) return;
    startLocalSpeechSegment(rms);
    reportBrowserSpeechSegment("started", rms).catch((error) => log(error.message));
    return;
  }

  state.localSpeechSegmentHotFrameCount += 1;
  state.localSpeechSegmentPeakRms = Math.max(state.localSpeechSegmentPeakRms, rms);
  if (Date.now() - state.localSpeechSegmentLastReportAt >= LOCAL_SPEECH_SEGMENT_UPDATE_MS) {
    reportBrowserSpeechSegment("updated", rms).catch((error) => log(error.message));
  }
}

function startRemoteAudioMonitor(track) {
  stopRemoteAudioMonitor();
  const mediaStreamTrack = track.mediaStreamTrack;
  if (!mediaStreamTrack || !window.AudioContext) return;

  // 元素播放事件只代表轨道开始播放，不能准确反映每轮 AI 音频首包。
  const audioContext = new AudioContext();
  const stream = new MediaStream([mediaStreamTrack]);
  const source = audioContext.createMediaStreamSource(stream);
  const analyser = audioContext.createAnalyser();
  analyser.fftSize = 512;
  source.connect(analyser);
  audioContext.resume().catch(() => {});

  state.remoteAudioContext = audioContext;
  state.remoteAudioAnalyser = analyser;
  state.remoteAudioSamples = new Uint8Array(analyser.fftSize);
  state.remoteAudioActive = false;
  state.remoteAudioHotTicks = 0;
  state.remoteAudioQuietTicks = 0;
  state.remoteAudioRms = 0;
  state.remoteAudioMonitorTimer = window.setInterval(
    checkRemoteAudioLevel,
    REMOTE_AUDIO_POLL_MS,
  );
}

function stopRemoteAudioMonitor() {
  if (state.remoteAudioMonitorTimer) {
    window.clearInterval(state.remoteAudioMonitorTimer);
  }
  if (state.remoteAudioContext) {
    state.remoteAudioContext.close().catch(() => {});
  }
  state.remoteAudioMonitorTimer = null;
  state.remoteAudioContext = null;
  state.remoteAudioAnalyser = null;
  state.remoteAudioSamples = null;
  state.remoteAudioActive = false;
  state.remoteAudioHotTicks = 0;
  state.remoteAudioQuietTicks = 0;
  state.remoteAudioRms = 0;
}

function checkRemoteAudioLevel() {
  if (!state.remoteAudioAnalyser || !state.remoteAudioSamples || !state.session) return;

  state.remoteAudioAnalyser.getByteTimeDomainData(state.remoteAudioSamples);
  let sumSquares = 0;
  for (const sample of state.remoteAudioSamples) {
    const centered = (sample - 128) / 128;
    sumSquares += centered * centered;
  }
  const rms = Math.sqrt(sumSquares / state.remoteAudioSamples.length);
  state.remoteAudioRms = rms;
  const audioStarted = rms >= REMOTE_AUDIO_START_RMS;
  const audioReleased = rms <= REMOTE_AUDIO_RELEASE_RMS;

  if (audioStarted) {
    state.remoteAudioHotTicks += 1;
    state.remoteAudioQuietTicks = 0;
  } else if (audioReleased) {
    state.remoteAudioQuietTicks += 1;
    state.remoteAudioHotTicks = 0;
  } else {
    state.remoteAudioHotTicks = 0;
    state.remoteAudioQuietTicks = 0;
  }

  if (!state.remoteAudioActive && state.remoteAudioHotTicks >= REMOTE_AUDIO_START_TICKS) {
    state.remoteAudioActive = true;
    state.remoteAudioHotTicks = 0;
    reportBrowserFirstAudio().catch((error) => log(error.message));
  } else if (
    state.remoteAudioActive &&
    state.remoteAudioQuietTicks >= REMOTE_AUDIO_RELEASE_TICKS
  ) {
    state.remoteAudioActive = false;
    state.remoteAudioQuietTicks = 0;
  }
}

async function reportBrowserReady() {
  if (!state.session) return;
  if (state.reportedBrowserReadyFor === state.session.callId) return;

  await api(`/ai-call/sessions/${state.session.callId}/browser-events`, {
    method: "POST",
    body: JSON.stringify({ type: "browser_ready" }),
  });
  state.reportedBrowserReadyFor = state.session.callId;
  log("已上报 browser_ready");
  await refreshAll();
}

function compactDiagnosticsObject(value) {
  if (!value || typeof value !== "object") return {};
  return Object.fromEntries(
    Object.entries(value).filter(([_key, item]) => item !== undefined && item !== null),
  );
}

function buildAudioInputDiagnostics(track) {
  const mediaStreamTrack = track?.mediaStreamTrack || null;
  const trackSettings =
    mediaStreamTrack && typeof mediaStreamTrack.getSettings === "function"
      ? compactDiagnosticsObject(track.mediaStreamTrack.getSettings())
      : {};
  const trackConstraints =
    mediaStreamTrack && typeof mediaStreamTrack.getConstraints === "function"
      ? compactDiagnosticsObject(track.mediaStreamTrack.getConstraints())
      : {};
  const requestedConstraints = compactDiagnosticsObject(state.session?.webAudioConstraints || {});
  return {
    diagnosticsVersion: "browser-audio-input-v1",
    source: "livekit_local_audio_track",
    trackLabel: mediaStreamTrack?.label || "",
    trackState: compactDiagnosticsObject({
      enabled: mediaStreamTrack?.enabled,
      muted: mediaStreamTrack?.muted,
      readyState: mediaStreamTrack?.readyState,
    }),
    requestedConstraints,
    trackConstraints,
    trackSettings,
    audioContext: compactDiagnosticsObject({
      sampleRate: state.localAudioContext?.sampleRate,
      baseLatency: state.localAudioContext?.baseLatency,
      outputLatency: state.localAudioContext?.outputLatency,
    }),
  };
}

async function reportBrowserAudioInputDiagnostics(track) {
  if (!state.session) return;
  if (state.reportedAudioInputDiagnosticsFor === state.session.callId) return;
  try {
    await api(`/ai-call/sessions/${state.session.callId}/browser-events`, {
      method: "POST",
      body: JSON.stringify({
        type: "browser_audio_input_diagnostics",
        timestamp: new Date().toISOString(),
        ...buildAudioInputDiagnostics(track),
      }),
    });
    state.reportedAudioInputDiagnosticsFor = state.session.callId;
    log("已上报 browser_audio_input_diagnostics");
  } catch (error) {
    log(`上报 browser_audio_input_diagnostics 失败：${error.message}`);
  }
}

async function reportBrowserFirstAudio() {
  if (!state.session || !state.pendingBrowserFirstAudioTurnId) return;
  const turnId = state.pendingBrowserFirstAudioTurnId;
  if (state.reportedBrowserFirstAudioTurnId === turnId) return;

  await api(`/ai-call/sessions/${state.session.callId}/browser-events`, {
    method: "POST",
    body: JSON.stringify({ type: "browser_first_audio" }),
  });
  state.pendingBrowserFirstAudioTurnId = null;
  state.reportedBrowserFirstAudioTurnId = turnId;
  log("已上报 browser_first_audio");
  await refreshStatus();
  await refreshEvents();
}

function startLocalSpeechSegment(rms) {
  const now = Date.now();
  state.localSpeechSegmentId = `browser-${now}-${Math.random().toString(36).slice(2, 8)}`;
  state.localSpeechSegmentStartedAt =
    now - (LOCAL_SPEECH_START_TICKS - 1) * LOCAL_SPEECH_POLL_MS;
  state.localSpeechSegmentHotFrameCount = state.localSpeechHotTicks;
  state.localSpeechSegmentPeakRms = rms;
  state.localSpeechSegmentLastReportAt = 0;
}

function clearLocalSpeechSegment() {
  state.localSpeechSegmentId = null;
  state.localSpeechSegmentStartedAt = 0;
  state.localSpeechSegmentHotFrameCount = 0;
  state.localSpeechSegmentPeakRms = 0;
  state.localSpeechSegmentLastReportAt = 0;
}

function localSpeechSegmentDurationMs() {
  if (!state.localSpeechSegmentStartedAt) return 0;
  return Math.max(0, Date.now() - state.localSpeechSegmentStartedAt);
}

function rmsToDbfs(rms) {
  return 20 * Math.log10(Math.max(rms, MIN_RMS_FOR_DBFS));
}

function roundAudioMetric(value) {
  return Math.round(value * 10) / 10;
}

async function reportBrowserSpeechSegment(phase, rms) {
  if (!state.session || state.localSpeechReportInFlight) return;
  if (!state.localSpeechSegmentId) return;
  state.localSpeechReportInFlight = true;
  try {
    const noiseFloorRms = Math.max(state.localSpeechBaselineRms, MIN_RMS_FOR_DBFS);
    const rmsDbfs = rmsToDbfs(Math.max(rms, state.localSpeechSegmentPeakRms));
    const noiseFloorDbfs = rmsToDbfs(noiseFloorRms);
    await api(`/ai-call/sessions/${state.session.callId}/browser-events`, {
      method: "POST",
      body: JSON.stringify({
        type: "browser_user_speech_segment",
        timestamp: new Date().toISOString(),
        segmentId: state.localSpeechSegmentId,
        phase,
        durationMs: localSpeechSegmentDurationMs(),
        rmsDbfs: roundAudioMetric(rmsDbfs),
        noiseFloorDbfs: roundAudioMetric(noiseFloorDbfs),
        snrDb: roundAudioMetric(rmsDbfs - noiseFloorDbfs),
        hotFrameCount: state.localSpeechSegmentHotFrameCount,
        remoteAudioActive: state.remoteAudioActive,
        remoteAudioRmsDbfs: roundAudioMetric(rmsToDbfs(state.remoteAudioRms)),
      }),
    });
    state.localSpeechSegmentLastReportAt = Date.now();
    log(`已上报 browser_user_speech_segment:${phase}`);
    await refreshStatus();
    await refreshEvents();
  } finally {
    state.localSpeechReportInFlight = false;
  }
}

function shouldReportBrowserDisconnect() {
  return (
    state.session &&
    state.reportedBrowserReadyFor === state.session.callId &&
    !state.suppressDisconnectReport &&
    state.disconnectReportedFor !== state.session.callId
  );
}

async function reportBrowserDisconnect(options = {}) {
  if (!shouldReportBrowserDisconnect()) return;
  const callId = state.session.callId;
  const body = JSON.stringify({ type: "browser_disconnect" });
  const url = `/ai-call/sessions/${callId}/browser-events`;
  state.disconnectReportedFor = callId;

  if (options.keepalive && navigator.sendBeacon) {
    navigator.sendBeacon(url, new Blob([body], { type: "application/json" }));
    return;
  }

  await api(url, {
    method: "POST",
    body,
  });
  log("已上报浏览器断开事件");
  await refreshEvents();
}

async function refreshStatus() {
  if (!state.session) return;
  const { callId } = state.session;
  let session;
  try {
    session = await api(`/ai-call/sessions/${callId}`);
  } catch (error) {
    try {
      const detail = await api(`/ai-call/records/${callId}`);
      if (!detail.record) throw error;
      session = { status: detail.record.status };
    } catch {
      throw error;
    }
  }
  state.session.status = session.status;
  renderSessionStatus(session.status);
  setStatus(session.status, session.status === "failed" ? "is-error" : "is-ready");
  if (session.metrics) {
    renderMetrics(session.metrics);
  }
  if (isTerminalStatus(session.status)) {
    state.suppressDisconnectReport = true;
    stopClientAudioRuntime();
    disableSessionControls();
    el.refreshEvents.disabled = false;
    el.refreshRecording.disabled = false;
    el.refreshDialogue.disabled = false;
    el.refreshHandoff.disabled = false;
    el.micState.textContent = "已断开";
  }
}

async function refreshEvents() {
  if (!state.session) return;
  // 长通话会产生大量音频事件，必须按游标增量拉取。
  const params = new URLSearchParams({ limit: "200" });
  if (state.lastEventId) {
    params.set("afterEventId", state.lastEventId);
  }
  const { callId } = state.session;
  const eventPath = isTerminalStatus(state.session.status)
    ? `/ai-call/records/${callId}/events`
    : `/ai-call/sessions/${callId}/events`;
  let data;
  try {
    data = await api(`${eventPath}?${params.toString()}`);
  } catch (error) {
    if (eventPath.includes("/records/")) throw error;
    data = await api(`/ai-call/records/${callId}/events?${params.toString()}`);
  }
  appendEvents(data.rows);
}

async function refreshRecording() {
  if (!state.session) return;
  const recording = await api(`/ai-call/records/${state.session.callId}/recording`);
  renderRecording(recording);
}

async function refreshHandoff() {
  if (!state.session) return;
  const { callId } = state.session;
  let handoff = null;
  let currentError = null;
  let historyChecked = false;

  try {
    handoff = await api(`/ai-call/sessions/${callId}/handoff`);
  } catch (error) {
    currentError = error;
  }

  if (
    !handoff &&
    (currentError || isTerminalStatus(state.session.status) || state.handoff)
  ) {
    const history = await api(`/ai-call/records/${callId}/handoffs`);
    historyChecked = true;
    handoff = (history.rows || [])[0] || null;
  }

  if (currentError && !handoff && !historyChecked) {
    throw currentError;
  }

  renderHandoff(handoff);
}

async function refreshDialogue() {
  if (!state.session) return;
  const endpoint = isTerminalStatus(state.session.status)
    ? `/ai-call/records/${state.session.callId}/dialogue-segments`
    : `/ai-call/sessions/${state.session.callId}/dialogue-preview`;
  const data = await api(endpoint);
  renderDialogue(data.rows || []);
}

async function refreshAll() {
  await refreshStatus();
  await refreshRecording();
  await refreshHandoff();
  await refreshDialogue();
  await refreshEvents();
  maybeStopPollingAfterRefresh();
}

function maybeStopPollingAfterRefresh() {
  if (state.session && isTerminalStatus(state.session.status) && !state.asrActive) {
    stopPolling();
  }
}

function startPolling() {
  stopPolling();
  state.pollTimer = window.setInterval(() => {
    refreshAll().catch((error) => {
      setStatus("刷新失败", "is-error");
      log(error.message);
    });
  }, 1500);
}

async function endSession() {
  if (!state.session) return;
  state.suppressDisconnectReport = true;
  stopClientAudioRuntime();
  await api(`/ai-call/sessions/${state.session.callId}/end`, { method: "POST" });
  state.session.status = "completed";
  renderSessionStatus("completed");
  setStatus("completed");
  disableSessionControls();
  el.refreshEvents.disabled = false;
  el.refreshRecording.disabled = false;
  el.refreshDialogue.disabled = false;
  el.refreshHandoff.disabled = false;
  el.micState.textContent = "已断开";
  await refreshRecording();
  await refreshHandoff();
  await refreshDialogue();
  await refreshEvents();
  if (state.asrActive) {
    startPolling();
  } else {
    stopPolling();
  }
  log("会话已结束");
}

async function requestHandoff() {
  if (!state.session || state.handoffRequesting) return null;
  state.handoffRequesting = true;
  updateHandoffControls();
  try {
    const handoff = await api(`/ai-call/sessions/${state.session.callId}/handoffs`, {
      method: "POST",
      body: JSON.stringify({
        source: "operator",
        reason: "customer_request",
        requestMessage: "验证页手工发起转人工",
      }),
    });
    renderHandoff(handoff);
    await refreshStatus();
    await refreshEvents();
    log(`已发起转人工：${handoff.handoffId}`);
    return handoff;
  } finally {
    state.handoffRequesting = false;
    updateHandoffControls();
  }
}

function bindActions() {
  el.createSession.addEventListener("click", () => {
    runWithLoading(
      {
        button: el.createSession,
        loadingText: "创建中...",
        title: "正在创建会话",
        desc: "正在请求服务端创建 LiveKit 验收会话",
      },
      createSession,
    )
      .then(() => notify("会话已创建"))
      .catch((error) => {
        const message = actionErrorMessage(error, "创建会话失败");
        setStatus("创建失败", "is-error");
        log(message);
        notify(message, "error");
      });
  });
  el.connectRoom.addEventListener("click", () => {
    runWithLoading(
      {
        button: el.connectRoom,
        loadingText: "连接中...",
        title: "正在连接麦克风",
        desc: "正在申请音频轨道并连接 LiveKit 房间",
      },
      connectRoom,
    )
      .then(() => notify("麦克风已连接"))
      .catch((error) => {
        const message = actionErrorMessage(error, "连接麦克风失败");
        setStatus("连接失败", "is-error");
        log(message);
        notify(message, "error");
      });
  });
  el.muteRoom.addEventListener("click", () => {
    toggleLocalMute().catch((error) => {
      log(error.message);
      notify(error.message || "麦克风操作失败", "error");
    });
  });
  el.endSession.addEventListener("click", () => {
    endSession()
      .then(() => notify("会话已结束"))
      .catch((error) => {
        setStatus("结束失败", "is-error");
        log(error.message);
        notify(error.message || "结束会话失败", "error");
      });
  });
  el.refreshStatus.addEventListener("click", () => {
    refreshStatus()
      .then(() => notify("运行态已刷新"))
      .catch((error) => {
        log(error.message);
        notify(error.message || "刷新失败", "error");
      });
  });
  el.refreshEvents.addEventListener("click", () => {
    refreshEvents()
      .then(() => notify("事件已刷新"))
      .catch((error) => {
        log(error.message);
        notify(error.message || "刷新事件失败", "error");
      });
  });
  el.refreshRecording.addEventListener("click", () => {
    refreshRecording()
      .then(() => notify("录音状态已刷新"))
      .catch((error) => {
        log(error.message);
        notify(error.message || "刷新录音失败", "error");
      });
  });
  el.refreshHandoff.addEventListener("click", () => {
    refreshHandoff()
      .then(() => notify("转人工状态已刷新"))
      .catch((error) => {
        log(error.message);
        notify(error.message || "刷新转人工状态失败", "error");
      });
  });
  el.refreshDialogue.addEventListener("click", () => {
    refreshDialogue()
      .then(() => notify("通话文本已刷新"))
      .catch((error) => {
        log(error.message);
        notify(error.message || "刷新通话文本失败", "error");
      });
  });
  el.requestHandoff.addEventListener("click", () => {
    requestHandoff()
      .then((handoff) => {
        if (handoff) notify("转人工已发起");
      })
      .catch((error) => {
        log(error.message);
        notify(error.message || "发起转人工失败", "error");
        refreshHandoff().catch(() => {});
      });
  });
}

window.addEventListener("pagehide", () => {
  reportBrowserDisconnect({ keepalive: true });
});

document.documentElement.dataset.livekitReady = String(Boolean(window.LivekitClient));
bindActions();
loadVoiceProfiles().catch((error) => log(`音色列表加载失败：${error.message}`));
loadPromptProfiles().catch((error) => log(`业务场景加载失败：${error.message}`));
