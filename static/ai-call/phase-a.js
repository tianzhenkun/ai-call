const state = {
  session: null,
  room: null,
  localTrack: null,
  pollTimer: null,
  events: [],
  eventIds: new Set(),
  lastEventId: null,
  pendingBrowserFirstAudioTurnId: null,
  reportedBrowserFirstAudioTurnId: null,
  reportedBrowserReadyFor: null,
  speechAudioContext: null,
  speechMonitorTimer: null,
  speechAnalyser: null,
  speechSamples: null,
  browserSpeechActive: false,
  browserSpeechHotTicks: 0,
  browserSpeechQuietTicks: 0,
  lastBrowserSpeechReportAt: 0,
  remoteAudioContext: null,
  remoteAudioMonitorTimer: null,
  remoteAudioAnalyser: null,
  remoteAudioSamples: null,
  remoteAudioActive: false,
  remoteAudioHotTicks: 0,
  remoteAudioQuietTicks: 0,
  remoteAudioElements: [],
};

const EVENT_RENDER_LIMIT = 300;
const BROWSER_SPEECH_POLL_MS = 40;
const BROWSER_SPEECH_START_RMS = 0.055;
const BROWSER_SPEECH_RELEASE_RMS = 0.025;
const BROWSER_SPEECH_START_TICKS = 4;
const BROWSER_SPEECH_RELEASE_TICKS = 8;
const BROWSER_SPEECH_REPORT_COOLDOWN_MS = 1200;
const REMOTE_AUDIO_POLL_MS = 40;
const REMOTE_AUDIO_START_RMS = 0.015;
const REMOTE_AUDIO_RELEASE_RMS = 0.006;
const REMOTE_AUDIO_START_TICKS = 2;
const REMOTE_AUDIO_RELEASE_TICKS = 6;

const el = {
  statusPill: document.querySelector("#status-pill"),
  createSession: document.querySelector("#create-session"),
  connectRoom: document.querySelector("#connect-room"),
  endSession: document.querySelector("#end-session"),
  refreshStatus: document.querySelector("#refresh-status"),
  refreshEvents: document.querySelector("#refresh-events"),
  voiceSelect: document.querySelector("#voice-select"),
  promptInput: document.querySelector("#prompt-input"),
  callId: document.querySelector("#call-id"),
  roomName: document.querySelector("#room-name"),
  modelName: document.querySelector("#model-name"),
  openingState: document.querySelector("#opening-state"),
  micState: document.querySelector("#mic-state"),
  constraints: document.querySelector("#constraints"),
  eventList: document.querySelector("#event-list"),
  log: document.querySelector("#log"),
  metricModelFirst: document.querySelector("#metric-model-first"),
  metricModelStats: document.querySelector("#metric-model-stats"),
  metricBrowserFirst: document.querySelector("#metric-browser-first"),
  metricBrowserStats: document.querySelector("#metric-browser-stats"),
  metricInterrupt: document.querySelector("#metric-interrupt"),
  metricQueue: document.querySelector("#metric-queue"),
};

function log(message) {
  const at = new Date().toLocaleTimeString();
  el.log.textContent = `[${at}] ${message}\n${el.log.textContent}`;
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok || body.code !== 200) {
    throw new Error(body.msg || `HTTP ${response.status}`);
  }
  return body.data;
}

function setStatus(text, mode = "") {
  el.statusPill.textContent = text;
  el.statusPill.className = `status-pill ${mode}`.trim();
}

function formatMetric(value) {
  return value === null || value === undefined ? "-" : `${value}ms`;
}

function formatMetricStats(count, p50, p90, max) {
  if (!count) return "-";
  return `P50 ${formatMetric(p50)} / P90 ${formatMetric(p90)} / Max ${formatMetric(max)}`;
}

function isTerminalStatus(status) {
  return status === "completed" || status === "failed";
}

function renderSession(session) {
  state.session = session;
  el.callId.textContent = session.callId;
  el.roomName.textContent = session.roomName;
  el.modelName.textContent = session.effectiveConfig.model;
  el.openingState.textContent = session.effectiveConfig.openingEnabled ? "启用" : "关闭";
  el.connectRoom.disabled = false;
  el.endSession.disabled = false;
  el.refreshStatus.disabled = false;
  el.refreshEvents.disabled = false;
  setStatus(session.status, "is-ready");
  renderConstraints(session.webAudioConstraints);
}

function renderConstraints(constraints) {
  if (!constraints) {
    el.constraints.innerHTML = '<span class="constraint">无</span>';
    return;
  }
  el.constraints.innerHTML = [
    ["echoCancellation", constraints.echoCancellation],
    ["noiseSuppression", constraints.noiseSuppression],
    ["autoGainControl", constraints.autoGainControl],
  ]
    .map(([name, enabled]) => {
      const text = enabled ? "on" : "off";
      return `<span class="constraint">${name}: ${text}</span>`;
    })
    .join("");
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
      const time = new Date(event.timestamp).toLocaleTimeString();
      return `
        <div class="event">
          <div>
            <div class="event-type">${event.type}</div>
            <div class="event-meta">${event.source}</div>
          </div>
          <div class="event-meta">
            <div>${time}</div>
            <div class="mono">${event.eventId}</div>
          </div>
        </div>
      `;
    })
    .join("");
}

function resetEventState() {
  state.events = [];
  state.eventIds = new Set();
  state.lastEventId = null;
  state.pendingBrowserFirstAudioTurnId = null;
  state.reportedBrowserFirstAudioTurnId = null;
  renderEvents([]);
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
  if (event.type === "opening_started" || event.type === "user_speech_stopped") {
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
  stopLocalSpeechMonitor();
  stopRemoteAudioMonitor();
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
  state.room = null;
  state.remoteAudioElements = [];
}

function resetClientRuntime() {
  stopPolling();
  stopClientAudioRuntime();
  resetEventState();
  state.session = null;
  state.reportedBrowserReadyFor = null;
  el.micState.textContent = "未连接";
}

function disableSessionControls() {
  el.connectRoom.disabled = true;
  el.endSession.disabled = true;
  el.refreshStatus.disabled = true;
  el.refreshEvents.disabled = true;
}

async function createSession() {
  resetClientRuntime();
  const payload = { voice: el.voiceSelect.value };
  const prompt = el.promptInput.value.trim();
  if (prompt) {
    payload.prompt = prompt;
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
    startRemoteAudioMonitor(track);
    document.body.appendChild(media);
    state.remoteAudioElements.push(media);
    log("已订阅远端音频");
  });
  room.on(RoomEvent.Disconnected, () => {
    el.micState.textContent = "已断开";
    log("LiveKit Room 已断开");
  });

  try {
    audioTrack = await createLocalAudioTrack({
      echoCancellation: state.session.webAudioConstraints.echoCancellation,
      noiseSuppression: state.session.webAudioConstraints.noiseSuppression,
      autoGainControl: state.session.webAudioConstraints.autoGainControl,
    });
    await room.connect(state.session.livekitUrl, state.session.participantToken);
    await room.localParticipant.publishTrack(audioTrack);
    state.localTrack = audioTrack;
    startLocalSpeechMonitor(audioTrack);
    await reportBrowserReady();
    el.micState.textContent = "已发布";
    el.connectRoom.disabled = true;
    log("麦克风已发布到 LiveKit Room");
  } catch (error) {
    stopLocalSpeechMonitor();
    if (audioTrack) {
      audioTrack.stop();
    }
    room.disconnect();
    state.room = null;
    state.localTrack = null;
    throw error;
  }
}

function startLocalSpeechMonitor(audioTrack) {
  stopLocalSpeechMonitor();
  const mediaStreamTrack = audioTrack.mediaStreamTrack;
  if (!mediaStreamTrack || !window.AudioContext) return;

  const audioContext = new AudioContext();
  const stream = new MediaStream([mediaStreamTrack]);
  const source = audioContext.createMediaStreamSource(stream);
  const analyser = audioContext.createAnalyser();
  analyser.fftSize = 512;
  source.connect(analyser);
  audioContext.resume().catch(() => {});

  state.speechAudioContext = audioContext;
  state.speechAnalyser = analyser;
  state.speechSamples = new Uint8Array(analyser.fftSize);
  state.browserSpeechActive = false;
  state.browserSpeechHotTicks = 0;
  state.browserSpeechQuietTicks = 0;
  state.lastBrowserSpeechReportAt = 0;
  state.speechMonitorTimer = window.setInterval(
    checkLocalSpeechLevel,
    BROWSER_SPEECH_POLL_MS,
  );
}

function stopLocalSpeechMonitor() {
  if (state.speechMonitorTimer) {
    window.clearInterval(state.speechMonitorTimer);
  }
  if (state.speechAudioContext) {
    state.speechAudioContext.close().catch(() => {});
  }
  state.speechMonitorTimer = null;
  state.speechAudioContext = null;
  state.speechAnalyser = null;
  state.speechSamples = null;
  state.browserSpeechActive = false;
  state.browserSpeechHotTicks = 0;
  state.browserSpeechQuietTicks = 0;
  state.lastBrowserSpeechReportAt = 0;
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

function checkLocalSpeechLevel() {
  if (!state.speechAnalyser || !state.speechSamples || !state.session) return;

  state.speechAnalyser.getByteTimeDomainData(state.speechSamples);
  let sumSquares = 0;
  for (const sample of state.speechSamples) {
    const centered = (sample - 128) / 128;
    sumSquares += centered * centered;
  }
  const rms = Math.sqrt(sumSquares / state.speechSamples.length);
  const speechStarted = rms >= BROWSER_SPEECH_START_RMS;
  const speechReleased = rms <= BROWSER_SPEECH_RELEASE_RMS;

  if (speechStarted) {
    state.browserSpeechHotTicks += 1;
    state.browserSpeechQuietTicks = 0;
  } else if (speechReleased) {
    state.browserSpeechQuietTicks += 1;
    state.browserSpeechHotTicks = 0;
  } else {
    state.browserSpeechHotTicks = 0;
    state.browserSpeechQuietTicks = 0;
  }

  if (
    !state.browserSpeechActive &&
    state.browserSpeechHotTicks >= BROWSER_SPEECH_START_TICKS
  ) {
    state.browserSpeechActive = true;
    state.browserSpeechHotTicks = 0;
    reportBrowserUserSpeechStarted().catch((error) => log(error.message));
  } else if (
    state.browserSpeechActive &&
    state.browserSpeechQuietTicks >= BROWSER_SPEECH_RELEASE_TICKS
  ) {
    state.browserSpeechActive = false;
    state.browserSpeechQuietTicks = 0;
  }
}

async function reportBrowserUserSpeechStarted() {
  if (!state.session) return;
  const now = Date.now();
  if (now - state.lastBrowserSpeechReportAt < BROWSER_SPEECH_REPORT_COOLDOWN_MS) return;
  state.lastBrowserSpeechReportAt = now;

  await api(`/ai-call/sessions/${state.session.callId}/browser-events`, {
    method: "POST",
    body: JSON.stringify({ type: "browser_user_speech_started" }),
  });
  log("已上报 browser_user_speech_started");
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

async function refreshStatus() {
  if (!state.session) return;
  const session = await api(`/ai-call/sessions/${state.session.callId}`);
  setStatus(session.status, session.status === "failed" ? "is-error" : "is-ready");
  renderMetrics(session.metrics);
  if (isTerminalStatus(session.status)) {
    stopPolling();
    stopClientAudioRuntime();
    disableSessionControls();
    el.micState.textContent = session.status === "completed" ? "已结束" : "已失败";
  }
}

async function refreshEvents() {
  if (!state.session) return;
  // 长通话会产生大量音频事件，必须按游标增量拉取。
  const params = new URLSearchParams({ limit: "200" });
  if (state.lastEventId) {
    params.set("afterEventId", state.lastEventId);
  }
  const data = await api(
    `/ai-call/sessions/${state.session.callId}/events?${params.toString()}`,
  );
  appendEvents(data.rows);
}

async function refreshAll() {
  await refreshStatus();
  await refreshEvents();
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
  stopClientAudioRuntime();
  await api(`/ai-call/sessions/${state.session.callId}/end`, { method: "POST" });
  stopPolling();
  setStatus("completed");
  disableSessionControls();
  el.micState.textContent = "已结束";
  await refreshEvents();
  log("会话已结束");
}

function bindActions() {
  el.createSession.addEventListener("click", () => {
    createSession().catch((error) => {
      setStatus("创建失败", "is-error");
      log(error.message);
    });
  });
  el.connectRoom.addEventListener("click", () => {
    connectRoom().catch((error) => {
      setStatus("连接失败", "is-error");
      log(error.message);
    });
  });
  el.endSession.addEventListener("click", () => {
    endSession().catch((error) => {
      setStatus("结束失败", "is-error");
      log(error.message);
    });
  });
  el.refreshStatus.addEventListener("click", () => {
    refreshStatus().catch((error) => log(error.message));
  });
  el.refreshEvents.addEventListener("click", () => {
    refreshEvents().catch((error) => log(error.message));
  });
}

document.documentElement.dataset.livekitReady = String(Boolean(window.LivekitClient));
bindActions();
