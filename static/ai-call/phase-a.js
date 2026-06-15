const state = {
        session: null,
        room: null,
        localTrack: null,
        pollTimer: null,
        pendingBrowserFirstAudio: false,
        browserFirstAudioReportInFlight: false,
        remoteAudioContext: null,
        remoteAudioMonitorTimer: null,
        remoteAudioAnalyser: null,
        remoteAudioSamples: null,
        remoteAudioHotTicks: 0,
        remoteAudioAttachments: [],
        reportedBrowserReadyFor: null,
        speechAudioContext: null,
        speechMonitorTimer: null,
        speechAnalyser: null,
        speechSamples: null,
        browserSpeechActive: false,
        browserSpeechHotTicks: 0,
        browserSpeechQuietTicks: 0,
        browserSpeechLastActiveAt: 0,
        browserSpeechReportArmed: false,
        lastBrowserSpeechReportAt: 0,
        browserSpeechNoiseFloor: 0,
        browserSpeechNoiseSampleCount: 0,
      };

      const BROWSER_SPEECH_POLL_MS = 40;
      const BROWSER_SPEECH_START_RMS = 0.055;
      const BROWSER_SPEECH_RELEASE_RMS = 0.025;
      const BROWSER_SPEECH_START_TICKS = 4;
      const BROWSER_SPEECH_RELEASE_TICKS = 8;
      const BROWSER_SPEECH_REPORT_COOLDOWN_MS = 700;
      const BROWSER_SPEECH_HOLD_MS = 900;
      const BROWSER_SPEECH_NOISE_SAMPLE_COUNT = 20;
      const BROWSER_SPEECH_NOISE_MULTIPLIER = 1.6;
      const BROWSER_SPEECH_NOISE_OFFSET = 0.018;
      const BROWSER_SPEECH_MAX_START_RMS = 0.068;
      const BROWSER_SPEECH_RELEASE_RATIO = 0.45;
      const REMOTE_AUDIO_POLL_MS = 40;
      const REMOTE_AUDIO_FIRST_RMS = 0.012;
      const REMOTE_AUDIO_FIRST_TICKS = 2;

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
        metricBrowserFirst: document.querySelector("#metric-browser-first"),
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
        el.metricBrowserFirst.textContent = formatMetric(metrics.lastBrowserFirstAudioMs);
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

      async function createSession() {
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
          detachRemoteAudioElements("track_subscribed");
          const media = track.attach();
          media.autoplay = true;
          media.onplaying = () => reportBrowserFirstAudio().catch((error) => log(error.message));
          state.remoteAudioAttachments.push({ track, media });
          document.body.appendChild(media);
          startRemoteAudioMonitor(track);
          reportBrowserRemoteAudioTrackState("track_subscribed", track).catch((error) =>
            log(error.message),
          );
          log(`已订阅远端音频，当前播放元素 ${state.remoteAudioAttachments.length}`);
        });
        room.on(RoomEvent.TrackUnsubscribed, (track) => {
          if (track.kind !== "audio") return;
          detachRemoteAudioElements("track_unsubscribed");
        });
        room.on(RoomEvent.Disconnected, () => {
          detachRemoteAudioElements("room_disconnected");
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
          detachRemoteAudioElements("connect_failed");
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

      function armBrowserFirstAudio() {
        state.pendingBrowserFirstAudio = true;
        state.remoteAudioHotTicks = 0;
      }

      function remoteAudioTrackId(track) {
        if (!track) return "";
        return track.sid || track.trackSid || track.mediaStreamTrack?.id || "";
      }

      function detachRemoteAudioElements(reason) {
        stopRemoteAudioMonitor();
        const detachedCount = state.remoteAudioAttachments.length;
        for (const attachment of state.remoteAudioAttachments) {
          const { track, media } = attachment;
          media.onplaying = null;
          media.pause();
          if (typeof track.detach === "function") {
            try {
              track.detach(media);
            } catch (_error) {
              try {
                track.detach();
              } catch (_ignored) {}
            }
          }
          media.remove();
        }
        state.remoteAudioAttachments = [];
        if (detachedCount > 0) {
          reportBrowserRemoteAudioTrackState(reason, null, { detachedCount }).catch((error) =>
            log(error.message),
          );
        }
      }

      async function reportBrowserRemoteAudioTrackState(reason, track = null, extra = {}) {
        if (!state.session) return;
        await api(`/ai-call/sessions/${state.session.callId}/browser-events`, {
          method: "POST",
          body: JSON.stringify({
            type: "browser_remote_audio_track_state",
            payload: {
              reason,
              trackSid: remoteAudioTrackId(track),
              remoteAudioElementCount: state.remoteAudioAttachments.length,
              ...extra,
            },
          }),
        });
      }

      function startRemoteAudioMonitor(track) {
        stopRemoteAudioMonitor();
        const mediaStreamTrack = track.mediaStreamTrack;
        if (!mediaStreamTrack || !window.AudioContext) return;

        const audioContext = new AudioContext();
        const stream = new MediaStream([mediaStreamTrack]);
        const source = audioContext.createMediaStreamSource(stream);
        const analyser = audioContext.createAnalyser();
        analyser.fftSize = 512;
        source.connect(analyser);

        state.remoteAudioContext = audioContext;
        state.remoteAudioAnalyser = analyser;
        state.remoteAudioSamples = new Uint8Array(analyser.fftSize);
        state.remoteAudioHotTicks = 0;
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
        state.remoteAudioHotTicks = 0;
      }

      function checkRemoteAudioLevel() {
        if (!state.remoteAudioAnalyser || !state.remoteAudioSamples) return;
        if (!state.pendingBrowserFirstAudio || state.browserFirstAudioReportInFlight) return;

        state.remoteAudioAnalyser.getByteTimeDomainData(state.remoteAudioSamples);
        let sumSquares = 0;
        for (const sample of state.remoteAudioSamples) {
          const centered = (sample - 128) / 128;
          sumSquares += centered * centered;
        }
        const rms = Math.sqrt(sumSquares / state.remoteAudioSamples.length);

        if (rms >= REMOTE_AUDIO_FIRST_RMS) {
          state.remoteAudioHotTicks += 1;
        } else {
          state.remoteAudioHotTicks = 0;
        }

        if (state.remoteAudioHotTicks >= REMOTE_AUDIO_FIRST_TICKS) {
          state.remoteAudioHotTicks = 0;
          reportBrowserFirstAudio().catch((error) => log(error.message));
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

        state.speechAudioContext = audioContext;
        state.speechAnalyser = analyser;
        state.speechSamples = new Uint8Array(analyser.fftSize);
        state.browserSpeechActive = false;
        state.browserSpeechHotTicks = 0;
        state.browserSpeechQuietTicks = 0;
        state.browserSpeechLastActiveAt = 0;
        state.browserSpeechReportArmed = false;
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
        state.browserSpeechLastActiveAt = 0;
        state.browserSpeechReportArmed = false;
        state.lastBrowserSpeechReportAt = 0;
        state.browserSpeechNoiseFloor = 0;
        state.browserSpeechNoiseSampleCount = 0;
      }

      function updateBrowserSpeechNoiseFloor(rms) {
        if (state.browserSpeechActive) return;
        if (state.browserSpeechNoiseSampleCount >= BROWSER_SPEECH_NOISE_SAMPLE_COUNT) return;
        if (rms >= BROWSER_SPEECH_START_RMS) return;

        state.browserSpeechNoiseFloor =
          (state.browserSpeechNoiseFloor * state.browserSpeechNoiseSampleCount + rms) /
          (state.browserSpeechNoiseSampleCount + 1);
        state.browserSpeechNoiseSampleCount += 1;
      }

      function currentBrowserSpeechStartRms() {
        if (state.browserSpeechNoiseSampleCount < BROWSER_SPEECH_NOISE_SAMPLE_COUNT) {
          return BROWSER_SPEECH_START_RMS;
        }
        return Math.min(
          BROWSER_SPEECH_MAX_START_RMS,
          Math.max(
            BROWSER_SPEECH_START_RMS,
            state.browserSpeechNoiseFloor * BROWSER_SPEECH_NOISE_MULTIPLIER +
              BROWSER_SPEECH_NOISE_OFFSET,
          ),
        );
      }

      function currentBrowserSpeechReleaseRms(startRms) {
        return Math.max(
          BROWSER_SPEECH_RELEASE_RMS,
          startRms * BROWSER_SPEECH_RELEASE_RATIO,
        );
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
        updateBrowserSpeechNoiseFloor(rms);
        const speechStartRms = currentBrowserSpeechStartRms();
        const speechReleaseRms = currentBrowserSpeechReleaseRms(speechStartRms);
        const speechStarted = rms >= speechStartRms;
        const speechReleased = rms <= speechReleaseRms;
        const now = Date.now();

        if (speechStarted) {
          state.browserSpeechHotTicks += 1;
          state.browserSpeechQuietTicks = 0;
          state.browserSpeechLastActiveAt = now;
          state.browserSpeechReportArmed = true;
        } else if (speechReleased) {
          state.browserSpeechQuietTicks += 1;
          state.browserSpeechHotTicks = 0;
        } else {
          state.browserSpeechHotTicks = 0;
          state.browserSpeechQuietTicks = 0;
          if (state.browserSpeechActive) {
            state.browserSpeechLastActiveAt = now;
            state.browserSpeechReportArmed = true;
          }
        }

        const speechHeldRecently =
          state.browserSpeechActive &&
          now - state.browserSpeechLastActiveAt <= BROWSER_SPEECH_HOLD_MS;

        if (
          !state.browserSpeechActive &&
          state.browserSpeechHotTicks >= BROWSER_SPEECH_START_TICKS
        ) {
          state.browserSpeechActive = true;
          state.browserSpeechHotTicks = 0;
          state.browserSpeechLastActiveAt = now;
          if (reportBrowserUserSpeechStarted()) {
            state.browserSpeechReportArmed = false;
          }
        } else if (
          state.browserSpeechActive &&
          state.browserSpeechReportArmed &&
          speechHeldRecently
        ) {
          state.browserSpeechHotTicks = 0;
          if (reportBrowserUserSpeechStarted()) {
            state.browserSpeechReportArmed = false;
          }
        } else if (
          state.browserSpeechActive &&
          !speechHeldRecently &&
          state.browserSpeechQuietTicks >= BROWSER_SPEECH_RELEASE_TICKS
        ) {
          state.browserSpeechActive = false;
          state.browserSpeechQuietTicks = 0;
          state.browserSpeechLastActiveAt = 0;
          state.browserSpeechReportArmed = false;
          armBrowserFirstAudio();
        }
      }

      function reportBrowserUserSpeechStarted() {
        if (!state.session) return false;
        const now = Date.now();
        if (now - state.lastBrowserSpeechReportAt < BROWSER_SPEECH_REPORT_COOLDOWN_MS) {
          return false;
        }
        state.lastBrowserSpeechReportAt = now;

        api(`/ai-call/sessions/${state.session.callId}/browser-events`, {
          method: "POST",
          body: JSON.stringify({ type: "browser_user_speech_started" }),
        })
          .then(() => log("已上报 browser_user_speech_started"))
          .catch((error) => log(error.message));
        return true;
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
        armBrowserFirstAudio();
        await refreshAll();
      }

      async function reportBrowserFirstAudio() {
        if (!state.session || !state.pendingBrowserFirstAudio) return;
        if (state.browserFirstAudioReportInFlight) return;

        state.browserFirstAudioReportInFlight = true;
        try {
          await api(`/ai-call/sessions/${state.session.callId}/browser-events`, {
            method: "POST",
            body: JSON.stringify({ type: "browser_first_audio" }),
          });
          state.pendingBrowserFirstAudio = false;
          log("已上报 browser_first_audio");
          await refreshStatus();
          await refreshEvents();
        } finally {
          state.browserFirstAudioReportInFlight = false;
        }
      }

      async function refreshStatus() {
        if (!state.session) return;
        const session = await api(`/ai-call/sessions/${state.session.callId}`);
        setStatus(session.status, session.status === "failed" ? "is-error" : "is-ready");
        renderMetrics(session.metrics);
      }

      async function refreshEvents() {
        if (!state.session) return;
        const data = await api(`/ai-call/sessions/${state.session.callId}/events?limit=200`);
        renderEvents(data.rows);
      }

      async function refreshAll() {
        await refreshStatus();
        await refreshEvents();
      }

      function startPolling() {
        window.clearInterval(state.pollTimer);
        state.pollTimer = window.setInterval(() => {
          refreshAll().catch((error) => {
            setStatus("刷新失败", "is-error");
            log(error.message);
          });
        }, 1500);
      }

      async function endSession() {
        if (!state.session) return;
        detachRemoteAudioElements("session_ending");
        stopLocalSpeechMonitor();
        if (state.localTrack) {
          state.localTrack.stop();
        }
        if (state.room) {
          state.room.disconnect();
        }
        await api(`/ai-call/sessions/${state.session.callId}/end`, { method: "POST" });
        window.clearInterval(state.pollTimer);
        setStatus("completed");
        el.connectRoom.disabled = true;
        el.endSession.disabled = true;
        el.refreshStatus.disabled = true;
        el.refreshEvents.disabled = true;
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
