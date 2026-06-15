const state = {
        session: null,
        room: null,
        localTrack: null,
        pollTimer: null,
        pendingBrowserFirstAudio: false,
        reportedBrowserFirstAudioFor: null,
        reportedBrowserReadyFor: null,
        speechAudioContext: null,
        speechMonitorTimer: null,
        speechAnalyser: null,
        speechSamples: null,
        browserSpeechActive: false,
        browserSpeechHotTicks: 0,
        browserSpeechQuietTicks: 0,
        lastBrowserSpeechReportAt: 0,
      };

      const BROWSER_SPEECH_POLL_MS = 40;
      const BROWSER_SPEECH_START_RMS = 0.055;
      const BROWSER_SPEECH_RELEASE_RMS = 0.025;
      const BROWSER_SPEECH_START_TICKS = 4;
      const BROWSER_SPEECH_RELEASE_TICKS = 8;
      const BROWSER_SPEECH_REPORT_COOLDOWN_MS = 1200;

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

        for (const event of events) {
          if (event.type === "user_speech_stopped") {
            state.pendingBrowserFirstAudio = true;
            state.reportedBrowserFirstAudioFor = null;
          }
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
          const media = track.attach();
          media.autoplay = true;
          media.onplaying = () => reportBrowserFirstAudio();
          document.body.appendChild(media);
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
        if (!state.session || !state.pendingBrowserFirstAudio) return;
        if (state.reportedBrowserFirstAudioFor === state.session.callId) return;

        await api(`/ai-call/sessions/${state.session.callId}/browser-events`, {
          method: "POST",
          body: JSON.stringify({ type: "browser_first_audio" }),
        });
        state.pendingBrowserFirstAudio = false;
        state.reportedBrowserFirstAudioFor = state.session.callId;
        log("已上报 browser_first_audio");
        await refreshStatus();
        await refreshEvents();
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
