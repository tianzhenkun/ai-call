const DEFAULT_TARGET_MODEL = "qwen3.5-omni-plus-realtime";

const state = {
  voices: [],
  voiceType: "",
};

const el = {
  openCreateVoice: document.querySelector("#open-create-voice"),
  closeCreateVoice: document.querySelector("#close-create-voice"),
  cancelCreateVoice: document.querySelector("#cancel-create-voice"),
  refreshVoices: document.querySelector("#refresh-voices"),
  voiceTypeButtons: [...document.querySelectorAll("[data-voice-type]")],
  voiceCount: document.querySelector("#voice-count"),
  voiceList: document.querySelector("#voice-list"),
  voiceModal: document.querySelector("#voice-modal"),
  voiceForm: document.querySelector("#voice-form"),
  voiceInput: document.querySelector("#voice-input"),
  displayNameInput: document.querySelector("#display-name-input"),
  genderSelect: document.querySelector("#gender-select"),
  targetModelInput: document.querySelector("#target-model-input"),
  remarkInput: document.querySelector("#remark-input"),
  formMessage: document.querySelector("#voice-form-message"),
};

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
  return body.data ?? body;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
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

function buildQuery() {
  const params = new URLSearchParams({
    pageSize: "500",
    targetModel: DEFAULT_TARGET_MODEL,
  });
  if (state.voiceType) params.set("voiceType", state.voiceType);
  return params.toString();
}

function renderVoices(rows) {
  el.voiceCount.textContent = `${rows.length} 个`;
  if (!rows.length) {
    el.voiceList.innerHTML = '<div class="empty-state">暂无音色</div>';
    return;
  }
  el.voiceList.innerHTML = rows
    .map(
      (voice) => `
        <article class="voice-row">
          <div class="voice-main">
            <strong>${escapeHtml(voice.displayName || voice.voice)}</strong>
            <span class="mono">${escapeHtml(voice.voice)}</span>
          </div>
          <div class="voice-tags">
            <span>${escapeHtml(voice.voiceType)}</span>
            <span>${escapeHtml(voice.gender)}</span>
            <span class="mono">${escapeHtml(voice.targetModel)}</span>
          </div>
          <p>${escapeHtml(voice.description || voice.remark || "-")}</p>
        </article>
      `,
    )
    .join("");
}

function setFilter(voiceType) {
  state.voiceType = voiceType;
  for (const button of el.voiceTypeButtons) {
    button.classList.toggle("is-active", button.dataset.voiceType === voiceType);
  }
}

function setFormMessage(text, mode = "") {
  el.formMessage.textContent = text;
  el.formMessage.className = `form-message ${mode}`.trim();
}

function openModal() {
  el.voiceModal.classList.add("is-open");
  el.voiceModal.setAttribute("aria-hidden", "false");
  el.voiceForm.reset();
  el.targetModelInput.value = DEFAULT_TARGET_MODEL;
  setFormMessage("");
  setTimeout(() => el.voiceInput.focus(), 0);
}

function closeModal() {
  el.voiceModal.classList.remove("is-open");
  el.voiceModal.setAttribute("aria-hidden", "true");
}

async function refreshVoices() {
  el.voiceList.innerHTML = '<div class="empty-state">加载中...</div>';
  const result = await api(`/ai-call/voice-profiles?${buildQuery()}`);
  state.voices = result.rows || [];
  renderVoices(state.voices);
}

async function createVoiceProfile(event) {
  event.preventDefault();
  setFormMessage("");
  const payload = {
    voice: el.voiceInput.value.trim(),
    displayName: el.displayNameInput.value.trim(),
    gender: el.genderSelect.value,
    targetModel: el.targetModelInput.value.trim() || DEFAULT_TARGET_MODEL,
    remark: el.remarkInput.value.trim() || null,
  };
  if (!payload.voice || !payload.displayName) {
    setFormMessage("voice 和展示名不能为空", "is-error");
    return;
  }
  const submitButton = el.voiceForm.querySelector('button[type="submit"]');
  submitButton.disabled = true;
  try {
    await api("/ai-call/voice-profiles", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    closeModal();
    setFilter("自定义复刻");
    await refreshVoices();
    notify("音色已保存");
  } catch (error) {
    setFormMessage(error.message, "is-error");
    notify(error.message || "保存音色失败", "error");
  } finally {
    submitButton.disabled = false;
  }
}

async function run(action) {
  try {
    await action();
  } catch (error) {
    el.voiceList.innerHTML = `<div class="empty-state is-error">${escapeHtml(error.message)}</div>`;
    notify(error.message || "操作失败", "error");
  }
}

el.refreshVoices.addEventListener("click", () =>
  run(async () => {
    await refreshVoices();
    notify("音色列表已刷新");
  }),
);
el.openCreateVoice.addEventListener("click", openModal);
el.closeCreateVoice.addEventListener("click", closeModal);
el.cancelCreateVoice.addEventListener("click", closeModal);
el.voiceForm.addEventListener("submit", createVoiceProfile);
el.voiceModal.addEventListener("click", (event) => {
  if (event.target === el.voiceModal) closeModal();
});
window.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && el.voiceModal.classList.contains("is-open")) {
    closeModal();
  }
});
for (const button of el.voiceTypeButtons) {
  button.addEventListener("click", () => {
    setFilter(button.dataset.voiceType || "");
    run(refreshVoices);
  });
}

setFilter("");
run(refreshVoices);
