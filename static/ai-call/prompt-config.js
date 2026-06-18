const state = {
  loadedProfileId: null,
  profiles: [],
  components: [],
};

const el = {
  refreshProfiles: document.querySelector("#refresh-profiles"),
  refreshComponents: document.querySelector("#refresh-components"),
  newProfile: document.querySelector("#new-profile"),
  saveProfile: document.querySelector("#save-profile"),
  profileList: document.querySelector("#profile-list"),
  componentList: document.querySelector("#component-list"),
  profileName: document.querySelector("#profile-name-input"),
  sceneCode: document.querySelector("#profile-scene-code-input"),
  providerKey: document.querySelector("#profile-provider-key-select"),
  promptText: document.querySelector("#profile-prompt-text-input"),
  openingMessage: document.querySelector("#profile-opening-message-input"),
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

function profilePayload() {
  const providerKey = el.providerKey.value || "static_profile";
  const isStatic = providerKey === "static_profile";
  return {
    sceneCode: el.sceneCode.value.trim(),
    name: el.profileName.value.trim(),
    providerKey,
    promptText: isStatic ? el.promptText.value.trim() : null,
    openingMessage: isStatic ? el.openingMessage.value.trim() : null,
  };
}

function validateProfilePayload(payload) {
  if (!payload.name) throw new Error("名称不能为空");
  if (!payload.sceneCode) throw new Error("场景编码不能为空");
  if (payload.providerKey === "static_profile" && !payload.promptText) {
    throw new Error("固定提示词不能为空");
  }
  if (payload.providerKey === "static_profile" && !payload.openingMessage) {
    throw new Error("固定开场白不能为空");
  }
}

function fillProfile(profile) {
  state.loadedProfileId = profile?.id || null;
  el.profileName.value = profile?.name || "";
  el.sceneCode.value = profile?.sceneCode || "";
  el.providerKey.value = profile?.providerKey || "static_profile";
  el.promptText.value = profile?.promptText || "";
  el.openingMessage.value = profile?.openingMessage || "";
  applyProviderMode();
  highlightSelectedProfile();
  renderPromptStructure();
}

function applyProviderMode() {
  const isStatic = el.providerKey.value === "static_profile";
  el.promptText.disabled = !isStatic;
  el.openingMessage.disabled = !isStatic;
  el.promptText.placeholder = isStatic
    ? ""
    : "业务查询模式：发起通话时由后端按场景编码查询业务表，并组装提示词。";
  el.openingMessage.placeholder = isStatic
    ? ""
    : "业务查询模式：发起通话时由后端按场景编码查询业务表，并组装开场白。";
  renderPromptStructure();
}

function highlightSelectedProfile() {
  for (const item of el.profileList.querySelectorAll(".profile-row")) {
    item.classList.toggle("is-active", item.dataset.profileId === String(state.loadedProfileId));
  }
}

function renderProfiles(rows) {
  if (!rows.length) {
    el.profileList.innerHTML = '<div class="subtle">暂无配置</div>';
    return;
  }
  el.profileList.innerHTML = rows
    .map(
      (profile) => `
        <button class="profile-row" type="button" data-profile-id="${escapeHtml(profile.id)}">
          <span>${escapeHtml(profile.name)}</span>
          <span class="mono">${escapeHtml(profile.sceneCode)}</span>
          <span>${escapeHtml(providerLabel(profile.providerKey))}</span>
        </button>
      `,
    )
    .join("");
  for (const item of el.profileList.querySelectorAll(".profile-row")) {
    item.addEventListener("click", () => {
      const profile = state.profiles.find((row) => String(row.id) === item.dataset.profileId);
      if (profile) {
        fillProfile(profile);
      }
    });
  }
  highlightSelectedProfile();
}

function providerLabel(providerKey) {
  if (providerKey === "static_profile") return "固定配置";
  if (providerKey === "business_query") return "业务查询";
  return providerKey || "-";
}

function componentByKey(key) {
  return state.components.find((component) => component.componentKey === key);
}

function currentBusinessContent() {
  const providerKey = el.providerKey.value || "static_profile";
  if (providerKey === "business_query") {
    return "{{业务话术}}";
  }
  return el.promptText.value.trim() || "{{固定提示词未填写}}";
}

function currentOpeningContent() {
  const providerKey = el.providerKey.value || "static_profile";
  if (providerKey === "business_query") {
    return "通话开始后，系统会触发你主动开场。请先自然说出这句开场白：{{开场白}}";
  }
  const openingMessage = el.openingMessage.value.trim() || "{{固定开场白未填写}}";
  return `通话开始后，系统会触发你主动开场。请先自然说出这句开场白：${openingMessage}`;
}

function renderPromptStructure() {
  if (!state.components.length) {
    el.componentList.innerHTML = '<div class="subtle">暂无结构</div>';
    return;
  }

  const commonKeys = ["platform_constraints", "handoff_capability"];
  const usedKeys = new Set([...commonKeys, "call_end_tool"]);
  const sections = [
    ...commonKeys
      .map((key) => componentByKey(key))
      .filter(Boolean)
      .map((component) => ({
        title: component.name,
        content: component.content,
        badge: "通用提示词",
      })),
    ...state.components
      .filter((component) => !usedKeys.has(component.componentKey))
      .map((component) => ({
        title: component.name,
        content: component.content,
      })),
    {
      title: "业务话术",
      content: currentBusinessContent(),
    },
    {
      title: "开场白约束",
      content: currentOpeningContent(),
    },
  ];
  const callEndComponent = componentByKey("call_end_tool");
  if (callEndComponent) {
    sections.push({
      title: callEndComponent.name,
      content: callEndComponent.content,
      badge: "通用提示词",
    });
  }

  el.componentList.innerHTML = sections
    .map(
      (section, index) => `
        <section class="prompt-section">
          <div class="prompt-step-index">${String(index + 1).padStart(2, "0")}</div>
          <div class="prompt-step-body">
            <div class="prompt-section-title">
              <span>${escapeHtml(section.title)}</span>
              ${section.badge ? `<span class="prompt-section-badge">${escapeHtml(section.badge)}</span>` : ""}
            </div>
            <pre class="component-content">${escapeHtml(section.content)}</pre>
          </div>
        </section>
      `,
    )
    .join("");
}

async function refreshProfiles() {
  const result = await api("/ai-call/prompt-profiles?pageSize=200");
  state.profiles = result.rows || [];
  renderProfiles(state.profiles);
  const selected = state.profiles.find((profile) => profile.id === state.loadedProfileId);
  if (selected) {
    fillProfile(selected);
  } else if (state.profiles[0]) {
    fillProfile(state.profiles[0]);
  } else {
    fillProfile(null);
  }
}

async function refreshComponents() {
  const result = await api("/ai-call/prompt-components");
  state.components = result.rows || [];
  renderPromptStructure();
}

async function saveProfile() {
  const payload = profilePayload();
  validateProfilePayload(payload);
  const isUpdate = Boolean(state.loadedProfileId);
  el.saveProfile.disabled = true;
  const path = isUpdate
    ? `/ai-call/prompt-profiles/${encodeURIComponent(state.loadedProfileId)}`
    : "/ai-call/prompt-profiles";
  const method = isUpdate ? "PUT" : "POST";
  try {
    const saved = await api(path, {
      method,
      body: JSON.stringify(payload),
    });
    fillProfile(saved);
    await refreshProfiles();
    notify(isUpdate ? "配置已保存" : "配置已创建");
  } finally {
    el.saveProfile.disabled = false;
  }
}

async function run(action) {
  try {
    await action();
  } catch (error) {
    console.error(error);
    notify(error.message || "操作失败", "error");
  }
}

el.refreshProfiles.addEventListener("click", () =>
  run(async () => {
    await refreshProfiles();
    notify("场景列表已刷新");
  }),
);
el.refreshComponents.addEventListener("click", () =>
  run(async () => {
    await refreshComponents();
    notify("提示词结构已刷新");
  }),
);
el.newProfile.addEventListener("click", () => {
  fillProfile(null);
  notify("已进入新建配置");
});
el.providerKey.addEventListener("change", applyProviderMode);
for (const input of [el.profileName, el.sceneCode, el.promptText, el.openingMessage]) {
  input.addEventListener("input", renderPromptStructure);
}
el.saveProfile.addEventListener("click", () => run(saveProfile));

run(async () => {
  await refreshProfiles();
  await refreshComponents();
});
