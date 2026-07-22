# 通用浏览器坐席中心后端实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 在现有 FastAPI AI Call 模块中实现可租户隔离的坐席档案、场景授权、公共待接池、原子认领、媒体确认、快速话后处理、跟进任务、人工回拨和管理查询闭环。

**架构：** 复用 `ai_call_handoff`、`ai_call_handoff_agent` 和现有 LiveKit/录音/语义分析链路；新增独立的 agent-console 路由、服务与跟进表。数据库是并发归属权威，LiveKit Participant 是 `connected` 的媒体事实权威，前端只消费状态，不持有抢单真相。

**技术栈：** Python 3.13、FastAPI、SQLAlchemy 2、PostgreSQL/SQLite 测试、LiveKit、pytest、ruff。

**权威规格：** `docs/superpowers/specs/2026-07-17-commercial-agent-console-design.md`（以实施时最新提交为准）。

---

## 文件结构

- 修改 `app/config/setting.py`：三个全局时限配置。
- 修改 `app/api/v1/ai_call/model.py`、`schema.py`、`crud.py`：表、DTO 和条件更新。
- 创建 `app/api/v1/ai_call/agent_console_schema.py`：坐席工作台与管理端专用 DTO。
- 创建 `app/api/v1/ai_call/agent_console_controller.py`：`/agent-console` 与 `/admin` 路由。
- 修改 `app/api/v1/ai_call/__init__.py`：挂载新路由。
- 创建 `app/services/ai_call/agent_console_service.py`：在线状态、权限、原子认领、媒体确认。
- 创建 `app/services/ai_call/follow_up_service.py`：话后处理、任务、尝试和人工回拨。
- 创建 `app/services/ai_call/agent_console_reconciler.py`：超时与异常占用补偿。
- 修改 `app/services/ai_call/handoff_service.py`：把旧 B3 状态映射到 V1 状态机并复用既有能力。
- 创建 `docs/livekit-ai-outbound/sql/phase-g-agent-console-migration.sql`：可重复执行的迁移 SQL。
- 创建 `tests/test_ai_call_agent_console_models.py`、`tests/test_ai_call_agent_console_claim.py`、`tests/test_ai_call_agent_console_api.py`、`tests/test_ai_call_follow_up.py`、`tests/test_ai_call_agent_console_reconcile.py`。

### 任务 1：冻结配置和数据库契约

**文件：**
- 修改：`app/config/setting.py:202-220`
- 修改：`app/api/v1/ai_call/model.py:636-770`
- 创建：`docs/livekit-ai-outbound/sql/phase-g-agent-console-migration.sql`
- 测试：`tests/test_ai_call_agent_console_models.py`

- [ ] 编写失败测试，断言默认配置为首次接入 15 秒、重连 15 秒、总等待 60 秒，并断言新增表及唯一约束存在。
- [ ] 运行 `uv run pytest tests/test_ai_call_agent_console_models.py -q`，预期因字段和表尚不存在而 FAIL。
- [ ] 在 `Settings` 增加：

```python
AI_CALL_AGENT_CLAIM_CONNECT_TIMEOUT_SECONDS: int = 15
AI_CALL_AGENT_RECONNECT_GRACE_SECONDS: int = 15
AI_CALL_HANDOFF_TOTAL_WAIT_SECONDS: int = 60
```

- [ ] 扩展 `AiCallHandoffModel`、`AiCallHandoffAgentModel`，新增 `AiCallAgentProfileModel`、`AiCallAgentSceneScopeModel`、`AiCallAfterCallWorkModel`、`AiCallFollowUpTaskModel`、`AiCallFollowUpAttemptModel`；字段和索引逐项匹配权威规格第 11 章，不使用物理外键和 JSONB。
- [ ] 编写幂等迁移 SQL：先建新表，再 `alter table ... add column if not exists` 扩展旧表；把旧 `AI_CALL_HANDOFF_TIMEOUT_SECONDS` 的运行读取迁移到新总等待配置，保留旧变量仅一个发布周期并记录弃用日志。
- [ ] 运行 `uv run pytest tests/test_ai_call_agent_console_models.py -q` 和 `uv run ruff check app/api/v1/ai_call/model.py app/config/setting.py`，预期 PASS。
- [ ] 提交：`git commit -m "feat(ai-call): add agent console data contract"`。

### 任务 2：接通真实登录身份与坐席场景授权

**文件：**
- 创建：`app/api/v1/ai_call/agent_console_schema.py`
- 创建：`app/services/ai_call/agent_console_service.py`
- 修改：`app/core/dependencies.py:205-230`
- 测试：`tests/test_ai_call_agent_console_api.py`

- [ ] 编写失败测试：未登录返回 401；已登录且档案启用的用户可进入工作台；档案停用或场景不匹配不能查看、认领；V1 管理接口只要求登录。
- [ ] 运行 `uv run pytest tests/test_ai_call_agent_console_api.py -q`，预期登录身份或坐席档案断言 FAIL。
- [ ] 让 agent-console 路由显式依赖 `get_current_user`，从登录用户映射 `user_id -> agent_profile -> agent_identity`，禁止请求体传任意坐席身份。
- [ ] V1 按 `LingChenAdmin` 现有方式仅校验登录，不消费宿主 permission code；`ai_call:agent:console` 和 `ai_call:agent:manage` 只用于前端菜单展示。管理接口同样显式依赖 `get_current_user`。坐席档案、场景范围、状态和负责人仍由业务服务端校验。
- [ ] 实现坐席档案 CRUD、场景范围整组替换和启停规则；启用时校验至少一个 `scene_code`，通话中停用只禁止新认领并在结束后转 `offline`。
- [ ] 运行定向测试和 `uv run ruff check app/core/dependencies.py app/services/ai_call/agent_console_service.py`，预期 PASS。
- [ ] 提交：`git commit -m "feat(ai-call): enforce agent identity and scene scope"`。

### 任务 3：实现在线状态、公共待接池和事务式原子认领

**文件：**
- 修改：`app/api/v1/ai_call/crud.py`
- 修改：`app/api/v1/ai_call/agent_console_controller.py`（任务 2 已创建并注册路由）
- 修改：`app/api/v1/ai_call/agent_console_schema.py`
- 修改：`app/services/ai_call/agent_console_service.py`
- 修改：`app/services/ai_call/handoff_service.py`（创建 handoff 时冻结来源通话 `scene_code`）
- 测试：`tests/test_ai_call_agent_console_claim.py`

- [ ] 编写失败测试覆盖：上线预检标记、心跳、暂停/下线、来源通话 `scene_code` 冻结、按 `scene_code` 过滤、两坐席抢同一 handoff 仅一个成功、同一坐席抢两单仅一个成功、同一 handoff + 登录坐席 + `console_session_id` 的安全重试返回同一结果。
- [ ] 运行 `uv run pytest tests/test_ai_call_agent_console_claim.py -q`，预期 FAIL。
- [ ] 在 repository 增加带条件的原子更新；PostgreSQL 使用 `SELECT ... FOR UPDATE`，SQLite 测试用条件 `UPDATE ... WHERE status='requested'` 验证影响行数。事务同时更新 handoff 与 agent。
- [ ] 认领成功写入 `accepted_at`、`claim_expires_at=min(now+15s, expires_at)`、`human_agent_identity`、`accepted_console_session_id`，坐席进入 `claiming`；事务提交后才签发 Token。
- [ ] 实现 bootstrap、presence、pending、claim 接口；冲突映射为规格中的稳定错误码，不返回 SQL 异常。
- [ ] 运行定向测试；增加 `pytest -n` 并发不可用时使用 `asyncio.gather` + 两个独立 session 验证竞争。
- [ ] 当前 `ai_call_record` 不保存 `tenant_id`，handoff 继续沿用既有 `000000` 单租户边界；不得跨租户兜底查询。若接入非 `000000` 宿主租户，必须先补充上游权威租户来源并统一迁移通话与 handoff 链路。
- [ ] 提交：`git commit -m "feat(ai-call): add atomic handoff claim pool"`。

### 任务 4：实现媒体确认、重连和 15/60 秒状态收敛

**文件：**
- 修改：`app/services/ai_call/handoff_service.py`
- 修改：`app/services/ai_call/agent_console_service.py`
- 修改：`app/api/v1/ai_call/agent_console_controller.py`
- 测试：`tests/test_ai_call_agent_console_claim.py`

- [ ] 编写失败测试：`accepted` 不等于 `connected`；Participant 和麦克风发布确认后才 connected；15 秒首次接入超时可重新入池；60 秒总等待超时创建唯一未接回访；已 connected 后重连独立计算 15 秒且失败不重新入池。
- [ ] 运行定向测试确认 FAIL。
- [ ] 实现 `media-ready` 的 LiveKit Participant 核验接口和 `reconnect-token`；进入重连写 `reconnect_expires_at`，恢复后清空；超时写 `failed` 并进入 `wrap_up_quick`。
- [ ] 总等待超时事务内以 `(tenant_id, source_handoff_id)` 唯一约束创建 `handoff_unanswered` 跟进任务，语义分析不得作为创建条件。
- [ ] 运行 `uv run pytest tests/test_ai_call_agent_console_claim.py -q`，预期 PASS。
- [ ] 提交：`git commit -m "feat(ai-call): close agent media timeout lifecycle"`。

### 任务 5：实现快速话后确认和跟进任务

**文件：**
- 创建：`app/services/ai_call/follow_up_service.py`
- 修改：`app/api/v1/ai_call/agent_console_schema.py`
- 修改：`app/api/v1/ai_call/agent_console_controller.py`
- 测试：`tests/test_ai_call_follow_up.py`

- [ ] 编写失败测试：3～5 秒流程只强制 disposition 与 needs_follow_up；摘要可空；需要跟进时与 ACW 同事务建任务；未接回访原子认领；负责人固定；关闭原因必填规则；重复提交不重复建任务。
- [ ] 运行 `uv run pytest tests/test_ai_call_follow_up.py -q`，预期 FAIL。
- [ ] 实现 ACW upsert、任务列表/详情/claim/complete/close 和 append-only attempt；`customer_callback_at` 仅接受客户明确预约，不实现自动重试策略。
- [ ] AI 摘要只在人工字段为空时写草稿；人工确认后设置保护条件，异步结果不得覆盖。
- [ ] 运行定向测试和 ruff，预期 PASS。
- [ ] 提交：`git commit -m "feat(ai-call): add quick wrap up and follow ups"`。

### 任务 6：实现浏览器人工回拨和异步结果查询

**文件：**
- 修改：`app/services/ai_call/follow_up_service.py`
- 修改：`app/services/ai_call/livekit_sip.py`
- 修改：`app/api/v1/ai_call/agent_console_controller.py`
- 测试：`tests/test_ai_call_follow_up.py`

- [ ] 编写失败测试：只允许负责人回拨；服务端由 `business_type + business_id + contact_ref` 解析号码；创建新 `call_id`；不启动 AI Runner；无人接听自动写 attempt 并回 pending；技术失败不算有效客户联系。
- [ ] 运行定向测试确认 FAIL。
- [ ] 实现 human-only SIP/LiveKit 会话工厂；接口响应只返回“已受理 + call_id”，最终 connected/no_answer/busy/rejected/invalid/technical_failure 由查询或推送更新。
- [ ] 对人工回拨增加坐席 availability 条件，避免 Twilio 文档所示的并发回拨与在途通话重叠问题。
- [ ] 运行定向测试，预期 PASS。
- [ ] 提交：`git commit -m "feat(ai-call): add human only callback flow"`。

### 任务 7：管理查询、补偿、推送和全量验证

**文件：**
- 创建：`app/services/ai_call/agent_console_reconciler.py`
- 修改：`app/api/v1/ai_call/agent_console_controller.py`
- 修改：`app/api/v1/ai_call/crud.py`
- 测试：`tests/test_ai_call_agent_console_reconcile.py`
- 测试：`tests/test_ai_call_agent_console_api.py`

- [ ] 编写失败测试覆盖三个管理列表指标、详情、异常释放限制、幂等 reconcile、SSE 事件序号与断线后 bootstrap 恢复。
- [ ] 运行两个测试文件确认 FAIL。
- [ ] 实现管理查询、`release-stale`、`reconcile` 和结构化审计；活动 Room 存在时返回 `STALE_RELEASE_NOT_ALLOWED`。
- [ ] 推送 handoff/task/presence 状态变化；推送失败不回滚数据库事实，客户端以 bootstrap/轮询补偿。
- [ ] 运行：

```bash
uv run pytest tests/test_ai_call_agent_console_models.py tests/test_ai_call_agent_console_claim.py tests/test_ai_call_agent_console_api.py tests/test_ai_call_follow_up.py tests/test_ai_call_agent_console_reconcile.py -q
uv run pytest tests/test_ai_call_phase_b1_records.py -q
uv run ruff check app/api/v1/ai_call app/services/ai_call
```

预期全部 PASS、ruff 0 error。

- [ ] 提交：`git commit -m "feat(ai-call): complete agent console backend closure"`。
