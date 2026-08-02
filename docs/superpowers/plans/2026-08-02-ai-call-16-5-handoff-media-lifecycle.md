# AI Call 16.5 转人工与媒体生命周期实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 在 16.4B PostgreSQL DB-only 外呼闭环上，把转人工认领、媒体 Evidence、Webhook 恢复、挂机和真实 Provider Effect 接入同一个 Owner/Command/Effect 控制面，为单号码 `Web 创建任务 -> SIP 呼叫 Linphone -> AI -> 浏览器坐席接管 -> 挂机清理` 真人验收建立安全门禁。

**架构：** PostgreSQL 继续是唯一事实源。API 只在短事务中写 Handoff、Presence、Command 或 Webhook Inbox；Runtime Owner 以 fencing 和处理 token 执行媒体状态提交及 Provider Effect；Webhook 只追加 Evidence 和命令，不能直接写 `connected`。生产 Provider 适配器复用现有 `LiveKitRoomManager`、`RealtimeCallAgentRunner`、`LiveKitSipClient` 和 `LiveKitEgressManager`，默认仍装配 Stub，只有隔离环境显式开关且 preflight 通过后才能装配真实适配器。

**技术栈：** FastAPI、SQLAlchemy 2 AsyncSession、PostgreSQL 16 `READ COMMITTED`、LiveKit Room/SIP/Egress SDK、pytest/anyio、Provider Stub。

---

## 冻结边界

- 本计划直接实现总设计第 16.5 节，不新增一轮架构设计评审。
- 真实电话前始终执行 `dry-run -> preflight -> guarded real-call`，本计划的自动实施阶段禁止拨号。
- `preview` 仍是生成并播放短音频，不创建 Room、Owner、Handoff 或通话记录。
- `sip_inbound` 不进入本计划；Linphone 在真人验收中是系统外呼的被叫端。
- Redis Streams/Consumer Group 不进入本计划；PostgreSQL 扫描和 `LISTEN/NOTIFY` 保持现状。
- Event/Dialogue/Recording/ASR/Semantic 的全链路租户回填和话后决策属于 16.6；16.5 只保证录音终止动作和终态依赖可恢复。
- 正式环境 `AI_CALL_OWNER_COMMAND_V1_ENTRIES` 必须保持为空；真实验证只用隔离数据库、隔离 Provider namespace 和单个白名单号码。

## 权威合同

- **H5-01 API 权限：** Handoff Trigger 只幂等创建 `requested`；坐席 API 只执行 `requested -> accepted` 和 `available -> claiming`；Runtime 才能提交 `connected/reconnecting/in_call/acw`。
- **H5-02 媒体证据：** join、audio track published、unmuted 只追加 Evidence；`AGENT_MEDIA_READY` 提交前 Runtime 必须再次查询 Provider，并匹配当前 `media_state_version`。
- **H5-03 媒体失效：** leave、track unpublished、muted 递增版本并创建唯一 `AGENT_MEDIA_INVALIDATED`；旧版本和旧 token 影响 0 行。
- **H5-04 取消与终止：** 未接通时 `CANCEL_HANDOFF` 可原子释放 `claiming`；已接通或重连中只能转唯一 `END_CALL`，不得把坐席直接释放为 available。
- **H5-05 Webhook 持久化：** 有效签名事件必须在返回 2xx 前写 Inbox 或 Quarantine；处理提交后崩溃可重领，未知租户或未匹配事件不能写主状态。
- **H5-06 Provider 边界：** API、Webhook、Dispatcher 和 Outbound 不调用 Provider；只有当前 Runtime Owner/cleanup Owner 可执行真实 Effect。
- **H5-07 终态依赖：** `DELETE_ROOM` 必须等待 `HANGUP_SIP`、`DISCONNECT_AGENT_PARTICIPANT` 和已有 `STOP_EGRESS` 全部在创建静默门禁后确认终态。
- **H5-08 默认安全：** 未显式启用隔离真实 Provider 时，Runtime 继续使用确定性 Stub；生产入口集合非空必须启动失败。

## 文件职责

- `docs/livekit-ai-outbound/sql/phase-i3-handoff-media-lifecycle.sql`：只负责 16.5 PostgreSQL migration，不创建物理外键，不使用 JSONB。
- `app/api/v1/ai_call/model.py`：扩展 Handoff/Presence 字段；业务 ID 继续按字符串返回前端。
- `app/services/ai_call/runtime_control/models.py`：定义 Handoff Media Evidence、Webhook Inbox 和 Quarantine 持久模型。
- `app/services/ai_call/runtime_control/handoff_repository.py`：唯一负责 Owner 模式 Handoff/Presence/Command 原子状态变更。
- `app/services/ai_call/runtime_control/webhook_repository.py`：唯一负责 Inbox/Quarantine 接收、短租约认领、Evidence 和命令登记。
- `app/services/ai_call/runtime_control/handoff_handlers.py`：Runtime Owner 的 Handoff 命令执行和 fencing 提交。
- `app/services/ai_call/runtime_control/livekit_provider.py`：真实 Provider Effect 适配器，不持有数据库事务。
- `app/services/ai_call/runtime_control/runtime_service.py`：只做命令类型路由和本地 handle fail-closed，不承载业务 Repository 逻辑。
- `app/api/v1/ai_call/controller.py`：认证、租户来源、幂等键和 owner/legacy 路径分流。
- `app/services/ai_call/runtime_control/lifecycle.py`：显式 Stub/真实 Provider 装配门禁。

### 任务 1：迁移 Handoff 媒体版本、Inbox 与 Quarantine

**文件：**

- 创建：`docs/livekit-ai-outbound/sql/phase-i3-handoff-media-lifecycle.sql`
- 修改：`app/api/v1/ai_call/model.py`
- 修改：`app/services/ai_call/runtime_control/models.py`
- 修改：`tests/test_ai_call_runtime_models.py`
- 修改：`tests/postgres/test_ai_call_runtime_control_postgres.py`

- [ ] **步骤 1：编写失败的模型和 migration 合同测试**

```python
def test_handoff_media_lifecycle_models_match_postgres_contract() -> None:
    assert AiCallHandoffModel.__table__.c.media_state_version.type.python_type is int
    assert AiCallHandoffModel.__table__.c.media_state_version.default.arg == 0
    assert AiCallRuntimeWebhookInboxModel.__table__.c.payload_json.type.python_type is str
    assert "provider_namespace" in AiCallHandoffMediaEvidenceModel.__table__.c
    assert "processing_generation" in AiCallRuntimeWebhookQuarantineModel.__table__.c
```

PostgreSQL 测试必须执行 migration 后查询 `information_schema.columns` 和 `pg_indexes`，验证三张新表、Handoff 字段、唯一索引、认领索引和 `timestamptz`；不得用 `metadata.create_all` 代替 migration 验收。

- [ ] **步骤 2：运行测试确认红灯**

```bash
uv run pytest -q tests/test_ai_call_runtime_models.py -k 'handoff_media_lifecycle'
tools/run_ai_call_runtime_postgres_tests.sh -q tests/postgres/test_ai_call_runtime_control_postgres.py -k 'handoff_media_migration'
```

预期：新模型和 migration 尚不存在而失败。

- [ ] **步骤 3：实现最小 Schema**

在 `ai_call_handoff` 增加：

```text
agent_participant_identity varchar(255) null
agent_participant_sid varchar(255) null
agent_audio_track_sid varchar(255) null
media_state varchar(32) not null default 'not_ready'
media_state_version bigint not null default 0
media_invalidated_at timestamptz null
last_media_evidence_at timestamptz null
```

在 `ai_call_handoff_agent` 支持 `offline/available/claiming/in_call/acw`，不再由 Owner 模式写旧 `online/busy`。创建：

```text
ai_call_handoff_media_evidence
ai_call_runtime_webhook_inbox
ai_call_runtime_webhook_quarantine
```

所有 JSON 载荷使用 `text`，所有表显式 `tenant_id`，不创建物理外键。Inbox 唯一键为 `(provider_namespace, provider_event_id)`；Evidence 唯一键为 `(tenant_id, handoff_id, provider_namespace, provider_event_id, evidence_type)`；Quarantine 对 `inbox_id` 唯一并具有独立 generation/token/lease。

- [ ] **步骤 4：运行测试、lint 和 diff 检查**

```bash
uv run pytest -q tests/test_ai_call_runtime_models.py -k 'handoff_media_lifecycle'
tools/run_ai_call_runtime_postgres_tests.sh -q tests/postgres/test_ai_call_runtime_control_postgres.py -k 'handoff_media_migration'
uv run ruff check app/api/v1/ai_call/model.py app/services/ai_call/runtime_control/models.py tests/test_ai_call_runtime_models.py tests/postgres/test_ai_call_runtime_control_postgres.py
git diff --check
```

- [ ] **步骤 5：提交**

```bash
git add docs/livekit-ai-outbound/sql/phase-i3-handoff-media-lifecycle.sql app/api/v1/ai_call/model.py app/services/ai_call/runtime_control/models.py tests/test_ai_call_runtime_models.py tests/postgres/test_ai_call_runtime_control_postgres.py
git commit -m "feat(ai-call): 建立转人工媒体生命周期账本"
```

### 任务 2：原子化坐席认领、取消和命令登记

**文件：**

- 创建：`app/services/ai_call/runtime_control/handoff_repository.py`
- 创建：`tests/test_ai_call_runtime_handoff_repository.py`
- 修改：`app/api/v1/ai_call/controller.py`
- 修改：`app/api/v1/ai_call/schema.py`
- 修改：`tests/test_ai_call_phase_b1_records.py`
- 修改：`tests/postgres/test_ai_call_runtime_control_postgres.py`

- [ ] **步骤 1：编写失败测试**

覆盖：

```python
async def test_accept_locks_record_handoff_presence_then_appends_command(): ...
async def test_two_agents_competing_for_one_handoff_have_one_winner(): ...
async def test_one_agent_cannot_claim_two_handoffs(): ...
async def test_cancel_before_connected_releases_claiming_atomically(): ...
async def test_cancel_after_connected_creates_the_unique_end_call(): ...
async def test_cross_tenant_handoff_action_changes_zero_rows(): ...
```

Owner 模式 API 必须从 `AuthSchema` 读取 tenant 和坐席身份，要求 `Idempotency-Key`；请求受理返回 `202`、`handoffId`、`commandId`、`commandStatus=PENDING`，不得声称已经接通。

- [ ] **步骤 2：确认红灯**

```bash
uv run pytest -q tests/test_ai_call_runtime_handoff_repository.py tests/test_ai_call_phase_b1_records.py -k 'owner_handoff or claiming'
```

- [ ] **步骤 3：实现 Repository 和 API 分流**

事务锁序固定为：

```text
Record -> Handoff -> Presence -> Command
```

`accept` 只允许 `requested -> accepted` 与 `available -> claiming`，同事务登记 `HANDOFF_ACCEPTED`。`cancel` 只允许未接通的 `requested/accepted` 进入 `canceled`；`connected/reconnecting` 调用 `request_end`。`legacy_local` 继续走现有 `AiCallHandoffService`，不得改变旧路径。

- [ ] **步骤 4：运行单元和 PostgreSQL 竞争测试**

```bash
uv run pytest -q tests/test_ai_call_runtime_handoff_repository.py tests/test_ai_call_phase_b1_records.py -k 'owner_handoff or claiming'
tools/run_ai_call_runtime_postgres_tests.sh -q tests/postgres/test_ai_call_runtime_control_postgres.py -k 'handoff and (claim or cancel or tenant)'
uv run ruff check app/services/ai_call/runtime_control/handoff_repository.py app/api/v1/ai_call/controller.py app/api/v1/ai_call/schema.py tests/test_ai_call_runtime_handoff_repository.py
git diff --check
```

- [ ] **步骤 5：提交**

```bash
git add app/services/ai_call/runtime_control/handoff_repository.py app/api/v1/ai_call/controller.py app/api/v1/ai_call/schema.py tests/test_ai_call_runtime_handoff_repository.py tests/test_ai_call_phase_b1_records.py tests/postgres/test_ai_call_runtime_control_postgres.py
git commit -m "feat(ai-call): 原子登记转人工坐席命令"
```

### 任务 3：持久化 Webhook Inbox、媒体 Evidence 与失效版本

**文件：**

- 创建：`app/services/ai_call/runtime_control/webhook_repository.py`
- 创建：`app/services/ai_call/runtime_control/webhook_service.py`
- 创建：`tests/test_ai_call_runtime_webhook_repository.py`
- 修改：`app/api/v1/ai_call/controller.py`
- 修改：`app/plugin/init_app.py`
- 修改：`tests/test_ai_call_phase_e_sip.py`
- 修改：`tests/test_ai_call_process_roles.py`

- [ ] **步骤 1：编写失败测试**

覆盖：

```python
async def test_authenticated_webhook_commits_inbox_before_returning_success(): ...
async def test_duplicate_provider_event_reuses_one_inbox_row(): ...
async def test_unmatched_valid_event_is_quarantined_without_state_write(): ...
async def test_join_publish_unmuted_create_one_ready_command_per_version(): ...
async def test_leave_unpublish_muted_increment_version_and_invalidate(): ...
async def test_expired_inbox_and_quarantine_leases_are_taken_over(): ...
async def test_old_processing_token_cannot_write_evidence_or_command(): ...
```

- [ ] **步骤 2：确认红灯**

```bash
uv run pytest -q tests/test_ai_call_runtime_webhook_repository.py tests/test_ai_call_phase_e_sip.py -k 'runtime_webhook or media_evidence'
```

- [ ] **步骤 3：实现接收和处理**

签名仍由 Controller 使用 LiveKit verifier 校验。有效事件先通过 `receive()` 写 Inbox；能够按全局唯一 `room_name` 关联 Owner 模式 Record 时写 tenant/call，不能关联时同事务写 Quarantine。Worker 使用数据库时间、`SKIP LOCKED`、generation/token/lease 认领。

Inbox Worker 只能：

```text
追加 Handoff Media Evidence
递增 media_state_version
登记 AGENT_MEDIA_READY / AGENT_MEDIA_INVALIDATED
为 SIP participant_left 登记 END_CALL Evidence
```

它不能直接写 `connected/reconnecting/in_call/acw`，也不能调用 Provider。`jobs` 角色运行 Inbox/Quarantine Worker；纯 `api` 只接收并提交事件。

- [ ] **步骤 4：验证和提交**

```bash
uv run pytest -q tests/test_ai_call_runtime_webhook_repository.py tests/test_ai_call_phase_e_sip.py tests/test_ai_call_process_roles.py -k 'runtime_webhook or media_evidence or jobs'
uv run ruff check app/services/ai_call/runtime_control/webhook_repository.py app/services/ai_call/runtime_control/webhook_service.py app/api/v1/ai_call/controller.py app/plugin/init_app.py tests/test_ai_call_runtime_webhook_repository.py
git diff --check
git add app/services/ai_call/runtime_control/webhook_repository.py app/services/ai_call/runtime_control/webhook_service.py app/api/v1/ai_call/controller.py app/plugin/init_app.py tests/test_ai_call_runtime_webhook_repository.py tests/test_ai_call_phase_e_sip.py tests/test_ai_call_process_roles.py
git commit -m "feat(ai-call): 持久恢复转人工媒体事件"
```

### 任务 4：Runtime Owner 提交 connected、reconnecting 与终态

**文件：**

- 创建：`app/services/ai_call/runtime_control/handoff_handlers.py`
- 创建：`tests/test_ai_call_runtime_handoff_handlers.py`
- 修改：`app/services/ai_call/runtime_control/runtime_service.py`
- 修改：`tests/test_ai_call_runtime_lifecycle.py`
- 修改：`tests/postgres/test_ai_call_runtime_control_postgres.py`

- [ ] **步骤 1：编写失败测试**

覆盖：

```python
async def test_media_ready_requires_current_owner_command_and_provider_query(): ...
async def test_media_ready_version_race_changes_zero_rows(): ...
async def test_media_invalidated_moves_connected_to_reconnecting_once(): ...
async def test_rejoin_with_higher_version_returns_to_connected(): ...
async def test_owner_loss_during_query_cannot_submit_connected(): ...
async def test_end_preempts_handoff_and_preserves_terminal_barrier(): ...
```

- [ ] **步骤 2：确认红灯**

```bash
uv run pytest -q tests/test_ai_call_runtime_handoff_handlers.py tests/test_ai_call_runtime_lifecycle.py -k 'handoff or media_ready or media_invalidated'
```

- [ ] **步骤 3：实现 Handler 和 Runtime 路由**

`HANDOFF_ACCEPTED`、`AGENT_MEDIA_READY`、`AGENT_MEDIA_INVALIDATED` 和 `CANCEL_HANDOFF` 分别进入专用 Handler。Provider 查询不得在数据库锁事务内执行；查询前读取当前版本，查询后按以下 CAS 提交：

```text
Record Owner + fencing + 未过期租约
Command processing owner + fencing + token + 未过期租约
Handoff tenant/call/status + media_state_version
Presence active_handoff_id + console_session_id
```

首次 ready 或重连 ready 才能提交 `connected + in_call`。失效提交 `reconnecting`，不释放坐席。任何非 END 命令在终态屏障后置为 `SUPERSEDED`，并只产生一个 `runtime_recovery END_CALL`。

- [ ] **步骤 4：验证和提交**

```bash
uv run pytest -q tests/test_ai_call_runtime_handoff_handlers.py tests/test_ai_call_runtime_lifecycle.py
tools/run_ai_call_runtime_postgres_tests.sh -q tests/postgres/test_ai_call_runtime_control_postgres.py -k 'handoff and (media or owner or end)'
uv run ruff check app/services/ai_call/runtime_control/handoff_handlers.py app/services/ai_call/runtime_control/runtime_service.py tests/test_ai_call_runtime_handoff_handlers.py
git diff --check
git add app/services/ai_call/runtime_control/handoff_handlers.py app/services/ai_call/runtime_control/runtime_service.py tests/test_ai_call_runtime_handoff_handlers.py tests/test_ai_call_runtime_lifecycle.py tests/postgres/test_ai_call_runtime_control_postgres.py
git commit -m "feat(ai-call): 由 Runtime 收口转人工媒体状态"
```

### 任务 5：装配真实 LiveKit/SIP/Egress Provider，但默认关闭

**文件：**

- 创建：`app/services/ai_call/runtime_control/livekit_provider.py`
- 创建：`tests/test_ai_call_runtime_livekit_provider.py`
- 修改：`app/services/ai_call/livekit_room.py`
- 修改：`app/services/ai_call/runtime_control/lifecycle.py`
- 修改：`app/config/setting.py`
- 修改：`tests/test_ai_call_process_roles.py`

- [ ] **步骤 1：编写 Fake 驱动的失败测试**

参数化验证：

```text
CREATE_ROOM -> create_room + room_exists
ATTACH_AGENT_PARTICIPANT -> 本地 handle start + Provider presence query
CREATE_SIP_PARTICIPANT -> LiveKitSipClient.create_participant
HANGUP_SIP -> remove_participant + absence query
DISCONNECT_AGENT_PARTICIPANT -> remove_participant + absence query
STOP_EGRESS -> stop_egress + terminal status query
DELETE_ROOM -> delete_room + room_exists == false
```

超时、仅受理、结果未知和 404 必须映射到现有 `ProviderObservationKind`，不得把调用已发出但结果未知映射为 `APPLIED`。

- [ ] **步骤 2：确认红灯**

```bash
uv run pytest -q tests/test_ai_call_runtime_livekit_provider.py tests/test_ai_call_process_roles.py -k 'livekit_provider or real_provider_gate'
```

- [ ] **步骤 3：实现适配器和装配门禁**

新增配置：

```text
AI_CALL_RUNTIME_PROVIDER_MODE=stub|livekit（默认 stub）
AI_CALL_RUNTIME_REAL_PROVIDER_ALLOWED=false（默认 false）
```

只有同时满足以下条件才能构建真实适配器：数据库为 PostgreSQL、非正式环境隔离标记开启、real-provider allowed 为 true、入口集合非空且号码仍通过现有白名单门禁。正式环境 16.5 期间入口集合非空或 Provider mode=livekit 必须启动失败。

- [ ] **步骤 4：验证和提交**

```bash
uv run pytest -q tests/test_ai_call_runtime_livekit_provider.py tests/test_ai_call_process_roles.py tests/test_ai_call_runtime_stub_handlers.py
uv run ruff check app/services/ai_call/runtime_control/livekit_provider.py app/services/ai_call/livekit_room.py app/services/ai_call/runtime_control/lifecycle.py app/config/setting.py tests/test_ai_call_runtime_livekit_provider.py
git diff --check
git add app/services/ai_call/runtime_control/livekit_provider.py app/services/ai_call/livekit_room.py app/services/ai_call/runtime_control/lifecycle.py app/config/setting.py tests/test_ai_call_runtime_livekit_provider.py tests/test_ai_call_process_roles.py
git commit -m "feat(ai-call): 门禁装配真实通话 Provider"
```

### 任务 6：双实例闭环、无拨号 preflight 与真人验收检查点

**文件：**

- 修改：`tests/postgres/test_ai_call_runtime_control_postgres.py`
- 修改：`tests/test_ai_call_process_roles.py`
- 创建：`docs/livekit-ai-outbound/phases/phase-i3-handoff-media-preflight.md`

- [ ] **步骤 1：增加 PostgreSQL 故障矩阵**

至少覆盖：

```text
两个坐席竞争一个 Handoff
两个 Inbox Worker 竞争一个 Provider event
Webhook PROCESSING 后崩溃和租约接管
ready 查询与 invalidated 乱序
END 与 ready/invalidated 同时提交
Owner 失联、cleanup 接管和旧 fencing/token 迟到提交
终止 Effect 依赖图、迟到创建和 DELETE_ROOM 门禁
最终 Worker/Reservation/Effect/Handoff/Presence 计数一致
```

- [ ] **步骤 2：运行完整自动验证**

```bash
uv run pytest -q --disable-warnings \
  tests/test_ai_call_runtime_models.py \
  tests/test_ai_call_runtime_handoff_repository.py \
  tests/test_ai_call_runtime_webhook_repository.py \
  tests/test_ai_call_runtime_handoff_handlers.py \
  tests/test_ai_call_runtime_livekit_provider.py \
  tests/test_ai_call_runtime_owner_repository.py \
  tests/test_ai_call_runtime_stub_handlers.py \
  tests/test_ai_call_process_roles.py \
  tests/test_ai_call_runtime_lifecycle.py \
  tests/test_ai_call_outbound_owner_runtime.py
tools/run_ai_call_runtime_postgres_tests.sh -q tests/postgres/test_ai_call_runtime_control_postgres.py
uv run ruff check .
codegraph sync
codegraph status
git diff --check
git diff --cached --check
```

- [ ] **步骤 3：执行只读、无拨号 preflight**

记录：

```text
cwd、branch、HEAD、dirty state
各 listener PID 的 cwd
PostgreSQL datasource/schema/transaction isolation
LiveKit、SIP、Egress、FreeSWITCH health
Linphone 注册与 Reachable
浏览器坐席登录、available 心跳和授权场景
单个白名单号码、线路快照和 max concurrency
正式入口集合为空、隔离入口集合只包含本次测试入口
```

preflight 只允许健康查询和配置校验，不创建 Room、Participant、Task 或 Attempt，不拨号，不重启服务。

- [ ] **步骤 4：提交自动化和 preflight 文档**

```bash
git add tests/postgres/test_ai_call_runtime_control_postgres.py tests/test_ai_call_process_roles.py docs/livekit-ai-outbound/phases/phase-i3-handoff-media-preflight.md
git commit -m "test(ai-call): 验证转人工生命周期恢复闭环"
```

- [ ] **步骤 5：停止并等待真实拨号确认**

只有自动化全部通过、preflight 没有红项、用户准备好 Linphone 和浏览器坐席后，才请求一次明确确认。确认后只创建一个单号码任务，依次验证：响铃、接听、AI 对话、客户说“转人工”、坐席 claim、15 秒内双向媒体、客户挂机、坐席挂机、录音终态和无 Provider 残留。

## 完成门禁

16.5 只有同时满足以下条件才允许进入真人验收：

1. Handoff、Presence、Command 和 Evidence 的权限矩阵由 Repository 强制执行；
2. Webhook 提交后崩溃可恢复，未匹配事件不丢失且不污染主状态；
3. `connected` 只由 Runtime Owner 在 Provider 重查后提交；
4. 媒体失效版本单调，旧版本、旧 token、旧 fencing 写入影响 0 行；
5. END 抢占后普通命令不能重开通话；
6. 真实 Provider 适配器默认关闭，Stub 回归和 legacy 路径不变；
7. 双实例 PostgreSQL 故障矩阵、单元测试、ruff、CodeGraph 和 diff 全部通过；
8. 无拨号 preflight 全绿；
9. 用户再次明确确认单个白名单号码拨打。
