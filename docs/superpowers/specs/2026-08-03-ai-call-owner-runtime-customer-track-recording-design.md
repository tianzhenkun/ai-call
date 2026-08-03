# Owner Runtime 客户分轨录音设计

## 1. 状态与目标

- 日期：2026-08-03
- 状态：已批准，待实施
- 上位合同：`2026-07-31-ai-call-single-owner-runtime-command-design.md`
- 前置设计：`2026-08-03-ai-call-owner-runtime-main-recording-design.md`
- 前置实现：Owner Command、Effect/Recovery、真实 LiveKit Runtime Provider、混合主录音

本切片只把客户 Participant 的单独音频接入 `owner_command_v1`。它与现有混合主录音
共同受一个总录音开关控制，用于离线 ASR、语义分析和客户声音质检；不增加 AI
Participant 分轨。PostgreSQL 继续是唯一事实源，浏览器事件、SIP 状态、通知、Provider
返回值和进程内状态均不能授予执行权。

成功标准：

1. 总录音开启且使用真实 LiveKit Provider 时，`START_CALL` 预登记唯一、可恢复的客户
   `START_TRACK_EGRESS`；Stub 不登记、不伪造分轨；
2. 客户分轨在客户媒体就绪前只落库、不调用 LiveKit；Web 的 `browser_ready` 和 SIP
   connected 只写数据库就绪事实；
3. 只有当前 Owner、未过期租约、fencing 和 Effect processing token 全部匹配，才允许
   调用 Participant Egress 或提交结果；
4. Provider 启动成功、调用超时、提交前崩溃和 Owner 接管均不会产生第二份逻辑客户分轨；
5. `END_CALL` 为每条已登记客户分轨生成独立 `STOP_TRACK_EGRESS`，该 Stop 是
   `DELETE_ROOM` 的完整 prerequisite；
6. 分轨失败受控 fail-open，不阻塞通话 ready、不覆盖混合主录音投影；
7. Egress 停止确认与 OSS 文件验证解耦，Runtime cleanup 不等待长时间对象可见性；
8. 单元和隔离 PostgreSQL 测试不连接真实 LiveKit、SIP、OSS、Provider 或电话。

## 2. 业务边界

### 2.1 本切片包含

- 一份客户 Participant audio 录音；
- 单一总开关下的“混合主录音 + 客户分轨”登记规则；
- `START_TRACK_EGRESS / STOP_TRACK_EGRESS` 的 claim、Provider 执行、观察和恢复；
- `answered_at` 就绪门禁及 Web/SIP 两类数据库写入口；
- Owner/fencing/Effect token 保护下的 `ai_call_recording_track` 业务投影；
- 客户分轨停止后的独立 OSS 验证接续；
- Owner 模式与 legacy 直接录音调用的角色隔离。

### 2.2 本切片不包含

- AI Participant 独立分轨；AI 文本、实际播放事实和混合主录音继续提供审计证据；
- 人工坐席 Participant 分轨；其生命周期与动态 Handoff 独立设计；
- 新的离线 ASR、语义分析或跟进任务实现；
- Redis Streams、前端重做或新的录音策略页面；
- 真实 LiveKit、SIP、OSS、Linphone 或电话验收。

本切片不删除已有 legacy 分轨数据，也不改变历史录音文件。旧
`AI_CALL_PARTICIPANT_RECORDING_ENABLED` 仅可暂留在 legacy 接线中兼容现有入口；
`owner_command_v1` 不读取它，产品有效开关只有 `AI_CALL_RECORDING_ENABLED`。旧开关的
最终删除属于后续 legacy 收口，不阻塞本切片。

## 3. 合同

| ID | 合同 |
| --- | --- |
| `CTR-01` | `AI_CALL_RECORDING_ENABLED=false` 时不登记混合主录音或客户分轨；为 true 且 Runtime Provider 明确具备真实录音能力时同时登记两者。Stub 的能力声明固定为 false。 |
| `CTR-02` | 当前切片只新增客户分轨；不得登记 AI 或人工坐席分轨，不得因“以后可能需要”增加动态角色抽象。 |
| `CTR-03` | 客户 `START_TRACK_EGRESS` 在 `START_CALL` Effect 登记事务中预登记，但 `answered_at is null` 时不可 claim，且不得产生外部 I/O。 |
| `CTR-04` | Web `browser_ready` 与 SIP connected 只写数据库就绪事实；通知只用于唤醒。外部执行权仍来自当前 Owner、数据库租约、fencing 和 Effect CAS。 |
| `CTR-05` | 同一租户、call、角色和 Participant identity 只允许一个逻辑客户分轨及一个稳定创建 Effect；Owner 接管不得按新 fencing 再登记一份。 |
| `CTR-06` | `START_TRACK_EGRESS` 结果不确定时进入 reconcile-only，通过持久化 Egress 引用或稳定资源键查询；不得盲目再次调用 Start。 |
| `CTR-07` | Effect submit 与 Track 投影共用一个事务；旧 Owner、过期 fencing、失效租约或错误 processing token 的提交必须整体影响 0 行。 |
| `CTR-08` | 每个已登记客户 Start 必须生成一个 Stop；从未领取的 `PENDING` Start 在 END 图登记事务中先收口为 `FAILED(no_resource)`，创建不确定和正常录音仍由 Stop 查询收口，三种情况都不能从 END 图中省略。 |
| `CTR-09` | `DELETE_ROOM` 只有在客户 `STOP_TRACK_EGRESS` 与其他全部非 Room 销毁 Effect 均 `APPLIED` 后才能执行。缺失 prerequisite 必须 fail closed。 |
| `CTR-10` | 客户分轨是辅助能力，其启动失败不得把已满足 Room、Agent 和必要 SIP 的 `START_CALL` 判失败。 |
| `CTR-11` | `STOP_TRACK_EGRESS APPLIED` 只证明 Provider 资源终态或不存在；Track `completed` 仍必须由独立 OSS 对象验证证明。 |
| `CTR-12` | 所有 Track 登记、投影、查询、唯一约束和对账 claim 均包含 `tenant_id`；禁止用全局 call_id 推断租户。 |
| `CTR-13` | Owner 模式禁止 API、lifecycle 或 legacy `RecordingService` 直接调用客户分轨 start/stop；不得形成 Effect 与 legacy 双执行。 |
| `CTR-14` | 错误字段只保存稳定错误码和安全摘要，不保存 OSS 密钥、Provider Token、请求体、音频内容或堆栈。 |

## 4. 方案选择

### 4.1 采用：预登记 Effect，数据库就绪后由 Owner 领取

```text
START_CALL
  -> 预登记 START_EGRESS(main)
  -> 预登记 START_TRACK_EGRESS(customer, stable identity)
  -> Room / Agent / 必要 SIP ready
  -> START_CALL 可成功；客户分轨仍可 PENDING

Web browser_ready 或 fenced SIP connected
  -> 只写 answered_at
  -> PostgreSQL 唤醒或周期扫描
  -> 当前 Owner claim START_TRACK_EGRESS
  -> Provider Start/Query
  -> 同一事务：提交 Effect + 投影 Track

END_CALL
  -> STOP_EGRESS(main)
  -> STOP_TRACK_EGRESS(customer)
  -> 全部销毁 prerequisite APPLIED
  -> DELETE_ROOM
```

这种方案让“什么时候具备录音条件”和“谁有权执行外部动作”保持分离。就绪写入口不需要
持有本地 Runtime handle，也不能直接调用 LiveKit；Runtime 崩溃后，另一进程可完全从
PostgreSQL 恢复。

### 4.2 不采用：在 `browser_ready` 或 SIP callback 中直接启动分轨

该方案把外部副作用放在 API/callback 进程，绕过 Owner/fencing。进程在 Provider 成功后
崩溃时没有统一恢复事实，也会与 Runtime Recovery 双执行。

### 4.3 不采用：客户和 AI 共用一个分轨 Effect

两个 Participant 的就绪时间、Egress 引用、失败和停止状态不同，合并 Effect 无法独立
恢复。产品当前也不需要 AI-only 音频；保留混合主录音和客户分轨即可满足完整回听与干净
客户音频分析，避免第三份录音的成本和复杂度。

## 5. 数据合同

### 5.1 `ai_call_recording_track`

在现有表上增加：

| 字段 | 类型 | 规则 |
| --- | --- | --- |
| `tenant_id` | `varchar(20)` | 非空；migration 由同 call 的 Record 回填，缺失或不唯一时 fail closed |
| `egress_generation` | `bigint` | nullable；Owner 客户分轨固定为稳定 generation `1`，legacy 历史行可为空 |

唯一约束调整为：

```text
(tenant_id, call_id, track_role, participant_identity)
```

索引至少覆盖：

- `(tenant_id, call_id, track_role)`；
- `(tenant_id, egress_id)`；
- `(tenant_id, oss_id)`；
- `(tenant_id, status, next_verify_at)`。

migration 顺序固定为：新增 nullable 字段、按 Record 回填、验证无空值和歧义、设置
`NOT NULL`、替换唯一约束和索引。不得静默写默认租户，不创建物理外键，不引入 JSONB。

已有 AI、human_agent legacy 行只做租户回填和约束迁移，不删除、不重新启动，也不纳入本次
Owner Effect 投影。

### 5.2 稳定资源标识

客户 Participant identity 在 Record 创建时已确定，跨 Owner 保持稳定。为满足 Runtime
Effect 的 160 字符键长度约束，稳定键使用完整 identity SHA-256 hex digest，Track 业务行
仍保存完整 identity：

```text
identity_digest          = sha256(utf8(participant_identity)).hexdigest()
effect_type              = START_TRACK_EGRESS
track_role               = customer
participant_identity     = record.participant_identity
idempotency_key          = start:{call_id}:ctr:{identity_digest}
provider_idempotency_key = egress:ctr:{call_id}:{identity_digest}
resource_key             = egress:track:{call_id}:customer:{identity_digest}
resource_generation      = 1
```

digest 按 identity 原始 UTF-8 字节计算，不做大小写折叠或 Unicode 归一化。Provider resolver
从 Record 读取完整 identity，重新计算 digest 并与 Effect 稳定键比较；不允许从 digest 反解
identity。以最大 64 字符 call ID 计算，上述两个 160 字符列均不会溢出。重复登记必须逐项
比较 Effect type、Provider namespace、稳定键、generation 和执行阶段；任一不一致均 fail
closed。

### 5.3 Track 业务投影

`START_TRACK_EGRESS` 只投影 `track_role=customer` 的 Track 行：

| Provider observation | Effect | Track |
| --- | --- | --- |
| 已确认存在且有 `egress_id` | `APPLIED` | `recording` |
| 状态不可证明或调用超时 | `RECONCILE_REQUIRED` | `starting`，安全错误摘要 |
| 保护期后确认不存在或永久前置失败 | `FAILED(no_resource)` | `failed` |

`STOP_TRACK_EGRESS` 只更新其 `source_create_effect_id` 对应的同租户客户 Track：

| Provider observation | Effect | Track |
| --- | --- | --- |
| 已终态或稳定查询确认不存在 | `APPLIED` | 有对象事实则 `verifying`；从未创建则保持 `failed` |
| 仅受理、仍活动或结果不确定 | `RECONCILE_REQUIRED` | `stopping` |

主录音 projector 只接受 `START_EGRESS / STOP_EGRESS`；客户分轨 projector 只接受
`START_TRACK_EGRESS / STOP_TRACK_EGRESS`。两者不得依据模糊资源键猜测投影目标。

## 6. 就绪门禁与启动流程

### 6.1 Effect 预登记

真实 LiveKit Provider 在 `AI_CALL_RECORDING_ENABLED=true` 时使用现有单一 recording
capability；Runtime 据此同时登记主录音和一条客户分轨 Start，不增加第二个分轨开关或可
独立漂移的 capability。二者都是 auxiliary spec，不进入 Room、Agent 和必要 SIP 的强制
readiness 计数。

Provider Stub 的单一录音 capability 固定为 false，因此 DB-only
Dispatcher/Runtime/Recovery 测试不会登记录音 Effect、创建 Track，或初始化 LiveKit/OSS
客户端。

### 6.2 数据库 claim gate

客户 Start 除通用 Owner/fencing/租约/终态条件外，还必须在同一原子 claim 语句中验证：

- Record `answered_at is not null`；
- Record `participant_identity` 与 Effect 稳定资源 identity 完全一致；
- Track role 为 `customer`，generation 为 `1`；
- `terminal_requested_at is null`；
- Effect 仍处于允许 claim/reconcile 的状态。

普通 Effect 不受该门禁影响。门禁必须位于数据库 claim CAS 内，禁止先查询
`answered_at`、再无条件更新 Effect 的 check-then-act 实现。

### 6.3 Web 就绪写入

`browser_ready` 先完成现有会话、租户和调用方权限校验，再以 Record 行锁串行化写入：

- 只允许 `owner_command_v1` 且非终态 Record；
- `answered_at` 仅在为空时写数据库时间；重复事件幂等；
- 可发 PostgreSQL notify 缩短等待，但 notify 不携带执行授权；
- Owner 模式不得调用 legacy `_start_browser_ready_recording_tracks()`。

若 `END_CALL` 先取得 Record 锁并写 `terminal_requested_at`，迟到的 `browser_ready` 影响
0 行；若 ready 先提交，随后是否能启动仍由 Start Effect 的原子 claim 与终态条件决定。

### 6.4 SIP 就绪写入

direct/outbound 继续复用当前 `record_sip_connected()`：只有匹配当前 Owner、fencing、未过期
Record/Worker 租约、active 容量和非终态状态时，才幂等写 `answered_at` 与 media connected
事件。它不直接调用 Egress。旧 Owner 的迟到 SIP 状态不得解锁客户分轨。

### 6.5 Provider 执行与恢复

首次 Start 使用 Participant Egress，并采用稳定对象名：

```text
build_participant_object_name(call_id, "customer", participant_identity)
```

执行后必须查询 Egress 事实；只有确认资源存在且取得 `egress_id` 才提交 `APPLIED`。
Provider 超时、引用缺失或查询不可证明时提交 `RECONCILE_REQUIRED`。后续恢复只能按已持久化
引用，或按 Room、Participant identity、稳定对象名查询，不得再次调用 Start。

## 7. END、恢复与终态屏障

1. `START_TRACK_EGRESS` 加入创建 Effect 类型，规范映射到
   `STOP_TRACK_EGRESS`；Stop 使用 execution phase 10。
2. `register_end_graph()` 必须从全部已登记客户 Start 生成 Stop，无论 Start 当前是
   `PENDING`、`APPLYING`、`RECONCILE_REQUIRED`、`APPLIED` 或 `FAILED`。若 Start 仍为
   从未领取、无 processing token 的 `PENDING`，END 图登记事务先将其写为
   `FAILED(no_resource)`；这是“从未发生外部调用”的数据库证明，使 create quiet gate 可以
   收口。已进入 `APPLYING` 或 reconcile 的 Start 禁止走该捷径。
3. `DELETE_ROOM` 继续使用 execution phase 20，并以所有非 Room destroy Effect 为完整
   prerequisite；客户 Stop 缺失、未终态或依赖行缺失时均不可 claim。
4. END 到达后：
   - 尚未 claim 的客户 Start 被终态屏障禁止创建；
   - 已进入 `APPLYING` 的 Start 可按现有保护窗口完成 observation 提交；
   - Provider 成功但提交前崩溃的 Start 由 Recovery 查询接管；
   - 对应 Stop 最终查询并停止资源，或证明稳定资源不存在。
5. Stop 只有在 Egress 已终态或稳定查询确认不存在时才 `APPLIED`。仅收到 Provider accepted
   不满足 Room 删除门禁。
6. 新 Owner 接管过期 `APPLYING` 或 `RECONCILE_REQUIRED` Effect 时，沿用原
   `source_create_effect_id`、稳定资源键和 protection deadline；不得新建第二条 Start。
7. `mark_cleanup_clean` 等待客户 Stop 与其他销毁 Effect 静默，但不等待 Track OSS 验证；
   cleanup clean 后释放 Owner、容量并停止本地 handle。
8. 独立录音 Reconciler 按租户 claim `verifying` Track：对象可读则登记/复用 `sys_oss` 并写
   `completed`；截止前不可见则退避；截止后仍不可证明则写 `failed`。它不占用 Runtime
   Owner、不修改 Effect，也不重新启动 Egress。

## 8. 并发、权限与锁序

- Web ready、SIP connected、Effect claim/submit 和 END 均先以 Record 作为串行化根；Track
  投影放在 Effect 之后，禁止从 Track 反向锁 Record、Worker、Command 或 Effect。
- Effect submit 的顺序保持：Record/Worker 授权校验、Command/Effect claim 校验、Effect
  状态转换、Track 投影、一次事务提交。具体实现必须遵循上位全局锁序，不新增反向边。
- Track Reconciler 只按租户锁 Track，并使用 `FOR UPDATE SKIP LOCKED` 或等价 claim；它不得
  在持有 Track 锁时再锁 Record 或 Effect。
- 两个 Runtime 同时看见 `answered_at` 时，只有一个能通过 Effect claim CAS；Provider
  外部 Start 调用最多由该 claim 持有者发起一次。
- Owner A 失权、Owner B 接管后，A 的晚 Start/Stop submit 因 Record fencing、租约或 Effect
  processing token 不匹配整体回滚，Track 影响 0 行。
- API 从认证上下文和 Record 获取租户，普通请求不得传入或覆盖 `tenant_id`；跨租户 Track
  查询返回空或拒绝。
- 错误日志可以包含 call_id、Effect id、角色和安全错误码，不得记录完整号码、凭证、原始
  Provider payload 或音频内容。

## 9. API 与前端

本切片不新增前端页面。现有录音详情可以在后续话后闭环中展示客户分轨，但接口合同必须
遵循：

- `starting / recording / stopping / verifying / completed / failed` 与主录音状态语义一致；
- 只有 `completed` 且已有受权限保护的 `playUrl` 时允许播放；
- 分轨失败不得把通话记录显示为失败，也不得覆盖主录音错误；
- AI 分轨不存在是产品设计，不显示“AI 录音缺失”告警；
- 所有 bigint ID 返回前端时按字符串处理。

异步 Start/Stop、Provider accepted 和 Egress terminal 均不等于录音文件已可播放。

## 10. 测试与验收

### 10.1 Migration 与 repository

1. PostgreSQL migration 能回填 legacy Track 租户，并在缺失/歧义 Record 时 fail closed；
2. 新唯一约束允许不同租户相同业务键，拒绝同租户重复客户 Track；
3. Track CRUD、查询、投影和 Reconciler claim 均显式租户隔离；
4. Start/Stop observation 幂等投影同一行，状态不倒退，主录音行不被修改；
5. 旧 fencing、过期租约、错误 processing token 和跨租户 submit 均影响 0 行。

### 10.2 Handler 与生命周期

1. 总开关关闭时不登记任何录音 Effect；开启时登记主录音与客户分轨，不登记 AI 分轨；
2. Stub 不登记录音 Effect、不创建 Track、不调用 Provider；
3. `answered_at is null` 时客户 Start 不可 claim，其他启动 Effect 正常推进；
4. Web ready 只幂等写就绪事实，Owner 模式不走 legacy 分轨调用；
5. fenced SIP connected 解锁客户 Start，旧 Owner SIP 状态不能解锁；
6. 客户分轨失败不阻塞 START readiness；
7. END graph 包含客户 Stop，且 DELETE_ROOM 在 Stop 终态前执行次数为 0。

### 10.3 隔离 PostgreSQL 并发与恢复

1. 两个 Runtime 竞争同一客户 Start，只有一个取得 claim 和提交权；
2. Provider 成功后、Effect submit 前崩溃，新 Owner 按稳定键查询并恢复，不重复 Start；
3. Start 超时后迟到创建可被 Recovery 发现并停止；
4. `browser_ready` 与 `END_CALL` 竞争只允许“先 claim 后必 Stop”或“终态禁止 Start”两种
   结果，不允许 Room 删除后晚创建；
5. 客户从未接通，Stop 查询无资源后安全 `APPLIED`，cleanup 可 clean；
6. Stop 仅受理、Stop 后状态迟到和 Provider 引用缺失等序列保持可恢复；
7. 旧 Owner 晚提交影响 0 行，新 Owner 的 Track、容量和 cleanup 状态不被覆盖；
8. Stop `APPLIED` 后 Owner 可释放，Track 仍能独立从 `verifying` 收口。

### 10.4 回归与外部边界

- 重跑主录音、Runtime lifecycle、Owner/fencing、Effect、Recovery、END cleanup、双
  Dispatcher/Runtime、Web ready、SIP connected 和 legacy Recording 受影响回归；
- 运行隔离 PostgreSQL 16、Ruff、CodeGraph sync/status 和 `git diff --check`；
- Provider、Egress 和 OSS 统一使用 Fake/Stub 验证超时、迟到和恢复；
- 不启动业务服务，不连接真实 LiveKit、SIP、OSS 或电话；真实录音验收必须在实现全绿后
  另行明确确认。

## 11. 实施切片与提交边界

后续实现计划应拆成以下独立提交：

1. Track 租户化 migration 与 repository；
2. 客户 Track Effect 类型、稳定规格和原子 claim gate；
3. Owner-aware Track projector；
4. LiveKit Participant Egress Start/Stop/observe/reconcile；
5. Web/SIP 就绪接线、END 图和 Recovery 闭环；
6. Track OSS 对账租户化与完整回归。

不得暂存或提交 `.playwright-cli/`、环境备份、密钥、本地数据库、音频文件或任何与本切片
无关的既有脏改动。不得 push、merge 或创建 PR，除非用户另行明确授权。
