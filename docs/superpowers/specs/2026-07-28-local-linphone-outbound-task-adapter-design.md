# 通用外呼任务本机 Linphone 测试适配器设计

## 1. 背景与目标

通用外呼任务已经具备任务、对象、拨打尝试、重试、暂停、继续、停止和取消能力，但当前任务执行器只接入 `MockOutboundDialer`，不会发起真实 SIP 呼叫。

项目现有的单次 SIP 通话链路已经可以复用：

```text
AiCallService
-> AiCallOrchestrator
-> LiveKit Server
-> LiveKit SIP
-> FreeSWITCH
-> Linphone 分机 1000
```

本阶段使用本机 Linphone 代替真实电话线路，验证以下闭环：

1. 单号码任务人工启动；
2. Linphone 真实响铃、接听和 AI 通话；
3. 整通电话结束后再固化任务结果；
4. 客户提出转人工后进入现有坐席待接池；
5. 浏览器坐席接单、与 Linphone 双向通话并结束；
6. Attempt、任务对象、通话记录和 Handoff 可以用同一个 `callId` 关联。

## 2. 当前现场基线

设计确认时的本地现场状态如下，实施和验收前仍需重新检查：

- 19011 LiveKit Server、LiveKit SIP、Egress、Redis 容器运行中；
- `sip_realtime_freeswitch` 运行且健康；
- Linphone 用户 `1000@192.168.0.111` 已注册；
- FreeSWITCH 显示 Linphone `Ping-Status: Reachable`；
- 测试号码 `19900001001` 由 FreeSWITCH 路由到注册分机 `1000`；
- 19011 API 健康；
- 当前任务执行器开关为关闭状态；
- 本地通用外呼任务和坐席使用默认租户 `000000`。

当前 19011 进程没有加载 `ENVIRONMENT=dev`，所以进入真人验收前必须按本地 SIP 基线重新启动并确认 `AI_CALL_SIP_OUTBOUND_ENABLED=true`。健康检查成功不代表 SIP 配置已加载。

## 3. 范围

### 3.1 本阶段包含

- 单号码、单对象任务的本地 Linphone 测试拨打；
- 号码白名单；
- 人工触发，不启用普通任务自动扫描；
- 真实 SIP 通话记录；
- AI 完整通话；
- AI 转人工、坐席接听和人工结束；
- 通话终态驱动 Attempt 和任务完成；
- 服务重启后的进行中通话对账；
- 任务详情中的测试入口、状态展示和主动结束当前通话。

### 3.2 本阶段不包含

- 批量名单真实自动拨打；
- 真实手机号和运营商 SIP 线路；
- 多并发拨号；
- 线路路由、CPS、并发配额和供应商切换；
- 正式多租户 SIP/Handoff 改造；
- 新建一套转人工状态机；
- 数据看板和质检中心。

## 4. 核心原则

1. 默认关闭：不配置本地测试开关时，不能发起 Linphone 测试。
2. 双重校验：前端控制按钮展示，后端独立执行完整安全校验。
3. 单通限制：同一时间只允许一通 Linphone 测试。
4. 稳定关联：任务执行器预先生成的 `callId` 同时用于 Attempt 和真实通话记录。
5. 最终一致：Linphone 接听不代表任务完成，必须等待通话记录终态。
6. 复用现有能力：SIP、AI Runner、录音、转人工和坐席工作台不重复实现。
7. 状态正交：拨打结果与转人工结果分开记录。

## 5. 配置

新增本地测试配置，默认值均为安全状态：

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED` | `false` | 是否开放本地 Linphone 任务测试 |
| `AI_CALL_OUTBOUND_LINPHONE_ALLOWED_CALLEE` | `19900001001` | 唯一允许的本地测试号码 |
| `AI_CALL_OUTBOUND_LINPHONE_POLL_SECONDS` | `1` | 通话终态轮询间隔 |
| `AI_CALL_OUTBOUND_LINPHONE_RECOVERY_GRACE_SECONDS` | `30` | Room 消失后的恢复宽限时间 |

`AI_CALL_OUTBOUND_EXECUTOR_ENABLED` 继续保持 `false`。本地 Linphone 测试由专用人工命令触发，不启动普通任务轮询。

## 6. 领域边界与关联

### 6.1 职责

`OutboundTaskExecutor` 负责：

- 任务、对象和 Attempt 状态；
- 重试规则；
- 任务计数；
- 生成稳定 `callId`；
- 根据 `DialResult` 完成 Attempt。

`LinphoneTestDialer` 负责：

- 校验本地号码；
- 使用稳定 `callId` 调用内部 `AiCallService.create_sip_session()`；
- 等待真实通话记录终态；
- 把真实记录映射为 `DialResult`；
- 不直接修改任务和对象状态。

`AiCallService` 继续负责：

- 创建 `sip_outbound` 通话记录；
- LiveKit Room、AI Runner 和 SIP Participant；
- 录音；
- 客户挂断和主动结束；
- 转人工请求、AI 挂起和坐席接入。

### 6.2 `callId` 契约

现有 `AiCallService.create_sip_session()` 增加仅供内部调用的可选参数：

- `call_id`：由任务执行器预生成；
- `business_type="outbound_task"`；
- `business_id=taskId`。

浏览器单次测试不传 `call_id`，保持原行为。

任务 Attempt 与真实 `AiCallRecord` 必须使用同一个 `callId`。真实模式不创建 `outbound_mock` 记录。

### 6.3 Attempt 补充字段

为保证命令幂等、重启恢复和 Mock/真实模式隔离，Attempt 增加以下可空字段：

| 字段 | 含义 |
| --- | --- |
| `dialer_type` | `mock` 或 `linphone_test` |
| `test_scenario` | `ai_only` 或 `handoff` |
| `command_idempotency_key` | 人工启动命令幂等键 |
| `active_slot` | 活动 Linphone 测试槽位，活动时固定为 `linphone_test` |

约束：

- `(tenant_id, command_idempotency_key)` 唯一；
- `(tenant_id, active_slot)` 唯一；
- Attempt 进入终态时清空 `active_slot`；
- 多条终态 Attempt 的 `active_slot` 均为 `NULL`，不互相冲突。

`active_slot` 的数据库唯一约束是 V1 单通限制的最终门禁，不能只依赖前端禁用按钮或进程内锁。

## 7. 人工启动 API

所有接口继续使用项目统一响应结构。

### 7.1 查询测试资格

```http
GET /ai-call/outbound-tasks/{taskId}/test-capability
```

响应字段：

| 字段 | 含义 |
| --- | --- |
| `enabled` | 本地测试能力是否开启 |
| `eligible` | 当前任务是否允许测试 |
| `reasons` | 不可测试原因列表 |
| `availableAgentCount` | 可用于转人工验收的在线坐席数 |
| `activeCallId` | 当前任务进行中的真实 `callId` |
| `canEndActiveCall` | 是否允许主动结束当前通话 |

### 7.2 启动测试

```http
POST /ai-call/outbound-tasks/{taskId}/test-run
Idempotency-Key: <客户端生成>
```

请求：

```json
{
  "scenario": "ai_only"
}
```

`scenario` 可取：

- `ai_only`：AI 完整通话；
- `handoff`：AI 转人工验收。

异步响应只表示命令已受理，不表示通话已完成。前端必须轮询测试状态。

后端受理条件：

1. 本地测试开关开启；
2. 租户为本地默认租户 `000000`；
3. 任务状态为 `SCHEDULED`；
4. 任务为单号码模式；
5. 任务恰好只有一个对象；
6. 号码严格等于配置的允许号码；
7. 当前没有其他 Linphone 测试通话；
8. SIP 预检通过；
9. `scenario=handoff` 时至少有一个可用测试坐席；
10. 同一个幂等键不能重复创建 Attempt。

### 7.3 查询测试状态

```http
GET /ai-call/outbound-tasks/{taskId}/test-status
```

响应字段：

| 字段 | 含义 |
| --- | --- |
| `taskId` | 任务 ID，字符串 |
| `targetId` | 对象 ID，字符串 |
| `attemptId` | Attempt ID，字符串 |
| `callId` | 真实通话 ID |
| `targetStatus` | 对象状态 |
| `attemptStatus` | Attempt 状态 |
| `callStatus` | 通话记录状态 |
| `handoffStatus` | 当前或最近一次 Handoff 状态 |
| `phase` | 页面派生阶段 |
| `endReason` | 通话结束原因 |
| `errorMessage` | 失败原因 |
| `canEndActiveCall` | 是否允许结束当前通话 |

`phase` 只用于页面展示，不落成新的任务状态：

- `dialing`
- `ai_call`
- `waiting_handoff`
- `human_call`
- `completed`
- `failed`

### 7.4 结束当前通话

```http
POST /ai-call/outbound-tasks/{taskId}/active-call/end
Idempotency-Key: <客户端生成>
```

该命令根据任务的活动 Attempt 找到 `callId`，复用现有会话结束能力。命令只结束当前通话，不改变“停止任务”的既有语义。

## 8. 状态流转

### 8.1 正常 AI 通话

```text
Task: SCHEDULED -> RUNNING -> COMPLETED
Target: PENDING -> DIALING -> IN_CALL -> COMPLETED
Attempt: DIALING -> IN_CALL -> COMPLETED
Call: created/preparing/ready -> connected/... -> completed
```

### 8.2 AI 转人工

```text
Target/Attempt: IN_CALL
Handoff: requested -> accepted -> connected -> completed
Call: 保持非终态
坐席结束 -> Call completed -> Attempt completed -> Task completed
```

`waiting_handoff` 和 `human_call` 由 Handoff 状态派生，不写入 Task 或 Target。

### 8.3 未接听或失败

```text
Task: SCHEDULED -> RUNNING -> COMPLETED
Target: PENDING -> DIALING -> RETRY_WAIT/COMPLETED
Attempt: DIALING -> FAILED
```

是否进入 `RETRY_WAIT` 继续使用任务配置快照中的重试次数、间隔和可重试结果。

## 9. 结果映射

拨打结果只描述电话线路是否接通：

| 条件 | `callResult` |
| --- | --- |
| `answered_at` 已存在 | `connected` |
| SIP 状态为 busy 或对应忙线原因 | `busy` |
| 响铃超时、用户不可达、连接超时 | `no_answer` |
| 预检、Room、SIP Participant、运行时或恢复失败 | `call_failed` |

已接听后发生模型异常、客户挂断、坐席结束或转人工失败，拨打结果仍为 `connected`；具体业务结束原因保存在 `AiCallRecord.end_reason`、失败字段和 Handoff 中。

## 10. 转人工

转人工完全复用现有链路：

```text
客户明确提出转人工
-> request_handoff / 语义触发
-> Handoff requested
-> 坐席工作台待接池
-> 坐席 claim
-> 浏览器加入 LiveKit Room
-> Handoff connected
-> AI 保持挂起
-> 坐席与 Linphone 双向通话
-> 坐席完成
-> 结束真实会话
```

本地测试任务、Linphone 通话和测试坐席必须都位于默认租户 `000000`。当前通话记录没有完整租户传播契约，所以本阶段不能据此宣称正式多租户 SIP 转人工已经完成。

## 11. 异常恢复

启动命令异步执行。进程重启后，对 `DIALING` 和 `IN_CALL` Attempt 按以下顺序恢复：

1. 找到同 `callId` 的终态通话记录：立即映射结果并完成 Attempt；
2. 通话记录非终态且 LiveKit Room 存在：保持进行中，恢复终态轮询；
3. 通话记录非终态、Room 不存在但仍在宽限期：等待下一轮；
4. Room 不存在且超过宽限期：`call_failed`；
5. Attempt 已创建但没有真实通话记录，且超过启动宽限期：`call_failed`。

真实 Linphone 模式不再使用“DIALING 超过固定时间直接失败”的 Mock 恢复规则。

## 12. 前端交互

任务详情页右上角增加“测试拨打”。

显示条件以 `test-capability` 为准，前端不自行复制全部业务规则。

确认弹窗展示：

- 客户名称；
- 脱敏号码；
- 提示词名称；
- 音色；
- 呼叫规则；
- 验收场景。

选择“AI 转人工通话”时展示操作提示：

1. 保持坐席工作台在线；
2. 接听 Linphone；
3. 向 AI 明确表达“转人工”；
4. 在坐席工作台接单；
5. 完成人工通话并点击结束。

启动后按钮进入 loading，并轮询 `test-status`。页面离开或隐藏时遵循现有可见性轮询策略。

进行中显示：

- 当前阶段；
- `callId`；
- 已通话时长；
- Handoff 状态；
- “查看通话记录”；
- “结束当前通话”。

失败时直接展示后端 `errorMessage`，不只显示“测试失败”。

## 13. 停止与结束语义

- “停止任务”：不再发起新的呼叫，不强制中断当前通话；
- “结束当前通话”：主动结束当前 LiveKit/SIP 会话；
- 当前通话结束后，处于 `STOPPING` 的任务转为 `STOPPED`；
- 两个动作使用独立确认文案，不能合并为一个模糊的“停止”按钮。

## 14. 测试

### 14.1 自动测试

- 本地测试功能默认关闭；
- 非允许号码被拒绝，且不会调用 SIP；
- 批量任务、多对象任务和非待执行任务被拒绝；
- 幂等键重复提交只生成一次 Attempt；
- 全局单通限制；
- 稳定 `callId` 同时关联 Attempt 和真实通话记录；
- Linphone 接听后进入 `IN_CALL`，任务不提前完成；
- 正常挂断后映射 `connected`；
- busy、no answer 和 call failed 映射；
- 重试规则复用；
- Handoff requested/connected/completed 的页面阶段派生；
- 主动结束当前通话；
- 服务重启后的五类恢复分支；
- 普通 Mock 执行器行为不回退；
- 前端按钮条件、确认弹窗、轮询和失败文案。

### 14.2 真人验收

真人验收前必须重新检查：

1. 19011 API 运行实例加载 `ENVIRONMENT=dev`；
2. `AI_CALL_SIP_OUTBOUND_ENABLED=true`；
3. Linphone 用户 1000 注册且 Reachable；
4. LiveKit、LiveKit SIP、Redis、Egress 和 FreeSWITCH 均健康；
5. 测试任务只有 `19900001001`；
6. 普通后台任务执行器仍关闭；
7. 转人工场景下测试坐席处于在线可接状态。

验收用例：

- AI 完整通话并由 Linphone 挂断；
- Linphone 不接听；
- 客户要求转人工、坐席接听并结束；
- 通话中主动点击“结束当前通话”；
- 通话进行中重启 19011，验证恢复对账。

真实双向音频、AI 听感和人工接管后的双向通话需要用户配合接听和说话，自动测试不能替代。

## 15. 发布与回滚

- 代码上线后本地测试配置仍默认关闭；
- 不执行数据库破坏性迁移；
- 关闭 `AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED` 后，页面入口和人工启动 API 均不可用；
- 普通任务执行器仍可保持 Mock/关闭状态；
- 回滚不删除已有 Attempt、通话记录或 Handoff。

## 16. 后续演进

本地闭环通过后，再单独设计：

1. 正式多租户通话记录和 Handoff 租户传播；
2. 事件驱动 Attempt 完成，替代单通轮询；
3. 批量任务并发、CPS 和线路配额；
4. 正式 SIP Trunk 和真实号码；
5. 线路级失败码标准化；
6. 运营监控和数据看板。
