# AI Call Direct SIP DB-only 明文号码设计补充

## 1. 状态与适用范围

- 日期：2026-08-01
- 状态：产品决策已确认，待设计复核后冻结
- 实施切片：16.3 Direct SIP DB-only Stub
- 上位设计：`2026-07-31-ai-call-single-owner-runtime-command-design.md`
- 范围修正文档：`2026-08-01-ai-call-16-2-scope-simplification-design.md`

本文是 16.3 Direct SIP 的产品决策补充。它不修改已经冻结的上位设计文件及其
SHA-256，但在本切片内明确覆盖上位设计第 8 节和第 16.3 节中“原始号码必须经
KMS/密钥服务加密后落库”的要求。

覆盖后的决定是：Direct SIP 原始号码允许以明文形式保存在 PostgreSQL 的
`ai_call_record` 专用列中，并与脱敏号码同时永久保留。当前切片不实现 KMS、应用层
号码加密、密钥版本或终态自动清除。

该决定只改变号码的数据库存储方式，不放宽租户隔离、认证、幂等、Owner、fencing、
终态屏障、Effect 状态机或进程角色隔离。

## 2. 目标与非目标

### 2.1 目标

1. 将现有 Direct SIP API 接入 PostgreSQL `START_CALL` 控制面。
2. 在 Record 中保存规范化明文号码、脱敏号码和现有判重 hash。
3. 保证完整号码不扩散到 Command/Effect JSON、日志、错误、接口响应或唤醒通知。
4. 让两个独立 Dispatcher/Runtime 仍以数据库 Owner、租约和 fencing 决定唯一执行权。
5. 以确定性 Provider Stub 验证 `CREATE_SIP_PARTICIPANT` 和 `HANGUP_SIP` 闭环。

### 2.2 非目标

- 不连接真实 LiveKit、SIP、Egress、Linphone 或 Provider。
- 不拨打真实电话，不启动或重启业务服务。
- 不实现 KMS、Vault、AES、信封加密或密钥轮换。
- 不引入 Redis Streams；PostgreSQL 仍是唯一事实源。
- 不迁移 Outbound、SIP inbound、Handoff、录音、ASR 或前端实时媒体。
- 不新增号码删除、归档或保留期任务；明文随 Record 生命周期永久保留。

## 3. 决策与合同

### DSIP-P01：明文唯一落点

新增 `ai_call_record.callee_phone_number varchar(32) nullable`。只有 `direct_sip` Record
允许写入规范化完整号码。完整号码不得写入：

- `ai_call_runtime_command.payload_json`；
- `ai_call_runtime_effect.payload_json`；
- `result_json`、`error_message` 或 Provider evidence；
- PostgreSQL `NOTIFY` payload；
- 应用日志、审计日志或 HTTP 响应。

Runtime 在持有当前 Record Owner、有效租约和 fencing 的前提下，按 `call_id` 从 Record
读取号码，并只在执行 `CREATE_SIP_PARTICIPANT` 的 Provider 调用边界使用。

### DSIP-P02：脱敏展示

`ai_call_record.callee_phone_number_masked` 保存脱敏号码。中国大陆 11 位手机号按
`前三位 + **** + 后四位` 展示，例如 `13812345678 -> 138****5678`。其他合法号码沿用
统一号码脱敏函数，不允许 API 或页面自行拼接不同规则。

所有面向普通业务查询、bootstrap、command query 和管理端的输出只允许返回脱敏号码。

### DSIP-P03：判重 hash

保留 `ai_call_record.callee_phone_number_hash`，用于现有活动 SIP 通话判重和索引查询。
该 hash 不是保密边界，不代替明文字段，也不能输出到普通前端接口。

### DSIP-P04：永久保留

通话成功、失败、`END_CALL` 完成和 cleanup clean 均不得自动清空
`callee_phone_number`。该字段随 Record 生命周期保留，只有未来单独批准的数据删除或
归档策略才能修改。当前切片不实现该策略。

### DSIP-P05：不使用 KMS

Direct SIP 创建不再要求 `sensitive_payload_ciphertext` 和 `payload_key_version`。
现有可空列为避免破坏 16.2A 数据库兼容性而保留，但本入口必须保持为空，不得用密文字段
名称保存明文。

### DSIP-P06：认证、租户与幂等

- 租户只能来自认证上下文，不能来自请求 body。
- `Idempotency-Key` 必填。
- `request_fingerprint` 使用租户、`START_CALL`、`direct_sip`、规范化完整号码和其他稳定
  业务参数计算；明文参与内存中的规范化计算，但只持久化摘要。
- 相同租户、相同幂等键和相同业务请求返回原 Record/Command。
- 相同幂等键但号码或其他稳定业务参数不同，返回 `409 IDEMPOTENCY_CONFLICT`。
- Record 与首个 `START_CALL` Command 必须在同一事务提交；任一失败都不得留下孤立
  Record 或 Command。

### DSIP-P07：旧新路径互斥

只有 `AI_CALL_OWNER_COMMAND_V1_ENTRIES` 显式包含 `direct_sip` 时，Direct SIP API 才能
进入新控制面；否则继续调用既有 legacy 路径。一次请求只允许进入一条路径，禁止同时创建
legacy Room/SIP Participant 和新的 `START_CALL`。

### DSIP-P08：Owner、fencing 与号码读取

- Dispatcher 只唤醒，不读取号码，也不授予执行权。
- Runtime 必须先通过数据库 CAS 获得当前 Owner 和 fencing，再读取 Record 明文号码。
- Owner、租约、fencing 或本地执行硬截止任一失效后，Provider 调用不得开始；已经等待的
  Provider await 必须按现有硬截止规则取消，且不得提交 `APPLIED`。
- Recovery 对尚未登记创建 Effect 的 `START_CALL` 可按现有合同重新分配；取得新 fencing
  的 Runtime 再读取同一 Record 号码。
- 已登记创建 Effect 后失去 Owner，继续遵循 `START-04` 终止收敛，不能盲目重放创建。

### DSIP-P09：Direct SIP Effect 图

Direct SIP 的创建图为：

```text
CREATE_ROOM
  -> ATTACH_AGENT_PARTICIPANT
  -> CREATE_SIP_PARTICIPANT
  -> READY
```

只有全部创建 Effect 为 `APPLIED`，Record 才能提交 Stub readiness。Provider Stub 使用稳定
资源键，不连接外部系统，不产生真实 Room 或 SIP Participant。

终止图复用 16.2A 已冻结的依赖合同：

```text
HANGUP_SIP
DISCONNECT_AGENT_PARTICIPANT
  -> DELETE_ROOM
  -> cleanup clean
```

`DELETE_ROOM` 必须等待完整非 Room 销毁图完成；缺失 prerequisite 必须 fail closed。

## 4. 数据库迁移

新增独立 PostgreSQL migration：

```text
ai_call_record.callee_phone_number varchar(32) null
```

约束：

- 不增加物理外键。
- 不修改或复用 `sensitive_payload_ciphertext` 保存明文。
- 不为明文号码建立普通索引；活动通话判重继续使用现有 hash 索引。
- 旧 Record 保持 `NULL`，本切片不做数据回填。
- ORM 模型、repository 创建参数和 PostgreSQL migration 必须同步更新。

## 5. 请求与响应

Direct SIP 创建请求继续接受 `callee_phone_number`。入口先执行统一规范化和校验，再在同一
事务中生成：

- `callee_phone_number`：规范化完整号码；
- `callee_phone_number_masked`：脱敏展示值；
- `callee_phone_number_hash`：现有活动通话判重值；
- `request_fingerprint`：包含规范化业务意图的摘要。

异步受理响应只返回：

- `callId`；
- `commandId`；
- `status`；
- `calleePhoneNumberMasked`（如现有响应需要）。

禁止返回完整号码、号码 hash、Command 敏感字段或 Stub Provider 内部参数。

## 6. 错误与恢复

- 号码格式错误：`422`，不创建 Record/Command。
- 缺少 `Idempotency-Key`：`400`，不创建 Record/Command。
- 幂等冲突：`409 IDEMPOTENCY_CONFLICT`。
- 未授权或租户不匹配：按现有认证合同拒绝，不允许通过 body 覆盖租户。
- Owner 分配超时：沿用 `ALLOCATION_TIMEOUT`，不得触发 Provider Stub。
- Stub 创建失败：按 Effect 状态机记录，不把号码写进错误信息。
- START 结果不确定或 Owner 丢失：沿用 `START-04` 建立终态屏障并执行销毁图。
- cleanup clean 后释放 Owner 和容量，但按照 `DSIP-P04` 保留明文号码。

## 7. 进程角色与权限矩阵

| 角色 | 明文号码权限 | 允许动作 |
| --- | --- | --- |
| API 创建入口 | 写 | 认证、规范化、创建 Record/Command；禁止回显和日志记录 |
| Dispatcher | 无 | 扫描/通知 `START_CALL`，不读取 Record 明文 |
| Runtime Owner | 条件读 | 仅有效 Owner/fencing 执行 Direct SIP 创建 Effect 时读取 |
| Recovery | 默认无 | 只做状态与资源对账；重新分配后由新 Runtime Owner 读取 |
| Provider Stub | 调用参数 | 进程内接收，禁止外发、持久化或日志记录 |
| bootstrap/command query | 无 | 只返回脱敏号码和状态 |
| 普通管理端/前端 | 无 | 只返回脱敏号码 |

数据库使用同一应用角色时，上表是应用层权限边界，不声称已经实现独立 PostgreSQL 列级
权限。数据库账号拆分和列级授权不在本切片范围内。

## 8. 测试与验收

必须覆盖：

1. PostgreSQL migration 可升级，旧 Record 的新列为 `NULL`。
2. Direct SIP 创建原子写入 Record 和 `START_CALL`，Record 同时保存明文、脱敏值和 hash。
3. Command/Effect JSON、结果、错误、HTTP 响应和 `NOTIFY` payload 均不含完整号码。
4. 相同幂等请求返回原 Command；相同键更换号码返回 `409`。
5. 认证租户不可由 body 覆盖，跨租户幂等与 Record 查询互相隔离。
6. 两个 Dispatcher/Runtime 竞争时只产生一套 Effect 和一套 Stub 资源。
7. Direct SIP readiness 必须等待 Room、Agent Participant 和 SIP Participant 全部完成。
8. Owner 过期、晚续租和旧 fencing 均不能开始或提交 Provider Effect。
9. `END_CALL` 先完成 `HANGUP_SIP` 和其他非 Room 销毁，再执行 `DELETE_ROOM`。
10. cleanup clean 后 Owner、容量和本地 handle 被释放，但 Record 明文号码仍然保留。
11. Provider Stub 不连接 LiveKit、SIP、Linphone、Redis 或真实 Provider。
12. 受影响单元测试、隔离 PostgreSQL 测试、双 Dispatcher/Runtime、一致性测试、lint 和
    `git diff --check` 全部通过。

## 9. 已知风险与未来升级

明文永久落库意味着获得数据库内容读取权限、数据库备份或副本访问权的主体可以直接看到
完整号码；脱敏字段不能降低该风险。本文将其记录为经产品确认的明确取舍，不将其描述为
加密、匿名化或业内最佳安全实践。

未来如改为加密存储，必须另立 migration 和兼容计划，至少处理：

- 旧明文批量加密与校验；
- 明文列清除；
- 密钥版本和轮换；
- 新旧 Runtime 混部读写兼容；
- 备份、副本和日志中的历史明文处置。

该未来升级不属于当前 16.3 DB-only Stub 的阻断条件。
