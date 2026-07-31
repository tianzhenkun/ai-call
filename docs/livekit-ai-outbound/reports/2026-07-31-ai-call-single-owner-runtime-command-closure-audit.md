# AI Call Single-Owner Runtime Command V1 闭环审计报告

> 状态：闭环通过，允许进入实现计划编写
> 审计对象：`2026-07-31-ai-call-single-owner-runtime-command-design.md`
> 原始冻结基线 SHA-256：`22ebc7137aac3119061306fcdee8f7de825cab2b580b8ec9796274946b8c45d6`
> 闭环冻结 SHA-256：`c3a4300d3426359ff9cecf3d051be5d700c820571838ecb74e88053d13e3ceb8`
> 闭环冻结规模：1829 行、12854 词、189263 字节
> 审计方式：原始版本先冻结并建立台账；集中修订后，对同一闭环哈希执行两轮只读冷审

## 1. 审计结论

原始冻结版本方向成立，但不能作为实现基线。首轮闭环审计确认：

- 4 个 P0：会直接破坏单 Owner、资源最终清理或恢复容量闭环；
- 10 个 P1：实现者必须自行补规则，多个合法实现会得到不同结果；
- 3 个 P2：不阻塞核心正确性，但应在冻结前统一；
- 6 个 Scope 决策：已经足够明确，应保持不扩张。

随后已按第 7 节执行一次集中合同化修订，并在冷审发现 P0/P1 时清零计数、修订后重新冻结。最终正文哈希 `c3a4300d3426359ff9cecf3d051be5d700c820571838ecb74e88053d13e3ceb8` 连续完成两轮只读冷审，结果均为 P0=0、P1=0；第 8 节退出条件已经满足，允许进入实现计划编写。第 2 至第 9 节保留原始审计证据和当时问题描述，第 10 节记录最终关闭结果，不能把历史“缺口”误读为闭环版本仍未修复。

## 2. 审计边界与证据

### 2.1 本轮审计范围

审查以下跨章节闭环：

1. Owner 分配、续租、失权和 cleanup 接管；
2. Command 生命周期、顺序、终态抢占和恢复；
3. Effect 登记、独立租约、迟到创建和销毁；
4. Runtime、API、Dispatcher、Reconciler、Jobs 的写权限；
5. Worker 容量、SIP Line Reservation 和长期异常；
6. START、Handoff、END 的状态与退出路径；
7. Redis 加速层与 DB-only 正确性；
8. Webhook Inbox、Quarantine 和多实例恢复；
9. PostgreSQL 事务、锁顺序和时间合同；
10. 需求、故障时间线和自动化测试的可追踪性。

### 2.2 现场核对边界

- 审计对象在当前工作树中已有未提交修改；本轮不覆盖这些修改，也不改主文档。
- `git diff --check` 在冻结时通过。
- 当前环境没有 `codegraph` 命令，因此本轮使用定向源码检索和文档逐段交叉核对，不把 CodeGraph 不可用描述为已验证。
- 项目依赖同时包含 PostgreSQL 与 MySQL 驱动，默认配置为 PostgreSQL；当前代码尚未实现本文新增的 Owner、Command、Effect 和启动对账字段。
- 当前实现仍存在 API 惰性创建 Orchestrator、Webhook 使用进程内异步任务、Outbound Executor 同步等待 `dial()` 等现状。它们证明重构目标真实存在，但不构成本设计内部合同正确性的证据。

## 3. V1 规范性合同清单

每条合同只表达一个必须始终成立的规则。后续主文档、表结构、状态矩阵和测试必须引用这些 ID；不能再用不同章节的自然语言隐式覆盖。

### 3.1 不变量

| ID | 规范性合同 |
| --- | --- |
| `INV-01` | PostgreSQL 是 `owner_command_v1` 的唯一业务事实源；Redis、本地内存和 Provider 查询都不能覆盖数据库终态。 |
| `INV-02` | 一个 `call_id` 在任意数据库时刻最多只有一个有效 Runtime Owner；所有 Runtime 状态提交都必须匹配 Owner、fencing 和未过期租约。 |
| `INV-03` | 非 Runtime 角色只能持久化意图、证据、资源预占或确定性投影，不能直接操作 Session 或 Provider。 |
| `INV-04` | `terminal_requested_at` 是吸收性屏障；建立后不得清除，普通命令不得重新打开通话或登记新的非终止创建 Effect。 |
| `INV-05` | 所有可改变 Provider 资源的创建和销毁动作都必须先登记 Effect，再发起外部调用；唯一例外必须被明确列为 fail-closed 紧急媒体动作。 |
| `INV-06` | V1 不恢复旧进程的 AI Session 或对话上下文；Owner 丢失后，新 Owner 只允许安全收尾。 |
| `INV-07` | 所有租户业务记录和命令必须具有明确租户；未解析租户的 Provider 事件只能进入 Provider 级隔离区。 |

### 3.2 Owner 与容量

| ID | 规范性合同 |
| --- | --- |
| `OWN-01` | 首次 Owner 只能由 Dispatcher 在完成 Worker 容量、SIP 线路和 Attempt 原子分配后写入。 |
| `OWN-02` | 过期 Owner 的接管只能由 Recovery Repository 按恢复矩阵和全局锁合同写入。 |
| `OWN-03` | Runtime 只能验证、续租和执行已分配给自己的 Owner；不得自行取得无主或过期 Record。 |
| `OWN-04` | 数据库不可达时，旧 Owner 必须在 monotonic 硬截止前停止本地 AI 媒体，且不得发起新的创建动作。 |
| `OWN-05` | 普通容量、短时 cleanup 执行容量、长期资源隔离占用必须分别计数；任一长期异常不得永久占住有限 cleanup 执行槽。 |

### 3.3 Command

| ID | 规范性合同 |
| --- | --- |
| `CMD-01` | 命令创建以租户、入口、幂等键和请求指纹判定重复；同键同指纹返回原结果，同键异指纹拒绝。 |
| `CMD-02` | 普通命令严格按 `command_seq` 前进；重试和重复投递不得跳号或回退 `last_applied_command_seq`。 |
| `CMD-03` | `END_CALL` 不受普通序号缺口阻塞，可撤销旧 Command token 并抢占；迟到普通命令只能 `SUPERSEDED`。 |
| `CMD-04` | 只有数据库 CAS 能把命令变为可执行的 `PROCESSING`；收到 Redis 消息本身不授予执行权。 |
| `CMD-05` | `DISPATCHING` 的发布权由 `dispatch_token` 隔离，迟到确认不得把数据库状态回退。 |
| `CMD-06` | Command 租约过期后的恢复必须按命令类型、Owner 状态、Effect 事实和终态屏障决议，不能统一改投新 Worker。 |

### 3.4 Effect 与资源收敛

| ID | 规范性合同 |
| --- | --- |
| `EFF-01` | 来源 Command token 只授权首次登记 Effect；Effect 登记后拥有独立生命周期。 |
| `EFF-02` | Effect 认领与完成必须使用 Effect 自身 Owner、fencing、token 和租约，并再次匹配 Record 当前 Owner。 |
| `EFF-03` | 旧 Owner、旧 Command token 或旧 Effect token 的迟到写入必须影响 0 行；外部迟到结果只能通过查询和新 Effect 尝试回填。 |
| `EFF-04` | 每个销毁 Effect 必须与其可能迟到的创建 Effect 或创建保护窗口关联；保护窗口未闭合时，一次“资源不存在”不能成为销毁终态。 |
| `EFF-05` | `resource_cleanup_status=clean` 必须同时证明所有销毁 Effect 已终态、所有创建保护窗口已闭合、Provider 资源不存在且线路已释放。 |

### 3.5 写权限

| ID | 规范性合同 |
| --- | --- |
| `WRITE-01` | API、Webhook、Outbound 和 Jobs 可创建命令、终止 Evidence 或自身 Job 状态，但不能提交 Runtime 结果。 |
| `WRITE-02` | Handoff Trigger 只创建 `requested`；Agent Console Claim 只执行 `requested -> accepted` 和 `available -> claiming` 的原子预占。 |
| `WRITE-03` | 当前 Runtime Owner 是 Record 运行态、Handoff `connected/reconnecting/ended`、坐席 `in_call/ACW` 和实时媒体状态的唯一写入者。 |
| `WRITE-04` | Outbound Attempt Reconciler 使用独立投影租约，单调写 Attempt、Target、Task，不能操作 Provider 或借用 Runtime Owner。 |
| `WRITE-05` | Inbox、ASR、语义分析等 Job Worker 只使用各自处理租约；产生 Runtime 动作时必须创建命令。 |
| `WRITE-06` | SIP Reservation 运行期转换必须同时匹配当前 Owner、Effect token 和 reservation token，且状态只能单调前进。 |

### 3.6 START、END、数据库与路由

| ID | 规范性合同 |
| --- | --- |
| `START-01` | 分配截止只适用于从未取得 Owner、Reservation 或 Effect 的 START；确认无资源后才允许无 END 失败。 |
| `START-02` | START 成功必须由同一组持久化 Room、Participant、Agent 和必要 Egress 事实共同证明，不能由单个返回值推断。 |
| `START-03` | `START_UNCERTAIN` 到聚合截止后必须离开普通 `preparing/RETRY_WAIT`，进入“确认无资源失败”或“建立 END 后清理”。 |
| `START-04` | 原 Owner 在任一创建 Effect 已登记后失联，新 Owner 不得把通话恢复为可服务状态；必须建立终态屏障并执行安全收尾。 |
| `END-01` | 多来源终止只能形成一条 `END_CALL`，每个来源 Evidence 独立去重保存。 |
| `END-02` | END 必须基于全部已登记及可能迟到的创建动作预登记完整销毁图。 |
| `END-03` | 所有 Egress 明确终态或确认不存在前，`DELETE_ROOM` 依赖不得满足。 |
| `END-04` | END 的逻辑完成与 Provider 资源清理完成分离；逻辑终态不能隐藏后续 Effect 对账。 |
| `DB-01` | V1 涉及的业务表和控制表必须位于同一 PostgreSQL 数据源和同一事务边界，隔离级别固定为 `READ COMMITTED`。 |
| `DB-02` | 所有会同时持有两类以上业务行锁的 Repository 必须遵循一份覆盖完整表集合的全局顺序。 |
| `DB-03` | 租约、截止时间、终态屏障和条件认领使用数据库时间、行锁、CAS 和 `SKIP LOCKED`，不依赖应用本机时间。 |
| `ROUTE-01` | Redis 只降低发现延迟；Redis 全丢失时，数据库扫描仍能完成全部正确性路径。 |
| `ROUTE-02` | Stream 消息的 ACK、重领和删除只能在读取数据库权威状态后进行。 |
| `ROUTE-03` | Worker 永久离线后，其旧 Stream 和 Pending 消息必须有跨 Worker janitor 收敛路径。 |
| `WEBHOOK-01` | Webhook 必须在 Inbox 或 Quarantine 持久化成功后才返回 2xx；持久化失败返回可重试 5xx。 |
| `WEBHOOK-02` | Quarantine 不能猜测租户，只有关联成功后才能在同一事务写主 Inbox。 |
| `WEBHOOK-03` | 多实例 Quarantine Worker 必须使用自己的 processing owner、token、租约和 fencing/CAS 合同。 |

## 4. 问题台账

### 4.1 P0：阻断实现

#### P0-01 Runtime 自行写 cleanup Owner，违反唯一 Owner 分配权

**证据**

- 主文档 403–414 行规定 Runtime 永远不得自行取得无主或过期 Record，首次分配和接管仅由 Dispatcher/Recovery 完成。
- 主文档 875 行又规定 `END_CALL.target_owner_id` 为空时，当前 Worker 自行占用清理槽并把自己写成 cleanup Owner。

**故障时间线**

1. Record 无有效 Owner，`END_CALL.target_owner_id` 为空；
2. 两个 Runtime 同时扫描到命令；
3. Runtime 走 875 行的本地领取路径；
4. 该路径绕过 Dispatcher/Recovery 的候选选择、旧容量转换和统一锁事务；
5. 即使最终 CAS 只留下一个 Owner，容量、Reservation 或 Attempt 仍可能不满足分配合同。

**必须修复**

- 删除 Runtime 自分配例外；
- 空目标 `END_CALL` 只能由 Recovery Repository 先分配 cleanup Owner 和目标 fencing；
- Runtime 只领取已经明确指向自己且 Owner/租约有效的 END。

**关联合同**：`INV-02`、`OWN-02`、`OWN-03`、`CMD-04`。

#### P0-02 Owner 丢失后仍允许“完成已有 Effect 的 START”，与不恢复 Session 冲突

**证据**

- 主文档 679 行允许 Owner 失联后，根据 START 结果矩阵“完成命令或建立 END_CALL”。
- 主文档 685、1218–1220 行明确新 cleanup Owner 没有旧 Session，只能安全收尾，不得恢复 AI 对话或创建新业务媒体。
- 主文档 563 行还写有“按命令终态决定重试创建”，没有排除 cleanup Owner。

**故障时间线**

1. 旧 Owner 已创建 Room/Participant，并在本地建立了不可恢复的 Session；
2. Provider 结果明确写入 Effect 后，旧 Owner 崩溃；
3. 新 cleanup Owner 看到资源存在；
4. 若按 679 行“完成 START”，Record 可进入 ready/preparing 后续态；
5. 实际上没有合法 Worker 持有原 AI 上下文，通话成为数据库成功但不可服务的僵尸会话。

**必须修复**

- 只有“尚未登记任何创建 Effect”的 START 才允许重新分配执行；
- 任一创建 Effect 已登记后发生 Owner 丢失，无论结果明确、不确定或迟到成功，都必须建立唯一 END 并由 cleanup Owner 收尾；
- cleanup Owner 禁止重试任何非终止创建动作。

**关联合同**：`INV-06`、`OWN-02`、`START-04`、`END-02`。

#### P0-03 销毁 Effect 可在迟到创建窗口关闭前错误进入 APPLIED

**证据**

- 主文档 536、566 行要求在创建保护窗口内重复查询，不能一次查无资源就宣告清理完成。
- 主文档 553 行把“销毁资源不存在”直接定义为 `APPLIED`。
- 主文档 1094–1098 行会在 END 时预登记销毁图，且在当前查询不存在时允许 noop 或完成依赖。
- Effect 表和 Dependency 目前没有“销毁等待对应创建 Effect 的保护窗口闭合”这一依赖类型。

**故障时间线**

1. `CREATE_SIP_PARTICIPANT` 已发出但超时，创建 Effect 为 `RECONCILE_REQUIRED`；
2. END 预登记 `HANGUP_SIP`；
3. 此刻 Provider 查询未发现 SIP，销毁 Effect 被写为 `APPLIED`；
4. 旧创建调用在保护窗口内迟到成功；
5. 销毁 Effect 已是单调终态，不会再被认领；
6. Provider 资源泄漏，甚至可能被错误写为 `resource_cleanup_status=clean`。

**必须修复**

- 每个销毁 Effect 增加对应创建 Effect/稳定资源键的依赖关联；
- 只有创建 Effect 明确 `FAILED(no_resource)`，或其 `reconcile_deadline_at` 已过且完成规定次数/持续时间的不存在确认，销毁才可因“不存在”进入 `APPLIED`；
- 创建保护窗口未闭合时，销毁保持 `RECONCILE_REQUIRED`，即使当前查询不存在；
- `clean` 必须复用同一门禁，不能只检查当前 Provider 快照。

**关联合同**：`INV-05`、`EFF-04`、`EFF-05`、`END-02`。

#### P0-04 attention_required 会永久占住有限 cleanup_capacity

**证据**

- 主文档 1215–1218 行要求 cleanup Owner 占用有限 `cleanup_capacity`。
- 主文档 921 行要求 Provider 长期不可确认时不释放容量/线路，并由 cleanup Owner 持续对账。
- 主文档 1114、1254 行允许最终进入 `attention_required`，但没有定义 Owner/cleanup 槽的停放与再次认领合同。

**故障时间线**

1. Provider 查询长期失败，资源存在性不可证明；
2. Record 进入 `attention_required`，线路继续隔离是正确的；
3. cleanup Owner 和 `active_cleanup_count` 也永久保留；
4. 累积少量长期异常后，所有 Worker 的 cleanup 槽耗尽；
5. 新的挂机和孤儿清理只能排队，终止发现与恢复 RTO 失去上界。

**必须修复**

- 区分“资源/线路逻辑隔离”与“正在执行的 cleanup 计算槽”；
- 一次有界对账尝试结束后，`attention_required` 释放 Runtime cleanup 执行槽和短租约，但保留 Effect、Provider 资源键、线路 Reservation 和告警；
- 到 `next_reconcile_at` 后由 Recovery Repository 重新占用一个 cleanup 槽、分配临时 cleanup Owner 并执行下一次尝试；
- 增加此状态下的容量守恒、饥饿和恢复时延测试。

**关联合同**：`OWN-05`、`EFF-05`、`END-04`。

### 4.2 P1：冻结前必须决议

| ID | 问题与证据 | 必须冻结的决议 |
| --- | --- | --- |
| `P1-01` | 主文档 1322–1326 行只说“控制面 PostgreSQL、旧业务可继续 MySQL”，但首次分配和投影事务还同时写 Record、Attempt、Target、Task 等业务表。 | 明确所有参与 V1 原子事务的业务表和控制表必须位于同一 PostgreSQL 数据源；业务表仍在 MySQL 的部署不得开启 V1，禁止跨库伪装成一个事务。 |
| `P1-02` | Effect 只有状态枚举和零散结果表，没有完整的合法状态迁移、终态、租约过期、重试耗尽矩阵。 | 给出 `PENDING/APPLYING/RECONCILE_REQUIRED/APPLIED/FAILED` 的唯一状态机，定义每条边的写入者、CAS 条件、是否终态和容量影响。 |
| `P1-03` | 全局锁顺序 305–316 行只覆盖 Record、Line、Worker、Command、Reservation、Effect；实际事务还会写 Attempt、Target、Task、Handoff、Presence。 | 审计所有跨表事务后给出完整唯一顺序；禁止某路径 `Record -> Attempt`、另一条路径 `Attempt -> Record`。现有测试 59 也必须覆盖完整表集合。 |
| `P1-04` | 896 行用“所有必需资源的最大调用超时”计算 START 聚合截止；Room、SIP、Agent、Egress 可能按依赖串行执行。 | 按资源依赖 DAG 的最坏关键路径加迟到窗口计算，而不是取单项最大值；持久化计算输入和配置版本。 |
| `P1-05` | 424 行要求 DB 不可达时直接静音/断开 AI Participant，但 `INV-05` 对所有 Provider 动作要求先登记 Effect。 | 明确这是唯一的 fail-closed 紧急例外：仅限本进程、当前 generation 的 AI 输出隔离，不提交业务成功状态；恢复后必须通过已有资源事实补建/执行标准终止对账。 |
| `P1-06` | 权限矩阵只授权 Runtime 写 Handoff `connected/reconnecting/ended` 和坐席 `in_call/ACW`；1122–1126 行的 `CANCEL_HANDOFF` 需要写 `canceled` 并把 `claiming` 释放为 `available`。 | 在权限矩阵加入准确状态和条件，明确是 Runtime 原子写，还是独立 Release Repository 写；不得让两者都能无条件释放。 |
| `P1-07` | Quarantine 表 752–767 行没有 processing owner/token/expires 字段，但角色矩阵允许 Jobs 多实例运行。 | 为 Quarantine 增加独立处理租约、CAS 认领和迟到提交隔离；不能只靠 `status + next_retry_at` 扫描。 |
| `P1-08` | `XAUTOCLAIM` 只覆盖同一 Owner 的消费者重启；每次启动使用新 `worker_id` 时，永久离线 Worker 的旧 Stream/Pending 没有收敛者，Stream 又要求 Pending 为零才删除。 | 定义跨 Worker Stream janitor：枚举过期 Worker Stream，读取 DB 权威状态后 ACK/重投，Pending 清零后删除 Stream，并给出幂等与限速规则。 |
| `P1-09` | 1335–1337 行规定就绪后签 Token，但没有定义签发瞬间对终态屏障、Owner 租约、Room/Participant readiness 的统一门禁。 | Token 签发前重新读取并校验 `runtime_control_mode`、无终态屏障、有效 Owner、当前 generation 和持久 readiness；Token 使用短 TTL，bootstrap 与加入失败均按数据库事实处理。 |
| `P1-10` | 媒体失效可把 Handoff 推进 `reconnecting`，但 join/unmute 后谁创建持久恢复动作没有冻结；仅递增 Evidence 不会自动回到 `connected`。 | 选择唯一入口。建议 Inbox 只追加重新就绪 Evidence，并在 identity、Handoff 和版本匹配时幂等创建 `AGENT_MEDIA_READY`；Runtime 重新查询后提交 `connected`。 |

### 4.3 P2：一致性与可运维性

| ID | 问题 | 建议 |
| --- | --- | --- |
| `P2-01` | 字段统一写 `datetime`，但正式库已冻结 PostgreSQL，租约和截止时间需要确定时区语义。 | 正式迁移明确使用 `timestamptz` 和 UTC；API 输出再按展示时区转换。 |
| `P2-02` | Provider 幂等/Quarantine 去重键只有 provider 与资源键；若存在多个 Provider account/cluster 命名空间，可能碰撞。 | 在稳定键和唯一约束中加入 provider account/cluster namespace；若 V1 明确只有一个命名空间，则写成部署约束。 |
| `P2-03` | 500ms、1s、2s、15s、120s 等数值分散在正文、测试和指标中。 | 集中列为配置与验收默认值，区分“协议上限”“默认配置”“SLO”，避免实现把示例常量硬编码到不同模块。 |

### 4.4 已冻结 Scope，不应在本轮扩张

| ID | 决策 |
| --- | --- |
| `SCOPE-01` | V1 不包含 `sip_inbound`；Phase F 呼入继续由 LiveKit dispatch/`JobContext` 持有，未来另行设计 `ADOPT_EXISTING_ROOM` 或独立 START 模式。 |
| `SCOPE-02` | V1 不支持无损恢复旧 AI 对话上下文；失联只做安全收尾。 |
| `SCOPE-03` | V1 不解决多地域主动-主动；正式控制面只定义单 PostgreSQL 权威域。 |
| `SCOPE-04` | Redis 不是事实源，16.2A/16.2B 必须先在 Redis 完全关闭时通过。 |
| `SCOPE-05` | 不承诺 Provider 侧严格 exactly-once；只承诺稳定资源键、迟到检测、幂等终止和最终可见告警。 |
| `SCOPE-06` | 16.1/16.2A 只使用 Provider stub 和空正式入口集合；Redis、真实 Provider、Webhook 入口和业务流量按后续切片启用。 |

## 5. 十类故障时间线审计

| 时间线 | 必须证明的结果 | 当前覆盖 | 缺口 |
| --- | --- | --- | --- |
| `F-01` 数据库提交前/后进程崩溃 | 未提交动作不执行；已提交意图可恢复；响应丢失幂等返回原记录 | `CMD-01`、测试 22、52 | 基本闭合 |
| `F-02` Command 提交后、Redis 发布前崩溃 | DB-only 扫描能发现并执行，Redis 不决定正确性 | `ROUTE-01`、测试 11–13、42、69 | 基本闭合 |
| `F-03` Effect 提交后、Provider 调用前崩溃 | 新 Effect token 可安全认领；创建先查再执行 | `EFF-01/02`、测试 19、41、63 | 需 P1-02 给出完整状态边 |
| `F-04` Provider 成功后、结果提交前崩溃 | 旧 token 不补写，新 Owner 用稳定键查询回填 | `EFF-03`、测试 26、55 | 基本闭合 |
| `F-05` Owner 丢失且 Provider 调用迟到 | 不恢复 Session；迟到创建必有仍可执行的销毁路径 | 测试 28、55 | 被 P0-02、P0-03 阻断 |
| `F-06` Command 已终态、Effect 未终态 | Effect 独立恢复，不复活 Command | `EFF-01/02`、测试 43、63 | 基本闭合 |
| `F-07` 终态建立后资源迟到出现 | 终止图保持有效，保护窗口内不误判 clean | 测试 28、57 | 缺少 P0-03 的创建-销毁窗口测试 |
| `F-08` Redis、数据库或 Provider 分别不可用 | Redis 可降级；DB 不可达 fail closed；Provider 不确定进入可见对账 | 测试 13、37、43、51 | 长期 Provider 不确定被 P0-04 阻断；紧急动作需 P1-05 |
| `F-09` 多 Dispatcher/Runtime/Reconciler 并发 | 单 Owner、单槽、单投影、无稳定锁反转 | 测试 2、41、48、54、59 | 完整锁集合和 Quarantine 多实例未覆盖 |
| `F-10` API/Runtime/Reconciler 跨表写入交错 | 每类状态只有一个权威写入者，非法迟到写入影响 0 行 | 测试 15、56、64 | CANCEL、Token、重连路径仍有权限缺口 |

## 6. 合同—测试追踪矩阵

“已有”表示现有 69 项测试可以覆盖该合同的主要行为；“补充”表示必须新增或扩展，否则不能证明修复有效。

| 合同组 | 现有测试 | 必须新增/扩展 |
| --- | --- | --- |
| `INV-*` | 8–10、15、16、21、32、40、45、49 | 紧急 fail-closed 动作例外不得提交业务成功状态 |
| `OWN-*` | 1、2、14、15、37、38、44、58、65 | `END target_owner_id=null` 只能由 Recovery 分配；`attention_required` 释放执行槽后可再次认领；大量长期异常不饿死新 END |
| `CMD-*` | 7–13、27、29、30、34–36、45–48、51、52、62 | Effect 完整状态机与 Command 终态组合的参数化测试 |
| `EFF-*` | 19、26、28、41、43、55、57、63、66 | 销毁第一次查无资源时，创建窗口未闭合不得 APPLIED；窗口内迟到创建必须触发同一销毁 Effect；`clean` 同步受门禁约束 |
| `WRITE-*` | 4–6、15、32、35、49、56、64 | CANCEL 的 Handoff/Presence 原子转换；非法角色写入逐表影响 0 行 |
| `START-*` | 3、25、26、31、47、52、54、60、66 | Owner 丢失且创建结果明确存在时只能 END；按资源依赖关键路径计算聚合截止 |
| `END-*` | 6、8–10、27、34–36、43、45–48、57、58 | 全部创建类型与对应销毁依赖的参数化完整性测试 |
| `DB-*` | 2、29、37、48、51、54、59、67、69 | Handoff、Presence、Attempt、Target、Task 纳入锁反转测试；控制面与业务表跨数据源时启动失败 |
| `ROUTE-*` | 11–13、29、42、48、69 | 永久失联 Worker 的旧 Stream/Pending 由其他实例收敛并删除 |
| `WEBHOOK-*` | 7、22、34、39、50、56 | 两个 Quarantine Worker 并发认领、租约过期、旧 token 迟到提交 |
| Token/重连 | 4、5、31、56 | 终态屏障与 Token 签发竞态；`reconnecting` 后重新就绪的唯一命令入口和版本 CAS |
| `SCOPE-*` | 16、23、33、40、53、61、68 | 无新增；保持配置门禁 |

## 7. 一次性主文档修订清单

用户确认本审计包后，只进行一轮集中修订，按以下顺序完成：

1. 在正文前部加入第 3 节合同 ID，并声明冲突时以规范性合同和权威矩阵为准；
2. 先修复 4 个 P0，统一 Owner、START 恢复、创建/销毁 Effect 依赖和长期 attention 容量；
3. 再冻结 10 个 P1 的状态机、写权限、数据库、锁、Token、重连和多实例规则；
4. 重写相关表结构字段，不保留与新合同冲突的旧句子；
5. 同步更新故障矩阵、自动化测试和成功指标，所有新增测试引用合同 ID；
6. 对全文执行术语和状态枚举一致性检查；
7. 生成新 SHA-256，进入只读冷审，冷审期间不直接改主文档。

禁止采用以下方式收口：

- 只在问题段落后追加“特殊情况说明”，保留前文冲突规则；
- 用“实现时注意”“视 Provider 而定”替代权威状态矩阵；
- 为通过审查继续扩大 V1 到 SIP 呼入、多地域或真实 Provider；
- 把 Redis、进程内 Registry 或人工操作当作数据库合同缺口的兜底。

## 8. 设计冻结退出条件

只有同时满足以下条件，才可以说“设计已完成”：

1. 本台账 4 个 P0 全部关闭，且对应合同和故障时间线无互相冲突；
2. 10 个 P1 均有唯一决议，不留给实现者二选一；
3. 每个规范性合同至少映射到一条正文规则、一条故障决议和一项测试或明确的 Scope 门禁；
4. 表结构字段足以实现所有 CAS、租约、依赖和审计条件；
5. PostgreSQL 单数据源、完整锁顺序和多实例认领在文档中可直接转成集成测试；
6. 第一轮冷审只允许报告遗漏，不允许边审边改；发现 P0/P1 时退回集中修订并重新开始冷审计数；
7. 连续两轮独立冷审均为：P0=0、P1=0；P2 只能是已接受且不改变状态机的文案/运维项；
8. 冻结版本记录 SHA-256、问题台账状态和明确的 V1 Scope，之后新增需求走变更单，不再悄悄改写冻结合同。

## 9. 为什么七轮后仍会出现新问题

根因不是“系统复杂，所以只能无限审”，而是此前采用了错误的完成判定：每轮围绕最新发现做局部修复，然后把“本轮问题已修”说成“设计已完成”。主文档同时承载状态机、权限、租约、Provider 不确定性、容量、迁移和测试，任何一个局部句子都可能改变其他章节的前提；没有合同 ID、问题台账、故障时间线和退出条件时，新会话只能重新从自然语言中推导全局模型，自然会不断发现新的交叉冲突。

本审计包把完成标准从“暂时没看到新问题”改为“有限合同集合在所有关键故障时间线上均有唯一结果，并由测试可追踪”。这才是可以终止审查循环的标准。

## 10. 闭环修订与冷审结果

### 10.1 冻结对象

| 项目 | 结果 |
| --- | --- |
| 主文档 | `docs/superpowers/specs/2026-07-31-ai-call-single-owner-runtime-command-design.md` |
| 原始审计基线 | `22ebc7137aac3119061306fcdee8f7de825cab2b580b8ec9796274946b8c45d6` |
| 最终闭环哈希 | `c3a4300d3426359ff9cecf3d051be5d700c820571838ecb74e88053d13e3ceb8` |
| 最终规模 | 1829 行、12854 词、189263 字节 |
| 规范性合同 | 46 条，ID 唯一且无重复 |
| 自动化场景 | 80 条，编号 1–80 连续 |
| 冷审结果 | 连续两轮 P0=0、P1=0 |
| 决议 | 设计闭环通过，允许编写分阶段实现计划 |

闭环哈希只覆盖主文档。后续任何正文修改都会产生新哈希；涉及 Owner、Command、Effect、容量、锁顺序、终态或 Provider 不确定性合同的修改必须走变更审计，不能沿用本报告的通过结论。

### 10.2 P0 关闭证据

| 问题 | 最终决议 | 对应合同/验证 |
| --- | --- | --- |
| `P0-01` Runtime 自分配 cleanup Owner | Runtime 对空目标、过期 Owner 或 `attention` Record 一律拒绝；只有 Recovery Repository 能占用 cleanup 槽、递增 fencing 并写目标 Owner。 | `OWN-02/03`、测试 65、70 |
| `P0-02` Owner 丢失后恢复已有 Effect 的 START | 只有零 Effect START 可重新分配；登记任一创建 Effect 后失联，无论结果明确或不确定都固定为 `SUPERSEDED + END_CALL`，不得恢复旧 Session。 | `INV-06`、`START-04`、测试 47、62、65 |
| `P0-03` 销毁提前终态导致迟到资源泄漏 | 销毁 Effect 关联 `source_create_effect_id` 并执行创建静默门禁。门禁前的“不存在”以及“销毁成功”都只能留作证据；创建静默后必须重新查询或幂等终止并写 `terminal_confirmed_at`，才能 `APPLIED`。 | `EFF-04/05`、`END-02/03`、测试 57、71、73 |
| `P0-04` attention 永久占 cleanup 槽 | `runtime_capacity_class` 分为 `none/active/cleanup/attention`；`active/cleanup -> attention` 原子释放对应 Worker 槽并清空 Owner/Effect token，保留线路和资源隔离；到期只能由 Recovery 执行 `attention -> cleanup`。 | `OWN-05`、测试 43、44、58、72 |

`P0-03` 在第二轮故障推演前额外覆盖了原台账未明确写出的顺序：销毁调用先返回成功，旧创建调用随后迟到落地。最终合同不再只防“一次查询不存在”，而是防门禁前的任何销毁结论被沿用为终态。

### 10.3 P1/P2 关闭证据

| 问题组 | 最终决议 |
| --- | --- |
| `P1-01` 数据库拓扑 | V1 参与表固定在同一 PostgreSQL datasource/database/schema/transaction manager，隔离级别 `READ COMMITTED`；跨 PostgreSQL 数据源或混用 MySQL 时相关角色启动失败。 |
| `P1-02` Effect 状态机 | 给出五状态的全部合法边、终态、同状态换 token、重试耗尽和 `FAILED(no_resource)` 条件；销毁不能因普通重试耗尽进入 `FAILED`。 |
| `P1-03` 全局锁顺序 | 顺序扩展到 Task、Target、Attempt、Record、Line、Worker、Handoff、Presence、Command、Reservation、Effect、Evidence、Event/Dialogue、Recording、ASR、Semantic、Decision/Follow-up、Inbox、Quarantine。任务认领和业务结果提交分事务，禁止持有任务行再回锁业务父行。 |
| `P1-04` START 截止 | 按资源 DAG 最坏关键路径计算串行和并行预算，再加迟到窗口与安全裕量；持久化策略版本和预算快照。 |
| `P1-05` fail-closed | 唯一例外仅限旧 Owner 对本进程当前 generation AI 媒体的紧急隔离；不能提交业务终态或 Effect 成功，恢复后仍走标准终止图。 |
| `P1-06` CANCEL 权限 | Runtime 只在人工媒体从未接通、Handoff/Presence 独占且 token 有效时原子取消并释放 `claiming`；`connected/reconnecting` 只能转唯一 END。 |
| `P1-07` Quarantine 多实例 | 增加 processing owner、generation、token、租约和 CAS；认领与关联结果提交分事务，旧 token 影响 0 行。 |
| `P1-08` 旧 Stream | Worker 行增加 Janitor 租约；跨 Worker `XAUTOCLAIM` 前读取数据库，未决命令先恢复/改投，PEL 清零和保留期满足后才删除。 |
| `P1-09` Token | 每次签名前重新读取 PostgreSQL，校验终态、Owner/Worker 租约、Room/Agent generation 和 Handoff claim；短 TTL Token 不作为业务成功证据。 |
| `P1-10` Handoff 重连 | Inbox 只写版本化 Evidence 并幂等创建 `AGENT_MEDIA_READY/INVALIDATED`；Runtime 强制查询当前媒体并用版本 CAS 写 `connected/reconnecting/ended`。 |
| `P2-01` 时间类型 | 正式 PostgreSQL 字段统一为 `timestamptz`、UTC；协议判断使用数据库时间。 |
| `P2-02` Provider 命名空间 | Effect、Inbox、Quarantine、Handoff Media Evidence 和 End Evidence 的稳定键加入 `provider_namespace`。 |
| `P2-03` 时间策略 | 使用版本化 `AiCallRuntimeTimingPolicy`，分离协议上限、默认配置和验收 SLO。 |

冷审过程中还补齐了原 `P1-03` 未覆盖的 Inbox、Quarantine、Evidence、录音和离线 Job 锁顺序。该补充属于实现正确性合同，因此每次发现后都按第 8 节将两轮冷审计数清零，而不是把边审边改算作通过。

### 10.4 两轮只读冷审

#### 第一轮：合同与静态一致性

- 审查对象固定为 `c3a4300d3426359ff9cecf3d051be5d700c820571838ecb74e88053d13e3ceb8`；
- 校验 46 个合同 ID：`INV=7`、`OWN=5`、`CMD=6`、`EFF=5`、`WRITE=6`、`START=4`、`END=4`、`DB=3`、`ROUTE=3`、`WEBHOOK=3`，无重复；
- 校验 80 个自动化场景编号连续；
- 定向扫描旧数据库双选项、Runtime 自抢 Owner、销毁无门禁 `APPLIED`、只释放 cleanup 槽和旧锁反转表述；
- 交叉核对写权限、Owner 恢复、START/END 结果矩阵、Effect 状态机、数据库拓扑和 `sip_inbound` Scope；
- 结果：P0=0、P1=0，`git diff --check` 通过。

#### 第二轮：故障事件序列

在不修改正文的前提下，对同一哈希逐条推演：

1. START Provider 调用中 Owner 失联，旧 Effect/Command token 迟到；
2. END 销毁先成功或查无资源，旧创建随后迟到；
3. active/cleanup 长期不确定后停放 attention，再由 Recovery 到期重领；
4. Redis 发布确认丢失、旧 Stream/Pending 和 DB-only 恢复；
5. Quarantine/Inbox 多实例认领与业务父行并发；
6. Handoff 失效、重连、Token 签发与终态屏障竞态；
7. PostgreSQL 跨表事务与完整锁顺序。

18 条核心只读断言和两条修正后的多行/措辞断言全部通过；最初两条未命中是检索字符串与换行写法不匹配，修正断言后通过，正文哈希从始至终未变化。结果：P0=0、P1=0。

### 10.5 冻结后的推进边界

- 可以开始编写实现计划，但实现计划必须按正文 16.1、16.2A、16.2B、16.2C 的门禁分阶段推进；
- 第一实现切片只允许 Schema、角色门禁、DB-only Command/Owner/Effect/END 和 Provider Stub，不接真实 Provider、不发真实电话；
- Redis 只能在 DB-only 故障注入通过后作为加速层加入；
- `sip_inbound`、多地域主动-主动、旧 AI 上下文恢复和 Provider 侧 exactly-once 继续不属于 V1；
- 本报告只批准设计进入实现计划，不代表代码已经实现、数据库已经迁移或真实 SIP 已验收。
