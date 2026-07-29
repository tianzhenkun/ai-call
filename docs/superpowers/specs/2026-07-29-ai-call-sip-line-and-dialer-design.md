# AI Call SIP 线路配置与正式拨号适配器设计

## 1. 文档状态

- 设计日期：2026-07-29
- 适用仓库：`/Users/liuhongli/.codex/worktrees/ed81/ai-call`
- 实施阶段：后端基础优先
- 真实拨打：本设计和自动化验证期间禁止发起 Linphone 或真实号码呼叫

## 2. 目标

将现有通用外呼任务从“Mock 执行器 + Linphone 人工测试命令”收口为一套正式执行流程：

1. 外呼任务创建时自动绑定当前租户的默认外呼线路，运营人员不在任务页面手工选择线路。
2. 立即任务和定时任务都由 `OutboundTaskExecutor` 领取。
3. 当前本地阶段使用 Linphone 作为 SIP 被叫终端，但不产生独立测试业务状态。
4. 后续切换真实 SIP Provider 时，只调整线路配置或线路基础设施，不修改任务、对象、重试、通话记录和转人工流程。
5. “创建 SIP Session”“接口已受理”“正在振铃”均不算接通；只有真实接听和媒体证据成立后才进入 `IN_CALL`。

## 3. 本阶段范围

### 3.1 包含

- 租户级 SIP 外呼线路数据模型。
- 线路增删改查、设为默认、启停和非拨号预检接口。
- 任务校验时自动解析默认线路。
- 正式任务保存线路标识及非敏感配置快照。
- `SipOutboundDialer` 正式拨号适配器。
- Linphone 与真实 SIP Provider 共用相同拨号器和任务状态。
- Attempt 保存实际使用的线路、Provider 结果和失败原因。
- 配置缺失、线路停用、忙、无人接听和线路失败的明确状态映射。
- 默认关闭的正式执行器开关及本地授权号码限制。

### 3.2 不包含

- 前端线路配置页面。
- 前端删除“测试拨打”入口；该工作在后端基础稳定后单独实施。
- 自动生成或修改 FreeSWITCH 配置文件。
- 在线管理 Provider 密码。
- 多线路负载均衡、按地区路由、号码池、CPS 调度和故障自动切线。
- 真实运营商 SIP 拨打。
- 修改现有转人工状态机。

现有 `test-capability`、`test-run` 和 `test-status` 接口本阶段不再扩展，只作为过渡期内部诊断能力保留；正式任务执行不得调用这些接口。

## 4. 核心架构

```text
任务校验
  -> DefaultSipLineResolver
  -> 保存 lineId、lineName 和非敏感线路快照

确认启动
  -> AiCallOutboundTask(status=SCHEDULED)
  -> OutboundTaskExecutor
  -> SipOutboundDialer
  -> AiCallService.create_sip_session(...)
  -> LiveKit SIP
  -> FreeSWITCH / SIP Provider
  -> Linphone 或真实电话

通话事件与记录
  -> SipOutboundDialer
  -> DialResult
  -> Attempt / Target / Task
```

职责边界：

- `OutboundTaskExecutor`：领取任务和对象、创建 Attempt、执行重试、刷新计数和任务终态。
- `DefaultSipLineResolver`：按租户解析任务绑定的有效线路，禁止跨租户读取。
- `SipOutboundDialer`：创建 SIP 会话、等待接听或终态、映射拨打结果，不直接修改任务。
- `AiCallService`：创建通话记录、LiveKit Room、SIP Participant、AI Runner、录音和转人工。
- FreeSWITCH 或 Provider：负责具体 SIP 路由和媒体互通，不承担业务任务状态。

## 5. 线路数据模型

新增 `ai_call_sip_line` 表。使用普通字段和 `text` JSON 字符串，不使用物理外键和数据库专有 JSON 类型。

| 字段 | 类型 | 规则 |
| --- | --- | --- |
| `id` | bigint | 雪花 ID，接口按字符串返回 |
| `tenant_id` | varchar(64) | 租户隔离 |
| `line_code` | varchar(64) | 租户内唯一、创建后不可修改 |
| `line_name` | varchar(100) | 用户可读名称 |
| `enabled` | boolean | 是否允许新任务使用 |
| `default_marker` | varchar(32) nullable | 默认外呼线路固定为 `OUTBOUND`，其他线路为空 |
| `adapter_type` | varchar(32) | V1 固定为 `livekit_sip` |
| `route_mode` | varchar(32) | `managed_trunk_id` 或 `inline_hostname` |
| `trunk_id` | varchar(128) nullable | LiveKit 已管理的 outbound trunk ID |
| `proxy_host` | varchar(255) nullable | inline route 的 SIP hostname，不包含密码 |
| `proxy_port` | integer nullable | SIP 端口 |
| `auth_mode` | varchar(32) | `managed_trunk` 或 `ip_allowlist` |
| `caller_number` | varchar(64) | Provider 授权的主叫号码 |
| `destination_country` | varchar(8) | 默认 `CN` |
| `max_concurrency` | integer | V1 最小值 1 |
| `originate_timeout_seconds` | integer | 建立呼叫超时 |
| `health_status` | varchar(32) | `UNKNOWN`、`AVAILABLE`、`MISCONFIGURED`、`UNAVAILABLE` |
| `health_message` | varchar(500) nullable | 最近一次预检结论 |
| `last_checked_at` | datetime nullable | 最近预检时间 |
| `deleted` | boolean | 业务软删除 |
| `deleted_at` | datetime nullable | 删除时间 |
| `created_by` / `updated_by` | bigint | 操作人 |
| `created_at` / `updated_at` | datetime | 审计时间 |

约束：

- `(tenant_id, line_code)` 唯一。
- `(tenant_id, default_marker)` 唯一；同一租户最多一条 `OUTBOUND` 默认线路。
- `managed_trunk_id` 必须提供 `trunk_id`，不得同时提供 inline route。
- `inline_hostname` 必须提供 `proxy_host` 和 `proxy_port`，不得提供 `trunk_id`。
- `managed_trunk` 不在业务库保存 Provider 密码。
- `ip_allowlist` 不要求用户名或密码。
- 删除默认线路前必须先指定其他默认线路或明确停用租户外呼。

V1 不增加用户名和密码字段。未来遇到账号密码型 Provider 时，必须先设计加密凭据或 Secret Reference，不能把明文密码写入本表。

## 6. 默认线路与任务快照

运营人员创建任务时不选择线路。任务校验服务按当前租户解析唯一默认线路：

1. 默认线路不存在、已删除或未启用：校验失败。
2. `health_status=MISCONFIGURED`：校验失败。
3. `health_status=UNKNOWN`：要求先执行线路预检。
4. `health_status=UNAVAILABLE`：校验失败并展示最近健康信息。
5. `health_status=AVAILABLE`：校验通过。

任务新增逻辑关联字段：

- `line_id`：实际绑定线路 ID；
- `line_name`：创建时线路名称；
- `config_snapshot_json` 增加非敏感 `sipLine` 快照。

快照包含：

- `lineId`
- `lineCode`
- `lineName`
- `adapterType`
- `routeMode`
- `trunkId` 或 `proxyHost`、`proxyPort`
- `authMode`
- `callerNumber`
- `destinationCountry`
- `maxConcurrency`
- `originateTimeoutSeconds`

快照不包含密码、Token、LiveKit Secret 或其他凭据。

定时任务执行时仍校验绑定线路是否存在、启用且未删除。线路失效时，不创建 Attempt、不消耗对象重试次数，任务进入 `FAILED`，并把明确原因写入 `error_message`。

## 7. 线路管理接口

所有 ID 按字符串返回，接口使用项目现有统一响应外壳。

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `GET` | `/ai-call/outbound-lines` | 租户线路分页 |
| `GET` | `/ai-call/outbound-lines/{lineId}` | 线路详情 |
| `POST` | `/ai-call/outbound-lines` | 新建线路 |
| `PUT` | `/ai-call/outbound-lines/{lineId}` | 修改线路 |
| `POST` | `/ai-call/outbound-lines/{lineId}/set-default` | 原子切换默认线路 |
| `POST` | `/ai-call/outbound-lines/{lineId}/enable` | 启用线路 |
| `POST` | `/ai-call/outbound-lines/{lineId}/disable` | 停用线路 |
| `POST` | `/ai-call/outbound-lines/{lineId}/preflight` | 执行非拨号预检 |
| `DELETE` | `/ai-call/outbound-lines/{lineId}` | 软删除线路 |

异步或动作接口返回“已受理”时不代表线路真实可拨；前端必须重新查询线路健康状态。

### 7.1 非拨号预检

预检只检查：

- 字段组合完整；
- LiveKit API 可访问；
- `trunk_id` 或 inline route 可构造官方请求；
- Caller Number、目的国家和超时参数有效；
- 本地运行配置满足 SIP 基础开关。

预检不发起 SIP INVITE，不产生通话记录，也不能证明真实 Provider 当前可接通。真实线路最终可用性仍需用户明确授权后执行单号码验收。

## 8. 正式拨号适配器

将 `LinphoneTestDialer` 的可复用部分收口为 `SipOutboundDialer`：

```python
class OutboundDialer(Protocol):
    dialer_type: str
    manages_call_record: bool

    async def dial(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        on_connected: ConnectedCallback,
    ) -> DialResult: ...
```

`OutboundDialRequest` 增加已解析的非敏感线路快照；`DialResult` 增加：

- `provider_status_code`
- `provider_reason`
- `hangup_cause`

`SipOutboundDialer` 的执行顺序：

1. 校验任务绑定线路与运行安全开关。
2. 使用任务执行器预生成的 `call_id` 调用 `AiCallService.create_sip_session()`。
3. 把 `line_id`、`target_id`、`attempt_no` 放入内部业务上下文。
4. 轮询或订阅真实通话记录。
5. 仅当 `answered_at` 已写入且媒体状态满足现有接通契约时，调用一次 `on_connected()`。
6. 等待通话记录终态。
7. 返回统一 `DialResult`。

禁止在 `create_sip_session()` 返回后立即调用 `on_connected()`。创建 Room、创建 SIP Participant、收到 183 SDP 或接口返回 202 都不算接通。

## 9. 状态与失败映射

| 证据 | `DialResult.call_result` | 说明 |
| --- | --- | --- |
| 已接听并进入媒体，随后正常结束 | `connected` | 计算真实通话时长 |
| SIP 486 或等价挂机原因 | `busy` | 按呼叫规则决定是否重试 |
| SIP 408、480、振铃超时 | `no_answer` | 按呼叫规则决定是否重试 |
| SIP 403 | `call_failed` | IP 白名单、显号或权限错误 |
| SIP 404、484 | `call_failed` | 号码格式或路由错误 |
| SIP 503、508、Q.850 31 | `call_failed` | Provider 或上游线路错误 |
| SIP 603 | `call_failed` | 被叫或上游明确拒绝 |
| 无法识别的终态 | `call_failed` | 保留原始原因 |

Attempt 增加：

- `line_id`
- `line_code`
- `provider_status_code`
- `provider_reason`
- `hangup_cause`

这些字段均为逻辑关联和诊断信息，不创建物理外键。

## 10. 安全与运行开关

- `AI_CALL_OUTBOUND_EXECUTOR_ENABLED` 默认继续为 `false`。
- 新增或复用正式 SIP 拨号器选择配置；未明确选择 `sip` 时不得发起网络调用。
- 本地阶段保留授权号码限制，但限制挂在正式拨号器入口，不再由前端测试按钮控制。
- 自动化测试只使用 Fake Dialer、Fake AiCallService 和临时数据库。
- 任何真实 Linphone 或手机号验收必须由用户明确发起。
- 线路接口永不返回密码、LiveKit Secret 或 Provider 凭据。

## 11. 兼容与迁移

- 现有 `MockOutboundDialer` 保留用于自动化测试和数据库演练。
- 现有 `linphone_test` Attempt 作为历史本地数据保留，不做兼容回填。
- 正式任务的新 Attempt 使用 `dialer_type=sip`。
- 过渡期诊断接口不进入正式任务执行链；前端迁移完成后再删除。
- 不修改现有 Handoff、录音和通话分析数据模型。

## 12. 测试与验收

### 12.1 自动化测试

- 同租户不能存在两个默认外呼线路。
- 不同租户的默认线路互不影响。
- IP 白名单线路无需密码即可通过字段预检。
- 缺少 trunk route、Caller Number 或超时配置时进入 `MISCONFIGURED`。
- 线路接口和快照不出现密码字段。
- 任务校验自动绑定默认线路，任务请求不接受用户传入任意 `lineId` 覆盖默认路由。
- 线路失效时任务失败，但不创建 Attempt、不增加对象拨打次数。
- `SipOutboundDialer` 创建 Session 后不会立即调用 `on_connected()`。
- 只有真实接听证据出现时才调用一次 `on_connected()`。
- busy、no_answer、403、503、508 和未知失败正确映射。
- Attempt 正确保存线路和 Provider 诊断字段。
- Mock、正式 SIP 和历史诊断路径互不串用。

### 12.2 静态与回归验证

- 线路、任务执行器、SIP 会话和通话记录目标测试通过。
- Ruff 通过。
- 后端全量测试通过。
- 使用独立临时 SQLite 验证迁移与任务状态，不读取或修改当前 `/tmp/ai_call_ed81_local.db`。

### 12.3 后续人工验收

后端自动化与前端正式流程完成后，由用户明确授权：

1. 用默认本地线路创建一个授权 Linphone 号码的立即任务。
2. 不调用测试接口，Linphone 自动响铃。
3. 未接听前对象保持 `DIALING`。
4. 接听且媒体成立后进入 `IN_CALL`。
5. 通话结束后 Attempt、Target、Task 和通话记录一致。
6. 再把默认线路切换为已确认的真实 SIP Provider，重复同一正式流程。

历史接通过的线路配置只能作为初始参数来源，不能替代本次 Provider 白名单、显号、端口和 RTP 的重新确认。
