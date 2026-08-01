# AI Call 16.2ABC 范围收口与实施顺序修正规格

**状态：** 已完成设计，待书面规格审查
**日期：** 2026-08-01

## 1. 目的

本规格只修正 16.2A、16.2B、16.2C 与后续业务入口之间的实施边界，避免把音色
试听、未来业务表集成和 Redis 加速提前变成 DB-Core 的冻结条件。

本规格不修改已冻结总设计原文，也不重新审查已经通过的 16.2A/B DB-Core 一致性
合同。发生实施范围冲突时，以本规格为准。

冻结依据：

- 总设计：`docs/superpowers/specs/2026-07-31-ai-call-single-owner-runtime-command-design.md`
- 总设计 SHA-256：`c3a4300d3426359ff9cecf3d051be5d700c820571838ecb74e88053d13e3ceb8`
- 16.2A/B 当前基线 HEAD：`911bf2fe0c077e4b274bb8d22f19f62a637af624`
- 音色管理轻量试听边界：前端工作树提交 `be5de0a8`

## 2. 决策摘要

1. 16.2A/B DB-Core 保持完成，不因本次范围修正重新打开一致性审查。
2. `preview` 不再是 `owner_command_v1` 的合法业务入口，不再进入 16.2、16.3、
   16.6 或正式灰度顺序。
3. Handoff、Presence、Task、Target、Attempt 的全局锁序仍是未来集成合同，但其业务
   状态和并发验收不再作为 16.2B 的关闭门槛。
4. 16.2C Redis Streams 暂缓，不是进入 Web 业务入口垂直切片的前置条件。
5. 下一实施切片只接 `web`，继续使用 DB-only Dispatcher/Runtime 和 Provider Stub，
   不接 LiveKit、SIP、真实 Provider 或 Redis。

## 3. 16.2A/B 有效冻结范围

### 3.1 保留并冻结

16.2A/B 已完成且继续有效的 DB-Core 包括：

- 单一 PostgreSQL `READ COMMITTED` 事实源；
- Runtime Worker 注册、active/cleanup/attention 容量；
- Owner 租约、fencing 和 monotonic fail-closed watchdog；
- Command 幂等、严格序列、处理租约和高优先级 `END_CALL`；
- 终态屏障、旧 token/旧 fencing 影响 0 行；
- Effect 首次登记授权、独立 processing token、接管和恢复；
- 创建—销毁保护截止、`START_UNCERTAIN` 三分支和 attention 停放；
- Worker 容量与 SIP Line Reservation 的原子占用、转换和释放；
- 双 Dispatcher/Recovery 竞争、锁后数据库时间和提交响应丢失；
- Provider Stub 下的 DB-only 逻辑终态与资源清理闭环。

这些能力直接保护真实通话免于双执行、线路超卖、旧 Owner 迟到写入和未知资源泄漏，
不属于本次删除或简化范围。

### 3.2 不再作为 16.2B 关闭门槛

以下表的完整锁序仍保留在总设计中，但仅在对应业务切片实际触及时验证：

| 业务事实 | 实际验收切片 | 16.2B 要求 |
| --- | --- | --- |
| Task、Target、Attempt | 16.4 Outbound | 不创建、不修改、不做业务并发验收 |
| Handoff、Agent Presence | 16.5 转人工与媒体生命周期 | 不创建、不修改、不做业务并发验收 |
| Recording、ASR、Semantic、Follow-up | 16.6 离线链路 | 不创建、不修改、不做业务并发验收 |

16.2B Repository 若事务没有触及这些行，不得为了“证明完整锁序”创建假业务行或测试
替身。后续切片一旦触及，仍必须遵守总设计的 `Record -> Line -> Worker -> Handoff ->
Presence -> Command -> Reservation -> Effect` 全局顺序，不允许另行定义反序。

因此，旧 16.2B 规格中 Handoff/Presence 快照只作为未来检查清单，不再产生 16.2B
阻断项；Attempt/Task/Target 同理。

## 4. 移除 Preview 控制面入口

### 4.1 产品边界

音色管理“试听”改为轻量音频生成和普通浏览器播放：不创建 Room、Call Record、
Runtime Owner、Command、Effect 或 Reservation。完整 Realtime + LiveKit 对话验证属于
独立 AI 通话测试。

### 4.2 Runtime 边界

`AI_CALL_OWNER_COMMAND_V1_ENTRIES` 的有效值收口为：

- `web`
- `direct_sip`
- `outbound`

`preview` 不再是合法配置值，也不参与后续正式灰度。实现计划必须删除或拒绝以下残留
路径：

- `OwnerCommandEntry.PREVIEW`；
- Runtime Token/Readiness 中的 `preview` 特判；
- 音色试听控制器切换到 `START_CALL` 的分支；
- Preview 相关 Record/Command/Owner 测试期望。

删除前先用调用方扫描证明这些分支只服务音色管理旧试听；AI 通话测试继续使用 `web`
入口，不得改名或映射为 `preview`。

## 5. 16.2C 决策

### 5.1 当前不实施

当前仓库没有独立 16.2C 实施计划。本规格明确：16.2C 不再阻塞后续 `web` 垂直切片，
也不因总设计已预留字段而自动进入实施。

当前继续使用：

- `END_CALL` 500 毫秒数据库扫描；
- 普通命令 1 秒数据库扫描；
- 数据库 CAS 授予执行权；
- PostgreSQL 中的 Command/Owner/Effect 作为唯一事实。

### 5.2 重新启动加速设计的门槛

先在 `web` Provider Stub 垂直切片测量“Command 提交到进入 `PROCESSING`”延迟。
满足以下条件时继续 DB-only，不实施 Redis 路由：

- P95 不超过 1 秒；
- 扫描批次可在下一周期前完成；
- 测试期间没有连接池饱和或持续扫描积压。

只有任一条件不满足并保留可复现实测证据，才允许重新启动通知加速设计。

### 5.3 加速方案顺序

未来若需要加速，按以下顺序重新评审：

1. **简单唤醒，优先**：PostgreSQL `LISTEN/NOTIFY` 或 Redis Pub/Sub 只发送
   `call_id/command_id` 唤醒信号；消息允许丢失，数据库扫描兜底，不增加 Command 状态。
2. **Redis Streams，最后选择**：只有简单唤醒在目标吞吐下仍不能满足延迟或背压要求，
   才考虑 `DISPATCHING/PUBLISHED`、Consumer Group、Pending、`XAUTOCLAIM` 和 Janitor。

Redis Streams 方案必须另写独立规格和计划，并用测量结果说明为什么简单唤醒不足；不能
仅因总设计中已有字段和状态就直接实施。

现有 nullable `dispatch_*`、`published_at`、`stream_message_id`、`stream_cleanup_*`
字段和 `DISPATCHING` 枚举暂时保持休眠，避免现在创建破坏性回滚迁移。DB-only 和下一
`web` 切片不得写入或依赖这些字段。后续决定永久放弃 Streams 时再单独清理 Schema。

## 6. 下一实施切片：Web DB-only 垂直入口

下一计划只接入一个业务入口：`web`。计划允许先删除或拒绝旧的 `preview` Runtime
分支作为范围清理，但不得实现新的 Preview Runtime 行为。

目标数据流：

```text
已认证 Web 创建请求
  -> 同事务创建 Record + START_CALL
  -> DB-only Dispatcher 分配 Owner
  -> DB-only Runtime 使用 Provider Stub 执行
  -> bootstrap/command query 返回持久状态
  -> END_CALL 建立终态屏障并完成 Stub 清理
```

范围内：

- 删除 `OwnerCommandEntry.PREVIEW` 和仅服务旧音色试听的 Runtime 分支，更新对应配置与
  测试，使 `preview` 明确被拒绝；
- 现有 Web 创建入口适配为持久 `START_CALL`；
- 认证租户、幂等键、请求指纹和 `202` 受理语义；
- bootstrap/command query 的等待、成功、失败和结束状态；
- API、Dispatcher、Runtime 三角色进程隔离；
- 两个独立 Runtime/Dispatcher 的 PostgreSQL + Provider Stub 集成测试；
- 证明不创建 Preview、SIP、Outbound、Handoff 或 Redis 事实。

范围外：

- LiveKit Room、Token、麦克风和真实 Qwen；
- 音色管理试听；
- Direct SIP、Outbound、Linphone、真实号码；
- Handoff/Presence、录音、ASR、语义和跟进；
- Redis、SSE 和前端完整实时对话改造。

本切片完成只证明 Web 入口已经接到可恢复的 DB-only 控制面，不证明浏览器实时音频或
正式通话完成。真实 Provider/LiveKit 接入必须另立后续切片和验收门禁。

## 7. 验收与停止规则

范围修正完成后：

1. 16.2A/B 的既有测试结果继续作为 DB-Core 证据，不重复开启全规格对抗审查。
2. Preview 相关失败只能阻止 Preview 轻量化改造，不能阻止 16.2A/B 冻结。
3. Handoff/Presence 只能在 16.5 成为阻断项，Task/Target/Attempt 只能在 16.4 成为
   阻断项。
4. Redis Streams/Janitor 未实现不构成 16.2A/B 或 Web DB-only 垂直切片缺陷。
5. 下一实施计划若新增或保留 Preview Runtime 行为，或者出现 Redis、真实 Provider、
   SIP 或未来业务表修改，必须停止并重新收窄；仅删除/拒绝旧 Preview 分支属于允许的
   范围清理。

## 8. 非目标

本规格不执行代码删除、不改数据库迁移、不清理现有脏改动、不启动业务服务，也不连接
Redis、LiveKit、SIP、Egress、Linphone 或真实 Provider。
