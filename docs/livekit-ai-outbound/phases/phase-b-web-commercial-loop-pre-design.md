# Phase B：Web 商用闭环预设计

最后更新：2026-06-15

## 1. 文档定位

本文档是 Phase B 的预设计，不是正式实施设计。

它用于提前明确 Phase B 的方向、范围和进入条件。Phase A 完成后，必须基于真实代码、测试数据和验收结果更新本文档，再形成 Phase B 正式技术设计。

当前 Phase A 实现已收尾，验收报告见 [phase-a-acceptance-report.md](phase-a-acceptance-report.md)。本文档已作为 Phase B1 正式设计的输入，B1 后续实现以 [phase-b1-record-query-design.md](phase-b1-record-query-design.md) 为准。

## 2. 阶段定位

Phase B 的目标是在继续使用 Web 入口的前提下，把 Phase A 已验证的端到端实时通话链路补成最小商用闭环。

Phase B 不改变实时主链路：

```text
Web 入口
  -> Call Session
  -> LiveKit Room
  -> Realtime Call Agent
  -> Qwen Omni Realtime
```

Phase B 要补齐的是通话后的可管理、可复盘、可追责能力，而不是重新设计实时通话链路。

## 3. 初步范围

Phase B 优先做这些能力：

1. 通话记录持久化。
2. 关键事件持久化。
3. 通话结果查询。
4. 录音方案定稿，并在条件成熟时接入最小录音闭环。
5. 最小转人工状态或接管请求，不做完整坐席系统。

Phase B 不做：

1. 真实 SIP 外呼。
2. 批量外呼。
3. 完整运营后台。
4. 完整坐席工作台。
5. 复杂质检、摘要、评分和业务分析。
6. 多租户、权限、计费、配额等完整平台能力。
7. 并发压测和容量结论。
8. 模型切换、模型路由、音色管理和后端音色白名单。

## 4. 能力拆分

Phase B 不应一次性实现所有重能力。建议按以下顺序切分：

| 子阶段 | 目标 | 说明 |
|---|---|---|
| B1：记录与查询 | 通话记录和关键事件可持久化查询 | Phase B 最小必做 |
| B2：录音闭环 | 录音文件可生成、索引、查询 | 依赖 LiveKit Egress 方案定稿 |
| B3：最小转人工 | 支持转人工请求状态和基础接管事件 | 不做完整坐席系统 |

如果 Phase A 结果显示实时链路仍不稳定，只允许进入 B1；B2/B3 应延后。

## 5. Phase A 完成后必须定稿

进入 Phase B 正式设计前，必须基于 Phase A 结果定稿：

1. `call_id` 生成规则和生命周期。
2. 会话状态枚举和最终状态定义。
3. 事件类型、事件字段和事件顺序。
4. 延迟指标口径，尤其是 `browser_first_audio_ms` 是否稳定可采集。
5. Room、Agent、Qwen WebSocket 的释放规则。
6. 固定模型、音色、VAD 和浏览器音频约束的运行配置边界。
7. Web 会话创建 API 是否需要调整。
8. 事件存储从内存或 JSONL 迁移到数据库的字段映射。
9. 录音是否使用 LiveKit Egress，录音文件如何进入 `sys_oss` 或 MinIO。
10. 最小转人工在 Phase B 是否只做状态事件，还是允许人工 WebRTC 加入 Room。
11. Phase A 暴露出的主要失败类型和错误码。
12. Phase B 的自动化测试范围和手工验收清单。

这些内容没有定稿前，不应直接实现 Phase B。

## 6. 初步模块方向

可能新增或扩展的模块：

```text
app/api/v1/ai_call/
  controller.py
  schema.py
  service.py

app/services/ai_call/
  call_record_service.py
  call_event_service.py
  call_result_service.py
  recording_service.py
  handoff_service.py
```

这里只定义方向，不冻结文件名、表名和字段。正式命名以 Phase A 实现后的代码结构为准。

## 7. 初步数据对象

Phase B 大概率需要以下对象：

| 对象 | 用途 |
|---|---|
| Call Record | 保存每通会话的开始、结束、状态、入口、Room 等摘要 |
| Call Event | 保存关键运行事件 |
| Recording File | 保存录音文件索引，不在数据库保存音频内容 |
| Handoff Request | 保存转人工请求和处理状态 |

注意：这里不是表结构定稿。B1 记录与查询的正式表设计已收敛到 [phase-b1-record-query-design.md](phase-b1-record-query-design.md)，并已明确不创建生效配置表和指标表。

## 8. 初步验收标准

Phase B 完成时至少应满足：

1. 每通 Web 会话结束后，可以按 `call_id` 查询通话摘要。
2. 可以查询关键事件时间线。
3. 如需展示简单耗时，优先由事件时间线临时计算。
4. 失败会话能看到失败原因和最后事件。
5. 如果接入录音，录音文件可以通过索引查询和播放。
6. 如果接入最小转人工，转人工请求、接管、失败或取消必须有状态记录。

## 9. 进入 Phase B 的条件

进入 Phase B 正式设计前，应对照以下条件：

| 条件 | 当前状态 |
|---|---|
| Phase A 核心链路已跑通 | 已满足 |
| Phase A 延迟、打断、误打断和断连验收已有记录 | 部分满足，详见 Phase A 验收报告 |
| Phase A 的状态、事件和指标没有明显缺口 | 基本满足，浏览器侧首包需复测补证 |
| Room、Agent、模型连接释放规则已验证 | 基本满足 |
| 总纲已更新 Phase A 收尾状态 | 已满足 |

## 10. 当前判断

当前已进入 Phase B1 正式技术设计评审。B1 可以先围绕记录与查询设计，不建议直接进入完整 Phase B 实现，也不建议在 B1 阶段抢跑录音和转人工。

Phase A 补证项不阻塞 B1 设计，但会影响 B1 实现后的验收结论。录音和转人工仍应在 B2/B3 单独定稿。
