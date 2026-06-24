# Phase B3.1：转人工异常闭环验收报告

最后更新：2026-06-24

## 1. 验收结论

Phase B3.1 已完成代码实现，并通过本地自动化验证。

2026-06-24 已补充 Web/LAN 真实通话手工验收，确认等待回铃声、30 秒超时自动结束、坐席接入后取消超时任务、客户主动挂断和坐席主动断开均可按状态闭环收口。样本与证据见 [phase-b3-handoff-live-closure-acceptance-report.md](phase-b3-handoff-live-closure-acceptance-report.md)。

## 2. 已实现范围

1. 转人工请求创建后启动运行态超时任务。
2. 默认等待超时时间为 30 秒，可通过 `AI_CALL_HANDOFF_TIMEOUT_SECONDS` 配置。
3. 转人工失败或超时时，停止等待回铃声并直接自动结束通话。
4. 默认不播放不可接入固定人声提示，也不保留额外自动结束延迟。
5. 自动结束时写入稳定结束原因：`handoff_timeout` 或 `handoff_failed`。
6. 录音结束、LiveKit Room 删除、通话记录完成、handoff 终态和事件记录按同一 `call_id` 复盘。
7. 坐席成功 connected 后取消超时任务，不再触发异常自动结束。
8. Phase A 验证页显示等待倒计时、超时时间和异常结束提示。

## 3. 未纳入范围

1. 不做人工 ASR。
2. 不做坐席排队、技能组、在线状态和真实坐席工作台。
3. 不做等待音乐循环。
4. 不用实时 TTS 播放固定失败话术。
5. 不新增 handoff 配置表。
6. 不做多实例任务恢复；该能力属于后续生产加固阶段。

## 4. 自动化验证

已执行全量测试：

```bash
pytest -q
```

结果：

```text
114 passed, 8 warnings in 12.70s
```

重点覆盖：

1. 转人工超时后直接自动结束。
2. 坐席 connected 后取消超时自动结束。
3. 转人工失败后直接自动结束。

## 5. 本地运行状态

本地服务已重启并可访问：

```text
http://127.0.0.1:19010/ai-call-api/v1/static/ai-call/customer.html
```

健康检查已通过：

```text
GET /ai-call-api/v1/ai-call/health
```

等待回铃音文件可访问：

```text
GET /ai-call-api/v1/static/ai-call/audio/handoff-ringback.wav
```

## 6. 手工验收建议与状态

以下手工验收项已在 2026-06-24 Web/LAN 样本中补证。SIP 真实线路和商用并发仍需后续独立验收。

### 6.1 超时自动结束

1. 打开 Phase A 验证页。
2. 创建会话并确认 AI 能正常说话。
3. 点击发起转人工。
4. 不点击一键坐席接入。
5. 等待 30 秒。
6. 确认通话自动结束。
7. 查询通话记录，确认 `endReason=handoff_timeout`。

### 6.2 坐席接入成功不自动结束

1. 创建会话。
2. 点击发起转人工。
3. 在 30 秒内点击一键坐席接入。
4. 确认 handoff 进入 connected/completed 路径。
5. 等待超过 30 秒，确认通话不会因为 B3.1 超时任务自动结束。

### 6.3 转人工失败自动结束

1. 创建会话。
2. 点击发起转人工。
3. 触发或调用 fail 路径。
4. 确认通话自动结束，通话记录 `endReason=handoff_failed`。

## 7. 风险与后续

1. 当前超时任务是单进程运行态任务，进程重启后的恢复不属于 B3.1 范围。
2. 真实 SIP 接入后，B3.1 的状态、事件、录音和结束原因仍可复用；只需要把媒体入口从 Web Room 换成 SIP Room。
3. 进入 Phase C 前，需要基于 Phase A/B 当前实现生成并发压测方案。
