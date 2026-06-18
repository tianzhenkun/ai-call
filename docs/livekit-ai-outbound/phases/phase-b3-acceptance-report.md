# Phase B3：最小转人工验收报告

最后更新：2026-06-17

## 1. 验收结论

Phase B3 最小转人工已完成本地实现和自动化验证，可以作为当前阶段收口基线。

B3 完成的是“最小但真实”的接管链路：

1. 可创建转人工请求并落 `ai_call_handoff`。
2. 请求转人工后，AI 停止当前输出并进入等待接管状态。
3. 优先由模型按当前音色播报固定转人工提示词。
4. 坐席可通过服务端签发的短期 LiveKit Token 加入同一个 Room。
5. 验证页提供一键坐席接入，内部完成 accept、join、connected、complete。
6. 转人工关键状态和事件可复盘。
7. 录音、事件查询和对话文本查询不因转人工链路中断。

## 2. 已实现范围

### 2.1 数据与状态

1. 新增 `ai_call_handoff` 表与 PostgreSQL DDL。
2. 支持状态：`requested`、`accepted`、`connected`、`completed`、`canceled`、`failed`、`expired`。
3. `id` 等 bigint 字段对前端按字符串返回。
4. 不使用 `jsonb`。
5. 不创建物理外键。
6. 不引入 `tenant_id`、审计字段或坐席域模型。

### 2.2 接口

1. 创建转人工请求。
2. 查询当前转人工请求。
3. 查询通话转人工记录。
4. 坐席接管并签发 LiveKit Token。
5. 标记坐席已连接。
6. 完成、取消、失败转人工请求。
7. 非分页接口保持经典 `code/msg/data` 三段式。

### 2.3 实时链路

1. 转人工请求成功后，会话先切到等待态，阻断模型缓存音频继续发布。
2. 清空待播放队列并取消当前模型输出。
3. 不再请求模型播报转人工提示。
4. 停止 AI Agent，不再生成新的 AI 回复。
5. 等待坐席接入期间播放固定回铃声。
6. 坐席以 `human-agent-{handoff_id}` 身份加入原 LiveKit Room。

### 2.4 前端验证页

1. 新增“转人工闭环”区域。
2. 保留三个核心操作：`发起转人工`、`一键坐席接入`、`取消接管`。
3. 不再暴露多个技术步骤按钮。
4. 对话气泡保留 `human_agent` 展示能力。

## 3. 自动化验证

已执行：

```bash
python3 -m py_compile app/services/ai_call/agent_runner.py app/services/ai_call/orchestrator.py app/config/setting.py app/services/ai_call/record_service.py
node --check static/ai-call/customer.js
pytest -q
```

验证结果：

```text
111 passed
```

重点覆盖：

1. 转人工请求创建和幂等。
2. 状态流转：accept、connected、complete、cancel、fail、expired。
3. 结束通话时活动 handoff 收敛。
4. 坐席 Token 返回。
5. `handoff_requested`、`handoff_accepted`、`handoff_connected`、`handoff_completed`、`handoff_expired` 等事件记录。
6. Agent 挂起前优先由模型播报转人工提示，不再播放固定人声兜底音频。
7. B1/B2/B2.5 查询能力不被破坏。

## 4. 本地运行态验证

本地服务已验证：

1. API 健康检查可用。
2. Phase A 静态验证页可访问。
3. 等待回铃音可通过静态资源路径访问。
4. 验证页已刷新为收口后的按钮形态。

访问入口：

```text
http://127.0.0.1:19010/ai-call-api/v1/static/ai-call/customer.html
```

## 5. 不属于 B3 的范围

以下能力不在 B3 收口范围内：

1. 转人工失败或超时后的失败提示音。
2. 转人工失败或超时后的自动挂断。
3. AI 自动恢复对话。
4. 人工坐席实时 ASR。
5. 坐席登录、在线状态、排队、技能组、分配算法。
6. 完整坐席工作台。
7. 真实 SIP 线路联调。

这些能力如需继续推进，应进入 B3.1 或后续独立阶段，不应继续塞回 B3。

## 6. 当前已知边界

1. 当前默认只使用等待回铃声：`handoff-ringback.wav`。
2. 转人工失败或超时只记录状态和事件，不自动播放失败音频，也不自动挂断。
3. `expired` 当前采用懒过期策略，在查询、接管或状态变更时收敛，不新增后台调度。
4. 单机双标签自测会受到物理麦克风、回声和扬声器回采影响，不能等价于真实双人坐席体验。
5. 真实 SIP 场景理论上复用同 Room 接管模型，但仍需 Phase E 单独联调验证。

## 7. 后续建议

下一步如继续增强转人工，建议进入 B3.1，范围只聚焦异常闭环：

1. 转人工失败或超时提示策略。
2. 失败或超时后的自动结束策略。
3. 结束原因和事件复盘。

B3.1 仍不建议加入人工 ASR、坐席排队、登录态工作台或真实 SIP 联调。
