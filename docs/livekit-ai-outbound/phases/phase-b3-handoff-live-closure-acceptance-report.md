# Phase B3/B3.1/B3.2：转人工真实通话闭环验收记录

最后更新：2026-06-24

## 1. 验收结论

当前 Web/LAN 验证链路下，转人工自动触发、无人接听超时、坐席接入、客户主动挂断、坐席主动断开和坐席状态释放已完成阶段验收。

本轮结论只覆盖本地浏览器 Web/LAN 链路、当前 SQLite 样本和 `intro_geo` 场景，不等同于 SIP 真实线路、商用并发、弱网、完整坐席工作台或人工 ASR 已通过。

本轮已确认：

1. 用户说“转人工”后，可自动进入既有 handoff 流程。
2. 无坐席接听时，handoff 进入 `expired`，通话以 `handoff_timeout` 收口。
3. 坐席点击“接入”后，后端可收到 `handoff_connected`，等待音停止，超时任务取消。
4. 坐席接入后，客户页主动挂断时，handoff 以 `web_user_end` 正常完成。
5. 坐席接入后，坐席页点击“断开坐席”时，handoff 和通话以 `agent_completed` 正常完成。
6. 坐席断开后，`ai_call_handoff_agent` 释放为 `online`，`active_handoff_id` 为空。
7. `handoff_connected` 之后未观察到新的 AI/agent/provider/system 语音响应事件，AI 已让出话语权。

## 2. 验收范围

本轮覆盖：

1. Phase B3 最小转人工状态闭环。
2. Phase B3.1 异常闭环：无人接听超时、等待音停止、自动结束。
3. Phase B3.2 自动触发：用户明确要求“转人工”后自动创建 handoff。
4. 坐席页 `accept -> LiveKit join -> connected -> complete` 链路。
5. 客户页主动挂断和坐席页主动断开两类结束路径。

本轮不覆盖：

1. SIP 真实线路转人工。
2. 多坐席排队、分配、技能组和登录态工作台。
3. 人工坐席实时 ASR 与人工通话文本落库。
4. 商用并发压测、弱网、外放/耳机差异。
5. AI 角色边界、上下文治理和业务语义准确率。

## 3. 当前运行态

| 项 | 值 |
| --- | --- |
| 客户页 | `http://192.168.0.106:19011/static/ai-call/customer.html` |
| 坐席页 | `http://192.168.0.106:19011/static/ai-call/agent.html` |
| 本地库 | `/private/tmp/ai_call_ed81_local.db` |
| 场景 | `intro_geo` |
| 坐席 | `agent-debug-001` |
| Handoff timeout | 30 秒 |
| LiveKit | 本地 LAN WebRTC |

说明：以下时间为 SQLite 落库时间，用于事件排序和状态复盘。

## 4. 真实通话样本

### 4.1 无坐席接听超时

| 项 | 值 |
| --- | --- |
| call_id | `call_328133725977083904` |
| handoff_id | `handoff_328133811817709568` |
| call end_reason | `handoff_timeout` |
| handoff status | `expired` |
| handoff end_reason | `timeout` |
| requested_at | `2026-06-24 11:26:37.168153` |
| accepted_at | 空 |
| connected_at | 空 |
| ended_at | `2026-06-24 11:27:07.178663` |

关键事件：

1. `handoff_requested`
2. `handoff_prompt_started`
3. `handoff_waiting_tone_started`
4. `handoff_expired`
5. `handoff_waiting_tone_stopped`
6. `handoff_unavailable_prompt_started`
7. `handoff_auto_ended`

判定：无人接听路径通过，超时后自动结束通话，结束原因为 `handoff_timeout`。

### 4.2 接入失败缺陷样本与修复

| 项 | 值 |
| --- | --- |
| call_id | `call_328139233538637824` |
| handoff_id | `handoff_328139297162035200` |
| call end_reason | `handoff_timeout` |
| handoff status | `expired` |
| accepted_at | `2026-06-24 11:48:35.510227` |
| connected_at | 空 |
| ended_at | `2026-06-24 11:48:54.987177` |

现象：

1. 坐席页点击“接入”后，后端 `/accept` 返回成功。
2. LiveKit 侧可看到 `human-agent-handoff_328139297162035200` 加入并发布麦克风音轨。
3. 坐席端很快主动离开，后端未收到 `/connected`，也未收到 `/fail`。
4. 最终 handoff 超时，通话以 `handoff_timeout` 结束。

根因收敛：

1. 坐席页接入链路过度依赖全局 `state.selectedHandoff`。
2. accept 后，如果轮询或状态刷新把 `selectedHandoff` 清空，后续 `markConnected()` 或 `failAcceptedHandoff()` 无法稳定拿到 `handoffId`。
3. 页面会展示“接入失败”，但后端缺少 connected/fail 证据。

修复：

1. 坐席页点击接入时固定 `requestedHandoff` 和 `acceptedHandoff` 快照。
2. `markConnected()` 和 `failAcceptedHandoff()` 改为优先使用快照里的 `handoffId`。
3. 连接失败时清理本地 room/track/token 状态，并刷新坐席和可接入列表。
4. `agent.html` bump `agent.js` 版本号，避免浏览器继续使用旧脚本。
5. 新增前端回归测试，模拟 `publishTrack` 后 `selectedHandoff` 被清空，要求仍然调用 `/connected` 和 `/fail`。

### 4.3 坐席接入后客户主动挂断

| 项 | 值 |
| --- | --- |
| call_id | `call_328144111644577792` |
| handoff_id | `handoff_328144249817534464` |
| call end_reason | `web_user_end` |
| handoff status | `completed` |
| handoff end_reason | `web_user_end` |
| accepted_at | `2026-06-24 12:08:15.683508` |
| connected_at | `2026-06-24 12:08:16.409224` |
| ended_at | `2026-06-24 12:09:12.117131` |

关键事件：

1. `handoff_requested`
2. `handoff_accepted`
3. `handoff_connected`
4. `handoff_timeout_task_canceled`
5. `handoff_waiting_tone_stopped`
6. `session_completed`
7. `handoff_completed`

判定：客户页主动挂断路径通过，handoff 和 call 均按 `web_user_end` 正常收口。

### 4.4 坐席接入后坐席主动断开

| 项 | 值 |
| --- | --- |
| call_id | `call_328145029572202496` |
| handoff_id | `handoff_328145132542365696` |
| call end_reason | `agent_completed` |
| handoff status | `completed` |
| handoff end_reason | `agent_completed` |
| accepted_at | `2026-06-24 12:11:47.014592` |
| connected_at | `2026-06-24 12:11:47.630103` |
| ended_at | `2026-06-24 12:12:36.162025` |

关键事件：

1. `handoff_requested`
2. `handoff_accepted`
3. `handoff_connected`
4. `handoff_timeout_task_canceled`
5. `handoff_waiting_tone_stopped`
6. `handoff_completed`
7. `session_completed`

补充确认：

1. `ai_call_handoff_agent` 最新状态为 `online`，`active_handoff_id` 为空。
2. `handoff_connected` 后，agent/provider/system 侧无新的 AI 语音响应事件。
3. 对话文本停在转人工前的 AI/客户段，未出现人工接入后 AI 继续抢话。

判定：坐席主动断开路径通过，坐席释放和通话结束原因均正确。

## 5. 自动化验证

本轮相关验证已执行：

```bash
node --check static/ai-call/agent.js
uv run pytest tests/test_ai_call_phase_a_core.py::test_agent_join_failure_clears_local_room_state_and_refreshes_lists -q
uv run pytest tests/test_ai_call_phase_a_core.py -q
uv run ruff check tests/test_ai_call_phase_a_core.py
uv run pytest tests/test_ai_call_phase_b1_records.py::test_handoff_timeout_closes_room_by_name_when_runtime_session_is_missing -q
uv run pytest tests/test_ai_call_phase_b1_records.py -q -x --tb=short
uv run pytest tests/test_ai_call_phase_b4_prompt_config.py::test_prompt_config_page_and_customer_page_use_business_fields -q
uv run pytest tests/test_ai_call_phase_a_core.py tests/test_ai_call_phase_b1_records.py tests/test_ai_call_phase_b4_prompt_config.py -q
uv run ruff check app/services/ai_call/handoff_exception_manager.py app/services/ai_call/orchestrator.py tests/test_ai_call_phase_a_core.py tests/test_ai_call_phase_b1_records.py tests/test_ai_call_phase_b4_prompt_config.py
git diff --check -- static/ai-call/agent.js static/ai-call/agent.html tests/test_ai_call_phase_a_core.py
git diff --check
rg -n "[[:blank:]]$" docs/livekit-ai-outbound
```

验证结果：

```text
node --check: 通过
回归单测: 1 passed, 2 warnings
Phase A 文件级测试: 124 passed, 6 warnings
Phase B1 文件级测试: 110 passed, 4 warnings
B4 页面字段单测: 1 passed, 2 warnings
Phase A/B1/B4 组合测试: 252 passed, 8 warnings
ruff: All checks passed!
git diff --check: 通过
docs 尾随空白检查: 通过
```

组合验证说明：

1. `tests/test_ai_call_phase_b1_records.py::test_handoff_timeout_closes_room_by_name_when_runtime_session_is_missing` 单独执行通过。
2. `tests/test_ai_call_phase_b1_records.py -q -x --tb=short` 整包执行通过。
3. B1 异步事件持久化仍沿用原内存 SQLite 夹具；新用例通过等待 `handoff_auto_ended` 并先关闭异常管理器，避免后台 closure task 与夹具释放抢同一连接。

## 6. 阶段判断

当前 Web/LAN 链路下，转人工闭环可以作为阶段基线：

1. 自动触发可以进入 handoff。
2. 无坐席时可以超时结束。
3. 坐席接入后可以稳定进入 connected。
4. 客户和坐席两侧都能触发正常结束。
5. AI 在人工接入后能让出话语权。

下一阶段不建议继续围绕“接入按钮”和单条状态做小补丁。更高价值的后续方向是：

1. AI 角色边界与上下文治理。
2. 业务语义和 ASR 准确率。
3. SIP 真实线路转人工样本。
4. 商用压测、弱网和多通连续样本统计。

## 7. 商用前补证

商用前仍需补齐：

1. SIP 真实线路下的转人工样本。
2. 外放、耳机和弱网条件下的客户/坐席听感对比。
3. 多通连续样本统计，包括 connected 成功率和 timeout/fail 分布。
4. 人工通话文本、质检、摘要和 CRM 回写边界。
5. 多坐席在线、忙碌、离线和并发接入冲突验证。
