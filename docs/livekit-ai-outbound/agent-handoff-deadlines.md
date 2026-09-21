# 坐席转人工期限规则

## 生效规则

坐席工作台按当前阶段计时，服务端是期限的唯一权威。

| 阶段 | 截止字段 | 默认时长 | 到期行为 |
| --- | --- | --- | --- |
| 排队等待认领 `requested` | `expires_at` | 从请求发起计 60 秒 | 请求过期，进入未接通收尾 |
| 已认领、媒体接入中 `accepted` | `claim_expires_at` | 从认领成功计 15 秒 | 释放坐席；排队期限未到则重新排队，否则过期收尾 |
| 已接通后重连 `reconnecting` | `reconnect_expires_at` | 15 秒 | 重连失败，进入话后处理，不返回公共队列 |

- 在排队截止前认领，获得完整的接入窗口，不被原排队期限截短。例如第 57 秒认领，接入截止为第 72 秒。
- 重复点击、接口重试、刷新页面不延长既有接入期限。重新排队后再次认领必须仍处于原排队期限内。
- 排队已经到期的未认领请求不能再认领；旧记录缺少 `claim_expires_at` 时沿用原 `expires_at`，不会获得无限等待时间。
- 客户通话结束优先于等待期限，尚未接通的认领取消并释放坐席。
- 浏览器、同步媒体确认、Owner 异步媒体确认、查询时的过期处理、定时器及恢复扫描遵循同一阶段期限。异步媒体查询跨过截止点也不能写入 `connected`。
- 排队期限不是已接通人工通话的最长时长，不得用于关闭正在进行的人工通话。

## 配置与兼容

- `AI_CALL_HANDOFF_TOTAL_WAIT_SECONDS` 保留既有配置名，含义为排队认领期限，默认 60 秒。
- `AI_CALL_AGENT_CLAIM_CONNECT_TIMEOUT_SECONDS` 为独立媒体接入窗口，默认 15 秒。
- `AI_CALL_AGENT_RECONNECT_GRACE_SECONDS` 为接通后重连窗口，默认 15 秒。
- 使用已有期限字段，不新增本次规则变更所需的数据库列。不会重写已保存的截止时间。
- 本地未发布的异常收尾修复另含 `sql/handoff-exception-close-lease.sql`，后续发布仍须先执行并验证该迁移。

## 验证边界

自动化用例覆盖临近排队截止认领、重复请求不续期、媒体确认跨期拒绝、并发认领、超时及客户挂断后下一通认领，以及查询、定时器、恢复扫描的一致性。

```bash
.venv/bin/python -m pytest -q --show-capture=no --tb=short \
  tests/test_ai_call_agent_console_claim.py \
  tests/test_ai_call_runtime_handoff_repository.py \
  tests/test_ai_call_runtime_handoff_handlers.py \
  tests/test_ai_call_handoff_exception_concurrency.py
```

这些测试隔离了电话和媒体提供方，不能代替真实电话的双向音频验收。线上变更另行授权后，需验证单通、三通同时排队、临近截止认领、失败后下一通恢复；直连与 TURN/TLS 中继也需要分别验证实际选路和双向音频。
