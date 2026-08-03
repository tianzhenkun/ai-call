# Owner Runtime 客户分轨录音冻结证据

日期：2026-08-03

## 结论

本轮冻结结论为 **GO**。

客户分轨实现、Owner 浏览器事件路由、本地 unit 证据和隔离 PostgreSQL 16 回归均已落地。
本结论只冻结代码与隔离验证，不声明真实 LiveKit、SIP、OSS 或 Provider 已通过。

## 基线

- worktree：`/Users/liuhongli/.codex/worktrees/ed81/ai-call`
- branch：`codex/ai-call-workflow-split`
- 实现基线 HEAD：`de4c419c0a4c50c4bf1b13cae9e573a4945d1be1`
- 既有未跟踪文件保持不变：`.playwright-cli/`、
  `env/.env.dev.bak-before-local-outbound-20260727`

任务 1 至任务 6 的独立提交：

- `06c2112 feat(ai-call): tenant-scope recording tracks`
- `0ab4374 feat(ai-call): gate customer track effects`
- `65dc3cb feat(ai-call): project fenced customer tracks`
- `b220c51 feat(ai-call): execute recoverable customer track egress`
- `c77ddea feat(ai-call): close customer track lifecycle`
- `1236c7a feat(ai-call): recover customer track verification`

任务 6 后的限定修复：

- `1ab8149 fix(ai-call): reconcile completed egress stops`
- `98ae20f fix(ai-call): route owner browser readiness`
- `caece78 fix(ai-call): route owner browser disconnect`
- `de4c419 fix(ai-call): route owner browser diagnostics`

## 本轮验证

完整相关 unit 回归只运行一次：

```text
450 passed, 1 failed, 5 warnings in 37.59s
```

唯一失败为合同外的人工转接测试
`test_end_session_keeps_connected_reconnecting_handoff_in_wrap_up[asyncio]`。该用例单独定向复现：

```text
1 passed, 2 warnings in 0.31s
```

因此登记为套件顺序/隔离后续项，不在客户分轨任务中修复，也不把它表述为完整回归全绿。

其他门禁：

- `uv run ruff check .`：通过；
- `git diff --check`：通过；
- `codegraph sync`：通过；
- `codegraph status`：索引最新；
- 隔离 PostgreSQL 16 回归：`100 passed, 2 warnings in 38.28s`。

## CTR-01 至 CTR-14 证据映射

| 合同 | 当前代码/测试证据 | 本轮结论 |
| --- | --- | --- |
| CTR-01 | `test_recording_capability_registers_main_and_customer_track_only`、`test_db_only_stubs_never_enable_recording_effects` | unit 已覆盖 |
| CTR-02 | `test_recording_capability_registers_main_and_customer_track_only` | unit 已覆盖 main + customer、无 AI/human |
| CTR-03 | `test_track_claim_requires_answered_and_matching_identity` | PostgreSQL 已覆盖 |
| CTR-04 | `test_owner_browser_ready_recording_writes_ready_without_legacy_track_start`、SIP connected fencing 测试 | unit 与 PostgreSQL 已覆盖 |
| CTR-05 | tenant model/repository、稳定键、重复投影测试 | unit 与 PostgreSQL 已覆盖 |
| CTR-06 | `test_livekit_provider_customer_track_timeout_recovery_never_restarts` | unit 已覆盖 reconcile-only |
| CTR-07 | Track projector 单调映射测试、`test_customer_track_effect_and_projection_commit_atomically` | unit 与 PostgreSQL 已覆盖 |
| CTR-08 | Effect 类型/END 映射 unit、`test_terminal_graph_handles_track_start_by_claim_state` | unit 与 PostgreSQL 已覆盖 |
| CTR-09 | `test_end_graph_delete_room_depends_on_main_and_customer_stops` | PostgreSQL 已覆盖 |
| CTR-10 | `test_auxiliary_customer_track_failure_does_not_block_start_readiness` | unit 已覆盖 |
| CTR-11 | Stop terminal Provider 测试与 Track reconcile 测试 | unit 已覆盖 stop/OSS 分离 |
| CTR-12 | tenant CRUD、错误 claim token、offline ASR fail-closed 测试 | unit 与 PostgreSQL 已覆盖 |
| CTR-13 | Owner browser ready 路由与 legacy spy 测试 | unit 已覆盖 |
| CTR-14 | Track terminal 单调性、缺 source fail-closed 和可见错误摘要测试 | unit 已覆盖 |

## 外部边界与后续进入条件

本轮只启动并删除了临时隔离 PostgreSQL 16 容器；没有启动业务服务，没有连接真实 LiveKit、
SIP、OSS 或 Provider，没有拨号，也没有修改或清理受保护文件。

客户分轨代码切片可以冻结。人工转接套件顺序/隔离问题登记为独立后续项，不阻断本合同；
真实 LiveKit、SIP、OSS 和 Provider 验收继续保留为独立外部边界。
