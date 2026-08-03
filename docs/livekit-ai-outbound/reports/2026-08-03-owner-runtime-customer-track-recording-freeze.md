# Owner Runtime 客户分轨录音冻结证据

日期：2026-08-03

## 结论

本轮冻结结论为 **NO-GO**。

客户分轨实现、Owner 浏览器事件路由和本地 unit 证据均已落地；但当前环境未提供
`AI_CALL_TEST_POSTGRES_DSN`，因此任务 7 要求的隔离 PostgreSQL 回归没有运行。本报告不得
替代 PostgreSQL 16 下的 migration、claim CAS、fencing、原子投影、END graph 和跨租户
验证，也不声明真实 LiveKit、SIP、OSS 或 Provider 已通过。

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
- 隔离 PostgreSQL 回归：未运行，`AI_CALL_TEST_POSTGRES_DSN` 缺失。

## CTR-01 至 CTR-14 证据映射

| 合同 | 当前代码/测试证据 | 本轮结论 |
| --- | --- | --- |
| CTR-01 | `test_recording_capability_registers_main_and_customer_track_only`、`test_db_only_stubs_never_enable_recording_effects` | unit 已覆盖 |
| CTR-02 | `test_recording_capability_registers_main_and_customer_track_only` | unit 已覆盖 main + customer、无 AI/human |
| CTR-03 | `test_track_claim_requires_answered_and_matching_identity` | PostgreSQL 测试存在，本轮未运行 |
| CTR-04 | `test_owner_browser_ready_recording_writes_ready_without_legacy_track_start`、SIP connected fencing 测试 | Web unit 已覆盖；PostgreSQL SIP 门禁本轮未运行 |
| CTR-05 | tenant model/repository、稳定键、重复投影测试 | unit 已覆盖；PostgreSQL 并发本轮未运行 |
| CTR-06 | `test_livekit_provider_customer_track_timeout_recovery_never_restarts` | unit 已覆盖 reconcile-only |
| CTR-07 | Track projector 单调映射测试、`test_customer_track_effect_and_projection_commit_atomically` | unit 已覆盖 projector；PostgreSQL 原子提交本轮未运行 |
| CTR-08 | Effect 类型/END 映射 unit、`test_terminal_graph_handles_track_start_by_claim_state` | PostgreSQL 竞争本轮未运行 |
| CTR-09 | `test_end_graph_delete_room_depends_on_main_and_customer_stops` | PostgreSQL graph 本轮未运行 |
| CTR-10 | `test_auxiliary_customer_track_failure_does_not_block_start_readiness` | unit 已覆盖 |
| CTR-11 | Stop terminal Provider 测试与 Track reconcile 测试 | unit 已覆盖 stop/OSS 分离 |
| CTR-12 | tenant CRUD、错误 claim token、offline ASR fail-closed 测试 | unit 已覆盖；PostgreSQL 跨租户 CAS 本轮未运行 |
| CTR-13 | Owner browser ready 路由与 legacy spy 测试 | unit 已覆盖 |
| CTR-14 | Track terminal 单调性、缺 source fail-closed 和可见错误摘要测试 | unit 已覆盖 |

## 外部边界与后续进入条件

本轮没有启动业务服务，没有连接真实 LiveKit、SIP、OSS 或 Provider，没有拨号，也没有修改
或清理受保护文件。

解除 NO-GO 只需要一个后续动作：提供明确隔离的 PostgreSQL 16 测试 DSN，运行计划任务 6
列出的 PostgreSQL 套件并记录实际通过、失败和跳过数量。人工转接套件顺序/隔离问题登记为
独立后续项，不阻断客户分轨合同修复。
