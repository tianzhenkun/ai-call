# Owner Runtime 对话持久化实施计划

> 依据：`docs/superpowers/specs/2026-08-02-ai-call-owner-runtime-dialogue-persistence-design.md`

## 目标

在不阻塞实时音频、不引入 Redis、也不连接真实 LiveKit/SIP/Provider 的前提下，
让 `owner_command_v1` 的完成句通过当前 Owner/fencing 校验后幂等写入 PostgreSQL，
并以 `complete / uncertain` 明确收口对话完整性。

## Task 1：数据合同

- 新增 PostgreSQL migration：Record 完整性状态、Dialogue `tenant_id`、历史回填门禁、
  租户化唯一约束和索引。
- 同步 SQLAlchemy model。
- 先写元数据与 migration 合同失败测试，再实现并运行相关 model 测试。
- 验证：Ruff、`git diff --check`，独立提交。

## Task 2：Owner-aware fenced repository

- 新建 Owner Runtime 专用 Dialogue repository，不复用无 fencing 的 legacy 写入口。
- 同一事务锁 Record，校验 mode、tenant、Owner、fencing、数据库租约和 `pending` 后批量
  upsert；旧 Owner 必须影响 0 行。
- 提供最大段号初始化、`complete / uncertain` CAS 与 Recovery uncertain 接管。
- 先写 repository 单元失败测试和隔离 PostgreSQL 竞争测试，再实现。
- 验证：单元、隔离 PostgreSQL、Ruff、`git diff --check`，独立提交。

## Task 3：非阻塞 Dialogue Bridge

- 新建 Runtime 专用 bridge，复用 `AiCallDialogueRuntimeStore` 的完成句、打断和未播放
  过滤。
- 每个 call 绑定不可变 Owner 上下文；来源键加入 fencing generation；监听器只
  `put_nowait`。
- 实现有界 drain、重试耗尽/队列满标记，正常 drain 才允许 `complete`。
- 先写事件、重放、队列满、drain 和 Stub 不启动 bridge 的失败测试，再实现。
- 验证：相关对话和 Runtime 单元测试、Ruff、`git diff --check`，独立提交。

## Task 4：Runtime 生命周期与 cleanup gate

- 在 LiveKit Runtime Provider 生命周期接入 bridge；Agent 启动前初始化，停止后
  finalize + bounded drain。
- Runtime 异常退出或 Recovery 接管把无法恢复的 `pending` 收口为 `uncertain`。
- `mark_cleanup_clean` 拒绝 `pending`，允许 `complete / uncertain`。
- `livekit_provider.py` 当前有既有脏改动：接线前必须保持其修改来源独立，不能混入
  本切片提交。
- 先写 lifecycle/cleanup 失败测试，再实现。

## Task 5：验证与交付

- 隔离 PostgreSQL 验证跨租户、旧 fencing、重放、接管续号、完整性终态和双 Runtime。
- 重跑 Runtime lifecycle、Owner、END_CALL/cleanup、对话回归。
- 运行 Ruff、`git diff --check`、CodeGraph sync。
- 不拨打真实电话；真实 Linphone 对话验收另行明确确认。
