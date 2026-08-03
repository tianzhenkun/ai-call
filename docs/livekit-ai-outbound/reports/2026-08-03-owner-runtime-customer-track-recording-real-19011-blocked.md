# Owner Runtime 客户分轨 19011 真实验收阻断记录

日期：2026-08-03

## 结论

本轮结论为 **BLOCKED（未拨号）**。

在 ed81 的 19011 环境提交唯一一次白名单外呼前，`START_CALL` 事务因测试租户标识超过
`ai_call_record.tenant_id varchar(20)` 回滚。数据库没有留下 Command、Record 或 Effect，
LiveKit、SIP、Egress 与 FreeSWITCH 日志也没有目标 `call_id` 或号码，因此不能声明真实
SIP、媒体、OSS、主录音或客户分轨已验收。

## 范围与基线

- worktree：`/Users/liuhongli/.codex/worktrees/ed81/ai-call`
- branch：`codex/ai-call-workflow-split`
- 起始 HEAD：`634b4520331ef5b55ea160adea5aba8f364b7d38`
- 环境：现有 19011 LiveKit/SIP/Egress/Redis 与 FreeSWITCH
- 白名单：仅 `199****1001`
- 另一个 cf0a 测试进程未停止、未清理、未纳入验收
- `.playwright-cli/` 与
  `env/.env.dev.bak-before-local-outbound-20260727` 保持不变

## 预检证据

- 19011 API、LiveKit、SIP、Egress 与 FreeSWITCH 均可用；
- Linphone `1000@192.168.0.111` 为 `Registered(UDP-NAT)`，Ping 为 `Reachable`；
- Owner Runtime 使用 PostgreSQL、`livekit` Provider、`direct_sip` Owner entry；
- 真实 Provider、Linphone 测试门禁和单号码 allowlist 均已显式启用；
- 提交前目标幂等键、租户 Record 均为 `0`。

## 唯一提交与阻断

第一次执行在导入 `RuntimeCommandRepository` 时触发现有循环导入，尚未访问数据库。
限定修复回合复用应用正常启动的导入顺序后，进入 `START_CALL` 事务，但 INSERT 失败：

```text
asyncpg.exceptions.StringDataRightTruncationError:
value too long for type character varying(20)
```

失败字段为测试租户标识 `tenant-owner-real-19011`。事务回滚后现场查询：

```text
command=0
record=0
effect=0
```

目标 `call_id` 在 LiveKit Server、LiveKit SIP、LiveKit Egress 和 FreeSWITCH 的近时段日志中
均无匹配。一次修复回合已用完，按执行合同没有缩短租户标识再次提交，也没有重拨。

## 清理与下一进入条件

- ed81 19011 临时 API 已优雅关闭；
- 本轮临时 PostgreSQL 容器已停止并删除；
- 现有 19011 LiveKit/SIP/Egress/Redis 与 FreeSWITCH 保持运行；
- 另一个测试环境保持不变。

下一轮只需使用长度不超过 20 的独立测试租户标识，重新做一次 no-dial preflight，获得用户
对同一白名单号码的新一次明确授权后，再提交一次新幂等键的真实外呼。
