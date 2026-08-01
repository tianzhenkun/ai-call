# AI Call Web DB-only 延迟测量报告

## 结论

本轮 20 条 Web `START_CALL` 批量测量未达到 `p95_ms <= 1000` 的停止规则：P95 为 `2495.995 ms`。本切片保持 PostgreSQL DB-only 实现，不增加 Redis Streams 或其他加速设施；下一切片如需降低唤醒延迟，只允许先独立评审简单唤醒方案。

一致性门禁通过：全部命令最终处理完成，扫描 backlog 为 0，Worker 未达到容量上限，且没有写入任何 dispatch/stream 字段。

## 测量环境与命令

- 日期：2026-08-01
- PostgreSQL：16.14（`server_version_num=160014`）
- 事务隔离级别：`read committed`
- 样本数：20
- Provider：`DeterministicWebProviderStub`，仅内存确定性结果，不访问网络或 SDK
- 推进方式：真实 `RuntimeEntryStartService`、`DispatcherControlService.run_once()` 和 `RuntimeControlService.run_once()`
- 时间来源：PostgreSQL `ai_call_runtime_command.created_at` 与首次 `claimed_at`

复现命令：

```bash
./tools/run_ai_call_runtime_postgres_tests.sh \
  tests/postgres/test_ai_call_runtime_control_postgres.py \
  -k 'web_db_only_latency' -q -s
```

## 结果

| 指标 | 数值 |
| --- | ---: |
| `sample_count` | 20 |
| `p50_ms` | 1600.125 |
| `p95_ms` | 2495.995 |
| `max_ms` | 2636.141 |
| `scan_backlog_remaining` | 0 |
| `worker_active_count` | 20 |
| `worker_capacity` | 64 |
| `dispatch_or_stream_fields_written` | 0 |

原始摘要：

```text
WEB_DB_ONLY_LATENCY {"dispatch_or_stream_fields_written": 0, "isolation_level": "read committed", "max_ms": 2636.141, "p50_ms": 1600.125, "p95_ms": 2495.995, "postgres_server_version_num": 160014, "sample_count": 20, "scan_backlog_remaining": 0, "worker_active_count": 20, "worker_capacity": 64}
```

## 判定与边界

- P95 超过 1000 ms，因此不能把当前批量路径描述为达到目标延迟。
- backlog 已清零且 Worker 使用量为 20/64，没有持续积压或容量池饱和证据。
- 本测量是 20 条命令在同一轮 Runtime 扫描中依次 claim/执行的批量结果，包含前序样本处理对后续样本 `claimed_at` 的排队影响。
- 测量没有连接 Redis、LiveKit、SIP、Egress、Linphone 或真实 Provider，没有启动业务服务，也没有拨号。
- 本报告只证明 DB-only Stub 下的调度与处理延迟，不证明浏览器实时语音、真实媒体或正式外呼可用。
