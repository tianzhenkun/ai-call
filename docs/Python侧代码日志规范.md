# Python 侧代码日志规范

## 1. 目标

Python 侧日志的目标不是“多打印”，而是生产或联调出问题后，能通过一个稳定业务 ID 快速回答：

1. 请求是否进入 Python。
2. 异步任务执行到了哪一步。
3. 失败发生在用户数据、Java 调用、Python 调度、数据库、OSS、OCR、LLM 或配置哪个边界。
4. 失败原因是否已经落到业务表，用户是否可以重试。

本规范适用于 FastAPI 接口、后台异步任务、批量处理、OCR/LLM/OSS 等外部调用。

## 2. 固定格式

日志使用普通文本 key-value 格式，方便 `rg`、`grep` 和日志平台全文检索。

推荐：

```python
log.info(
    "asset-parse event=submit-accepted "
    f"batchId={batch_id} tenantId={tenant_id} userId={user_id} "
    f"attachmentCount={total} durationMs={duration_ms}"
)

log.warning(
    "persona-classify event=record-failed "
    f"batchId={batch_id} tenantId={tenant_id} recordId={record_id} "
    f"errorType={type(exc).__name__} message={log_summary(exc)}"
)

log.opt(exception=exc).error(
    "asset-parse event=parse-task-failed "
    f"batchId={batch_id} tenantId={tenant_id} stage=parse-task "
    f"durationMs={duration_ms} message={log_summary(exc)}"
)
```

不推荐：

```python
log.info("开始处理")
log.error(f"失败了: {exc}")
log.info(f"Authorization={authorization}")
```

约定：

1. 第一个词必须是稳定业务关键词，例如 `asset-parse`、`persona-classify`。
2. 每条关键日志必须包含 `event=xxx`，`event` 使用小写短横线。
3. 高价值字段名称保持稳定，例如 `batchId`、`recordId`、`attachmentId`、`tenantId`、`userId`、`stage`、`status`、`durationMs`、`errorCode`。
4. 非预期异常必须使用 `log.opt(exception=exc).error(...)` 保留异常栈。
5. 外部响应体只记录截断摘要，建议不超过 500 字符。

## 3. 日志级别

| 级别 | Python 侧使用场景 |
| --- | --- |
| `DEBUG` | 单条记录的正常分支、模型正常返回、默认值命中等高频细节。 |
| `INFO` | 接口受理、异步任务调度、任务开始/完成、阶段开始/完成、批次汇总。 |
| `WARNING` | 可预期业务失败、单条记录失败、外部服务业务失败、无可重试数据、模型返回不符合约束。 |
| `ERROR` | 非预期异常、任务无法继续、数据库异常、OSS/OCR/LLM 调用异常、数据一致性风险。 |

用户上传数据不合法、单条附件解析失败、单条画像分类失败通常使用 `WARNING`，并把失败原因写入业务表。只有任务边界无法继续或出现非预期异常时才使用 `ERROR`。

## 4. 必须记录的边界

### 4.1 接口入口

启动和重试接口必须记录：

1. 请求已受理：`batchId`、`tenantId`、`userId`、关键参数摘要、耗时。
2. 同步校验失败：`batchId`、`tenantId`、`userId`、`stage`、错误类型、错误摘要。
3. 异步任务是否已调度：`event=task-scheduled` 或业务对应的 accepted 事件。

不打印完整请求体、完整 token、OSS 签名 URL 或文件内容。

### 4.2 异步任务

异步任务必须能通过 `batchId` 串起链路。

必须覆盖：

1. `event=task-start` 或模块内更明确的 `parse-task-start`、`retry-task-start`。
2. `event=stage-start` 或模块内更明确的 `attachment-stage-start`、`batch-page-finished`。
3. `event=stage-finished`，带处理数量和耗时。
4. `event=task-finished`，带总数、成功数、失败数和耗时。
5. `event=task-failed`，带异常栈和失败阶段。
6. `event=status-updated` 或 `event=status-update-failed`，记录关键状态变化或状态更新失败。

### 4.3 外部服务调用

外部服务包括 OSS、OCR、LLM 网关、第三方 API。

建议记录：

1. 调用开始：服务名、接口或模型、业务 ID、请求摘要。
2. 调用完成：HTTP 状态、业务状态、耗时。
3. 调用失败：HTTP 状态、错误码、截断响应摘要、异常栈。

批量任务中如果每条记录都会调用外部服务，正常成功日志优先使用 `DEBUG` 或按批次汇总，失败必须使用 `WARNING` 或 `ERROR`，避免生产 INFO 被单条成功记录刷屏。

### 4.4 重试

重试属于改变业务走向的用户动作，必须记录：

1. `batchId`、`tenantId`、`userId`。
2. 重试前失败数量。
3. 无可重试数据时的明确日志。
4. 重试任务开始、完成、失败。
5. 重试后仍失败的数量和错误摘要。

## 5. 模块关键词和字段

| 模块 | 关键词 | 主业务 ID | 辅助 ID |
| --- | --- | --- | --- |
| 资产包附件解析 | `asset-parse` | `batchId` | `attachmentId`、`zipOssId`、`pathHash` |
| 画像分类 | `persona-classify` | `batchId` | `recordId`、`debtId` |

资产包解析关键字段：

1. `batchId`：批次 ID。
2. `tenantId`：租户 ID。
3. `userId`：触发人。
4. `zipOssId`：资产包 ZIP 的 OSS ID。
5. `attachmentId`：附件执行记录 ID。
6. `pathHash`：附件路径哈希，避免打印完整路径过长或包含敏感信息。
7. `stage`：`start-check`、`parse-task`、`retry-task`、`ocr`、`llm` 等。
8. `totalFiles`、`successFiles`、`failedFiles`：批次结果。

画像分类关键字段：

1. `batchId`：批次 ID。
2. `tenantId`：租户 ID。
3. `userId`：触发人。
4. `recordId`：`persona_classify_record.id`。
5. `debtId`：债务记录 ID。
6. `sourceStatuses`：本轮读取哪些状态。
7. `processed`、`successCount`、`failedCount`：批次页或任务汇总。

## 6. 敏感信息

禁止打印：

1. `Authorization`、Bearer token、access token、refresh token。
2. 密码、验证码、私钥、签名密钥。
3. 完整身份证号、银行卡号、手机号。
4. OSS 预签名 URL。
5. OCR 原文、合同全文、提示词全文、大段 LLM 请求或响应。

允许打印：

1. `hasAuth=true/false`。
2. `tenantId`、`userId`、`batchId`、`recordId`、`attachmentId`。
3. OSS 数字 ID，但不打印临时 URL。
4. 截断后的错误摘要，建议使用 `log_summary(value, max_length=500)`。

## 7. 批量任务要求

批量任务不要逐条打印 INFO。

推荐：

1. 批次开始打印一次。
2. 每个处理页或处理阶段完成打印一次汇总。
3. 单条失败打印 WARNING，成功不逐条打印 INFO。
4. 外部调用成功日志如果是逐条记录，优先 DEBUG。
5. 最终打印任务汇总：总数、成功数、失败数、轮次、耗时。

## 8. 排查命令

按批次排查资产包：

```bash
rg -n "2056545424662208514|asset-parse|ERROR|WARN" logs
```

按批次排查画像分类：

```bash
rg -n "2056545424662208514|persona-classify|ERROR|WARN" logs
```

按附件排查：

```bash
rg -n "attachmentId=123456|batchId=2056545424662208514" logs
```

按画像分类记录排查：

```bash
rg -n "recordId=123456|batchId=2056545424662208514" logs
```

## 9. 提交前检查清单

1. 是否有稳定业务关键词。
2. 是否有 `event=xxx`。
3. 是否包含主业务 ID，例如 `batchId`。
4. 是否包含租户上下文 `tenantId`。
5. 异步链路是否覆盖受理、调度、开始、阶段完成、任务完成、任务失败。
6. 外部调用失败是否包含服务名、状态码、错误码、耗时和响应摘要。
7. 非预期异常是否保留异常栈。
8. 是否避免打印敏感信息。
9. 是否避免循环内大量 INFO。
10. 失败原因是否同步写入 `error_message` 等业务字段。
