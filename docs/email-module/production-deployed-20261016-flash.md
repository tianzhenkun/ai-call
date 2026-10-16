# 2026-10-16 邮件 AI Flash 发布记录

邮件生成、修改及中文审阅翻译已切换为 `deepseek-flash`，明确开启 `thinking.type=enabled`、`reasoning_effort=high`。输出校验失败后最多纠正一次，每次调用的请求编号、实际 usage、耗时及错误状态均保存到 AI 任务记录。

部署及用量记录链路通过验证。内容验收保留一个限制：中文来信要求英文回复时，首稿仍返回中文；页面明确重述语言要求后再次生成成功，随后英文转中文审阅成功。此次语言偏差没有触发结构校验，不能记作自动纠正成功或首稿全部通过。

## 生效版本

- 发布目录：`/opt/lingchen/deployments/reach-email-flash-20261016T145016Z`。
- 部署代码：`cdf01cc62c0175dc64e249079b8fd95962cb6975`。
- 源码分支：`codex/release-email-flash-20261016`，从线上邮件基线 `289dc67` 派生。
- `PROMPT_VERSION=7`，避免同一输入继续复用旧模型生成结果。
- Compose：`/opt/lingchen-migration-20261002/reach-compose.json`，项目 `reach-migrated`。
- 模型通过 API 与 email worker 的 `REACH_EMAIL_LLM_MODEL=deepseek-flash` 生效。

| 容器 | 新镜像 | 镜像 ID |
| --- | --- | --- |
| `ai-call-118-api-1` | `ai-call-local:email-flash-api-20261016T145016Z` | `sha256:51d789a108d6e0171abd0b033f7729bb20ab1745f3fdb787eadb9be7b5364093` |
| `ai-call-118-email-worker-1` | `ai-call-local:email-flash-worker-20261016T145016Z` | `sha256:9899f71d6e0cf590ed7e2f841e84d993b0833ddf231ea9df6431c346ff0f4491` |

镜像分别基于原 API、worker 构建，只覆盖 `reach_email` 的 `ai.py`、`worker.py`、`models.py`、`migrate.py`、`service.py`，逐文件核对 SHA256。保留已部署的回复关联、附件及其他配置；前端版本、知识解析容器未变更。

## 纠正及记录边界

- 无效 JSON、缺少主题或正文、主题变量错误、未定义变量、变量数量变化、翻译链接变化等输出校验失败，使用原始输入和校验原因纠正一次，再次校验。
- HTTP、鉴权、超时、上下文超限、拒答及输出截断不进入这次输出纠正机制。
- 调用记录保留返回模型、response ID、usage、缓存命中量、推理 Token、请求哈希、思考参数及耗时；失败输出使用已有加密能力保存，不保存思考正文，不在普通 API 响应中暴露。
- 语言、事实及业务意图仍需要内容审阅；结构校验通过不代表内容质量全部通过。
- 既有上下文按完整有效往来正文组装，本次未增加邮件数量或输入字符截断。

## 工程与部署验证

1. 在隔离 PostgreSQL 16.9 上执行邮件模块测试：**221 项通过，0 失败、0 错误、0 跳过**；相关 Ruff 检查通过。
2. 从旧版 AIJob 表演练迁移并重复执行，确认旧结果保留、`attempts` 列可空且迁移幂等。隔离数据库验证后停止，无常驻测试服务。
3. 线上数据库备份为发布目录内 `database-before.dump`，2,329,543 字节，`pg_restore -l` 检查通过；Compose 与原镜像信息同时保留。
4. 切换前两次核对通话、待启动任务、占用、转接、发信、运行及排队 AI、LiveKit 房间均为零；停止旧 worker 并等待租约自然到期，再替换 API 和 worker。
5. 两容器健康、重启次数为零；实际加载模型为 Flash、提示版本为 7，`attempts` 列存在；API 健康接口与 worker 健康检查均通过。

## 真实页面验证及实际用量

页面：`https://reach.lingchen-ai.com/reach/email/`，线索管理 → 持续跟进 → 测试成本 → 回复邮件。相同会话使用 2 封往来记录，任务版本为 6。

| 操作 | 内容结果 | 输入 Token | 缓存命中 | 输出 Token | 其中推理 Token | 模型请求耗时 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 首次回复生成 | GEO 报价内容符合约束，但未遵从明确英文要求 | 2,432 | 256 | 578 | 315 | 4.358 秒 |
| 页面重述要求后再次生成 | 英文正文通过审阅，保留原主题和模板变量 | 2,656 | 256 | 1,097 | 804 | 6.741 秒 |
| 中文审阅翻译 | 英文转简体中文通过审阅，原文保留 | 2,403 | 0 | 726 | 460 | 4.127 秒 |
| 合计 | 3 次独立页面操作、3 次模型请求 | 7,491 | 512 | 2,401 | 1,579 | 15.226 秒 |

推理 Token 已包含在输出 Token 内，缓存命中已包含在输入 Token 内，不能重复计费。以上是供应商实际返回 usage 与单次请求耗时，金额仍须按适用价格及账单抵扣对账，不能直接称为实际扣款；耗时合计也不是用户页面总等待时间。

三条任务状态均为 `ready`、HTTP 200，模型及返回模型均为 `deepseek-flash`，思考均为 enabled/high；`ready` 只代表任务与结构校验成功。对应任务及 response ID：

| 操作 | AI job ID | response ID |
| --- | --- | --- |
| 首次生成 | `24de7adda2d74a0cacb117fce4114f93` | `eca4b773-2a96-4421-bab6-ec84718ab907` |
| 再次生成 | `5c03a6dd36bf4d0ca1ef11469a01311f` | `92446850-9395-4816-b83e-8f263e25df33` |
| 中文审阅 | `79f3d0b7a3ed45398a6c484c1f37d342` | `1c99fbd5-26fe-44d0-bcbd-968a4b2493b7` |

此次未发送邮件，目标会话的 outbound 记录从 2 条到 2 条。实际页面请求均未触发自动纠正；纠正的成功、失败及用量保留路径由工程测试覆盖。

## 证据及回退

远端 `release-result.json` 状态为 `applied`；`ui-after.json` 状态为 `workflow_passed_with_quality_limitation`，保留上述首稿失败与后续人工重述结果。

本地证据位于 `/Users/liuhongli/.codex/worktrees/qwen-audio-3-1/ai-call/build/reach-email-flash-20261016T145016Z/`：`postgres-tests.xml`、`postgres-verification.json`、`server-evidence/`、`ui-final-dom.txt`、`ui-translation.jpg`。下载证据包 SHA256：`cafbe4871a61df68e64a51258dddf0365d7c29605071e186c7153df07058af02`。数据库备份和含密钥配置只保留在服务器受限目录，不纳入下载证据包或 Git。

需要回退时，在发布主机执行：

```sh
python3 /opt/lingchen/deployments/reach-email-flash-20261016T145016Z/release.py rollback
```

脚本先检查空闲、停止 worker 并等待租约，再恢复发布前两镜像及 Compose。新增可空 `attempts` 列及已记录用量保留，不删除历史数据。本次未执行回退。
