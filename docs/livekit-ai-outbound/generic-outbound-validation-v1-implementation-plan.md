# 通用外呼批量名单校验 V1 实现计划

## 现场基线

- 工作树：`/Users/liuhongli/.codex/worktrees/ed81/ai-call`
- 分支：`codex/ai-call-workflow-split`
- 开始实现时 HEAD：`10af1eb409dae611d588059add3270c7519ab36a`
- `19011`：由本工作树的 `uvicorn main:create_app --factory` 进程监听；本次不重启。
- 工作树已有大量未提交修改。本实现只新增 outbound 边界文件，并对唯一的路由聚合入口做精确补丁；不格式化、暂存或提交既有改动。
- 仓库中未找到 `CLAUDE.md`；已读取并遵循根目录 `AGENTS.md`。

## 契约决策

本实现以 2026-07-28 的直接上传决策为准。前端旧规格中 `ossId` 两段式描述已过期，不作为后端实现依据。

- `POST /ai-call/outbound-validations/batch`
- `multipart/form-data`
- `file`：单个 `.xlsx`，最大 10 MB
- `request`：任务配置 JSON 字符串
- 成功受理返回若依统一响应，`data` 至少包含字符串 `validationId`、`status=VALIDATING` 和 `accepted=true`
- 不调用 `sys_oss`，不接收或保存 `ossId`

配套接口：

- `GET /ai-call/outbound-validations/{validationId}`：查询校验状态
- `GET /ai-call/outbound-validations/{validationId}/issues`：问题分页
- `POST /ai-call/outbound-validations/{validationId}/issues/export`：导出问题明细
- `POST /ai-call/outbound-validations/{validationId}/retry`：仅重试解析完成后的系统校验错误
- `POST /ai-call/outbound-targets/import-template`：下载名单模板

## 数据边界

新增两张独立表：

1. `ai_call_outbound_validation`
   - 显式保存 `tenant_id`
   - 保存任务配置、处理阶段、临时文件元数据、状态、统计、错误和重试能力
   - JSON 内容统一保存为 `text`
2. `ai_call_outbound_validation_row`
   - 显式保存 `tenant_id` 和 `validation_id`
   - 同时保存有效行与错误行
   - 错误原因保存在本表的 `reasons_json`，不新增第三张问题表

两表无物理外键。所有读写条件显式包含 `tenant_id`。所有 bigint / 业务 ID 在 API 输出时转换为字符串。

校验通过前不创建 `ai_call_outbound_task` 或正式外呼对象。后续“确认任务”能力必须重新校验同租户、`PASSED` 状态和配置一致性，再按主键游标分批复制有效行；本阶段不实现任务调度或 SIP 拨号。

## Phase H2：呼叫规则与正式任务

本阶段以当前前端 `aiCallRules/service.ts`、`aiCallTasks/service.ts` 和
`domain.ts` 为接口真源，仍使用直接上传方案，不恢复旧 OSS 两段式。

- `ai_call_outbound_rule`：租户级规则，呼叫时段、重试间隔和可重试结果以
  `text` JSON 保存，删除使用软删除。
- `ai_call_outbound_task`：由同租户 `PASSED` 校验结果创建，保存规则、提示词、
  音色和请求参数快照；`Idempotency-Key` 在租户内唯一。
- `ai_call_outbound_target`：从有效校验明细按主键游标分批复制，不一次性读取
  全量名单；单号校验复制一条对象。

创建任务前必须重新核对：校验结果与当前租户一致、状态为 `PASSED`、请求参数与
校验固化配置完全一致、规则未删除且启用、提示词与场景匹配、音色存在。三张表均
显式保存 `tenant_id`，只使用逻辑 ID 关联，无物理外键、无 `jsonb`。

当前没有任务调度器和 SIP 执行器，因此无论立即执行还是定时执行，创建后都只落
`SCHEDULED`。立即执行的 `scheduledAt` 为 `null`；定时执行保存计划时间。
`cancel` 仅支持 `SCHEDULED`，不会伪造 `RUNNING`、`PAUSED` 或停止过程状态。
本阶段不触发 LiveKit、FreeSWITCH、Linphone 或真实号码外呼。

## 处理流程

1. 请求线程校验文件名和任务 JSON。
2. 按固定大小读取上传流并写入系统临时目录；超过 10 MB 立即拒绝并删除临时文件。
3. 创建 `VALIDATING / UPLOADED` 校验记录并提交，随后调度进程内后台任务。
4. 后台以 `openpyxl` 的 `read_only=True` 打开工作簿，逐批读取，不把完整名单载入内存。
5. 每批校验手机号、空行和重复号码，批量写入明细表；重复检测通过“当前批次集合 + 已落库行查询”完成。
6. 解析完成后删除临时文件，再基于已落库明细计算终态：
   - 有业务数据问题：`FAILED`
   - 无问题：`PASSED`
   - 解析阶段异常：`SYSTEM_ERROR`、不可重试、要求重新上传
   - 解析完成后的系统校验异常：`SYSTEM_ERROR`、允许按 `validationId` 重试
7. 应用启动时扫描本租户无关的所有 `VALIDATING` 记录：
   - 仍需解析且临时文件存在：继续处理
   - 仍需解析但临时文件不存在：标记不可重试的 `SYSTEM_ERROR`
   - 已解析：直接从明细表继续系统校验

## TDD 与验收

先写失败测试，再实现最小代码。聚焦测试至少覆盖：

- 路由和 multipart 字段契约
- `.xlsx` 与 10 MB 门禁
- ID 字符串化和若依响应外壳
- 模型无外键、显式租户字段、JSON 使用 `text`
- 有效名单 `PASSED`
- 格式错误、空行、重复号码进入同一明细表并得到 `FAILED`
- 分页、筛选和问题导出只读取校验明细表
- 所有查询的租户隔离
- 解析完成和解析失败后的临时文件清理
- 解析阶段失败不可重试
- 解析后系统错误可按 `validationId` 重试
- 重启时临时文件存在则续跑，不存在则要求重传

验证只证明当前源码和测试数据库中的名单闭环成立。由于本次不重启 `19011`，不得把测试通过表述为 `19011` 已加载，更不得表述为真实 SIP 已验通。
