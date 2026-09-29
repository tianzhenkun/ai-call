# REACH 前后端发布记录（2026-09-29）

2026-09-29 北京时间 11:03，发布 `reach-all-20260929T024955Z` 到 <https://reach.lingchen-ai.com/>。数据库迁移、API 更新和前端切换均已完成。

## 代码与验证

| 仓库 | 分支 | 构建对应提交 |
| --- | --- | --- |
| ai-call | `codex/feat/reach-platform-integration` | `1246b0824848804f06d74eb225cb2cab34f0d471` |
| lingchen-platform-web | `codex/release-reach-email-20260919` | `a496ddb69ccec6395aee51a7816a0c7af864df4e` |

两项提交均已推送并核对远端一致；发布记录另行提交。后端包含提示词模块保存、草稿、修订、版本和软删除，以及外呼重试、记录和跟进改动；前端包含对应提示词编辑、手动异常重呼、任务和记录展示。两个仓库现有业务代码改动均已纳入，未合并到 `main`。本地 `.playwright-cli/`、`storage/`、环境配置和凭据未提交或部署。

- 后端全量首次 2236 项通过，7 项失败、8 项夹具错误。补齐旧运行入口测试的可用场景，更新故障注入函数的参数，并显式复用提示词测试夹具后，受影响的 142 项全部通过。worker 生命周期复测以 `AI_CALL_QUALITY_SCORING_ENABLED=false` 隔离本机模型配置，未调用真实质检模型；生产配置不变。
- 临时 PostgreSQL 16 的提示词迁移、并发编辑、异常重呼和运行控制测试 87 项通过，临时容器和数据卷已清理。
- 前端 REACH 的 81 个测试集、736 项测试通过；全仓 Biome、TypeScript、Ant Design lint 和生产构建通过。Ant Design 有既存弃用和性能提示，无检查错误。提交钩子执行后源码哈希未变化。
- 后端相关文件 Ruff、两个仓库差异空白检查通过。单元测试中的供应商和媒体替身仅用于隔离，不代表真实通话结果。

## 生产结果

后端服务器 `118.25.125.221`：

- 发布目录：`/opt/lingchen/deployments/reach-all-20260929T024955Z`；备份目录：`/opt/lingchen/backups/reach-all-20260929T024955Z`。
- API 镜像：`ai-call-transfer/api:reach-all-20260929T024955Z`，平台 `linux/amd64`，image config digest：`sha256:2d2f6fb04e58be6a967fcba902ebe81a6aeb502b68a4153b2355ae4ec4b0df5c`。
- 切换前活动通话、待执行 START_CALL、线路预留、活动转人工和 LiveKit 房间均为 0；另有 1 条既存历史 `ending` 记录，未修改。
- 在原七份 Compose 后追加本次 `api.override.yml`，仅重建 API。邮件代码及其共享依赖本次没有变化，worker 继续使用 `reach-seat-20260921T051325Z`；其容器及其他 AI Call 容器 ID 均未变化。生产 `.env` 和主 Compose 哈希不变。
- API 健康为 `ok`；API 和邮件 worker 均 `healthy`、重启数 0。容器内 237 个源码及迁移文件哈希与发布清单一致；新增提示词接口已出现在 OpenAPI。生产只读事务通过新版服务逐个读取 5 个场景，均为 `READY`、`editRevision=1`。
- 使用生产 SDK 的只读服务发现查询确认 Nacos 中 `lingchen-reach-python` 的健康实例为 `172.20.0.15:19011`。

数据库：

- 迁移前执行 `pg_dump -Fc`，备份约 1.2 MB；在 PostgreSQL 容器内用 `pg_restore -l` 校验归档可读取，并保存数据库 SHA256。备份配置文件权限为 0600。归档可读不代表本轮完成整库恢复演练。
- 使用新镜像中的 `tools/migrate_prompt_editor.py`，在同一事务执行 `sql/phase-b4-prompt-editor-postgres.sql` 和版本指针核对；锁等待上限 5 秒、语句上限 30 秒。
- 首次演练完整执行后回滚，正式执行前再演练一次，随后提交迁移。5 个场景没有空名称、规范化后重名或不完整静态配置；无版本指针修复。4 个旧场景没有内容匹配的历史版本，保留实际内容和现有空指针；没有创建、删除或覆盖历史版本。
- 新增 `lifecycle_status`、`edit_revision`、`creation_key`、`deleted_at`、`deleted_by` 和名称／创建键唯一索引。迁移不创建草稿、不删除场景、不改任务快照。

前端服务器 `81.68.166.109`：

- `current` 已原子切到 `/home/lingchen/web/ai-reach/releases/reach-all-20260929T024955Z`，上一版本为 `reach-all-20260923T144114Z`。
- 406 个构建文件校验通过，补留 351 个旧版哈希资源；生产 `runtime-config.js` 与上一版本字节一致。
- 公网 257 个 JS、CSS、关键页面和协议资源的内容哈希与构建清单一致。浏览器正常加载 REACH 登录页；当前会话未登录，未执行登录后的保存操作。

## 下次上线顺序

1. 核对两个仓库的分支、远端和全部业务差异；确认正在运行的镜像、完整 Compose 顺序、前端 `current` 和磁盘空间。
2. 跑改动相关回归、类型和 lint 检查，提交并推送业务代码；构建固定来源的前端和 amd64 后端，记录文件哈希及 image config digest。
3. 对照生产 schema 演练所需迁移；提示词迁移报告存在空名称、重名或无法解释的数据时停止，不自动改名、合并或删除。
4. 将产物上传到唯一版本目录并校验；前端先准备新目录，保留生产运行配置及旧哈希资源。
5. 再检查活动通话及房间；备份数据库和全部 Compose，验证备份可读；提交已审阅迁移，然后仅更新受影响服务。
6. 验证服务健康、实际镜像、容器源码哈希和只读业务查询，再原子切换前端并检查公网资源。登录态和真实业务验收按实际证据单列。
7. 保存发布、备份、迁移报告和回退目标；提交并推送发布记录。通用操作见 [上线手册](../email-module/production-runbook.md)。

## 回退与验收边界

后端在无活动通话窗口执行本次发布目录的 `rollback.sh`。脚本先检查草稿及软删除：两者均为 0 才允许恢复上一版七份 Compose。存在任一状态时，旧后端会重新暴露草稿或删除场景，脚本拒绝回退；应保留新增列和具备状态过滤的兼容后端，不能清空删除标记、删除草稿或整库覆盖来绕过检查。

发布后只读回退检查结果为 `allowed=true`、`drafts=0`、`deleted=0`；实际回退前必须重新执行，不能复用本次结果。

前端先确认 `current` 仍指向本次目录，再用临时软链接和 `mv -Tf` 原子切回 `/home/lingchen/web/ai-reach/releases/reach-all-20260923T144114Z`。前后端合同必须协调回退：仅切回旧前端时，其更新请求缺少修订令牌，会被新后端拒绝。数据库新增列默认保留；整库恢复需要另行评估发布后的业务数据。

本轮没有发起真实电话、邮件或付费模型请求；真实媒体、打断、识别、跟进效果，以及登录后编辑保存，尚不属于本轮验收结论。
