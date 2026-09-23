# REACH 前后端全量改动发布记录

2026-09-23 北京时间 22:56，发布 `reach-all-20260923T144114Z` 到 <https://reach.lingchen-ai.com/>。先部署数据库和 API，再原子切换前端。

## 源码与验收

- 后端 `ai-call` 分支 `codex/feat/reach-platform-integration`，提交 `9a354bf6310013198882d83275088cc241ad2342`，已推送；前端 `lingchen-platform-web` 分支 `codex/release-reach-email-20260919`，提交 `50a8465c904cc4223a32b03227b0eb29def672c4`，已推送。前端合并了远程 `main` 的产品协议与注册同意流程，保留 REACH 邮件和本轮改动。
- 后端涉及通话结束归因、GEO 提示词、开场白允许打断、音色表达风格及坐席流程；前端涉及对应页面、知识库、任务、记录、回访和音色界面。`.playwright-cli/` 与 `storage/` 为本地数据，未提交或部署。
- 后端全量首次 2229 项通过、1 项因旧文案断言失败；该断言与新的 60 字、1 到 2 句约束对齐后，目标测试通过。隔离 PostgreSQL 迁移及运行控制测试 83 项通过，Ruff 通过。
- 前端 REACH 测试 682 项通过。合并后整仓 2544 项通过、6 项因两处旧测试设置失败；只修正测试路由位置和邮件编辑器的计费确认替身后，受影响 18 项通过。Biome、TypeScript、Ant Design 检查和正式构建通过。

## 生产部署

- API 镜像 `ai-call-transfer/api:reach-all-20260923T144114Z`，linux/amd64，配置摘要 `sha256:7de0fc1dc98d83c4d0f27f091ca67f22ee182f360c846dddc95f1e917391ddd5`。后端发布包位于 `/opt/lingchen/deployments/reach-all-20260923T144114Z`，数据库和配置备份位于 `/opt/lingchen/backups/reach-all-20260923T144114Z`。
- 切换前执行活动通话和 LiveKit 房间检查，保留旧六份 Compose，新增第七份 `api.override.yml`，只重建 API。邮件 worker 继续使用 `reach-seat-20260921T051325Z` 镜像，未重建。生产 `.env`、主 Compose 和其他 AI Call 容器 ID 未变化。
- 迁移前以 `pg_dump -Fc` 备份数据库并校验归档列表。应用 `sql/prompt-opening-barge-in-postgres.sql` 和 `sql/voice-speaking-style-postgres.sql`，新增字段缺省分别为 `true`、`natural`；迁移可重复执行。数据库约束保留；回退旧 API 时不删字段。
- API 健康为 `ok`，镜像和容器内 234 个源码文件散列匹配；API、邮件 worker 均为 `healthy`、重启数 0。生产 OpenAPI 包含音色表达风格 PATCH 接口。
- 前端此前为 `legal-word-20260922T090601Z`，本次版本位于 `/home/lingchen/web/ai-reach/releases/reach-all-20260923T144114Z`。408 个构建文件核验通过，额外保留旧版 301 个散列资源；生产 `runtime-config.js` 原样复制。`current` 已原子切换到本次目录，公网核验 250 个 JS、CSS 和关键页面/协议文件的内容散列一致。

## 回退与验收边界

- 后端在无活动通话时执行发布包中的 `rollback.sh`，回到上版六份 Compose；两项数据库字段为兼容新增，回退时保留。备份中的 `ai_call.dump` 可用于单独的数据恢复评估，不能直接覆盖发布后产生的新业务数据。
- 前端在 `/home/lingchen/web/ai-reach` 下核对 `current` 仍指向本次目录，再建立指向 `releases/legal-word-20260922T090601Z` 的临时软链接，用 `mv -Tf` 原子替换 `current`。
- 本次验证覆盖自动化、数据库迁移、服务健康、API 路由及公网静态资源；未发起真实电话、模型试听或登录后页面操作。真实通话的双向媒体、首句打断、结束归因和音色听感仍需受控号码验收。
