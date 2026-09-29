# REACH 提示词列表发布记录（2026-09-29）

2026-09-29 北京时间 16:48:41，发布 `reach-all-20260929T084018Z` 到 <https://reach.lingchen-ai.com/>，完成 API 更新和前端原子切换。上一版本为同日上午发布的 `reach-all-20260929T024955Z`，本次没有数据库迁移。

## 代码来源与验证

| 仓库 | 分支 | 构建对应提交 |
| --- | --- | --- |
| ai-call | `codex/feat/reach-platform-integration` | `efe64c1d48c9754726d02e119bac97c3a9a28563` |
| lingchen-platform-web | `codex/release-reach-email-20260919` | `568c6feec55594c3eb51dc37032ac6e60a33e7f8` |

两项业务提交均已推送并核对远端一致，未合并到 `main`。部署执行记录单独提交，不改变上表的构建来源。本次纳入后端 4 个、前端 10 个业务文件的全部本地改动；本地 `.playwright-cli/`、`storage/`、环境配置和凭据未提交或部署。

后端新增提示词名称和 `DRAFT`／`READY` 状态筛选，筛选在分页及计数之前生效，名称中的 `%`、`_` 按普通字符处理。前端将提示词列表与新建、查看、编辑页面分离，保留列表查询条件和删除时的修订校验、结果确认；沿用既有模块保存及版本合同。邮件代码、共享依赖和数据库结构没有变化。

- 后端提示词编辑、B4 配置和知识库相关测试 **85 项通过**；临时 PostgreSQL 的提示词编辑测试 **5 项通过**。临时容器及数据卷已清理。测试使用 `AI_CALL_QUALITY_SCORING_ENABLED=false` 隔离本机模型配置，生产配置未变。
- 前端 REACH **82 个测试集、751 项测试通过**；全仓 Biome、TypeScript、Ant Design lint 和生产构建通过。Ant Design 有既存弃用及性能提示，无检查错误。
- 后端相关文件 Ruff、两个仓库差异空白检查通过。提交钩子执行后业务源码哈希不变；打包前再次核对源码及提交来源。

以上为相关回归检查，不表示本次执行了后端全量测试，也不替代真实通话或登录后的保存验收。

## 生产验证

后端服务器 `118.25.125.221`：

- 发布目录：`/opt/lingchen/deployments/reach-all-20260929T084018Z`；备份目录：`/opt/lingchen/backups/reach-all-20260929T084018Z`。
- API 镜像：`ai-call-transfer/api:reach-all-20260929T084018Z`，平台 `linux/amd64`；image config digest：`sha256:e53217b1e5a5fa5dbc636c87aa77ca2780edc03754f4702b1b1ff8e4fb8b2c73`。该值来自镜像归档的 config 内容，并与容器实际 image ID 核对。
- 备份前及 API 切换前，活动通话、待执行 START_CALL、线路预留、活动转人工和 LiveKit 房间均为 0。另有 1 条既存历史 `ending` 记录，未修改。
- 执行 `pg_dump -Fc`，在 PostgreSQL 容器内用 `pg_restore -l` 验证归档可读取，保存 SHA256；同时备份全部 Compose、生产 `.env` 和旧镜像信息。归档可读不代表本轮完成整库恢复演练。
- 在原八份 Compose 后追加本次 `api.override.yml`，仅重建 API。邮件 worker 继续使用 `ai-call-transfer/api:reach-seat-20260921T051325Z`；其他 AI Call 容器 ID、生产 `.env` 和主 Compose 哈希均未变化。
- API 健康为 `ok`；API 和邮件 worker 均 `healthy`、重启数 0。容器内 **237 个源码及迁移文件**哈希与清单一致；本次未执行迁移脚本。
- OpenAPI 的列表接口包含 `name`、`lifecycleStatus`、`pageNum`、`pageSize`、`includeDrafts`。通过生产数据库只读事务，核对 5 个场景的名称／状态筛选、计数、单条分页和无匹配查询，结果通过。没有创建、编辑或删除业务数据。
- 使用生产 Nacos SDK 只读查询确认 `lingchen-reach-python` 的健康实例为 `172.20.0.15:19011`。

前端服务器 `81.68.166.109`：

- `current` 已原子切到 `/home/lingchen/web/ai-reach/releases/reach-all-20260929T084018Z`，上一目标为 `/home/lingchen/web/ai-reach/releases/reach-all-20260929T024955Z`。
- **409 个构建文件**上传校验通过，补留 **361 个旧版哈希资源**；生产 `runtime-config.js` 与上一版本字节一致。
- 公网 **260 个 JS、CSS、关键页面和协议资源**的内容哈希与构建清单一致，验证输出 `PUBLIC_ASSETS_OK 260`。
- 浏览器正常加载 REACH 登录页；当前会话未登录，未执行登录后的列表、编辑和保存操作。

本地产物、来源清单、测试和部署日志保存在被忽略的 `build/reach-all-20260929T084018Z/`。本次没有发起真实电话、邮件或付费模型请求。

## 回退与下次发布

本次回退目标是 `reach-all-20260929T024955Z`，其数据库结构、草稿和软删除过滤、修订合同与本次兼容。后端在无活动通话窗口执行：

```bash
bash /opt/lingchen/deployments/reach-all-20260929T084018Z/rollback.sh
```

脚本先确认运行的 API 标签仍为本次版本，再检查活动通话和房间，然后使用发布前保存的八份 Compose 仅恢复 API 并等待健康。邮件 worker 及数据库保持原状。

前端在 `81.68.166.109` 上确认 `current` 仍为本次目录后，使用临时软链接和 `mv -Tf` 原子切回上一目标，再校验资源：

```bash
set -euo pipefail
cd /home/lingchen/web/ai-reach
test "$(readlink -f current)" = /home/lingchen/web/ai-reach/releases/reach-all-20260929T084018Z
test ! -e current.rollback-reach-all-20260929T084018Z
ln -s /home/lingchen/web/ai-reach/releases/reach-all-20260929T024955Z current.rollback-reach-all-20260929T084018Z
mv -Tf current.rollback-reach-all-20260929T084018Z current
readlink -f current
```

如果需要继续退回不支持草稿／软删除的 9 月 23 日后端，必须重新执行 `tools/migrate_prompt_editor.py --check-rollback` 并协调前后端修订合同，不能套用本次相邻版本的兼容结论。默认保留新增列，不清空删除标记、不删草稿、不自动整库恢复。

下次发布按[上线操作手册](../email-module/production-runbook.md)重新核对当前镜像、完整 Compose、前端目标、活动通话和源码差异。仅在 schema 有变化时演练并执行迁移；API 和 worker 分别按实际代码及依赖变化更新，不能复用历史健康结果。
