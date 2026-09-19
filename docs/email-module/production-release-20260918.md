# REACH 邮件发布准备（2026-09-18）

本文保留 2026-09-18 发布前的准备快照，不是当前上线状态或下次发布指令。该版本后来已上线，实际增加了计费覆盖及迁移；结果见[实际发布记录](production-deployed-20260918.md)。**下次上线使用[可复用上线手册](production-runbook.md)**，不要直接照抄下方历史三文件 Compose 或旧版本回滚步骤。

## 发布对象和回滚基线

| 对象 | 当前线上位置 |
| --- | --- |
| 前端服务器 | `81.68.166.109` |
| 前端入口 | `/home/lingchen/web/ai-reach/current` |
| 旧前端目标 | `/home/lingchen/web/ai-reach/releases/web-package-ui-20260904T160925Z` |
| 后端服务器 | `118.25.125.221` |
| 后端运行目录 | `/opt/ai-call/runtime` |
| 主 Compose | `/opt/ai-call/runtime/compose.yml` |
| 原覆盖文件 | `/opt/lingchen/deployments/platform-integration-20260903T172008Z/deploy/reach-compose.override.yml` |
| 旧 API 镜像 | `ai-call-transfer/api:platform-integration-20260903T172008Z` |
| 邮件数据目标 | 现有 PostgreSQL 的 `ai_call` 库；只迁移 `reach_email_*` |

REACH 网关已有 `/reach-api/v1/**` → `lb://lingchen-reach-python`、`StripPrefix=2`；当前健康注册 `172.20.0.15:19011` 对应现有 API 容器。注册健康不代替登录态业务验收。

## 上线前必须补齐

1. **菜单与授权**：用户已在 Codex 右侧登录线上 REACH。当前账号侧栏没有邮件菜单，直接访问 `/reach/email` 显示“当前账号或租户未获得该页面的后端路由授权”。上线前须确认 `reach/email/index`、页面 `/reach/email` 在 REACH 菜单树内，并分配给本次账号所在租户套餐和目标角色。平台数据库文件凭据查询认证失败，尚不能区分全局菜单未登记、套餐未包含或角色未授权；不得盲目插入菜单或扩大授权。
2. **邮件密钥**：在生产服务器受限目录 `/opt/ai-call/runtime/secrets/email` 配置 `llm-api-key` 和 `encryption-key`。后者须为有效 Fernet 密钥、生成一次后稳定保留；不得复用平台 JWT 密钥，也不得打入镜像。文件须可由镜像 UID 10001 读取，其他用户不可读。发布备份中安全保存加密密钥，否则邮箱凭据无法解密。
3. **模型**：用户已确认沿用本地邮件 AI 配置。本次读取本地 worker 同源配置，确认 `https://api.deepseek.com`、`deepseek-v4-pro` 且密钥存在，已将地址和模型显式写入 API/worker 共用配置。密钥尚未上传；生产网络及实际模型调用仍需验收。
4. **配置一致性**：Compose 中 worker 读取现有 `/opt/ai-call/runtime/.env`；现场确认其数据库类型、地址、端口、库名、用户与 API 的最终配置一致，API 的额外环境覆盖不能遗漏。文件引用与直接密钥变量不可同时配置。
5. **备份与窗口**：保存原 Compose、镜像 ID、前端链接和数据库备份并验证可读。升级 API 会影响同进程的通话能力，确认没有活跃通话并安排窗口。邮件上线前不得导入或执行客户真实发信队列。

## 候选产物

本地目录：`build/reach-email-20260918/`（已由仓库 `build/` 忽略规则排除）。

- `backend/`：当前源码快照、`uv.lock` 导出的固定版本依赖及平台 SDK wheel；不携带 `env/`、`storage/`、本地数据库或凭据。
- `frontend/`：当前前端构建结果，复用从线上获取的公开 `runtime-config.js`。该配置已通过当前前端的 `parseRuntimeConfig` 校验：`/dev-api`、`/resource/sse`、加密开启。
- `compose.production.yml`：与既有两个 Compose 文件叠加的邮件配置，API 与 worker 使用同一镜像，worker 不开放端口。
- `manifest.json`、`SHA256SUMS`：候选产物来源、校验值和验证结果。源码含当前未提交修改，Git HEAD 不能单独代表本次产物；以源码文件哈希清单为准。

此候选包不是通过 `lingchen-release` 完整 source-lock/release-plan 流程生成的正式发布归档，不得混用旧 release lock 声称正式发布完成。

## 后端执行顺序（待上线确认）

将新覆盖文件放入新部署目录，保留旧目录和镜像不动。设置新镜像为已核验的镜像 ID 或仓库 digest，并以变量 `REACH_EMAIL_IMAGE` 传给 Compose。

切换前的备份示例（在后端服务器执行，目录权限限制为当前用户）：

```bash
umask 077
backup_dir="/opt/lingchen/backups/reach-email-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$backup_dir"
cp /opt/ai-call/runtime/compose.yml "$backup_dir/compose.yml"
cp /opt/lingchen/deployments/platform-integration-20260903T172008Z/deploy/reach-compose.override.yml "$backup_dir/reach-compose.override.yml"
docker inspect ai-call-118-api-1 --format '{{.Image}}' > "$backup_dir/api-image-id.txt"
docker exec ai-call-118-postgres-1 sh -c 'pg_dump -U "$POSTGRES_USER" -d ai_call -Fc' > "$backup_dir/ai_call.dump"
docker exec -i ai-call-118-postgres-1 pg_restore --list < "$backup_dir/ai_call.dump" > "$backup_dir/restore-list.txt"
test -s "$backup_dir/restore-list.txt"
```

发布时另行安全备份 `.env` 和邮件密钥，不把它们加入公开产物哈希清单。以上检查只证明归档可读取，不能冒充已完成恢复演练。

```bash
cd /opt/ai-call/runtime
REACH_EMAIL_OVERLAY=/opt/lingchen/deployments/reach-email-20260918/deploy/compose.production.yml
dc=(docker compose -f compose.yml -f /opt/lingchen/deployments/platform-integration-20260903T172008Z/deploy/reach-compose.override.yml -f "$REACH_EMAIL_OVERLAY")

# 镜像已加载、密钥与配置一致性已检查、数据库备份已完成之后执行。
# --entrypoint 绕过现有 API 的 uvicorn 启动包装，不启动其他服务。
"${dc[@]}" run --rm --no-deps --entrypoint python api -m app.services.reach_email.migrate
"${dc[@]}" run --rm --no-deps --entrypoint python api -m app.services.reach_email.migrate --apply
"${dc[@]}" up -d --no-deps api email-worker
"${dc[@]}" ps api email-worker
"${dc[@]}" exec -T email-worker python -m app.services.reach_email.health
```

不要使用整个项目的 `down` 或无服务限定的 `up`，避免重建数据库、SIP、LiveKit 等服务。worker 的 restart 策略处理进程退出；Docker 的 `unhealthy` 本身不会自动重启，积压不应靠盲目重启解决。健康检查失败需接入现有监控；本次没有新增邮件、短信或聊天告警外发。

## 前端切换与验收

将前端归档解压到新的 release 目录，核对 `SHA256SUMS` 和运行配置，再用临时软链接加原子 rename 切换 `current`。不要覆盖旧 release 文件，也不要将空的仓库 runtime-config 模板发布到线上。

```bash
# 在前端服务器，新目录已解压并校验后执行。
cd /home/lingchen/web/ai-reach
test -s releases/reach-email-20260918/index.html
test -s releases/reach-email-20260918/runtime-config.js
ln -s /home/lingchen/web/ai-reach/releases/reach-email-20260918 current.reach-email-20260918
mv -Tf current.reach-email-20260918 current
```

归档的顶层目录为 `frontend/`，解压进上述新 release 目录时使用 `--strip-components=1`。临时链接若已存在，先调查前次发布状态，不强制覆盖。

验收包括：首页和资源返回、实际发布账号登录、邮件菜单及权限、邮件列表和编辑、附件上传下载、真实模型生成、worker 心跳。收发及回复关联仅使用用户指定的受控测试邮箱；SMTP 接受、收到邮件及回信同步分别记录。调用和媒体服务的回归不得擅自拨打客户号码。

## 回滚

1. 停止新邮件 worker，保留队列、发送结果及密钥；发送结果不明的邮件不得重新排队。
2. 用**原两个 Compose 文件**运行 `up -d --no-deps api`，恢复旧 API 镜像；复查实际 image ID 和健康状态。
3. 将前端 `current` 原子切回上述旧目录，核对公网资源。
4. 保留新增邮件表，不做自动 DROP 或生产库整体恢复。首次迁移和新增字段与旧 API 独立；如需恢复备份，先评估升级后的其他业务写入，另行批准。

## 已执行验证

- 前端邮件和编辑器：9 个测试套件，69 项通过。
- 前端生产构建、Biome、TypeScript 通过；Ant Design 检查退出 0，报告 19 项既有警告，未修改无关页面。
- 后端邮件及认证：隔离测试密钥后 119 项通过；两个 PostgreSQL 专项在新建本地隔离库中另行通过。
- 邮件迁移在本地隔离 PostgreSQL 中成功创建 11 张表；不涉及生产迁移。
- `linux/amd64` 候选镜像构建及 `pip check` 通过；镜像内邮件模块、SDK、表定义和健康路由导入通过，并已在同一本地隔离 PostgreSQL 中执行镜像内迁移。
- Compose 与仓库主配置的合并、API/worker 镜像一致性、worker 命令和健康检查断言通过。另在生产服务器通过标准输入只读合并现有两个 Compose 和新增配置，确认 API/worker 六项数据库配置一致、API 启动包装保留、worker 独立且不开放端口；未启动服务。
- 前端运行配置通过当前代码校验；已用用户提供的线上登录会话确认旧版数据看板可访问、邮件路由被拒绝。尚未进行新版线上登录、模型、附件及邮件收发验收。
