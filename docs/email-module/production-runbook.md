# REACH 前后端生产上线操作手册

本手册按 2026-10-10 在生产现场核对的部署拓扑编写。主机、镜像、容器、目录和数据库可能再次迁移；每次上线前都要重新读取运行态，不能只凭本文切换。2026-09-18 的双机邮件发布操作保存在[历史手册](production-runbook-20260918.md)，日期发布记录仍是历史事实，不代表当前部署入口。

## 当前部署入口

| 对象 | 2026-10-10 现场值 |
| --- | --- |
| 域名 | `https://reach.lingchen-ai.com/` |
| 前后端主机 | `110.42.223.109` |
| 前端 | 主机 Nginx，`/home/lingchen/web/ai-reach/current` → `releases/<版本>` |
| 后端 | Compose 项目 `reach-migrated`，容器 `ai-call-118-api-1` |
| 生效 Compose | `/opt/lingchen-migration-20261002/reach-compose.json` |
| 发布与回滚资料 | `/opt/lingchen/deployments/<版本>` |
| 关联服务 | `ai-call-118-email-worker-1`、`ai-call-118-knowledge-parser-1`，按本次代码依赖决定是否更新 |
| 媒体服务 | `118.25.125.221` 承担历史媒体链路；不把它当作当前前后端发布主机 |

前端源码在相邻的 `lingchen-platform-web` 仓库，Python API 在本仓库。`ai-call-118-*` 容器名保留了旧主机编号，不能据此判断主机地址。旧文档中的 `81.68.166.109` 前端和 `/opt/ai-call/runtime/compose.yml` 后端路径均不是上述现场的生效入口。

## 1. 确定范围与固定来源

1. 在两个仓库分别核对分支、远端、`git status`、待发布差异和 worktree。排除其他隔离会话的未完成工作；需要上线的业务改动和测试先提交、推送，记录完整提交 SHA。不得提交环境变量、密钥、数据库、录音和构建缓存。
2. 运行改动相关测试、静态检查及前端正式构建。核对本次是否涉及数据库 schema、共享依赖、邮件 worker、知识解析或运行配置；只有相关对象才加入切换，不能默认为每次只换 API。
3. 生成唯一版本号的后端源码快照或补丁、`linux/amd64` 镜像、前端归档及逐文件 SHA-256 清单。若使用旧镜像增量构建，先比对旧快照与运行镜像内源码哈希，再核对新镜像内文件与本次 Git 提交一致。新版本不得复用旧镜像标签或覆盖旧发布目录。

**验收：**提交、测试、归档和镜像中的源码一一对应；迁移及关联服务的范围有明确结论。

## 2. 核对现场、备份并检查切换窗口

在 `110.42.223.109` 只读核对 DNS、Nginx 入口、当前前端目标、API image ID、Compose 项目及配置文件、关联容器、磁盘空间。不要打印包含密钥的完整 Compose 或环境变量。

```bash
getent ahostsv4 reach.lingchen-ai.com
readlink -f /home/lingchen/web/ai-reach/current
docker inspect ai-call-118-api-1 --format '{{.Image}} {{.State.Health.Status}} {{.RestartCount}}'
docker inspect ai-call-118-api-1 --format '{{index .Config.Labels "com.docker.compose.project.config_files"}}'
docker ps --filter name=ai-call-118 --format '{{.Names}} {{.Image}} {{.Status}}'
df -h /opt /home
```

将生效 Compose 原文、旧镜像 ID、前端旧软链接目标保存到权限受限的新发布目录；即使本次无迁移，也备份生产数据库并用 `pg_restore -l` 检查归档可读。若有 schema 变更，先在隔离库演练并审阅迁移。切换前核对活跃通话、待发起命令、SIP 预留、转接及 LiveKit 房间均为空；单条历史 `ending` 记录不能替代房间检查，也不要为上线清理业务数据。邮件 worker 需要更新时，还需等待当前发送结束，按租约规则接管。

**验收：**回滚所需的旧 Compose、镜像、数据库备份、前端目标均已固定；切换时没有在途通话。

## 3. 预备后端和前端

发布包通过已验证的通道上传到 `/opt/lingchen/deployments/<版本>`，在主机上重新校验 SHA-256。构建镜像后检查架构及镜像内文件哈希。以现场生效 Compose 为输入，只改本次要切换的服务镜像，先生成 `reach-compose.before.json` 与 `reach-compose.after.json`，执行 `docker compose -p reach-migrated -f reach-compose.after.json config --quiet`；不要输出解析后的密钥配置。

前端解压到 `/home/lingchen/web/ai-reach/releases/<版本>`，逐文件核对构建清单。若生产配置未变，沿用当前 `runtime-config.js`；保留旧页面仍会请求的哈希资源，遇到同名不同内容立即停止。**切换前检查新目录及各级子目录为 Nginx 可遍历、静态文件可读**：当前基线是目录 `0755`、文件 `0644`。发布会话若设置 `umask 077`，解压脚本创建的 `0700/0600` 文件会使切换后的页面返回 403。用 Nginx 运行用户或等效权限做读取检查，再切换软链接。

**验收：**新镜像、前端文件、生产运行配置和访问权限全部可校验，线上版本尚未改变。

## 4. 切换与验证

再次检查没有活动通话，并确认 `before.json` 与当前主 Compose 完全相同，然后只重建本次涉及的服务。例如 API 单独更新：

```bash
release=/opt/lingchen/deployments/<版本>
docker compose -p reach-migrated -f "$release/reach-compose.after.json" up -d --no-deps ai-call-118-api-1
docker inspect ai-call-118-api-1 --format '{{.Image}} {{.State.Health.Status}} {{.RestartCount}}'
docker exec ai-call-118-api-1 python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:19011/ai-call/health', timeout=5).status)"
```

确认新 API image ID、`healthy`、重启数 0、内部健康接口和 Nacos 注册后，原子更新主 Compose 文件为候选版本。邮件 worker 或解析服务未纳入本次发布时，确认它们继续健康且镜像未变化。不要执行整个项目 `down` 或 `--remove-orphans`。

随后用临时软链接和 `mv -Tf` 原子切换前端 `current`。验证 `readlink -f current`、`current/index.html` 与域名实际返回 HTML 的 SHA-256 相同；再校验本次 JS/CSS、关键懒加载页面和 `runtime-config.js`。公网未登录的业务 API 返回 401 只证明认证边界，不等于业务验收。带登录态的关键页面及接口、真实 SIP 双向媒体和电话行为分别记录证据；无受控号码时不要拨打真实电话。

**验收：**容器、Compose、前端目录和域名资源均指向本次版本，关联服务正常；明确列出尚未完成的业务验收。

## 5. 回滚与留档

后端失败时，重新核对活动通话，然后用本次 `reach-compose.before.json` 恢复原 API，确认旧容器健康后再原子同步主 Compose。前端异常时，将 `current` 原子指回 `previous-target.txt` 记录的旧目录并验证域名文件哈希；不要覆盖或删除新旧发布目录。若已执行数据库迁移，按该迁移的审阅回滚方案处理，不能直接假设恢复旧镜像即可兼容。

发布记录应写明两个提交 SHA、镜像 ID、前端目标、备份路径、测试及生产验证、故障处理和未验收边界。参见[2026-10-10 发布记录](../livekit-ai-outbound/production-deployed-20261010-all.md)。
