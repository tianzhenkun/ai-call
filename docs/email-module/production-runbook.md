# REACH 前后端上线操作手册

适用于当前 REACH Python 后端、平台 REACH 前端及独立邮件 worker。根据 2026-09-18 的实际发布整理；服务器、容器、域名和配置路径是当次基线，下次必须现场核对。本文是操作说明，整理文档不代表再次执行上线。

**顺序：确认范围 → 构建并固定产物 → 核对配置与全部数据库差异 → 备份 → 上传校验 → 迁移 → 后端健康 → 前端切换 → 登录态和受控业务验收 → 留档。**

相关资料：

- [2026-09-18 实际发布及验收记录](production-deployed-20260918.md)
- [2026-09-19 Git 提交发布记录](production-deployed-20260919.md)
- [菜单权限配置记录](production-permissions-20260918.md)
- [邮件 Compose 模板](../../deploy/email-worker/compose.production.yml)
- [计费 Compose 模板](../../deploy/email-worker/compose.credit.yml)
- [worker 托管与健康检查](../../deploy/email-worker/README.md)

## 1. 确认发布对象和当前版本

| 对象 | 基线 |
| --- | --- |
| 前端源码 | 相邻仓库 `lingchen-platform-web`，REACH 产品构建 |
| 后端源码 | 本仓库 `ai-call`；镜像内包含平台 Python SDK |
| 前端服务器 | `81.68.166.109` |
| 前端入口 | `https://reach.lingchen-ai.com/` |
| 前端部署 | `/home/lingchen/web/ai-reach/current` → `releases/<版本>` |
| 后端服务器 | `118.25.125.221` |
| 后端运行目录 | `/opt/ai-call/runtime` |
| 主 Compose | `/opt/ai-call/runtime/compose.yml` |
| 平台连接覆盖 | `/opt/lingchen/deployments/platform-integration-20260903T172008Z/deploy/reach-compose.override.yml` |
| 邮件版基线目录 | `/opt/lingchen/deployments/reach-email-20260918` |
| 容器 | `ai-call-118-api-1`、`ai-call-118-email-worker-1` |
| 平台管理端 | `https://app.lingchen-ai.com/` |
| 数据库 | 同机 `ai-call-118-postgres-1` 内的 `ai_call` |

前端 `/dev-api/` 经平台网关访问 `/reach-api/v1/**`，网关通过 Nacos 找到 REACH API。基线还存在 `/ai-call-agent-api/` 经 WireGuard 转发到后端 19011 的链路；发布不能破坏原外呼入口。不要将源码快照目录当作正在运行的代码目录：API 使用镜像，上传 Python 源文件不会替换容器代码。

后端只读检查：

```bash
hostname
docker ps --filter name=ai-call-118 --format '{{.Names}} {{.Image}} {{.Status}}'
docker inspect ai-call-118-api-1 --format '{{.Image}}'
docker inspect ai-call-118-api-1 --format '{{index .Config.Labels "com.docker.compose.project.config_files"}}'
docker inspect ai-call-118-email-worker-1 --format '{{.Image}}'
df -h /opt
```

前端只读检查：

```bash
hostname
readlink -f /home/lingchen/web/ai-reach/current
df -h /home
```

**可验证结果：**确认各仓库分支、提交、未提交文件；记录当前 API/worker image ID、完整 Compose 顺序、前端软链接目标。下次回滚到这次查到的版本，不能默认退回 9 月 3 日或 9 月 4 日。

## 2. 构建和固定发布包

1. 前端执行本次改动相关测试、类型检查和生产构建。后端执行相关测试、迁移验证和镜像内依赖/模块导入检查。历史测试数量不能代替本次结果。
2. 后端构建 `linux/amd64` 镜像；固定基础镜像、锁定依赖和 SDK 版本。前端构建结果必须带经过当前解析器验证的生产 `runtime-config.js`，不能发布空模板。
3. 发布目录使用唯一版本号，不覆盖旧目录或复用旧镜像标签。包内包括后端快照、前端归档、镜像归档、邮件及计费两个覆盖文件、必要迁移 SQL、来源清单与 SHA256 清单。
4. 禁止打包 `.env`、本地数据库、录音、邮箱凭据和其他密钥。密钥通过受限部署通道单独配置。
5. 正式 `lingchen-release` 发布按其当前 `source-lock.json`、`release-plan.json` 和干净源码要求执行。本次这类含未提交改动的候选快照，必须明确记录逐文件哈希，不能仅以 Git HEAD 表示来源。

后续要求 Git 留档的发布，应先将业务代码和必要文档提交、推送，确认远端提交一致，再从对应提交构建。分支发生分叉时不强推；独立发布分支应注明尚未合入主分支。部署后的执行记录可单独提交，记录中的构建提交保持原值。

**可验证结果：**本地归档校验通过，来源可追踪，部署的确为已测试产物。Docker 的 OCI index digest 与旧版 Docker 显示的 image config ID 可能不同，应读取导出包的 `index.json`、`manifest.json` 确认层级；不能看到 ID 不同就跳过验证。

## 3. 切换前补齐配置、权限和迁移

### 配置核对

生产 API 需要主 Compose、平台覆盖、邮件覆盖、计费覆盖，按此顺序合并。API 与 worker 使用同一新镜像，数据库、邮件加密密钥、AI 和对象存储配置必须一致；worker 不开放业务端口，也不承担 Nacos API 注册。

| 配置 | 要求 |
| --- | --- |
| 邮件 AI | 显式确认模型与地址；基线为 `https://api.deepseek.com` / `deepseek-v4-pro` |
| AI 密钥 | `/opt/ai-call/runtime/secrets/email/llm-api-key` |
| 邮箱加密密钥 | 同目录 `encryption-key`；后续升级必须保留，不能重新生成 |
| 邮件密钥权限 | 基线目录 0700、文件 0400、UID 10001；镜像用户变化时重新验证可读性 |
| 平台 JWT / Nacos | 保留原启动包装和密钥挂载，不覆盖为本地值 |
| 正式外呼计费 | API 加载生产 REACH 专用签名密钥；基线网关 `http://geo-uat-ruoyi-gateway:19090` |
| 对象存储 | 核对数据库 active OSS 配置，并实际验证附件上传/读取 |

只检查配置存在性和一致性，不打印密钥。不把包含解析后环境变量的 `docker compose config` 输出贴到日志；若留档，仅写入受限目录。`docker exec python` 不自动继承入口脚本后来 export 的变量，因此不能据此断言主进程没有 JWT/Nacos/计费密钥。

### 权限核对

在平台管理端确认“邮件管理”：仅产品端、路由 `reach/email`、组件 `reach/email/index`、权限 `reach:email:manage`。确认 REACH 产品菜单根、目标租户套餐、目标用户角色三层关联。已有配置不重复登记。

需要变更时，保留原菜单选择，只增加目标菜单；“同步套餐”会重设租户管理员菜单并清理其他角色超出套餐的权限，执行前明确目标租户及影响。基线租户为 960001，但不是所有未来发布的固定目标。

### 数据库和切换窗口

- **比较新镜像全部相关模型与生产 schema，再生成迁移清单。**检查表、字段及改动涉及的约束、索引、默认值和数据兼容性；不能只检查邮件模块。
- 邮件使用 `python -m app.services.reach_email.migrate --apply`。2026-09-18 还需要仓库已有 `docs/livekit-ai-outbound/sql/phase-k2-credit-metering-postgres.sql`，建立 `reach_credit_usage_outbox`；下次先查是否已存在及定义是否符合新版本。
- 在隔离数据库验证适用迁移及重复执行。生产上只执行本次已审阅的迁移，不使用全量 `metadata.create_all` 代替差异分析。
- 核对外呼任务调度和 LiveKit 实际房间/参与者，安排无活跃通话的窗口；不能仅凭历史 `ending` 状态判断当前通话。切换前再次复核，防止检查后新任务启动。
- 后续升级还需处理**邮件发送中的任务**：停止新增执行入口、等待当前 SMTP 操作收尾，再按既有租约规则切换 worker。`unknown` 邮件不能盲目重发，也不能删除租约抢占。

当前 worker 全局租约有效期为 120 秒，容器停止宽限期为 90 秒。旧进程退出后租约可能仍有效；先读 `reach_email_worker_lease.expires_at`，等到过期再启动新 worker。若新 worker 已启动并因 `EMAIL_WORKER_ALREADY_RUNNING` 重启，等待租约自然过期并确认新 token、持续续租和稳定进程，不能仅凭健康接口看到旧租约就判定接管成功。未来排队但尚未到期的邮件应保留原状态和时间。

**可验证结果：**权限、依赖配置和迁移均有明确结论；不存在影响现有外呼的未解决缺项，切换窗口可用。

## 4. 备份并保存回滚基线

以下后端命令使用 Bash。将版本号改为本次唯一值；目录存在时停止调查，不覆盖旧备份。

```bash
set -euo pipefail
umask 077
release_id='reach-email-YYYYMMDDTHHMMSSZ'
backup_dir="/opt/lingchen/backups/$release_id"
mkdir "$backup_dir"
cp /opt/ai-call/runtime/compose.yml "$backup_dir/compose.yml"
cp /opt/ai-call/runtime/.env "$backup_dir/runtime.env"
cp -a /opt/ai-call/runtime/secrets "$backup_dir/secrets"
docker inspect ai-call-118-api-1 --format '{{.Image}}' > "$backup_dir/api-image-id.txt"
docker inspect ai-call-118-api-1 --format '{{index .Config.Labels "com.docker.compose.project.config_files"}}' > "$backup_dir/compose-files.txt"
docker inspect ai-call-118-email-worker-1 --format '{{.Image}}' > "$backup_dir/worker-image-id.txt"
docker exec ai-call-118-postgres-1 sh -c 'pg_dump -U "$POSTGRES_USER" -d ai_call -Fc' > "$backup_dir/ai_call.dump"
docker exec -i ai-call-118-postgres-1 pg_restore --list < "$backup_dir/ai_call.dump" > "$backup_dir/restore-list.txt"
test -s "$backup_dir/restore-list.txt"
sha256sum "$backup_dir/ai_call.dump" > "$backup_dir/database.sha256"
```

另将 `compose-files.txt` 列出的**全部覆盖文件**备份，并保留其原路径、顺序及运行工作目录；不要只备份主文件。首次尚无 worker 时，跳过 worker inspect 并记录“不存在”。前端保存 `readlink -f current` 的结果及运行配置。备份包含秘密，不能加入普通发布包。归档可列出只证明可读取，完整恢复能力需隔离恢复验证。

## 5. 上传校验并执行后端部署

SSH/SCP 或 Termius SFTP 均可，使用已验证的主机身份。上传到新目录 `/opt/lingchen/deployments/<版本>`，先校验 SHA256，再解压快照及加载镜像。前端包上传到前端服务器的新 release 目录，单独校验，暂不切换。

后端部署目录约定：

```text
<版本>/
  api-amd64.tar.gz
  backend.tar.gz
  manifest.json
  SHA256SUMS
  deploy/reach-compose.email.yml   # 来自 compose.production.yml
  deploy/compose.credit.yml
  deploy/本次所需迁移.sql
```

邮件和计费配置、迁移文件也必须纳入校验清单。检查上传文件集与清单一致，清单缺文件时不能忽略报错。

```bash
# 后端服务器；继续使用上节的 release_id，或在新终端重新设置。
release_dir="/opt/lingchen/deployments/$release_id"
cd "$release_dir"
sha256sum -c SHA256SUMS
docker load -i api-amd64.tar.gz

# 替换成已经核对归档 config digest 的新镜像标签或本机 image ID。
export REACH_EMAIL_IMAGE='ai-call-transfer/api:本次唯一版本'
docker image inspect "$REACH_EMAIL_IMAGE" --format '{{.Id}} {{.Architecture}}'
cd /opt/ai-call/runtime
dc=(docker compose -p ai-call-118
  -f /opt/ai-call/runtime/compose.yml
  -f /opt/lingchen/deployments/platform-integration-20260903T172008Z/deploy/reach-compose.override.yml
  -f "$release_dir/deploy/reach-compose.email.yml"
  -f "$release_dir/deploy/compose.credit.yml")
"${dc[@]}" config --quiet
"${dc[@]}" run --rm --no-deps --entrypoint python api -m app.services.reach_email.migrate
# 此处输出的是邮件表清单，不是完整 schema 差异或 SQL dry-run。
"${dc[@]}" run --rm --no-deps --entrypoint python api -m app.services.reach_email.migrate --apply
# 其他已审阅 SQL 在此执行，使用 psql -v ON_ERROR_STOP=1；失败则停止切换。
"${dc[@]}" up -d --no-deps api email-worker
"${dc[@]}" ps api email-worker
"${dc[@]}" exec -T email-worker python -m app.services.reach_email.health
"${dc[@]}" exec -T api python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:19011/ai-call/health', timeout=5).status)"
```

通过 SSH 执行多行脚本时，优先上传脚本后按文件执行。`docker compose run` 默认可读取标准输入，可能消费 `ssh ... bash -s` 后续脚本文本；需要明确禁用交互输入，不能只凭 SSH 退出码认定后续切换命令执行过。

**可验证结果：**实际容器 image ID 正确，API 和 worker 都 healthy；worker 输出 `workerOnline=true`、`overdueCount=0`、`status=ok`；Nacos 注册地址对应真实 API；启动及观察窗口无未解决异常。计费资格检查使用真实目标租户/用户，不能改平台计费开关或伪造身份绕过失败。

只重建 `api email-worker`，不执行整个项目 `down`，不加 `--remove-orphans` 删除知识库等其他服务。健康失败先恢复已知可用版本再排查，不提前切换前端。Docker 的 unhealthy 本身不等于自动重启，不能把 restart 策略当作完整告警方案。

## 6. 前端原子切换

在前端服务器定义本次 `release_id`。归档已校验，且顶层目录确认为 `frontend/` 后再使用 `--strip-components=1`。

```bash
set -euo pipefail
release_id='reach-email-YYYYMMDDTHHMMSSZ'
web_root=/home/lingchen/web/ai-reach
cd "$web_root"
old_frontend=$(readlink -f current)
new_frontend="$web_root/releases/$release_id"
mkdir "$new_frontend"
printf '%s\n' "$old_frontend" > "releases/$release_id.previous-target.txt"
tar -xzf "releases/$release_id.tar.gz" -C "$new_frontend" --strip-components=1
test -s "$new_frontend/index.html"
test -s "$new_frontend/runtime-config.js"
```

核对运行配置和资源清单。若生产运行配置不变，比较新旧 `runtime-config.js`；若确有变更，按本次批准的配置验证。**切换前保留旧页面可能请求的旧哈希 JS/CSS/资源**：只补新目录中不存在的文件，不覆盖新版本文件，不把旧 index.html 或旧运行配置覆盖过来。有同名不同内容时停止调查，不能盲目复制；也可采用经验证的 Nginx 旧资源回退策略。

```bash
ln -s "$new_frontend" "current.$release_id"
mv -Tf "current.$release_id" current
readlink -f current
curl -fsS https://reach.lingchen-ai.com/ | sha256sum
sha256sum current/index.html
```

**可验证结果：**公网 HTML 与新 index.html 一致，新页面及原来已打开页面的懒加载路由无资源 404。只验证首页 200 不足以证明前端发布成功。

## 7. 业务验收与留档

| 检查 | 通过证据 |
| --- | --- |
| 登录和权限 | 正常登录用户看到邮件菜单，实际路由可访问，不是待迁移占位页 |
| 邮件 API | 任务、发送记录、线索、账号、worker-health 请求成功；空列表不冒充已有数据验证 |
| 编辑与任务 | 名单校验、必要资料校验、保存/读取、状态转换符合当前版本要求 |
| AI | 从生产网络通过当前邮件适配器实际生成有效内容；不向模型提交无关客户数据 |
| 附件 | 真实上传/读取、大小及哈希一致；收件附件也核对 |
| SMTP/IMAP | 每个本次使用的邮箱分别连接成功 |
| 真实发送与收件 | 仅使用已授权受控地址；分别记录 SMTP accepted 与收件箱收到 |
| 回复关联 | 标准回复关联到原任务/线索；无法证明关联的邮件保留未匹配 |
| 原功能 | 原数据看板和本次影响的业务请求正常；页面健康不等于真实电话媒体验收 |
| worker | 心跳、队列积压及日志正常；发送结果不明时保留证据，不盲目重发 |

账号迁移只在需要且已授权时做：保留租户/用户归属、权重、限额和 IMAP 同步位置；解密后通过安全通道传递，并以生产密钥重新加密。不重复导入已有账号，不把本地待发送任务或客户名单顺带迁入。下一次上线通常直接沿用生产邮箱和加密密钥。

记录版本、提交及文件哈希、构建/测试结果、迁移清单、完整 Compose 命令、实际 image ID、前端目标、备份位置、测试任务 ID 和验证边界。SMTP accepted、独立收件观察、产品“已确认送达”统计采用的证据不同，不能为让计数一致而修改业务数据。

## 8. 回滚

触发条件：启动/迁移失败、关键接口不可用、权限异常、无法解释的发信异常或原业务回归失败。

1. 停止新邮件 worker，确认进程已退出；保留队列、租约及 unknown 记录。不要同时运行两个版本的 worker。
2. 使用第 1、4 步保存的**上一版本完整 Compose 文件列表与镜像**恢复。下一次发布的上一版已包含邮件和计费覆盖，不能照抄首次发布“只用旧两份 Compose”的回滚命令。上一版 API/worker 镜像若不同，分别指定，不用一个变量覆盖两者。
3. 若上一版含 worker，恢复 API 和 worker；若上一版没有邮件功能，只恢复 API，保持新 worker 停止。验证实际 image ID、健康和原业务接口。
4. 前端用临时软链接加 `mv -Tf` 原子切回本次保存的 `previous-target.txt` 目标，核对公网资源。
5. 默认保留新增表及生产密钥，不 DROP、不自动整库恢复。恢复整库可能丢弃发布后的业务写入，须另行评估。已经 SMTP 接受或结果不明的邮件不得重新排队。

回滚也要形成执行记录：恢复目标、时间、实际健康结果和仍需处理的数据，不将“已执行回滚命令”视为恢复成功。
