# REACH 坐席与通话结束策略发布记录

发布版本：`reach-seat-20260921T051325Z`。入口：<https://reach.lingchen-ai.com/>。

## 源码与授权范围

- 后端提交 `35b8aa93142976a78077ed20ba26a1994ae95854`，分支 `codex/feat/reach-platform-integration`。
- 前端提交 `9c46f3bf6b703a3044bb93c85dc3c2193d081dbd`，分支 `codex/release-reach-email-20260919`，未合并 main。
- 两者均已推送；只包含交接 source-lock 的后端 38、前端 26 个白名单文件，提交内容逐文件匹配验收快照。未提交本地邮件、导航、AGENTS、.gitignore 及运行数据。
- 交接目录 `build/reach-seat-preflight-20260921-WnRPKi/`；source-lock SHA256 `abb7b2e0725d281b67c79971e76fa11b1d953a52abdaf466fa7254dcebe0e479`。验收证据和 388 个正式前端文件散列均重新核对通过。前端使用该正式产物，后端从白名单提交导出构建。
- 本次不实施延期的无工具转接承诺预播校验，不发起电话、批量拨号或其他有费用测试。

## 测试与构建

交接验收：后端 18 文件 855 项通过、17 个警告，Ruff 通过；前端 28 套 232 项通过，Biome、TypeScript、构建通过，Ant Design 检查退出 0、保留 18 项既有弃用和 1 项性能提示。独立 PostgreSQL 16 迁移测试通过，验证重复执行及旧数据保留。合成 API 配合正式 bundle 的 15 个布局场景通过，不能作为真实电话媒体验收。

- 镜像 `ai-call-transfer/api:reach-seat-20260921T051325Z`，linux/amd64，实际 Python 3.12.9，pip check 通过。
- config digest `sha256:0633cbfb3019161a2fb2d2fec3fa005d0f31f52920ef7bbba6a59e8a40ecdfb7`。
- SDK 沿用 `485b8a77e203bc769c9d24e4109fea723ef1c9b9`，未升级 LiveKit SDK。
- 后端发布目录 `/opt/lingchen/deployments/reach-seat-20260921T051325Z`；manifest.json、source-lock.json、verification.json 和 SHA256SUMS 留档，九项交付校验通过。

## 迁移、备份与安全窗口

后端 `118.25.125.221`。备份 `/opt/lingchen/backups/reach-seat-20260921T051325Z` 包含数据库 dump、可读取目录验证、完整四份 Compose、原镜像 ID 和受限环境/密钥备份，不加入 Git。

切换前活动转人工状态计数为 0，LiveKit 房间为 0，无活动或到期邮件；唯一未结束通话记录为 2026-08-11 的历史 ending 记录，未手工改写。

在正确数据库 ai_call 执行 `handoff-exception-close-lease.sql`，lock_timeout 5 秒、statement_timeout 30 秒，事务提交成功。三个新增 nullable 列为 `exception_close_token`、`exception_close_expires_at`、`exception_prompt_completed_at`。新镜像对生产 69 张模型表和字段核对通过。

只切换 API 与邮件 worker，保留生产 .env、证书、密钥和现有 TURN/TLS/DNS/安全组，未部署仓库内基础设施模板。旧邮件 worker 退出后等待原租约自然过期再启动新 worker。

## 上线验收

- API 与邮件 worker 均 healthy，重启计数均为 0；两个容器各 233 个应用文件散列匹配发布 manifest。API 健康接口返回 200，Nacos 注册正常。
- worker 已取得新租约，健康检查 workerOnline=true、overdueCount=0；观察期未发现租约冲突重启或恢复错误。
- 前端服务器 `81.68.166.109` 的 `/home/lingchen/web/ai-reach/current` 已原子切换到 `/home/lingchen/web/ai-reach/releases/reach-seat-20260921T051325Z`。保留 143 个旧哈希资源，避免已打开页面引用失效。
- 公网首页 SHA256 为 `257c8afe2431c29593602add7825038dcddd02e3f079d467a318e3abcba1240d`，与发布文件一致；通过 HTTPS 域名核对 366 个 JS/CSS/HTML 文件，散列全部匹配。
- 使用已有登录态验收：新版坐席工作台正常显示离线和空等待队列；通话列表正常返回 53 条历史记录；详情正常加载基本信息、结束原因及转人工结果，其中历史样本显示“转人工等待超时”。未上线接听、未播放录音、未修改业务记录。
- 切换前后生产 .env、主 Compose 散列，以及 LiveKit/SIP/Redis 等媒体容器 ID 和启动时间一致；未操作本地联调服务或 Java 服务。
- 本次未发起真实通话、邮件发送或付费 AI 生成。上述结果证明发布一致性、服务健康与只读页面链路，不替代真实通话、接管及媒体验收。

本地发布日志与核验脚本保留于 `build/reach-seat-20260921T051325Z/`，含 `deploy-backend.log`、`migrate.log`、`cutover.log`、`verify-public.log`。本记录单独提交，不改变镜像对应的后端代码提交。

## 回滚点

- API/worker：`ai-call-transfer/api:reach-email-20260920T035101Z`，config digest `sha256:2d198eea6c5d9b01572847c41523f9b5a475d8eda8bd6b5910cfe18548103330`。
- 原邮件与计费覆盖来自 `/opt/lingchen/deployments/reach-email-20260920T035101Z/deploy/`，四份配置顺序记录于备份 `compose-files.txt`。
- 前端：`/home/lingchen/web/ai-reach/releases/reach-email-20260920T051012Z`。回滚使用临时软链接原子切回。
- 回滚保留三个兼容列，不删列、不清业务数据、不恢复整库、不回滚 TURN/TLS。
