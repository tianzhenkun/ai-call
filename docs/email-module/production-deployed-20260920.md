# REACH 2026-09-20 发布记录

发布版本：`reach-email-20260920T035101Z`；入口：<https://reach.lingchen-ai.com/>。

## Git 与产物

| 对象 | 已推送分支 | 构建提交 |
| --- | --- | --- |
| 后端 ai-call | `codex/feat/reach-platform-integration` | `745ebd4af5faa6d20ce5aeb05549bd3d2c57f276` |
| 前端 lingchen-platform-web | `codex/release-reach-email-20260919` | `9f0c7079880e8dc80a6b82c64aeab3e12a7abb1f` |
| 平台 SDK | `main` | `485b8a77e203bc769c9d24e4109fea723ef1c9b9` |

本次沿用现有分支，未合并前端主分支。源码提交后推送，再从提交构建；排除本地 AGENTS.md 修改、storage、浏览器临时文件及凭据。

- 内容：手动写信与回复、任务设置、跟进排期和附件校验；出站附件最多 5 个、合计 15 MB，MIME 上限 25 MB，入站限制独立维护。
- 镜像：`ai-call-transfer/api:reach-email-20260920T035101Z`，linux/amd64。
- config digest：`sha256:2d198eea6c5d9b01572847c41523f9b5a475d8eda8bd6b5910cfe18548103330`。
- 后端发布目录：`118.25.125.221:/opt/lingchen/deployments/reach-email-20260920T035101Z`。
- 前端发布目录：`81.68.166.109:/home/lingchen/web/ai-reach/releases/reach-email-20260920T035101Z`。
- 发布清单：目录内 `manifest.json` 和 `SHA256SUMS`；包含提交、镜像 config digest、逐文件哈希和六项交付校验。

## 验证和部署

- 后端 202 项测试通过；隔离 PostgreSQL 另外两项通过，迁移连续执行两次通过。
- 前端 10 套、142 项测试通过；Biome、TypeScript、生产构建通过。Ant Design lint 退出码 0，保留原有 18 项弃用和 1 项性能提示。
- 生产运行配置通过当前解析器；Nginx 全局上传限制 100 MB，可容纳附件请求。
- 数据库模型与迁移相对上一版未变；生产 69 张模型表及字段检查通过，无需生产 DDL。
- 发布前无活跃邮件任务、到期队列或发送中邮件，LiveKit 房间数 0。
- 前端新目录逐文件哈希通过，并保留 119 个旧哈希资源。

上传六项交付校验通过，镜像模块导入通过。数据库备份已验证 `pg_restore --list` 可读取；仅重建 API 和邮件 worker，沿用四份 Compose、生产邮箱、加密密钥及平台配置。

新 API 健康接口返回 200，Nacos 注册 `lingchen-reach-python`、`172.20.0.15:19011`、`geo-prod/DEFAULT_GROUP`。两个既有受控账号 SMTP/IMAP 真实连接成功；生产邮件 AI 适配器实际生成有效主题和正文，未保存或发送。平台签名计费资格 `eligible=true`，`meteringEnabled=false` 沿用原设置。

旧 worker 停止后，脚本只读轮询原租约，剩余时间归零才启动新 worker；未清空租约或发送记录。新进程取得新 token，连续续租，重启次数 0，未出现租约冲突。API 与 worker 均 healthy，健康命令为 `workerOnline=true`、`overdueCount=0`、`status=ok`。

两个容器的 image config digest 与清单一致，各自 233 个应用源码文件哈希与构建快照一致。其他媒体、数据库、Redis 和知识库容器维持原实例。

前端已原子切换至本次目录。公网首页 SHA256 为 `91a599152983e89f8f6e28c6a5abecde309d8950f2d4533358569396594f0366`，与新产物一致。本机批量 HTTPS 检查两次遇到 TLS EOF，改从前端服务器访问线上 HTTPS 域名，367 个 JS/CSS/HTML 文件逐一哈希通过；这证明该网络路径的资源一致性，不将本机连接中断解释为代码错误或已定位的网络根因。

验收边界：公网邮件 API 匿名请求返回预期 401；当前浏览器位于登录页，本次未验收登录态页面操作，也未重复真实发送、收件及回复关联。邮箱连接和 AI 草稿生成不代表真实邮件业务全链路通过。

## 备份与回滚

备份目录：`/opt/lingchen/backups/reach-email-20260920T035101Z`。包含数据库 dump、原镜像 ID、全部四份 Compose、环境及受限密钥备份；不提交秘密文件。

回滚目标为 9 月 19 日版本：

- API/worker 镜像：`ai-call-transfer/api:reach-email-20260919T052636Z`；config digest `sha256:681cb73bd4ce5643eedf6c5a1607ea2a50c1ca7241a5901fc5fdf8f5181708c5`。
- 四份 Compose 原路径及顺序保存在备份 `compose-files.txt`，邮件和计费覆盖来自上一版发布目录的 `deploy/`。
- 前端目标：`/home/lingchen/web/ai-reach/releases/reach-email-20260919T052636Z`。
- 先停止新 worker，等待租约自然过期，再按原完整配置恢复；原子切回前端。不清空队列、不重发 unknown、不自动恢复整库。

通用步骤见 [上线操作手册](production-runbook.md)。执行记录文档的后续提交不改变上述构建提交。
