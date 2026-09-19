# REACH 2026-09-19 发布记录

发布版本：`reach-email-20260919T052636Z`。入口：<https://reach.lingchen-ai.com/>。

## Git 与产物来源

| 对象 | 已推送分支 | 构建提交 |
| --- | --- | --- |
| 后端 ai-call | `codex/feat/reach-platform-integration` | `7906919de13f71c901d60dd3f8350006d510c37e` |
| 前端 lingchen-platform-web | `codex/release-reach-email-20260919` | `730890e580cb9fe4cdee5dbba05424cd54c3e001` |
| 平台 Python SDK | `main` | `485b8a77e203bc769c9d24e4109fea723ef1c9b9` |

前端本地主分支与远程分叉，本次创建发布分支保存当前 REACH 代码，未合并远程两笔 Sales 提交，未强推主分支。后端保留工作区原有 AGENTS.md 修改及本地数据，不纳入发布提交。密钥、storage 和浏览器临时文件未提交。

后端从 Git 提交导出源码构建 linux/amd64 镜像；依赖锁与上一版一致，SDK wheel 沿用上述干净提交。前端在干净发布分支完成生产构建，运行配置沿用线上并通过当前解析器验证。

- 镜像：`ai-call-transfer/api:reach-email-20260919T052636Z`
- 镜像 config digest：`sha256:681cb73bd4ce5643eedf6c5a1607ea2a50c1ca7241a5901fc5fdf8f5181708c5`
- 后端目录：`118.25.125.221:/opt/lingchen/deployments/reach-email-20260919T052636Z`
- 前端目录：`81.68.166.109:/home/lingchen/web/ai-reach/releases/reach-email-20260919T052636Z`
- 来源及文件校验：发布目录内 `manifest.json`、`SHA256SUMS`。

## 发布前验证

- 后端 132 项测试通过；隔离 PostgreSQL 两项测试通过，邮件迁移连续执行两次通过。
- 前端 10 套测试、80 项通过；Biome、TypeScript、生产构建通过。
- Ant Design lint 退出码 0，现有 18 项弃用和 1 项性能提示未扩散修改。
- 镜像 `pip check`、实际模块导入通过；上传的六项交付文件校验通过。
- 相比上一版，数据库模型及迁移未变；生产 69 张模型表和字段检查通过，本次未执行生产 DDL。
- 切换前无活跃邮件任务、到期队列或发送中邮件，LiveKit 房间数为 0。已有一条未来执行的排队回复，保留原时间及状态。
- 前端逐文件哈希验证通过，保留 115 个旧哈希资源；未覆盖新产物及线上运行配置。

## 备份与切换

数据库备份及完整配置位于 `/opt/lingchen/backups/reach-email-20260919T052636Z`，包括 pg_dump 自定义格式备份、可读取目录验证、原 API/worker 镜像 ID、全部四份 Compose、运行环境和密钥备份。该目录受限，不加入 Git。

使用主 Compose、平台覆盖、本次邮件覆盖、本次计费覆盖，项目名 `ai-call-118`。停止旧 worker 后只重建 API 和邮件 worker；其他媒体及知识库容器不重启。沿用生产邮箱、邮件加密密钥、AI 和平台签名配置。

## 上线后验收

- API 与 worker 实际镜像 config digest 均与发布清单一致；各自 232 个应用源码文件哈希与提交构建快照一致。
- API 健康地址返回 200，Nacos 注册 `lingchen-reach-python`、`172.20.0.15:19011`、`geo-prod/DEFAULT_GROUP` 正常。
- 旧 worker 退出后租约尚未过期，新 worker 曾出现 `EMAIL_WORKER_ALREADY_RUNNING`。等待 120 秒租约自然过期后重启新 worker，取得新 token 并持续续租；没有删除租约或修改队列。
- worker 健康命令返回 `workerOnline=true`、`overdueCount=0`、`status=ok`；接管后进程稳定，未出现新的 worker 错误。
- 两个既有受控账号的 SMTP/IMAP 真实登录连接成功，生产加密凭据可正常读取。
- 通过本次镜像的邮件 AI 适配器实际生成有效主题及正文，不保存草稿、不触发发送。
- 生产平台签名计费资格检查 `eligible=true`，`meteringEnabled=false` 沿用平台原配置。
- 前端 `current` 已指向本次目录，公网首页 SHA256 为 `7b86847346a028d6f9e621dcb90d19d65bc7c382407f76e6af643cfbc5a9870a`，与发布文件一致；367 个 JS/CSS/HTML 公网文件逐一哈希核对通过。
- 公网邮件 API 匿名访问返回预期 401。当前浏览器停在平台登录页，本次未复验登录态页面操作，也未重复执行真实发送、收件及回复关联；这些业务链路的上一轮证据见 9 月 18 日记录，不能当作本次复验结果。
- LiveKit/SIP/FreeSWITCH、数据库、Redis 及知识库容器维持原运行实例；未拨打真实电话。

非阻断的现有边界：历史版本接口默认返回 20 条，前端没有更早历史的分页入口；本次发布未扩大范围修改。

## 回滚目标

- 上一版 API/worker：`ai-call-transfer/api:reach-email-20260918-candidate`，config ID `sha256:c8f917060a40fb9e342b44d770691b7fc63ea862ef2e4b474e451d77594abcf2`。
- 上一版四份 Compose 的原路径、顺序保存在备份 `compose-files.txt`；邮件及计费覆盖来自 `/opt/lingchen/deployments/reach-email-20260918/deploy/`。
- 上一版前端：`/home/lingchen/web/ai-reach/releases/reach-email-20260918`。
- 回滚时先停新 worker，再用原完整配置恢复 API/worker，原子切回前端；不清空队列、不重发 unknown 邮件，不自动恢复整库。

后续操作遵循 [上线操作手册](production-runbook.md)，下一次以上述发布结果为起点重新现场核查。
