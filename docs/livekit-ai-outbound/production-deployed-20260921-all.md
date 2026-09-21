# REACH 全部本地业务改动发布记录

发布版本 `reach-all-20260921T143422Z`，2026-09-21 北京时间 22:41 切换 API，随后切换前端。入口：<https://reach.lingchen-ai.com/>。

## 代码范围与验证

- 后端提交 `b42d5c6a663ca6e51a79bb8e84b02c9947a9b276`，分支 `codex/feat/reach-platform-integration`，31 个文件，已推送。
- 前端提交 `66b5a8b3400d94059c499f5e97f7f3dbdea55596`，分支 `codex/release-reach-email-20260919`，30 个文件，已推送。
- 包含客户发言审核与连续离题证据、拒绝转人工识别、语音信箱业务统计/重试/质检口径，以及坐席、邮件、提示词、记录、统计和任务页面与全局布局修改。所有本轮业务源码均纳入；只保留未跟踪的 `.playwright-cli/`、`storage/` 本地数据，不上传。
- 后端全量 2169 项中首次 2165 通过、4 项因旧测试缺少新增审核器所需模型配置失败。仅在对应服务构造测试中补充占位 LLM_API_KEY，保留原断言和生产缺配置校验；受影响两组完整重跑 114 项全部通过。没有跳过失败项。
- PostgreSQL 16 隔离并发结算测试 1 项通过，测试容器与卷由脚本清理；首次脚本因 uv 尝试联网获取已固定 SDK 失败，改用 UV_NO_SYNC=1 复用已有环境执行。
- Ruff、diff 检查通过。前端 81 套 660 项测试、全量 Biome lint、TypeScript 和正式构建通过。Jest 曾提示异步句柄退出延迟，最终正常退出。Ant Design 检查退出成功。
- 构建前冻结文件散列，提交后复核匹配。额外改动仅为两份测试夹具，无业务代码修改。

## 产物与部署

- API 镜像 `ai-call-transfer/api:reach-all-20260921T143422Z`，linux/amd64；配置摘要 `sha256:3004eac5ea0de91446ad20160e8d5b6ecdf03de5cd8b6d5a9f535a40f041a53f`。
- 基于上次 call-fix 镜像，复制本次完整 app 和 main.py，无依赖或数据库结构变化。镜像离线导入成功，生产容器 232 个 app 文件散列全部匹配清单。
- 后端目录 `/opt/lingchen/deployments/reach-all-20260921T143422Z`，备份目录 `/opt/lingchen/backups/reach-all-20260921T143422Z`。保留源码包、镜像包、release-manifest.json、SHA256SUMS、api.override.yml、idle-check.py、rollback.sh。
- 沿用原五份 Compose，并追加本次第六份 api.override.yml，仅覆盖 API。REACH_EMAIL_IMAGE 继续使用 seat 版本，邮件代码无变化，邮件 worker 不重建。
- 切换前活动通话、待执行 START_CALL、活动 SIP 预留、活动转人工、LiveKit 房间全部为 0；一条历史 ending 记录保持原状。
- 生产 qwen-plus 文本模型地址和密钥已配置，VAD 仍为 server_vad；未修改生产配置或调用付费模型验证。
- API、邮件 worker 均 healthy、重启 0；API 健康返回 status=ok，Nacos 注册成功。其他容器 ID、生产 .env 和主 Compose 散列不变。无数据库迁移、历史数据批量回写或主动重拨。
- 在生产数据库只读事务中，调用新版统计仓库聚合现有租户历史数据成功，新分类不再返回 voicemail/transport_connected。
- 前端 `81.68.166.109` 的 `/home/lingchen/web/ai-reach/current` 原子切到 `/home/lingchen/web/ai-reach/releases/reach-all-20260921T143422Z`。392 个正式文件校验通过，沿用原 runtime-config.js，保留 170 个旧哈希资源。
- 公网 HTTPS 校验 370 个 JS/CSS/HTML 文件全部匹配；首页 SHA256 `dd6a8adca985d7d8385c99870ba61cbe9b190450acfa79dd0ffb1ced669b9a03`。
- 浏览器登录态已过期，本轮尚未完成登录后的页面验收；已请求用户登录。未自动拨号或发送邮件，隔离回归和只读统计不替代真实通话效果验收。

## 回滚与证据

后端确认空闲后执行本发布目录 rollback.sh，使用原五份 Compose 回到 `ai-call-transfer/api:reach-call-fix-20260921T084006Z`，原配置摘要 `sha256:16a25dda58e07d15f7af60d6c399fcdf923f52deceb50565f6102f033acfe738`。前端原子切回 `/home/lingchen/web/ai-reach/releases/reach-complete-20260921T054131Z`。不清理业务数据，不回滚媒体、TLS 或生产密钥。

本地源码散列、测试/构建/部署日志与核验脚本位于 `build/reach-all-20260921T143422Z/`。后续部署仍应先核对当前 Git 与运行版本、验证源码和构建、备份配置、确认通话空闲、仅更新对应服务、原子切换前端，最后校验健康、文件散列与业务页面。
