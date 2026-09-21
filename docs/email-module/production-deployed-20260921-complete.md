# REACH 剩余本地改动发布记录

版本：`reach-complete-20260921T054131Z`。入口：<https://reach.lingchen-ai.com/>。

## 发布范围与 Git

- 前端 `9723039a`：邮件模板及回复弹窗保留 AI 生成和中文审阅翻译，移除译文 AI 修改入口；邮件导航增加分组间隔。五个剩余修改文件全部提交并推送到 `codex/release-reach-email-20260919`。
- 后端仓库 `514cc41`：同步邮件设计文档、AGENTS.md 和运行目录忽略规则，已推送到 `codex/feat/reach-platform-integration`。
- 当前没有剩余未提交业务代码。`.playwright-cli/` 和 `storage/` 属于本地验收或运行数据，未提交、未上传。
- 后端业务代码与上一轮部署相同，沿用 `35b8aa93142976a78077ed20ba26a1994ae95854` 对应镜像 `ai-call-transfer/api:reach-seat-20260921T051325Z`，未重启 API 或 worker，无新增数据库迁移。

## 构建与验证

- 邮件模块 8 套、127 项测试通过；全量 Biome lint、TypeScript 和正式构建通过。Ant Design 检查通过，保留既有 18 项弃用和 1 项性能提示。
- 隔离快照首次构建因 utoopack 不接受指向根目录外的 node_modules 符号链接而失败；改用原仓库构建。构建前后及提交后逐项校验五个修改文件散列一致，前端工作区干净。
- 正式 dist 共 388 个文件，runtime-config.js 使用并校验现有生产配置；远端解包逐文件校验，保留 147 个旧哈希资源。
- 通过线上 HTTPS 域名校验 366 个 JS/CSS/HTML 文件，全部匹配发布文件。
- 首页 SHA256：`f9bd027865ce2c4b5df456d78ee034d3c2fa1d1f7010fae7afbf463bf7d9d603`。
- 使用现有登录态验证：邮件列表加载 6 个任务，邮件模板弹窗正常加载，显示“AI 生成要求”和中文审阅翻译开关，不显示“AI 修改”；取消关闭，没有保存、发送或调用 AI。
- API 与邮件 worker 均 healthy，重启计数 0。测试中的 AI 请求使用模拟，不能替代真实 AI 生成和 SMTP/IMAP 验收；本次未拨号或发送邮件。

## 路径与回滚

前端服务器：`81.68.166.109`。`/home/lingchen/web/ai-reach/current` 原子切换到 `/home/lingchen/web/ai-reach/releases/reach-complete-20260921T054131Z`。同目录保留发布压缩包、manifest 文件及 previous-target 文件。生产配置、后端和媒体服务均未调整。

回滚目标：`/home/lingchen/web/ai-reach/releases/reach-seat-20260921T051325Z`，通过临时软链接配合 `mv -Tf` 原子切回 current，无需回滚数据库。

本地日志、文件散列和核验脚本：`build/reach-complete-20260921T054131Z/`。
