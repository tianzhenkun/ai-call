# 2026-10-12 Omni Plus / Flash SIP 对照发布

状态：发布候选已完成相关回归，尚未切换线上。

## 来源与范围

- 后端基线：`f1c495746a3d42710e8076546ef288d5dc4a6c56`，现场镜像 `ai-call-transfer/api:reach-cancel-recovery-20261010T112535Z`。
- 前端基线：`504d30a2b10f6a66d9fde3e5b15890fd2b477b22`，现场目录 `reach-non-qwen-20261010T094237Z`。
- 两个发布工作区位于 `/Users/liuhongli/.codex/worktrees/omni-sip-comparison/`，分支均为 `codex/release-omni-sip-comparison-20261012`。
- 新建任务可选择 `qwen3.5-omni-plus-realtime` 或 `qwen3.5-omni-flash-realtime`，默认 Plus。模型和匹配音色进入同一份任务校验、冻结快照与创建请求。
- 模型穿过任务执行器、持久化命令、owner 分配和 LiveKit 会话，最终用于供应商连接。自定义音色按租户、状态和目标模型校验。
- 通话配置事件和知识查询审计记录本次模型，供 SIP 对照核验。
- 现有全局 Plus、`server_vad`、关闭客户发言审核、打断、收尾、历史轮次和恢复策略保持原配置与实现。已有未指定模型的请求仍使用默认值。
- 本次不涉及表结构、依赖、公共 Java、邮件 worker、知识解析 worker、LiveKit 或 SIP 栈变更。

## 本地验证

- 后端相关 12 个测试文件覆盖 1041 项；新增事件观测修正后，涉及的 agent 测试 368 项复跑通过，其余相关测试在完整运行中通过。测试隔离外部媒体与数据库，不构成真实电话验收。
- Python 修改文件通过 Ruff `F,E9` 检查，差异通过 `git diff --check`。
- 前端任务创建、详情、服务及确认流程共 7 个测试文件、49 项通过；TypeScript 检查通过，Biome 对 `src` 的 1080 个文件检查通过，AntD 检查无错误，保留既有提示。
- 在 `.codex` 工作区中直接执行 `npm run lint` 被原有忽略配置跳过，因此分别执行完整 `src` lint 和 TypeScript 检查。正式构建需使用工作区内的依赖目录，utoopack 不支持指向工作区外的 `node_modules` 软链接。

## 发布与回滚

按 `docs/email-module/production-runbook.md` 在 `110.42.223.109` 发布。复用 Termius 已保存的认证连接，不改变 SSH 凭据。

发布包固定提交和 SHA-256。准备阶段校验当前镜像源码，备份数据库、Compose 与前端目标，只构建新 API 镜像并预备前端目录；切换前再次检查活动通话、启动命令、SIP 预留、转接和 LiveKit 房间。仅更新 API 与前端，保留生产 `runtime-config.js` 和旧静态资源。

回滚前先暂停本次新建的 Plus / Flash 对照任务并等待通话结束，避免旧 API 执行带 Flash 配置的新任务。随后恢复本次保存的旧 API Compose 与前端软链接；本次无迁移，不直接恢复数据库。

## 电话验收边界

本次发布提供受控对比入口，不代表 Flash 已通过电话验收。使用同一测试号码、SIP 线路、话术、冻结知识范围和音色，检查响应等待、插话与恢复、转人工、费用回答、结束及录音。保留原始事件、分轨录音和实际模型记录后再决定是否替换默认模型。
