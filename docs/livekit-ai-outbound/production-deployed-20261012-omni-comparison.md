# 2026-10-12 Omni Plus / Flash SIP 对照发布

状态：2026-10-12 已完成线上 API 与前端发布，健康、注册及公网资源校验通过；尚未进行本版本的真实 SIP 通话验收。

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
- `npm run build` 正式构建通过，发布产物共 407 个文件。

## 线上发布证据

- 发布编号：`reach-omni-comparison-20261012T022911Z`。
- 后端已部署提交：`3d9e70e13258b1680ba79fd54fd8c603d8ccc82b`；前端已部署提交：`cd3dfdac6944b55be70fe85a307b710e09a5e2d0`。两个提交均已推送到上述发布分支。
- 发布包 SHA-256：`4d0d39dad6d03a03378fdf5bee6f37bfe43b42708ba49be1bb0c5c7dbef31304`，服务器上传后校验一致。
- API 镜像：`ai-call-transfer/api:reach-omni-comparison-20261012T022911Z`，ID 为 `sha256:33c13906af9abd0f663ae275be8cd962fd7a9c3a4ba31b1d55efe1d6bb845bba`。部署前校验实际旧镜像的相关源码，部署后校验 23 个发布源码文件与清单一致。
- 发布目录：`/opt/lingchen/deployments/reach-omni-comparison-20261012T022911Z`。数据库备份 `ai-call-before.dump` 为 1,662,600 字节，`pg_restore -l` 检查通过；Compose 前后版本、旧镜像 ID 和前端目标均保存在此目录。
- 切换前活动通话、待处理启动命令、SIP 预留、转接及 LiveKit 房间数均为 0。保留已有的 1 条 `ending` 历史记录，没有清理业务数据。
- API 容器健康、重启数为 0，内部 `/ai-call/health` 返回 `200 {"status":"ok"}`，`lingchen-reach-python` 在生产 Nacos 注册成功。邮件与知识解析 worker 的容器 ID、镜像和健康状态保持不变。
- 容器内实测默认模型仍为 Plus，`server_vad`、发言审核关闭、owner 入口 `web,outbound` 和 SIP 拨号模式保持不变。两种模型均通过当前配置下的校验，创建任务请求 schema 同时接受 Plus / Flash；此检查没有连接模型或发起通话。
- 主 Compose 仅替换 API 的镜像。前端 `current` 指向 `/home/lingchen/web/ai-reach/releases/reach-omni-comparison-20261012T022911Z`，保留生产 `runtime-config.js` 和 389 个旧 JS/CSS 资源；新文件哈希和 259 项公网页面及静态资源哈希检查通过。
- 浏览器访问线上任务创建入口正常跳转到登录页并渲染。当前浏览器没有线上登录态，本次未提交线上任务或发起电话。

## 发布与回滚

按 `docs/email-module/production-runbook.md` 在 `110.42.223.109` 发布。复用 Termius 已保存的认证连接，不改变 SSH 凭据。

发布包固定提交和 SHA-256。准备阶段校验当前镜像源码，备份数据库、Compose 与前端目标，只构建新 API 镜像并预备前端目录；切换前再次检查活动通话、启动命令、SIP 预留、转接和 LiveKit 房间。仅更新 API 与前端，保留生产 `runtime-config.js` 和旧静态资源。

回滚前先暂停本次新建的 Plus / Flash 对照任务并等待通话结束，避免旧 API 执行带 Flash 配置的新任务。随后恢复本次保存的旧 API Compose 与前端软链接；本次无迁移，不直接恢复数据库。

## 电话验收边界

本次发布提供受控对比入口，不代表 Flash 已通过电话验收。使用同一测试号码、SIP 线路、话术、冻结知识范围和音色，检查响应等待、插话与恢复、转人工、费用回答、结束及录音。保留原始事件、分轨录音和实际模型记录后再决定是否替换默认模型。
