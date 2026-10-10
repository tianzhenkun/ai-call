# 2026-10-10 回复停滞恢复发布记录

北京时间 15:09，在 `110.42.223.109` 发布 API 提交 `69d5ae06b537ddf7870c6de2bc2d0bd2267abb92`，分支 `codex/reach-response-recovery` 已推送。变更为回复停滞和取消无确认的有界恢复、旧事件隔离、故障提示及诊断事件，行为和测试边界见[恢复说明](response-recovery.md)。没有数据库 schema 变更。

发布目录为 `/opt/lingchen/deployments/reach-response-recovery-20261010T070017Z`。API 镜像为 `ai-call-transfer/api:reach-response-recovery-20261010T070017Z`，image ID 为 `sha256:44e38aabef02320e397e67c3408d8691080da46989f03ec9fbd8dae082d80f49`。发布包 SHA-256 为 `d288eb95e2e7ee6c9547644bcf0958aaf3a42f8eb7501cdba551a53f1856d302`。

发布前，6 个相关测试套件共 749 项通过，Ruff 和差异检查通过。真实 `qwen3.5-omni-plus-realtime` 接口的正常回复、原连接恢复、换连接恢复各验证 1 次，分别输出 16、50、46 帧音频；恢复后保留合成测试上下文。测试人为丢弃输出与确认事件，音频仅送至计数接收器，不经过 SIP 或客户电话。

现场核对旧镜像及三个 Python 文件哈希与基线一致；新镜像仅替换三个 Python 文件并增加固定提示 WAV，四个文件哈希均与已提交源码一致。新旧 Compose 经结构比较仅 API 镜像字段不同。保存了 `reach-compose.before.json`、`reach-compose.after.json`、新旧镜像 ID、文件清单及 `ai-call-before.dump`；数据库归档为 1,492,489 字节，`pg_restore -l` 检查通过。完整配置及数据库只保存在服务器受限目录。

准备阶段和切换前分别确认：活动通话、待拨命令、SIP 预留、活跃转接、LiveKit 房间均为 0。另有 1 条原有 `ending` 记录，没有活动房间，本次未清理。

API 容器 `ai-call-118-api-1` 于 `2026-10-10T07:09:01.500261245Z` 启动，状态 `healthy`、重启数 0；内部 `/ai-call/health` 返回 HTTP 200、`{"status":"ok"}`。07:09:06 UTC 的日志确认 `lingchen-reach-python` 已向 Nacos 注册。主 Compose `/opt/lingchen-migration-20261002/reach-compose.json` 已原子更新为发布后的候选文件。

邮件 worker 与知识解析容器的 ID、镜像未变，均继续 `healthy`。前端 `current` 仍指向 `/home/lingchen/web/ai-reach/releases/reach-all-20261010T043225Z`，公网首页 HTTP 200，HTML 哈希与该目录的 `index.html` 一致。本次没有带登录态的业务页面/API 验收。

首次准备脚本使用宿主机默认 Python 3.6，因不支持 `capture_output` 在读取容器状态前失败，仅创建了空发布目录。保留该空目录后改用已安装的 Python 3.11，准备与发布均完成；此失败没有触发 API 切换。准备、切换、最终容器状态及模型验证结果已保存在发布目录，本地副本位于被忽略的 `build/response-recovery/deployed-evidence/`。

真实电话验收尚未执行：等待用户提供方便接听的受控测试号码。仍需核对正常多轮、连续补话、打断后续答、双向媒体、录音及诊断事件；本次接口验证和服务健康不能替代这些结果。

回滚材料保留在本次发布目录。需要回滚时先重新确认在途任务与房间均为空，再用 `reach-compose.before.json` 仅重建 API，恢复旧镜像 `sha256:8e808783588500f0c7a04f4466eb4111327d6597b776c77bdeda1df717dbc36a`；健康确认后原子恢复主 Compose，并重新核对 Nacos 和公网。此次没有数据库迁移，常规镜像回滚不需要还原数据库。后续操作按[上线手册](../email-module/production-runbook.md)重新核对现场。
