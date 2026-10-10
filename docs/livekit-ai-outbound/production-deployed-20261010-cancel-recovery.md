# 2026-10-10 连续打断取消恢复发布记录

北京时间 19:27，按“上线部署”聊天及[上线手册](../email-module/production-runbook.md)，在 `110.42.223.109` 发布提交 `f1c495746a3d42710e8076546ef288d5dc4a6c56`。分支 `codex/release-cancel-recovery-20261010` 已推送，基线为线上 `7c6554caa3123cedf9845a58ac6a6462dfe87b15`。

本次修复 `call_367257620696510464` 暴露的提前故障结束：客户连续打断导致取消超时时，允许继续替换模型连接，不消耗回复停滞的恢复次数。连续无输出、重连失败、旧音频清理失败和工具处理中超时的退出限制保留。运行镜像只替换 `app/services/ai_call/agent_runner.py`；无依赖、配置、数据库结构或前端变更，详见[恢复说明](response-recovery.md)。

## 发布版本与回滚材料

- 发布目录：`/opt/lingchen/deployments/reach-cancel-recovery-20261010T112535Z`。
- 新镜像：`ai-call-transfer/api:reach-cancel-recovery-20261010T112535Z`，ID `sha256:85e364ef628c7ccb44401543259131fbe326653520db7cac3e0e1891f16ed638`。
- 旧镜像：`ai-call-transfer/api:reach-non-qwen-20261010T094237Z`，ID `sha256:a51426fce688ae87c2bd46730e5152c0988a3e3f4b8583d3c977a4cd6cabd22a`。
- 发布包 SHA-256：`3fda8945a237d319f529b1adc7ad01d4db836780e2146d28085a07c85de2170c`。
- 已提交源码、新镜像和运行容器的 Runner SHA-256 一致：`32a8d6107538d3c7c57db99e7da393c3e2bad3cbdf7481a5982bafa9e9af8eed`。

发布目录保留 `reach-compose.before.json`、`reach-compose.after.json`、新旧镜像 ID、发布清单及 `ai-call-before.dump`。数据库备份为 1,616,032 字节，`pg_restore -l` 检查通过。完整配置和数据库备份仅留在服务器受限目录。

准备阶段及切换前，活动通话、待拨命令、SIP 预留、活跃转接和 LiveKit 房间均为 0。另有 1 条历史 `ending` 记录，没有活动房间，本次未清理。

## 验证结果

修复阶段六个相关套件共 772 项通过，Ruff 和差异检查通过。发布工作区从线上基线应用修复后，与已验证代码逐文件比对一致，26 项回复恢复专项测试再次通过。

API 容器 `ai-call-118-api-1` 于 `2026-10-10T11:27:23.999472594Z` 启动；19:31 终态检查为 `healthy`，重启数 0。内部 `/ai-call/health` 返回 HTTP 200、`{"status":"ok"}`。11:27:29 UTC 的日志确认 `lingchen-reach-python` 向 Nacos 注册成功。主 Compose `/opt/lingchen-migration-20261002/reach-compose.json` 已原子更新，与发布后的候选文件一致。

邮件 worker、知识解析容器的 ID 和镜像均未变，继续 `healthy`。前端 `current` 仍指向 `/home/lingchen/web/ai-reach/releases/reach-non-qwen-20261010T094237Z`，公网首页 HTTP 200，HTML 哈希与该目录的 `index.html` 一致。使用现有登录态刷新通话记录页，列表及 `call_367257620696510464` 详情正常加载；旧通话的故障结束记录不代表新版本测试结果。

上线后使用实际容器源码、真实 `qwen3.5-omni-plus-realtime` 接口和合成语音，执行一次连续打断测试：

| 核验项 | 结果 |
| --- | --- |
| 取消超时 / 成功替换连接 | 2 次 / 2 次 |
| 两次连接替换耗时 | 约 1.168 秒 / 1.159 秒 |
| 最终回复 | 1 次 `completed`，向计数音频出口发布 1062 帧 |
| 故障结束 / 脚本错误 | 均未发生 |
| 脚本总耗时 | 21.094 秒，包含输入、取消等待、恢复和完整回答 |

该测试直接导入部署模块，没有临时替换 Runner；结果中的 `patched=false` 表示未加载测试替代模块。测试使用正式的 2 秒取消、5 秒重连时限，仅在独立进程修改提示词并移除工具以生成较长回答；使用内存会话和计数音频出口，不创建 SIP 电话，不丢弃或伪造供应商事件。单个成功样本不能代表故障发生率、电话首声延迟或连续打断听感。

准备、切换、终态核验和接口测试证据保存在服务器发布目录。本地脱敏副本为主工作区 `build/call-analysis-20261010/call-367257620696510464-release-evidence/`，归档 SHA-256 为 `8bda9e0b7537cfecdd551d34b5feda7accf4d4fdba4bfc2bda2f32f95fb0a2cc`；归档不包含完整配置或数据库备份。

真实电话验收尚未执行。当前证据确认发布生效、服务正常及真实模型接口连续恢复成功，尚未验证实际 SIP 链路上的双向媒体、打断听感、恢复后上下文及录音。

需要回滚时，按上线手册重新核对现场和空闲状态，用本次 `reach-compose.before.json` 仅重建 API；确认旧镜像恢复健康后，原子恢复主 Compose，再核对 Nacos 和公网。此次没有数据库迁移，常规镜像回滚不需要还原数据库；本次未执行回滚。
