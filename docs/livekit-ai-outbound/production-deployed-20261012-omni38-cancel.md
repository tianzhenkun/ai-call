# 2026-10-12 Omni 取消时序修复发布记录

状态：后端修复已于北京时间 16:28 发布。代码指纹、容器健康、Nacos 注册及公网首页检查通过；尚未执行修复后的真实 SIP 通话验收。

## 问题与修复

通话 `call_367940689544138752` 使用 `qwen3.8-omni-flash-realtime`，在客户询问收费时以 `model_error` 结束。07:46:17 UTC 的事件链显示：上一条回复已生成结束但仍在播放，插话触发无目标取消；随后新回复尚未收到创建确认，又发送取消。模型返回 `Cannot cancel response before response.created`，运行器进入故障结束。

修复统一四处主动取消入口：只有已确认创建且仍在生成的回复才发送 `response.cancel`；创建确认尚未到达时保留取消意图，收到 `response.created` 后发送一次；生成已经结束时只停止本地播放。重复、迟到事件隔离和既有取消超时恢复继续生效。此约束符合[阿里云实时模型客户端事件文档](https://help.aliyun.com/zh/model-studio/client-events)对创建与取消事件的说明。

- 修复提交：`4344c6181ec3561838802972f79727d0fa037f2c`，分支 `codex/fix-omni38-cancel-race`。
- 生产代码仅变更 `app/services/ai_call/agent_runner.py`。前端、模型默认值、环境配置和数据库结构均沿用原发布版本。
- 补丁已同步至 `/Users/liuhongli/.codex/worktrees/qwen-audio-3-1/ai-call`；三个目标文件反向扣除本次补丁后与备份逐字节一致，原评估改动保留。

## 验证

- 原代码在新增的四个复现场景中失败，覆盖创建确认前插话、生成结束后的尾音插话，以及候选和确认两条入口。
- 修复后 591 项相关测试全部通过，包含响应恢复、连续插话、播放与旧音频隔离、超时、转接和结束逻辑；修改文件 Ruff 和 `git diff --check` 通过。
- 当前评估工作区同步后，30 项响应恢复测试通过。
- 三个真实模型连接均验证通过：`qwen3.8-omni-flash-realtime`、`qwen3.5-omni-plus-realtime`、`qwen3.5-omni-flash-realtime`。每个连接延迟交付首条创建确认，发起插话并排队新问题；确认前没有发送取消，确认后发送一次，随后新问题产生音频，协议错误、故障结束、重连均为 0。
- 真实接口验证使用合成文字、内存会话和只计数的音频接收端，未连接业务数据库、浏览器媒体或 SIP。这些结果不替代电话端的听感与双向媒体验收。

本地证据位于本工作区 `build/omni38-cancel-race/`：`red.log`、`regression-final.log`、`live_cancel_probe.py`、三个 `live-*.json` 和发布包。评估工作区原文件备份及同步验证位于其 `build/omni38-cancel-race-before/`。

## 生产发布

- 主机：`110.42.223.109`；发布编号：`reach-omni38-cancel-20261012T081850Z`。
- 发布包 6,130 字节，SHA-256 为 `80b79876ac156def7eadf46a64aa9fac8e48740080be71c73e86f276f646e539`，服务器上传后校验一致。
- 镜像：`ai-call-transfer/api:reach-omni38-cancel-20261012T081850Z`。
- 镜像 ID：`sha256:be120175e502b7efe10676715c272c7c17434bf6a738659934ed631fce898d4d`。
- 容器 `ai-call-118-api-1` 内目标文件 SHA-256：`35fe02e32756175c93d17c9453b307c83006a4f1afa2dfda9b50cfe9e0c1c97d`，与已测试文件一致。
- 准备与切换前均确认活动通话、待启动呼叫命令、SIP 预留、活动转接、LiveKit 房间为 0。历史遗留 1 条 `ending` 记录未改动。
- 数据库备份 1,773,590 字节，`pg_restore -l` 通过。旧 Compose、旧镜像 ID 与备份保存在 `/opt/lingchen/deployments/reach-omni38-cancel-20261012T081850Z/`。
- 发布后内部健康接口返回 `200 {"status":"ok"}`，容器 `healthy`、重启数 0，`lingchen-reach-python` 在生产 Nacos 注册成功。容器内 Omni3.8 配置校验通过，默认模型仍为 Omni3.5 Plus。
- 邮件和知识解析 worker 的容器 ID、镜像及健康状态未变；前端软链接未变，公网首页哈希与当前前端文件一致。
- 远端 `apply.log` 与 `deployed.json` 保存在发布目录。发布脚本使用 `python3.11`；服务器默认 `python3` 为 3.6，不支持脚本所用的 `subprocess.run(capture_output=...)`。

## 回滚

本次无数据库迁移。确认无活动通话后，以发布目录下 `reach-compose.before.json` 重新创建 API 服务，恢复 `/opt/lingchen-migration-20261002/reach-compose.json`，再检查健康和 Nacos 注册。旧镜像 ID 为 `sha256:b63c093a0ba5864249c7b622da39569bcfa78484030743ae3e4f2a2ddfeafcc6`。前端无需回滚。
