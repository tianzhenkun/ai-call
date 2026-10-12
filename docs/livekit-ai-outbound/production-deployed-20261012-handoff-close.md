# 2026-10-12 模型关闭超时修复发布与 SIP 验收

## 发布内容与版本

- 源码提交：`3f94ccd8c8a65808e0c1a57d7741f243f7c34d79`，分支 `codex/fix-omni38-handoff-close`。
- 发布版本：`reach-omni38-handoff-close-20261012T113134Z`；API 镜像标签为 `ai-call-transfer/api:reach-omni38-handoff-close-20261012T113134Z`。
- 基线镜像：`ai-call-transfer/api:reach-omni38-cancel-20261012T081850Z`。
- 运行时代码仅将 `aliyun_qwen_realtime.py` 的 WebSocket 关闭握手超时从 10 秒改为 1.5 秒。远端不回应关闭握手时，`websockets` 终止 TCP，避免同步等待阻塞转人工提示。
- 该文件 SHA-256：`5841af1fd30ffafc09be729c8b5a47b8796ef3bc623c0b9f859d5f253b228250`。
- 本次沿用原生语音输出、默认 Omni3.5 Plus 和任务音色 Tina。隔离工作区的输出审核与独立 TTS 实验未进入发布包。

## 发布核验

生产主机为 `110.42.223.109`。发布前、切换前均检查到活动通话、待执行 START_CALL、线路预留、活动转接、LiveKit 房间为 0；保留原有 1 条历史 ending 记录。

服务器目录：`/opt/lingchen/deployments/reach-omni38-handoff-close-20261012T113134Z`。

该目录保存发布前后 Compose、旧新镜像 ID、数据库备份、原前端路径及 worker 标识。数据库备份为 1,773,678 字节，已通过 `pg_restore -l` 校验。

最初归档传输因编码失败被拦截，未执行解包或发布。随后复用服务器已有发布脚本，逐一核验 5 个文件的 SHA-256 与本地发布内容相同，再构建增量镜像。

已确认：镜像架构 amd64、运行文件指纹与提交一致、API healthy 且重启数为 0、健康接口 HTTP 200、Nacos `lingchen-reach-python` 注册成功。仅重建 API，两个 worker 的容器及镜像保持一致且健康；前端当前目录及公网首页指纹一致。主 Compose 已原子更新。

## 验证结果

独立干净源码快照仅应用本次 3 文件补丁：旧实现可复现关闭超时；修复后相关回归 **430 passed**。新增测试使用真实本地 TCP/WebSocket，仅模拟远端不回应关闭握手，检查两秒内结束、连接 CLOSED 和运行器资源释放。

2026-10-12 19:41–19:46（北京时间）完成两通真实 SIP 人工接入验收，号码尾号 8743，坐席 admin。用户分别明确确认两通均“双方都能听清”。

| 项目 | Omni3.5 Plus | Omni3.8 Flash |
| --- | --- | --- |
| 通话 ID | `call_368000315529830400` | `call_368001153186865152` |
| 任务 ID | `368000315148148736` | `368001152016654336` |
| 转接请求到模型暂停 | 1.034764 秒 | 1.505477 秒 |
| 人工接入 | admin，已完成 | admin，已完成 |
| 接通时长 | 85.579 秒 | 56.744 秒 |
| 数据库录音时长 | 84.690 秒 | 56.208 秒 |
| 通话资源清理 | clean | clean |
| 对话持久化 | complete | complete |
| 结束原因 | sip_participant_left | sip_participant_left |

耗时口径为 `handoff_requested` 到 `agent_suspended_for_handoff` 的服务器事件差，包含运行器暂停处理，不能解释为客户话尾到人工耳端的延迟。两项均低于本轮 2 秒验收线。

两通运行事件确认实际模型分别为 `qwen3.5-omni-plus-realtime`、`qwen3.8-omni-flash-realtime`，均使用 Tina、server_vad、800 ms 静音阈值、0.5 VAD threshold。提示词快照、规则和线路相同；知识快照仅 `frozenAt` 时间不同，其他内容相同。提示词为 GEO 产品介绍 v5，规则为“单次真实外呼测试”，重试 0 次。

两通均由客户转写的 `rule_fallback` 触发人工请求，`handoff_tool_requested` 数为 0。本轮证明转接后的模型关闭和人工接入链路可用，不能据此认定模型首次漏调工具、虚假转接进度或事实编造问题已修复。两次实际对话及环境噪声不同，也不能当作严格的能力或成本胜负比较。

两通结束后再次核验：活动通话、待执行 START_CALL、线路预留、活动转接及 LiveKit 房间均为 0，原有 1 条历史 ending 记录未变化；API 和两个 worker 均健康，API 重启数仍为 0。

## 证据与剩余验收

- 服务器 `human-evidence.jsonl`：391 行、339,173 字节，SHA-256 为 `1027c6198ccca108073fca31faed13fb227534a0acd86ab16e06fc19d25ad4d8`，含 313 个事件、46 个对话片段、2 份任务快照、2 份录音、4 份分轨记录及转接记录。只读事务导出，错误文件为空。
- 服务器 `human-summary.txt` 保存计算结果。其原始 `knowledge=false` 表示包含 `frozenAt` 的整份快照不相等；已逐字段确认差异仅为冻结时间。
- 本地忽略目录 `build/omni38-handoff-release/` 保存两通下载录音、页面证据和计算脚本。MP3 分别为 1,356,433 / 900,858 字节，均全程解码成功；听感结论来自电话和坐席两端的用户确认。
- 无坐席回退两通任务已准备；本阶段尚未拨打，等待 admin 下线后执行。因此当前结论为“修复已发布、人工接入验收通过、无坐席回退待验收”。默认模型替换和事实可靠性验收仍未通过。

## 回滚

先使用发布目录的 `idle-check.py` 在 API 容器中确认无活动通话及房间，然后使用 `reach-compose.before.json` 仅恢复 API：

```sh
docker compose -p reach-migrated -f /opt/lingchen/deployments/reach-omni38-handoff-close-20261012T113134Z/reach-compose.before.json up -d --no-deps ai-call-118-api-1
```

核验旧镜像、健康接口及注册后，将备份 Compose 原子恢复至 `/opt/lingchen-migration-20261002/reach-compose.json`。本次没有数据库结构变更，回滚不恢复数据库备份。
