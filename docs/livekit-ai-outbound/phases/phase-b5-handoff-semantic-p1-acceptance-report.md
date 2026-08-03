# Phase B5：转人工后语义分析 P1 验收报告

最后更新：2026-08-04

## 1. 验收结论

当前 `19012` 本地 Web/LAN 验证链路与 `19011` 正式任务本地 SIP 链路下，转人工后语义分析 P1 已完成，可进入联调和试运行。

本轮已确认：

1. 语义分析范围覆盖整通电话，包括 AI 与客户阶段、转人工过程、坐席与客户阶段。
2. `transcript_snapshot_json` 已包含 `handoffs[]`、`human_agent` turns 和人工阶段客户 turns。
3. 人工坐席文本以 `role=assistant`、`speaker_type=human_agent` 进入 snapshot，只作为服务方上下文。
4. 客户事实只从 `role=user`、`speaker_type=customer` 的客户文本中提取。
5. `record_only`、低置信 ASR、冲突来源和坐席轨道串音不会进入强总结。
6. 最新真实转人工样本的 semantic acceptance 和 timeline audit 均通过。
7. `19011` 正式业务任务已补证任务执行、SIP 接通、自动转人工、坐席接入、主录音、客户/人工分轨、离线 ASR 和语义分析完整闭环。

本结论不等同于商用最终发布完成。商用发布前仍建议补最小回归样本集、验收报告归档和产品展示口径。

## 2. 验收范围

本轮覆盖：

1. 转人工后整通电话语义分析链路。
2. `customer` 与 `human_agent` 离线 ASR 入库。
3. `handoffs[]` 元数据进入 transcript snapshot。
4. 人工阶段客户文本进入客户事实候选。
5. 坐席文本、AI 文本、低置信转写和 `record_only` 的采信边界。
6. 五字段 `analysis_result` 输出：`summary`、`feedback_type`、`key_points`、`time_hint`、`tags`。
7. `19011` 正式外呼任务经本地 LiveKit SIP、FreeSWITCH 和 Linphone 完成转人工后的录音、转写与语义闭环。

本轮不覆盖：

1. 公网运营商或生产 Provider 的真实号码转人工后语义验收。
2. 多坐席排队、技能组和商用坐席运营能力。
3. 商用并发压测、弱网和长时间稳定性。
4. 大规模语义样本评测集。
5. 面向业务后台的最终展示交互。

## 3. 当前运行态

| 项 | 值 |
| --- | --- |
| 工作树 | `/Users/liuhongli/.codex/worktrees/6390/ai-call` |
| 服务端口 | `19012` |
| 健康检查 | `{"status":"ok"}` |
| 本地库 | `/private/tmp/ai_call_6390_19012_semantic_p1.db` |
| 入口类型 | `web` |
| 核心场景 | `intro_geo` |
| 坐席 | `agent-debug-001` |
| Snapshot 契约 | [phase-b5-handoff-semantic-snapshot-contract.md](phase-b5-handoff-semantic-snapshot-contract.md) |

## 4. 真实通话样本

### 4.1 正常转人工后业务咨询样本

| 项 | 值 |
| --- | --- |
| call_id | `call_333872434722619392` |
| entry_type | `web` |
| scene_code | `intro_geo` |
| call status | `completed` |
| end_reason | `web_user_end` |
| started_at | `2026-07-10 07:29:51.487204` |
| ended_at | `2026-07-10 07:32:00.472354` |
| duration_ms | `125893` |

Handoff：

| 项 | 值 |
| --- | --- |
| handoff_id | `handoff_333872748573999104` |
| status | `completed` |
| request_source | `customer` |
| request_reason | `customer_request` |
| request_message | `模型判断用户需要转人工` |
| human_agent_identity | `agent-debug-001` |
| requested_at | `2026-07-10 07:31:06.315243` |
| accepted_at | `2026-07-10 07:31:18.781946` |
| connected_at | `2026-07-10 07:31:19.745414` |
| ended_at | `2026-07-10 07:32:00.476270` |
| end_reason | `web_user_end` |

录音轨道：

| track_role | status | oss_id | duration_ms | handoff_id |
| --- | --- | --- | ---: | --- |
| `ai` | `completed` | `333872970851139584` | 121611 | 空 |
| `customer` | `completed` | `333872957639081984` | 118583 | 空 |
| `human_agent` | `completed` | `333872975469068288` | 39527 | `handoff_333872748573999104` |

离线 ASR：

| track_role | provider | model | status | segment_count |
| --- | --- | --- | --- | ---: |
| `customer` | `dashscope_paraformer` | `paraformer-v2` | `completed` | 5 |
| `human_agent` | `dashscope_paraformer` | `paraformer-v2` | `completed` | 3 |

对话分段：

| speaker_type | source | count |
| --- | --- | ---: |
| `ai` | `qwen_realtime` | 4 |
| `customer` | `offline_asr` | 5 |
| `customer` | `qwen_realtime` | 4 |
| `human_agent` | `offline_asr` | 3 |

语义结果：

| 项 | 值 |
| --- | --- |
| analysis_status | `2` |
| transcript_hash | `1b34d766e59e4f321281645445f1cdc424ffcc710f6c298aa8590a8e3cd8940f` |
| feedback_type | `中性` |
| time_hint | 空 |
| tags | `价格咨询`、`试用意向`、`推荐概率关注`、`通话主动终止`、`转写噪声风险提示`、`转写噪声风险` |

最终摘要：

```text
客户初始回应简短（'方。'），后明确表达关注点为‘推荐概率’；随后主动询问试用可能性，继而提出价格与收费模式问题（‘多少钱啊，怎么卖的，怎么收费的？’），并进一步确认计费周期（‘1万块钱是一年还是一个月？’）；最终主动提出结束通话（‘挂了吧’）。
```

关键点：

1. 客户关注 GEO 服务对品牌被 AI 推荐概率的提升效果。
2. 客户主动提出试用意向。
3. 客户询问产品定价及收费方式。
4. 客户明确要求确认 1 万元报价对应的时间周期。
5. 客户主动提出结束通话。

判定：通过。AI 阶段与人工阶段客户表达均被纳入；坐席文本未被误当作客户事实；低置信坐席轨道串音只体现为噪声风险标签。

### 4.2 话题漂移与串音风险样本

| 项 | 值 |
| --- | --- |
| call_id | `call_333810168216141824` |
| handoff_id | `handoff_333810293936209920` |
| 结论 | 通过 |

该样本中，人工阶段出现“麒麟西瓜、哪个超市买”等客户生活话题，以及 `human_agent` 轨道低置信串音。语义结果只采信客户真实话语，并标记转写噪声风险。

判定：通过。话题漂移不会被强行解释为业务需求；坐席轨道污染不会进入客户事实。

### 4.3 正式任务本地 SIP 转人工闭环样本

该样本通过正式业务入口创建任务并立即执行，链路为 `19011 AI Call -> LiveKit SIP -> FreeSWITCH -> Linphone`，不是通话测试台或 Stub。

| 项 | 值 |
| --- | --- |
| task_id | `342733715933970432` |
| target_id | `342733715959136256` |
| attempt_id | `342733717548777472` |
| call_id | `call_342733717557166080` |
| task status | `COMPLETED`，1/1 完成，1/1 接通，0 失败 |
| call status | `completed` |
| end_reason | `agent_completed` |
| duration_ms | `218254` |
| dialogue_persistence_status | `complete` |
| resource_cleanup_status | `clean` |

Handoff：

| 项 | 值 |
| --- | --- |
| handoff_id | `handoff_342733998902689792` |
| status | `completed` |
| request_reason | `customer_request` |
| human_agent_identity | `agent-admin` |
| requested_at | `2026-08-03 18:22:33.028439+00` |
| accepted_at | `2026-08-03 18:22:49.157078+00` |
| connected_at | `2026-08-03 18:22:50.200053+00` |
| ended_at | `2026-08-03 18:25:09.196884+00` |
| end_reason | `agent_completed` |

录音与对象访问：

| role | format | status | oss_id | duration_ms | 可解析时长 |
| --- | --- | --- | --- | ---: | ---: |
| `main` | MP3 | `completed` | `342734671916515328` | 222443 | 222.537 秒 |
| `customer` | OGG/Opus | `completed` | `342734693504598016` | 222414 | 213.894 秒 |
| `human_agent` | OGG/Opus | `completed` | `342734656254984192` | 139007 | 138.763 秒 |

三份对象均可通过 OSS 读取并由 `ffprobe` 解析；主录音为 MP3 44.1kHz 双声道，两条分轨为 Opus 48kHz 双声道。

对话分段：

| speaker_type | source | segment_status | count |
| --- | --- | --- | ---: |
| `ai` | `qwen_realtime` | `final` | 1 |
| `ai` | `qwen_realtime` | `interrupted` | 5 |
| `customer` | `qwen_realtime` | `final` | 7 |
| `customer` | `offline_asr` | `final` | 11 |
| `human_agent` | `offline_asr` | `final` | 13 |

语义结果：

| 项 | 值 |
| --- | --- |
| analysis_status | `2`（`SUCCEEDED`） |
| customer_intent | `neutral` |
| follow_up_suggested | `false` |
| follow_up_consent | `missing` |
| analysis_retry_count | `0` |
| tags | `转人工意愿明确`、`意向弱`、`转写噪声高`、`无强业务事实`、`低信息密度`、`转写噪声风险` |

判定：通过。正式任务只拨打一个目标；客户说“转人工”后坐席成功接入同一通话，客户与人工分轨均完成并产出离线 ASR，语义分析成功，任务、通话、转人工、录音和资源清理均正常终止。

## 5. 自动化验证

本轮已执行：

```bash
curl -fsS http://127.0.0.1:19012/ai-call/health
uv run pytest -q tests/test_ai_call_semantic_analysis.py tests/test_ai_call_semantic_acceptance.py
uv run ruff check app/services/ai_call/semantic_analysis.py tests/test_ai_call_semantic_analysis.py tools/ai_call_semantic_acceptance.py tests/test_ai_call_semantic_acceptance.py
uv run python tools/ai_call_semantic_acceptance.py --base-url http://127.0.0.1:19012 --call-id call_333872434722619392 --timeout-seconds 20 --json
```

验证结果：

```text
health: {"status":"ok"}
pytest: 65 passed, 2 warnings
ruff: All checks passed!
semantic acceptance: PASS, high=0, review=0
timeline audit: passed=true, issueCount=0, highSeverityCount=0
```

语义验收摘要：

| 项 | 值 |
| --- | --- |
| requested | 1 |
| passed | 1 |
| failed | 0 |
| review | 0 |
| high | 0 |
| reviewIssues | 0 |
| verdict | `PASS` |
| turnCount | 15 |
| userTurnCount | 8 |
| assistantTurnCount | 7 |
| recordOnlyUserTurnCount | 2 |
| usableUserSignalCount | 6 |
| qualitySignals | `realtime_supplement`、`human_agent_track_crosstalk` |
| qualityReasons | `human_agent_track_customer_overlap` |

说明：验收结果中 `storedDiffersFromRebuilt=true` 是诊断项，表示存储 snapshot 与当前重建 snapshot 存在差异；本次没有产生 high 或 review 问题，不影响该样本通过。发布前可统一重跑语义分析以刷新存量 snapshot。

## 6. P1 完成标准对照

| 标准 | 状态 | 说明 |
| --- | --- | --- |
| 整通电话进入语义分析 | 满足 | AI 阶段、转人工过程、人工阶段均进入 snapshot。 |
| `handoffs[]` 元数据可见 | 满足 | 已记录 request、accept、connect、end 等状态时间。 |
| `human_agent` turns 可见 | 满足 | 坐席文本进入 turns，`role=assistant`。 |
| 人工阶段客户文本可采信 | 满足 | 人工阶段客户继续问价格、收费方式、周期，进入 key points。 |
| 坐席文本不当客户事实 | 满足 | 坐席文本不生成 `semantic_evidence`，输出中未冒充客户事实。 |
| `record_only` 不进强总结 | 满足 | 验收脚本 high=0。 |
| 低置信/串音风险可识别 | 满足 | `human_agent_track_crosstalk` 标记并进入风险标签。 |
| 输出不泄露内部证据字段 | 满足 | 验收脚本 high=0，review=0。 |

## 7. 已知边界和后续项

P1 后续进入联调/试运行前，可暂不补大规模语义标签体系。

商用发布前建议补齐：

1. 最小回归样本集：10-20 条即可，覆盖未接通、坐席报价、客户价格/试用、弱反馈、时间承诺、串音、话题漂移。
2. 产品展示口径：明确正常结果、低置信结果、未接通人工、人工阶段无转写时的前端展示。
3. 存量 snapshot 刷新策略：对 `storedDiffersFromRebuilt=true` 的历史样本统一重跑或标记为旧口径。
4. 公网 Provider 样本：本地 SIP 正式任务链路已通过，生产前仍需用受控真实号码补运营商线路验收。

## 8. 阶段判断

转人工后语义分析 P1 当前可作为研发闭环基线。

建议状态：

```text
P1 已完成，可进入联调/试运行；商用发布前补最小回归样本和产品展示口径。
```
