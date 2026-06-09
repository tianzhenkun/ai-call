# 分阶段实施路线

最后更新：2026-06-09

## 1. 文档定位

本文档定义 `LiveKit SIP + AI Agent` 智能外呼项目的阶段路线、阶段目标、优先级和门禁。

本文档不展开每个阶段的详细任务。具体执行细节写入 `phases/` 下的阶段文档。

完整数据模型设计见：[04-data-model.md](04-data-model.md)。

当前只建议把最近两个阶段写到可执行级别：

1. [phases/phase-00-web-business-loop.md](phases/phase-00-web-business-loop.md)
2. [phases/phase-01-real-sip-entry.md](phases/phase-01-real-sip-entry.md)

对应操作验证说明：

1. Web 版商业闭环验证步骤写在 [phases/phase-00-web-business-loop.md](phases/phase-00-web-business-loop.md) 中。
2. 真实 SIP 拨测步骤写在 [ops/real-sip-line-runbook.md](ops/real-sip-line-runbook.md) 中。

后续阶段先保留概要，避免过早设计被前期验证结果推翻。

## 2. 总体策略

阶段推进原则：

1. 分阶段是按依赖和风险推进，不是故意把能一次做完整的基础能力做成半截。
2. 表结构、异步创建、状态机、事件记录这类基础能力能一次做完整就一次做完整。
3. 先做基于 WebRTC 的商业闭环，再做真实 SIP 入口接入。
4. 先验证 Room 内 Agent、消息、事件、录音、分析和轻量转人工，再验证真实电话线路。
5. 先把状态、录音、分析、转人工和页面查询做闭环，再扩大并发。
6. 每个阶段必须有明确通过标准，不能只以“代码写完”为完成。
7. 阶段执行包含代码实现、数据库迁移、服务启动、日志排查和验收验证；在用户授权的开发或测试环境中，实施者可以按阶段文档直接执行必要操作。
8. 生产环境、共享业务库、已有基座表结构、真实 SIP 外呼、批量压测和停服操作必须先确认边界，再执行。

## 3. 阶段总览

| 阶段 | 名称 | 核心目标 | 当前状态 | 详细文档 |
|---|---|---|---|---|
| Phase 00 | Web 版商业闭环 | 不接真实 SIP，先跑通 WebRTC 用户、Agent、消息、事件、录音、语义分析和轻量转人工 | 待执行 | [phase-00-web-business-loop.md](phases/phase-00-web-business-loop.md) |
| Phase 01 | 真实 SIP 入口接入 | 将真实电话用户作为 SIP Participant 接入 Phase 00 已验证的 Room 内闭环 | 待执行 | [phase-01-real-sip-entry.md](phases/phase-01-real-sip-entry.md) |
| Phase 02 | AI 模型、话术和语义分析配置体系 | 拆分 ASR / LLM / TTS / 通话后分析配置，沉淀 Agent 和话术配置边界 | 待规划 | 待补充 |
| Phase 03 | 音色配置与自定义音色体系 | 独立建设内置音色、自定义音色、试听、训练状态、供应商 voice_id 映射和音色使用策略 | 待规划 | 待补充 |
| Phase 04 | 业务系统接入和运营接口增强 | 对接上游业务系统、完善业务查询、鉴权、审计和运营接口 | 待规划 | 待补充 |
| Phase 05 | 录音和分析增强 | 分轨录音、质检展示、分析补偿和录音存储治理 | 待规划 | 待补充 |
| Phase 06 | 转人工增强 | 完整坐席台、坐席状态、超时回退、转人工结果回填 | 待规划 | 待补充 |
| Phase 07 | 生产加固 | HA、监控、告警、回滚、安全、合规 | 待规划 | 待补充 |
| Phase 08 | 扩容和成本优化 | 并发、CPS、多线路、模型成本优化 | 待规划 | 待补充 |

## 4. Phase 00：Web 版商业闭环

目标：在不接真实 SIP 线路的前提下，先完成一套基于 WebRTC 的商业级核心业务闭环。

核心价值：

1. 不消耗真实电话线路即可验证核心系统能力。
2. Phase 01 接 SIP 时主要替换入口，不重写 Room 内能力。
3. 提前验证状态、参与者、事件、消息、混音录音、`sys_oss`、语义分析和轻量 WebRTC 转人工。

通过标准：

```text
不拨真实电话
浏览器能进入 LiveKit Room
浏览器麦克风音频能被 Agent 收到
Agent 回复能在浏览器播放
状态、参与者、事件、消息可查询
混音录音进入 sys_oss 并写入 ai_call_recording
JSON 语义分析写入 ai_call_analysis
WebRTC 人工坐席可接管并看到前文
页面可按 call_id 完整复盘
```

操作验证步骤见：[phases/phase-00-web-business-loop.md](phases/phase-00-web-business-loop.md)。

## 5. Phase 01：真实 SIP 入口接入

目标：验证真实电话用户能作为 SIP Participant 接入 Phase 00 已跑通的 Room 内业务闭环。

核心价值：

1. 补齐当前最大事实缺口。
2. 证明真实 SIP Participant 可以复用 Web 阶段已验证的 Agent、消息、事件、录音、分析和转人工能力。
3. 暴露并处理 SIP 专属问题，例如振铃、未接、拒接、失败码、codec、RTP、端口白名单和 FreeSWITCH 冲突。

通过标准：

```text
手机震铃
接听成功
SIP 200 OK
PCMU/PCMA RTP 协商成功
LiveKit Room 有 SIP Participant
Agent 加入同一个 Room
电话侧能听到 Agent
Agent 能收到电话侧音频或 ASR 结果
用户挂机后状态正常闭环
录音、消息、事件和分析链路可复用
```

真实拨测操作、FreeSWITCH 停止/恢复和抓包步骤见：[ops/real-sip-line-runbook.md](ops/real-sip-line-runbook.md)。

## 6. Phase 02：AI 模型、话术和语义分析配置体系

目标：把 Phase 00 / Phase 01 中依赖程序默认值或接口快照的 Agent、ASR、LLM、TTS、通话后语义分析和话术能力，沉淀为可管理、可复盘、可灰度的配置体系。

本阶段不处理自定义音色。TTS 模型接入只保证能稳定使用系统默认声音完成实时播报；内置音色选择、自定义音色、试听、训练状态和 voice_id 映射放到 Phase 03。

计划交付：

1. `ai_agent_config`、`ai_model_config`、`ai_script_config` 的最终字段评审和落库。
2. Agent 行为配置，例如最大通话时长、静音超时、打断开关、是否允许转人工。
3. 流式 ASR provider adapter。
4. LLM provider adapter。
5. 流式 TTS provider adapter。
6. 通话后 JSON 语义分析模型配置。
7. 话术 prompt、变量 schema 和配置版本。
8. 模型失败、超时、降级和成本记录。
9. 配置快照与历史通话复盘能力。
10. 评审创建通话接口是否开放高级参数，或只允许传 `agent_config_id`、`model_config_id`、`script_config_id` 等配置引用。
11. 评审 `variables` 的使用边界，例如变量 schema、默认值、必填校验、敏感字段过滤和快照留存。
12. 明确高级参数的优先级：服务端默认配置、配置表引用、业务侧传参三者谁覆盖谁。

当前推荐方向：

1. 创建通话接口优先接收配置引用，例如 `agent_config_id`、`asr_model_config_id`、`llm_model_config_id`、`tts_model_config_id`、`script_config_id`。
2. 不建议默认允许业务侧在创建通话接口中直接传完整 `agentConfig`、`modelConfig`、`script` 大对象，避免配置不可控、难复盘、难灰度。
3. `variables` 可以作为业务变量传入，但必须有 `variables_schema` 校验，并在快照中过滤敏感字段。
4. 后端最终仍要把实际执行配置写入 `ai_call_session` 快照字段，确保历史通话不受配置后续修改影响。

本阶段开始前必须完成：

1. Phase 00 Web 版商业闭环。
2. Phase 01 真实 SIP 入口接入验证通过。
3. 重新和业务、研发确认 `ai_agent_config`、`ai_model_config`、`ai_script_config` 的最新表结构，不能机械沿用早期草案。

## 7. Phase 03：音色配置与自定义音色体系

目标：把音色作为独立资产建设，而不是简单挂在 TTS 模型字段上。

计划交付：

1. 内置音色列表和可用状态。
2. 自定义音色创建、上传样本和试听。
3. 音色训练或克隆状态，例如待处理、训练中、可用、失败、禁用。
4. 供应商侧 `voice_id` / `speaker_id` 映射。
5. 音色样本文件和试听文件复用 `sys_oss`。
6. Agent 默认音色绑定。
7. 通话发起时指定音色的使用规则。
8. 音色不可用时的降级策略。
9. 音色权限、合规和审计。
10. 自定义音色对延迟、并发和成本的影响验证。

本阶段开始前必须完成：

1. Phase 02 的 TTS provider adapter 已稳定。
2. 已确认目标 TTS 供应商是否支持自定义音色、训练、试听和商用授权。
3. 重新评审是否新增 `ai_voice_config` 等音色配置表，不在早期阶段提前锁死字段。

## 8. Phase 04：业务系统接入和运营接口增强

目标：在核心通话能力、真实 SIP 入口和模型配置体系稳定后，正式对接上游业务系统和运营后台。

计划交付：

1. 统一 HTTP 外呼创建接口。
2. `request_id` 幂等创建。
3. 外呼状态查询接口。
4. 按 `business_type + business_id` 查询通话记录。
5. 业务鉴权、权限和审计。
6. 运营台列表、筛选、详情和重试入口。
7. 业务侧失败重试和取消策略。
8. 回调或主动查询机制的最终选择。

## 9. Phase 05：录音和分析增强

目标：在 Phase 00 混音录音和基础语义分析之上，补齐更强的审计、质检和补偿能力。

计划交付：

1. 必要时增加用户、AI、人工分轨录音。
2. 录音失败告警和补偿。
3. 录音存储生命周期和成本治理。
4. 通话后转写增强。
5. 质检展示。
6. 分析任务补偿和重跑。
7. 分析结果版本管理。

## 10. Phase 06：转人工增强

目标：在 Phase 00 轻量 WebRTC 坐席接管之上，补齐完整坐席台和运营能力。

计划交付：

1. 坐席登录。
2. 坐席在线 / 忙碌 / 离线。
3. AI 触发转人工通知。
4. 坐席台任务列表。
5. 坐席加入 LiveKit Room。
6. 坐席结果回填。
7. 坐席超时回退。
8. 转人工后录音连续。

## 11. Phase 07：生产加固

目标：让系统具备商业生产运行能力。

计划交付：

1. Redis HA。
2. Agent Worker 池。
3. Egress Worker 独立部署。
4. LiveKit / SIP 节点健康检查。
5. 监控和告警。
6. 状态查询和主动对账。
7. 安全组和防火墙。
8. 回滚 runbook。
9. 抓包和排障 runbook。
10. 合规审计。

## 12. Phase 08：扩容和成本优化

目标：稳定承载批量外呼，并控制单通成本。

计划交付：

1. 并发压测。
2. CPS 限流。
3. 多 trunk 或多线路路由。
4. ASR / TTS / LLM 成本统计。
5. 模型供应商降级策略。
6. 录音存储成本优化。
7. 业务维度并发和成本看板。

## 13. 阶段门禁原则

每个阶段完成必须满足：

1. 有可复现的测试步骤。
2. 有明确通过标准。
3. 有失败场景验证。
4. 有关键日志和指标。
5. 有回滚或兜底方案。
6. 不把未验证能力写成已完成事实。

## 14. 当前建议

当前不要同时展开所有阶段执行细节。

建议下一步：

1. 先审查本文档的阶段顺序。
2. 审查 Phase 00 和 Phase 01 的执行范围。
3. 对齐后优先实现 Phase 00。
4. Phase 00 可运行后，再进入 Phase 01。
