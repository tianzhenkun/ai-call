# LiveKit AI 外呼文档入口

最后更新：2026-06-24

## 当前架构

当前主线是 `LiveKit + 每通会话一个 Realtime Call Agent + 端到端 S2S 模型`。

实时通话不再走旧的 `ASR -> LLM -> TTS` 三段式链路。三段式只保留给通话后转写、摘要、质检、离线分析或兜底。

当前模型固定为阿里百炼 `qwen3.5-omni-plus-realtime`。音色由 Phase A 前端下拉选择阿里官方 `voice` 参数，后端透传给模型，不建设模型切换、模型路由或音色管理能力。

当前 Phase A 实现已收尾，Phase A 打断稳定化已通过当前 Web 链路真实通话样本验收；2026-06-24 的打断与通话文本展示收口记录已确认 `AI（已打断）` 展示、被打断 AI 段保留、迟到事件补标和重复文本治理可作为当前开发基线。Phase B1 记录与查询已形成当前实现基线；事件明细采用进程内后台队列异步持久化，实时会话事件查询与历史记录事件查询分离。Phase B2 录音闭环和 Phase B2.5 对话文本闭环已完成设计、接口、数据表、Phase A 验证页入口和本地闭环验收；录音状态、`ossId`、`playUrl`、对象访问、Range 支持此前已按 MP4 主混音验证，当前配置为主混音 MP3、分参与方录音 OGG，避免 LiveKit Participant Egress MP3 编码兼容问题；对话文本预览和通话后查询已完成。Phase B3 最小转人工、Phase B3.1 异常闭环和 Phase B3.2 自动触发已完成 Web/LAN 真实通话验收，覆盖无人接听超时、坐席接入、客户主动挂断和坐席主动断开；Phase B4 业务提示词配置与组装设计已生成，待实现。Phase E SIP 真实线路最小接入设计已生成，明确只新增电话入口并复用现有 Room、Agent、录音、文本和转人工状态机；当前仍未接入 LiveKit SIP service，也未完成真实手机验收。Phase A/B Web 链路剩余生产补证项以验收报告为准，SIP、弱网、商用并发和业务语义仍需单独关闭。

## 阅读顺序

1. [OUTLINE.md](OUTLINE.md)：总纲，包含系统目标、总体架构图、阶段规划和当前阶段。
2. [phases/phase-a-e2e-core-engine.md](phases/phase-a-e2e-core-engine.md)：Phase A 技术设计。
3. [phases/phase-a-acceptance-report.md](phases/phase-a-acceptance-report.md)：Phase A 验收报告和补证项。
4. [phases/phase-a-interrupt-stabilization-acceptance-report.md](phases/phase-a-interrupt-stabilization-acceptance-report.md)：Phase A 打断稳定化验收报告。
5. [phases/phase-a-interrupt-dialogue-display-acceptance-report.md](phases/phase-a-interrupt-dialogue-display-acceptance-report.md)：Phase A 打断与通话文本展示收口验收记录。
6. [phases/phase-b1-record-query-design.md](phases/phase-b1-record-query-design.md)：Phase B1 记录与查询正式技术设计。
7. [phases/phase-b2-recording-closure-design.md](phases/phase-b2-recording-closure-design.md)：Phase B2 录音闭环正式技术设计。
8. [phases/phase-b2-5-dialogue-text-closure-design.md](phases/phase-b2-5-dialogue-text-closure-design.md)：Phase B2.5 对话文本闭环正式技术设计。
9. [phases/phase-b3-minimal-handoff-design.md](phases/phase-b3-minimal-handoff-design.md)：Phase B3 最小转人工正式技术设计与收口基线。
10. [phases/phase-b3-acceptance-report.md](phases/phase-b3-acceptance-report.md)：Phase B3 验收报告。
11. [phases/phase-b3-1-handoff-exception-closure-design.md](phases/phase-b3-1-handoff-exception-closure-design.md)：Phase B3.1 转人工异常闭环正式技术设计与实现基线。
12. [phases/phase-b3-1-acceptance-report.md](phases/phase-b3-1-acceptance-report.md)：Phase B3.1 验收报告。
13. [phases/phase-b3-2-auto-handoff-trigger-design.md](phases/phase-b3-2-auto-handoff-trigger-design.md)：Phase B3.2 转人工自动触发技术设计。
14. [phases/phase-b3-handoff-live-closure-acceptance-report.md](phases/phase-b3-handoff-live-closure-acceptance-report.md)：Phase B3/B3.1/B3.2 Web/LAN 转人工真实通话闭环验收记录。
15. [phases/phase-b-business-semantics-asr-followup.md](phases/phase-b-business-semantics-asr-followup.md)：Phase B 后续业务语义与 ASR 识别准确率问题记录。
16. [phases/phase-b4-prompt-config-design.md](phases/phase-b4-prompt-config-design.md)：Phase B4 业务提示词配置与组装设计。
17. [phases/phase-e-sip-minimal-entry-design.md](phases/phase-e-sip-minimal-entry-design.md)：Phase E SIP 真实线路最小接入设计。
18. [sql/phase-b2-b25-postgres.sql](sql/phase-b2-b25-postgres.sql)：Phase B2/B2.5 PostgreSQL 建表脚本。
19. [sql/phase-b3-handoff-postgres.sql](sql/phase-b3-handoff-postgres.sql)：Phase B3 PostgreSQL 建表脚本。
20. [sql/phase-b4-voice-profile-postgres.sql](sql/phase-b4-voice-profile-postgres.sql)：端到端音色配置表和 Qwen Omni Realtime 内置音色种子数据。
21. [CALL_SCENARIOS.md](CALL_SCENARIOS.md)：通话中通用场景和验收清单。

Phase B 预设计：[phases/phase-b-web-commercial-loop-pre-design.md](phases/phase-b-web-commercial-loop-pre-design.md)。它已作为 Phase B1/B2/B2.5/B3 正式设计的输入，后续实现以对应阶段正式设计文档为准。

AI 智能体实现前必须先读 `OUTLINE.md`，再读当前阶段文档，最后用代码和测试核对真实进度。

## 文档维护

1. 总体方向、架构图、阶段状态只写入 `OUTLINE.md`。
2. 当前阶段实现细节只写入 `phases/` 下对应阶段文档。
3. 通话场景和验收输入只写入 `CALL_SCENARIOS.md`。
4. 阶段收尾时，必须在 `phases/` 下生成或更新验收报告。
5. 进入新阶段前，再生成对应阶段技术设计文档。

## 敏感信息

不要在文档中写入 API Key、数据库密码、完整 Token、完整手机号、身份证、银行卡、录音原文或可直接用于攻击的生产配置。
