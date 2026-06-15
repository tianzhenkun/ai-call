# LiveKit AI 外呼文档入口

最后更新：2026-06-15

## 当前架构

当前主线是 `LiveKit + 每通会话一个 Realtime Call Agent + 端到端 S2S 模型`。

实时通话不再走旧的 `ASR -> LLM -> TTS` 三段式链路。三段式只保留给通话后转写、摘要、质检、离线分析或兜底。

当前模型固定为阿里百炼 `qwen3.5-omni-plus-realtime`。音色由 Phase A 前端下拉选择阿里官方 `voice` 参数，后端透传给模型，不建设模型切换、模型路由或音色管理能力。

当前 Phase A 实现已收尾，可以进入 Phase B 正式技术设计；Phase A 剩余补证项以验收报告为准。

## 阅读顺序

1. [OUTLINE.md](OUTLINE.md)：总纲，包含系统目标、总体架构图、阶段规划和当前阶段。
2. [phases/phase-a-e2e-core-engine.md](phases/phase-a-e2e-core-engine.md)：Phase A 技术设计。
3. [phases/phase-a-acceptance-report.md](phases/phase-a-acceptance-report.md)：Phase A 验收报告和补证项。
4. [CALL_SCENARIOS.md](CALL_SCENARIOS.md)：通话中通用场景和验收清单。

Phase B 预设计：[phases/phase-b-web-commercial-loop-pre-design.md](phases/phase-b-web-commercial-loop-pre-design.md)。它可以作为 Phase B 正式技术设计的输入，但不能直接作为实现依据。

AI 智能体实现前必须先读 `OUTLINE.md`，再读当前阶段文档，最后用代码和测试核对真实进度。

## 文档维护

1. 总体方向、架构图、阶段状态只写入 `OUTLINE.md`。
2. 当前阶段实现细节只写入 `phases/` 下对应阶段文档。
3. 通话场景和验收输入只写入 `CALL_SCENARIOS.md`。
4. 阶段收尾时，必须在 `phases/` 下生成或更新验收报告。
5. 进入新阶段前，再生成对应阶段技术设计文档。

## 敏感信息

不要在文档中写入 API Key、数据库密码、完整 Token、完整手机号、身份证、银行卡、录音原文或可直接用于攻击的生产配置。
