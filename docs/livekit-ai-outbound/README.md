# LiveKit AI 智能外呼文档说明

最后更新：2026-06-09

## 1. 文档用途

本文档是 `LiveKit SIP + AI Agent` 智能外呼项目的文档入口，用于说明各个文档的作用、阅读顺序和维护边界。

这组文档服务于商业级生产项目，但当前仍处于分阶段验证和建设阶段。所有文档必须区分：

1. 已验证事实。
2. 架构目标。
3. 阶段计划。
4. 待确认风险。
5. 当前可执行任务。

## 2. 推荐阅读顺序

第一次了解项目时，建议按以下顺序阅读：

1. [01-architecture.md](01-architecture.md)：先理解目标架构和设计边界。
2. [02-current-validation-report.md](02-current-validation-report.md)：再确认当前真实测试到了哪一步。
3. [03-roadmap.md](03-roadmap.md)：然后看阶段拆分和优先级。
4. [phases/phase-00-web-business-loop.md](phases/phase-00-web-business-loop.md)：查看 Web 版商业闭环阶段。
5. [phases/phase-01-real-sip-entry.md](phases/phase-01-real-sip-entry.md)：查看真实 SIP 入口接入阶段。

## 3. 文档清单

| 文档 | 作用 | 适合读者 | 更新频率 |
|---|---|---|---|
| [01-architecture.md](01-architecture.md) | 生产级总体架构设计，说明最终目标态、组件职责、关键边界和生产保障要求 | 技术负责人、架构评审、核心研发 | 低 |
| [02-current-validation-report.md](02-current-validation-report.md) | 当前真实测试情况、拨测证据、已验证事实和未验证边界 | 研发、测试、运维、项目负责人 | 中 |
| [03-roadmap.md](03-roadmap.md) | 阶段路线、优先级、阶段门禁和后续演进方向 | 项目负责人、研发负责人、测试负责人 | 中 |
| [04-data-model.md](04-data-model.md) | 智能外呼独立模块的数据表结构、业务场景和枚举值说明 | 后端研发、架构评审、测试 | 中 |
| [phases/phase-00-web-business-loop.md](phases/phase-00-web-business-loop.md) | Web 版商业闭环的目标、范围、服务端依赖、验证步骤和验收标准 | 当前阶段执行人员 | 高 |
| [phases/phase-01-real-sip-entry.md](phases/phase-01-real-sip-entry.md) | 真实 SIP 入口接入的目标、任务和验收标准 | 当前阶段执行人员 | 高 |
| [ops/real-sip-line-runbook.md](ops/real-sip-line-runbook.md) | 真实 SIP 拨测步骤、FreeSWITCH 停止/恢复、抓包和证据保存 | 研发、运维、测试 | 高 |

## 4. 当前阶段

当前建议执行顺序：

1. 先做 `phase-00-web-business-loop`，建立不依赖真实 SIP 线路的 Web 版商业闭环。
2. 再做 `phase-01-real-sip-entry`，验证真实电话用户能作为 SIP Participant 接入同一套 Room 内闭环。
3. 两个阶段通过后，再展开 AI 模型、话术和语义分析配置体系。
4. 音色配置与自定义音色作为独立阶段建设，不并入 Phase 00 或 Phase 02。
5. 后续再展开业务系统接入、录音分析增强、转人工增强、生产加固和扩容优化。

当前不能把已有真实拨测解释为完整 AI 外呼已跑通。已有拨测只证明 SIP trunk 到 LiveKit Room 的基础链路成立，尚未证明真实电话入口可复用 Web 阶段的 Agent、录音、语义分析、转人工和并发能力。

## 5. 维护规则

### 5.1 写入规则

1. 架构原则、目标拓扑、组件职责写入 `01-architecture.md`。
2. 真实拨测、日志证据、测试环境、已验证和未验证边界写入 `02-current-validation-report.md`。
3. 阶段顺序、阶段门禁、阶段目标写入 `03-roadmap.md`。
4. 单阶段任务、接口草案、验收用例、回滚要求写入 `phases/` 下对应文档。
5. 一次性命令、临时操作流水和排障命令不要写进架构正文，应沉淀到 `ops/` runbook。
6. 关键技术决策后续应沉淀到 `adr/`，例如是否引入 SBC、是否接 RocketMQ、是否使用 SIP REFER 转人工。

### 5.2 事实标注规则

文档中必须区分以下表述：

| 类型 | 说明 |
|---|---|
| 已验证 | 当前已有测试证据支撑 |
| 待验证 | 方案上需要，但尚未完成测试 |
| 建议 | 当前推荐做法，允许后续根据测试结果调整 |
| 不做 | 当前阶段明确不纳入范围 |
| 风险 | 可能影响生产上线，需要门禁或兜底 |

### 5.3 敏感信息规则

非密钥连接信息应写入对应执行文档。Web 版商业闭环依赖写入 [phases/phase-00-web-business-loop.md](phases/phase-00-web-business-loop.md)，真实 SIP 拨测依赖写入 [ops/real-sip-line-runbook.md](ops/real-sip-line-runbook.md)。

以下内容不得写入仓库文档：

1. 完整被叫手机号。
2. 数据库密码、Redis 密码、LiveKit API secret、模型 API key。
3. 录音原文中的敏感信息。
4. 身份证、住址、银行卡等个人敏感信息。
5. 可直接用于生产攻击的完整安全组或密钥配置。

### 5.4 阶段实施操作边界

阶段文档应明确实施者在授权环境中的数据库和服务器操作边界。这里的实施者包括研发、运维、测试人员，以及后续在用户授权下协助执行的自动化或 AI 编程助手。

允许直接执行的操作：

1. 在开发库、测试库或用户明确授权的验证环境中创建、修改和查询本阶段自有表。
2. 执行阶段文档中声明的迁移 SQL、接口验证、健康检查、日志查询和服务启动命令。
3. 写入必要的测试数据、验证数据和阶段产物。
4. 启动、停止或重启本阶段明确涉及的测试服务。
5. 读取日志、抓包结果、数据库结构和运行状态，用于排障和验收。

必须先确认或备份的操作：

1. 修改已有基座表结构，例如 `sys_oss`、`sys_oss_config`。
2. 删除、截断、覆盖已有业务数据。
3. 在共享测试环境或生产环境执行 DDL。
4. 停止现有生产服务，例如 FreeSWITCH、网关或业务 Worker。
5. 修改安全组、防火墙、SIP 端口、公网 IP、密钥、模型供应商配置。
6. 发起真实 SIP 外呼、批量外呼或可能产生费用的压测。

阶段执行后必须沉淀：

1. 执行过的关键命令或 SQL 摘要。
2. 涉及的数据库、表、服务器和服务名。
3. 执行结果、失败原因和回滚方式。
4. 新增或修改的 `.env` 变量名，但不写变量值中的密钥。
5. 可用于验收的 `call_id`、日志片段、截图或查询结果。

## 6. 后续建议补充的文档

当前 v0 只展开最近两个阶段。后续进入生产建设时，建议继续补充：

```text
ops/real-sip-line-runbook.md
ops/production-deploy-runbook.md
ops/troubleshooting.md
adr/0001-use-livekit-sip-agent.md
adr/0002-use-http-integration-before-rocketmq.md
adr/0003-sbc-boundary-decision.md
```

这些文档暂不提前写细，避免被前两个阶段的真实验证结果推翻。
