# intro_document 提示词静态覆盖报告

> 本报告检查 27 条测试用例的关键约束是否已经出现在最终拼接提示词中。它不能证明模型真实回答质量。

- 场景：`intro_document`
- 模式：`静态提示词覆盖`
- 结论：`通过`
- 通过数：`27/27`
- 是否允许打断：`否`
- 提示词哈希：`sha256:13a403c00981d560ff2c6a324239a87af359f713126fb25f3bb0f51d5783a40b`
- 开场白：您好张总，我是灵宸智能助手，想简单介绍一下我们的跨境单证智能审核产品，请问现在方便吗？

| 用例 | 类别 | 结果 | 缺失锚点 | 首轮客户问题 |
|---|---|---|---|---|
| `document_availability_001` | `document_availability` | 通过 | - | 我现在在开会，没空。 |
| `document_availability_002` | `document_availability` | 通过 | - | 你先说重点，我只有半分钟。 |
| `document_reject_001` | `document_availability` | 通过 | - | 不需要，我们没有这块业务。 |
| `document_basic_001` | `document_basic` | 通过 | - | 你们这个跨境单证智能审核是做什么的？ |
| `document_basic_002` | `document_basic` | 通过 | - | 这和普通 OCR 有什么区别？ |
| `document_method_001` | `document_method` | 通过 | - | 你们具体怎么审核单证？ |
| `document_method_002` | `document_method` | 通过 | - | 能审哪些单证？ |
| `document_method_003` | `document_method` | 通过 | - | 什么是单单一致性审核？ |
| `document_rules_001` | `document_rules` | 通过 | - | 你们支持 UCP600 和 ISBP 吗？ |
| `document_rules_002` | `document_rules` | 通过 | - | 我们的内部审单规则可以配置吗？ |
| `document_rules_003` | `document_rules` | 通过 | - | 信用证 46A 缺单这种能查吗？ |
| `document_metrics_001` | `document_metrics` | 通过 | - | 效果怎么看？有哪些指标？ |
| `document_metrics_002` | `document_metrics` | 通过 | - | 能保证识别准确率多少？能保证不拒付吗？ |
| `document_metrics_003` | `document_metrics` | 通过 | - | 多久能上线？两周能出效果吗？ |
| `document_security_001` | `document_data_security` | 通过 | - | 这些单证数据很敏感，数据安全吗？ |
| `document_security_002` | `document_data_security` | 通过 | - | 能私有化部署吗？ |
| `document_integration_001` | `document_integration` | 通过 | - | 能接我们的国结系统吗？ |
| `document_integration_002` | `document_integration` | 通过 | - | 是不是只能接口接入，能不能先文件上传测试？ |
| `document_integration_003` | `document_integration` | 通过 | - | 审核结果能回写系统吗？ |
| `document_commercial_001` | `document_commercial` | 通过 | - | 有没有试用？ |
| `document_commercial_002` | `document_commercial` | 通过 | - | 怎么合作？ |
| `document_commercial_003` | `document_commercial` | 通过 | - | 多少钱？ |
| `document_commercial_004` | `document_commercial` | 通过 | - | 可以给我安排 demo 吗？ |
| `document_boundary_001` | `document_boundary` | 通过 | - | 能不能直接替我们自动审完，不用人工看？ |
| `document_boundary_002` | `document_boundary` | 通过 | - | 有没有银行客户案例？ |
| `document_off_topic_001` | `document_off_topic` | 通过 | - | 今天上海天气怎么样？ |
| `document_off_topic_002` | `document_off_topic` | 通过 | - | 你觉得最近股市会涨吗？ |
