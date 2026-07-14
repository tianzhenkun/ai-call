# intro_contract 提示词静态覆盖报告

> 本报告检查 28 条测试用例的关键约束是否已经出现在最终拼接提示词中。它不能证明模型真实回答质量。

- 场景：`intro_contract`
- 模式：`静态提示词覆盖`
- 结论：`通过`
- 通过数：`28/28`
- 是否允许打断：`否`
- 提示词哈希：`sha256:83d1db0f35028e85634ebb1c72735f00f7b023abcd982b6eaf0435b8c7dc922d`
- 开场白：您好张总，我是灵宸智能助手，想简单介绍一下我们的合同智能审查产品，请问现在方便吗？

| 用例 | 类别 | 结果 | 缺失锚点 | 首轮客户问题 |
|---|---|---|---|---|
| `contract_availability_001` | `contract_availability` | 通过 | - | 我现在在开会，没空。 |
| `contract_availability_002` | `contract_availability` | 通过 | - | 你先说重点，我只有半分钟。 |
| `contract_reject_001` | `contract_reject` | 通过 | - | 不需要，我们暂时没有合同审查需求。 |
| `contract_basic_001` | `contract_basic` | 通过 | - | 你们这个合同智能审查是做什么的？ |
| `contract_basic_002` | `contract_basic` | 通过 | - | 这和普通大模型审合同有什么区别？ |
| `contract_term_001` | `contract_basic` | 通过 | - | DeepLaw 是什么？ |
| `contract_method_001` | `contract_method` | 通过 | - | 你们具体怎么审合同？ |
| `contract_method_002` | `contract_method` | 通过 | - | 能审哪些风险？ |
| `contract_method_003` | `contract_method` | 通过 | - | 会给修改建议吗，还是只提示风险？ |
| `contract_rules_001` | `contract_rules` | 通过 | - | 我们的红线条款可以配置进去吗？ |
| `contract_rules_002` | `contract_rules` | 通过 | - | 风险依据从哪里来？ |
| `contract_metrics_001` | `contract_metrics` | 通过 | - | 效果怎么看？有哪些指标？ |
| `contract_metrics_002` | `contract_metrics` | 通过 | - | 你们能保证准确率多少？能保证不会出纠纷吗？ |
| `contract_metrics_003` | `contract_metrics` | 通过 | - | 多久能上线？两周能见效吗？ |
| `contract_security_001` | `contract_data_security` | 通过 | - | 合同数据很敏感，安全吗？ |
| `contract_security_002` | `contract_data_security` | 通过 | - | 能私有化部署吗？ |
| `contract_integration_001` | `contract_integration` | 通过 | - | 能接我们的 OA 审批系统吗？ |
| `contract_integration_002` | `contract_integration` | 通过 | - | 是不是只能接口接入，能不能先上传合同测试？ |
| `contract_integration_003` | `contract_integration` | 通过 | - | 能导出 Word 审查报告吗？ |
| `contract_commercial_001` | `contract_commercial` | 通过 | - | 有没有试用？ |
| `contract_commercial_002` | `contract_commercial` | 通过 | - | 怎么合作？ |
| `contract_commercial_003` | `contract_commercial` | 通过 | - | 多少钱？ |
| `contract_commercial_004` | `contract_commercial` | 通过 | - | 可以给我安排 demo 吗？ |
| `contract_boundary_001` | `contract_boundary` | 通过 | - | 能不能直接替我们法务把合同审完，不用人看？ |
| `contract_boundary_002` | `contract_boundary` | 通过 | - | 如果对方违约了，我这个条款能不能告赢？ |
| `contract_boundary_003` | `contract_boundary` | 通过 | - | 有没有银行或者大企业案例？ |
| `contract_off_topic_001` | `contract_off_topic` | 通过 | - | 今天上海天气怎么样？ |
| `contract_off_topic_002` | `contract_off_topic` | 通过 | - | 你觉得最近股市会涨吗？ |
