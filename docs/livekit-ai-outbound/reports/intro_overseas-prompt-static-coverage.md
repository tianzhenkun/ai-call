# intro_overseas 提示词静态覆盖报告

> 本报告检查 25 条测试用例的关键约束是否已经出现在最终拼接提示词中。它不能证明模型真实回答质量。

- 场景：`intro_overseas`
- 模式：`静态提示词覆盖`
- 结论：`通过`
- 通过数：`25/25`
- 是否允许打断：`否`
- 提示词哈希：`sha256:189c442b621df17dae9ccfbe2e306705d53537461396a78f7854fb1d5746f52d`
- 开场白：您好张总，我是灵宸智能助手，想简单介绍一下我们的海外获客智能体，请问现在方便吗？

| 用例 | 类别 | 结果 | 缺失锚点 | 首轮客户问题 |
|---|---|---|---|---|
| `overseas_availability_001` | `overseas_availability` | 通过 | - | 我现在在开会，没空。 |
| `overseas_availability_002` | `overseas_availability` | 通过 | - | 你先说重点，我只有半分钟。 |
| `overseas_reject_001` | `overseas_reject` | 通过 | - | 不需要，我们暂时不做海外。 |
| `overseas_basic_001` | `overseas_basic` | 通过 | - | 你们这个海外获客智能体是做什么的？ |
| `overseas_basic_002` | `overseas_basic` | 通过 | - | 这和买海外客户名单有什么区别？ |
| `overseas_method_001` | `overseas_method` | 通过 | - | 你们具体怎么帮我们找客户？ |
| `overseas_method_002` | `overseas_method` | 通过 | - | 什么是目标客户画像？ |
| `overseas_method_003` | `overseas_method` | 通过 | - | 线索是怎么评分的？ |
| `overseas_metrics_001` | `overseas_metrics` | 通过 | - | 效果怎么看？你们有什么指标？ |
| `overseas_metrics_002` | `overseas_metrics` | 通过 | - | 能保证多少线索吗？回复率能提升多少？ |
| `overseas_metrics_003` | `overseas_metrics` | 通过 | - | 多久能见效？两周可以看到成交吗？ |
| `overseas_data_001` | `overseas_data_compliance` | 通过 | - | 你们线索数据从哪里来？ |
| `overseas_data_002` | `overseas_data_compliance` | 通过 | - | 会不会涉及隐私问题？ |
| `overseas_data_003` | `overseas_data_compliance` | 通过 | - | 能不能自动群发 LinkedIn 或邮件？ |
| `overseas_integration_001` | `overseas_integration` | 通过 | - | 能接我们的 CRM 吗？ |
| `overseas_integration_002` | `overseas_integration` | 通过 | - | 能导出 Excel 吗？ |
| `overseas_integration_003` | `overseas_integration` | 通过 | - | 能接邮箱和 LinkedIn 吗？ |
| `overseas_commercial_001` | `overseas_commercial` | 通过 | - | 有没有试用？ |
| `overseas_commercial_002` | `overseas_commercial` | 通过 | - | 怎么合作？ |
| `overseas_commercial_003` | `overseas_commercial` | 通过 | - | 多少钱？ |
| `overseas_commercial_004` | `overseas_commercial` | 通过 | - | 可以给我安排 demo 吗？ |
| `overseas_boundary_001` | `overseas_boundary` | 通过 | - | 能不能直接给我 1000 条客户名单？ |
| `overseas_boundary_002` | `overseas_boundary` | 通过 | - | 有没有同行案例？ |
| `overseas_off_topic_001` | `overseas_off_topic` | 通过 | - | 今天上海天气怎么样？ |
| `overseas_off_topic_002` | `overseas_off_topic` | 通过 | - | 你觉得最近股市会涨吗？ |
