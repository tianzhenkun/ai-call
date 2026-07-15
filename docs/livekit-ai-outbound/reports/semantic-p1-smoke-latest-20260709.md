# AI Call 语义分析 P1 验收报告

> 本报告优先使用语义分析入库时保存的 transcript snapshot 复核结果；timeline 与 AI 口吻检查仍基于当前展示分段重建。历史 analysis_result 不会因代码修复自动回填。

- 基地址：`http://127.0.0.1:19012`
- 入口类型：`web`
- 请求通话数：`1`
- 结论：`通过`
- 高危问题：`0`
- 需复核问题：`0`
- 拉取失败：`0`

| Call ID | 场景 | 结论 | 高危 | 需复核 | Timeline 高危 | 用户轮次 | record_only | 质量原因 |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `call_333406123335614464` | `intro_document` | 通过 | 0 | 0 | 0 | 16 | 8 | `offline_asr_span_realtime_divergence`<br>`offline_asr_shadowed_by_richer_realtime` |

## 问题明细

- 未发现高危或需复核问题。
