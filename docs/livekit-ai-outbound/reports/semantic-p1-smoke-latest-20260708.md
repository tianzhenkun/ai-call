# AI Call 语义分析 P1 验收报告

> 本报告基于当前代码重建 transcript snapshot，并与历史通话的事件线和已存语义分析结果做对比。历史 analysis_result 不会因代码修复自动回填。

- 基地址：`http://127.0.0.1:19012`
- 入口类型：`web`
- 请求通话数：`1`
- 结论：`失败`
- 高危问题：`1`
- 需复核问题：`1`
- 拉取失败：`0`

| Call ID | 场景 | 结论 | 高危 | 需复核 | Timeline 高危 | 用户轮次 | record_only | 质量原因 |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `call_333182159408410624` | `intro_contract` | 失败 | 1 | 1 | 1 | 12 | 5 | - |

## 问题明细

- `call_333182159408410624` type=`slow_ai_first_audio_after_customer_turn` severity=`high` reason=slow_response_audio_after_customer_turn text=-
- `call_333182159408410624` type=`stale_stored_transcript_snapshot` severity=`review` reason=stored transcript snapshot differs from snapshot rebuilt by current code text=-
