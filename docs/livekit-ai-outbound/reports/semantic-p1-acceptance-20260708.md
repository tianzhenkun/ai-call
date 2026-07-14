# AI Call 语义分析 P1 验收报告

> 本报告基于当前代码重建 transcript snapshot，并与历史通话的事件线和已存语义分析结果做对比。历史 analysis_result 不会因代码修复自动回填。

- 基地址：`http://127.0.0.1:19012`
- 入口类型：`web`
- 请求通话数：`10`
- 结论：`失败`
- 高危问题：`13`
- 需复核问题：`25`
- 拉取失败：`0`

| Call ID | 场景 | 结论 | 高危 | 需复核 | Timeline 高危 | 用户轮次 | record_only | 质量原因 |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `call_333159447668371456` | `intro_document` | 通过 | 0 | 0 | 0 | 7 | 4 | - |
| `call_333155283616481280` | `intro_overseas` | 失败 | 1 | 3 | 1 | 18 | 12 | - |
| `call_333151411657326592` | `intro_contract` | 失败 | 1 | 2 | 0 | 14 | 10 | - |
| `call_333144718539116544` | `intro_geo` | 失败 | 1 | 3 | 1 | 19 | 10 | - |
| `call_333134408060080128` | `intro_contract` | 失败 | 1 | 1 | 1 | 11 | 6 | - |
| `call_333076563464048640` | `intro_geo` | 失败 | 1 | 5 | 1 | 15 | 8 | - |
| `call_333071141309095936` | `intro_contract` | 失败 | 1 | 3 | 0 | 15 | 6 | - |
| `call_333054662788419584` | `intro_contract` | 需复核 | 0 | 5 | 0 | 7 | 5 | - |
| `call_333047300754763776` | `intro_contract` | 失败 | 4 | 0 | 4 | 9 | 7 | - |
| `call_333005895715184640` | `intro_geo` | 失败 | 3 | 3 | 2 | 13 | 6 | - |

## 问题明细

- `call_333155283616481280` type=`slow_ai_first_audio_after_customer_turn` severity=`high` reason=slow_response_audio_after_customer_turn text=-
- `call_333155283616481280` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=你们什么时候找这个海外的客户啊？
- `call_333155283616481280` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=先看线索量还是触达建议
- `call_333155283616481280` type=`stale_stored_transcript_snapshot` severity=`review` reason=stored transcript snapshot differs from snapshot rebuilt by current code text=-
- `call_333151411657326592` type=`assistant_text_leaked_into_semantic_result` severity=`high` reason=stored semantic result contains text from assistant turn text=主要是短期合作协议
- `call_333151411657326592` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=你们的准确率怎么样啊
- `call_333151411657326592` type=`stale_stored_transcript_snapshot` severity=`review` reason=stored transcript snapshot differs from snapshot rebuilt by current code text=-
- `call_333144718539116544` type=`slow_ai_first_audio_after_customer_turn` severity=`high` reason=call_end_tool_ignored_before_next_audio text=-
- `call_333144718539116544` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=第不赛个
- `call_333144718539116544` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=Come没窝吗
- `call_333144718539116544` type=`stale_stored_transcript_snapshot` severity=`review` reason=stored transcript snapshot differs from snapshot rebuilt by current code text=-
- `call_333134408060080128` type=`ai_started_during_customer_speech` severity=`high` reason=- text=-
- `call_333134408060080128` type=`stale_stored_transcript_snapshot` severity=`review` reason=stored transcript snapshot differs from snapshot rebuilt by current code text=-
- `call_333076563464048640` type=`unexpected_stale_audio_drop` severity=`high` reason=session_not_ai_speaking text=-
- `call_333076563464048640` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=有demo吗？
- `call_333076563464048640` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=可以测试吗？
- `call_333076563464048640` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=你不之前有别的公司接入你们产品吗？
- `call_333076563464048640` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=你在那里吧？
- `call_333076563464048640` type=`stale_stored_transcript_snapshot` severity=`review` reason=stored transcript snapshot differs from snapshot rebuilt by current code text=-
- `call_333071141309095936` type=`record_only_user_text_leaked_into_semantic_result` severity=`high` reason=stored semantic result contains record_only user text text=可以的以后再说吧
- `call_333071141309095936` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=把我的核桃上了，上去看一下效果
- `call_333071141309095936` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=我强把我的核桃上了，上去看一下效果
- `call_333071141309095936` type=`stale_stored_transcript_snapshot` severity=`review` reason=stored transcript snapshot differs from snapshot rebuilt by current code text=-
- `call_333054662788419584` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=强大一点
- `call_333054662788419584` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=费时费力
- `call_333054662788419584` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=强大一点
- `call_333054662788419584` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=可以了
- `call_333054662788419584` type=`stale_stored_transcript_snapshot` severity=`review` reason=stored transcript snapshot differs from snapshot rebuilt by current code text=-
- `call_333047300754763776` type=`unexpected_stale_audio_drop` severity=`high` reason=session_not_ai_speaking text=-
- `call_333047300754763776` type=`unexpected_stale_audio_drop` severity=`high` reason=session_not_ai_speaking text=-
- `call_333047300754763776` type=`slow_ai_first_audio_after_customer_turn` severity=`high` reason=slow_response_audio_after_customer_turn text=-
- `call_333047300754763776` type=`slow_ai_first_audio_after_customer_turn` severity=`high` reason=slow_response_audio_after_customer_turn text=-
- `call_333005895715184640` type=`ai_started_during_customer_speech` severity=`high` reason=- text=-
- `call_333005895715184640` type=`unexpected_stale_audio_drop` severity=`high` reason=session_not_ai_speaking text=-
- `call_333005895715184640` type=`semantic_result_changed_by_evidence_gate` severity=`high` reason=current semantic evidence gate would change stored result text=-
- `call_333005895715184640` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=AI是否推荐品牌
- `call_333005895715184640` type=`semantic_result_quote_absent_from_rebuilt_user_turns` severity=`review` reason=stored semantic result quotes user text absent from rebuilt snapshot text=那种优化吧
- `call_333005895715184640` type=`stale_stored_transcript_snapshot` severity=`review` reason=stored transcript snapshot differs from snapshot rebuilt by current code text=-
