# 自定义音色表达风格

音色管理支持自然、温和、专业、活泼、严肃五种表达风格。创建音色时可选，已有自定义音色可在列表的“表达风格”列修改并保存；取消不保存。内置音色保持默认表达。

风格通过固定模板写入 Qwen Realtime 的 `instructions`，引导情绪和表达方式，实际表现由模型生成。默认“自然”不追加指令；这项配置不提供精确语速、音高或情绪强度参数。

## 生效范围

- 试听音频与隔离试听会话读取音色当前设置。
- 新建外呼任务将设置冻结到 `config_snapshot_json.voice.speakingStyle`。正式 OwnerRuntime 和旧 SIP 调用入口均读取任务快照，修改音色不会改变已建任务。升级前没有该字段的任务仍使用自然表达。
- 无外呼任务快照的直接会话读取当前租户的音色设置；进行中的会话不热更新。
- 风格只调整表达，不替换业务话术、产品事实、开场白和工具规则；最终指令重新计算 `prompt_hash`。

## 接口与迁移

`PATCH /ai-call/tenant-voice-profiles/{id}/speaking-style`，请求为 `{"speakingStyle":"gentle"}`。需 `ai_call:voice:manage` 权限，只允许修改当前租户处于 `ENABLED`、`DISABLED`、`DELETE_FAILED` 的音色。拒绝其他枚举及额外字段。

复刻请求与音色查询响应均增加 `speakingStyle`；复刻请求省略时为 `natural`。

先执行 [增量迁移](sql/voice-speaking-style-postgres.sql)，再发布 API、运行时和前端。迁移为旧音色补齐 `natural`，可重复执行，数据库约束限定五种值。新建库的音色建表 SQL 已同步字段。

## 本地验证

```sh
UV_NO_SYNC=1 uv run pytest -q tests/test_ai_call_voice_service.py tests/test_ai_call_voice_preview.py tests/test_ai_call_voice_api.py tests/test_ai_call_voice_models.py tests/test_ai_call_outbound_rule_task.py tests/test_ai_call_runtime_livekit_provider.py tests/test_ai_call_phase_b4_prompt_config.py
UV_NO_SYNC=1 bash tools/run_ai_call_runtime_postgres_tests.sh -q tests/postgres/test_ai_call_voice_style_migration.py
```

前端仓库执行 `npm test -- --runInBand src/modules/reach/pages/voices` 和 `npm run tsc`。

自动化使用隔离数据库与模型替身，验证保存、租户隔离、失败回滚、冻结和指令传递；不能替代真实模型的听感验收。当前 8012 前端代理线上后端且登录已过期，尚未完成真实页面保存及模型试听验证。

2026-09-23 已核对 19013 进程使用本机隔离 PostgreSQL，并执行增量迁移；旧音色保留为自然表达。未发布线上。
