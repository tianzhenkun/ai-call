# 开场白允许打断

提示词场景在“开场白”旁提供“允许打断”开关，对应 API 字段 `openingBargeInEnabled`、数据库字段 `opening_barge_in_enabled`。新场景和旧数据缺省均为 `true`，保持已有行为。

| 系统 `AI_CALL_BARGE_IN_ENABLED` | 场景开场白开关 | 开场白 | 后续对话 |
| --- | --- | --- | --- |
| 关闭 | 任意 | 不允许打断 | 不允许打断 |
| 开启 | 关闭 | 完整播放后再回应客户 | 按现有打断策略 |
| 开启 | 开启 | 按现有打断策略 | 按现有打断策略 |

系统总开关沿用现有后端配置，不新增全局页面入口。场景开关不能覆盖关闭的总开关。

关闭开场白打断时，从请求模型开场白之前开始保护，直到音频队列和播放器缓冲播放完毕。模型生成完成并不代表音频播放完成。保护期间继续接收音频、记录和识别客户话语，有效话语在开场白播放完毕后进入原有回复流程；挂断、模型或播放错误仍按原有终止流程清理。

开关随场景保存、预览、版本详情、版本对比和历史版本恢复流转。外呼任务创建时把值写入提示词快照；静态和动态业务提示词都使用任务快照中的策略，动态业务内容仍按原有方式解析。旧快照缺字段默认 `true`。修改场景不追溯改变已创建任务或进行中的通话。

## 部署顺序

1. 在目标数据库执行 `sql/prompt-opening-barge-in-postgres.sql`，再部署 API 和语音运行时；迁移可重复执行，不覆盖已保存的 `false`。
2. 部署 REACH 前端；前后端同时支持新字段后再保存开关。
3. 用获准的测试号码分别验证 Web 与 SIP：开启时可以打断；关闭时首句完整播放、客户语音保留且随后得到回应；首句结束后恢复总开关策略；通话挂断正常收敛。

## 本地验证

```bash
.venv/bin/python -m pytest tests/test_ai_call_phase_b4_prompt_config.py tests/test_ai_call_outbound_rule_task.py tests/test_ai_call_runtime_livekit_provider.py tests/test_ai_call_phase_a_core.py -q --show-capture=no
UV_NO_SYNC=1 bash tools/run_ai_call_runtime_postgres_tests.sh tests/postgres/test_ai_call_opening_barge_in_migration.py -q
```

第二条使用已安装的 `.venv`，由现有脚本创建并销毁独立 PostgreSQL 容器，不连接业务数据库。运行时回归使用合成 PCM 和隔离的 provider/publisher，覆盖首帧前插话、生成中插话、缓冲播放阶段插话、总开关关闭、无音频响应、挂断以及模型和播放异常，不能替代真实电话验收。

本次浏览器检查验证了本地页面开关位置、开启和关闭状态；没有保存线上配置或发起真实通话。

生产发布见 [发布记录](production-deployed-20260923-all.md)；真实通话打断效果仍需受控号码验收。
