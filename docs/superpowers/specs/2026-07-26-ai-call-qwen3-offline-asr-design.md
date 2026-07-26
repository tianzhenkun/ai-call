# AI Call Qwen3 离线转写改造设计

## 背景

当前 AI Call 在通话结束后，将 `customer` 和 `human_agent` 原始分轨交给
DashScope `paraformer-v2`，转写结果写入 `ai_call_dialogue_segment`，再供通话记录和
语义分析使用。

在通话 `call_339735770500182016` 中，同一份原始分轨经过交叉识别后得到：

- `paraformer-v2` 将多句正常口语识别为“Yelyellow”“七碗金龙元宵”等错误内容；
- `qwen3-asr-flash-filetrans` 能恢复“转人工”“下面条”“喝水”“挂了吧”等实际对话；
- 客户与坐席分轨不存在足以解释主要错误的长期回声或串音。

因此本次修复目标是替换不适配的离线转写模型，不通过语义分析门禁掩盖错误转写。

## 目标

1. 支持 DashScope `qwen3-asr-flash-filetrans` 文件转写协议。
2. 将新环境默认离线 ASR provider 切换为 Qwen3 文件转写。
3. 保留现有 Paraformer provider，可通过配置快速回退。
4. 继续复用现有 ASR job、对话分段和语义分析数据流，不修改表结构。
5. 用自动化测试锁定请求协议、结果解析、默认配置和回退能力。

## 非目标

1. 不同时运行两套模型，也不增加自动“选优”逻辑。
2. 不修改实时 Qwen 转写和 `48k -> 16k` 音频重采样。
3. 不修改坐席页面、通话测试台或语义分析门禁。
4. 不自动重跑历史 ASR job；新配置只影响重启后创建的新任务。
5. 不删除 Paraformer provider。

## 方案选择

### 方案一：直接删除 Paraformer 并硬切 Qwen3

改动最少，但出现供应商模型回归时无法通过配置快速回退，不采用。

### 方案二：每通同时运行 Paraformer 和 Qwen3

能持续收集对比数据，但成本和延迟翻倍，而且系统缺少可靠的自动真值判定，不采用。

### 方案三：可配置单 provider，Qwen3 作为默认值

每通只运行一套模型，保留 Paraformer 回退入口；既修复当前根因，又维持运行边界简单。
本次采用该方案。

## 配置

保留现有配置名称：

```text
AI_CALL_OFFLINE_ASR_PROVIDER
AI_CALL_OFFLINE_ASR_MODEL
AI_CALL_OFFLINE_ASR_LANGUAGE_HINTS
```

默认值调整为：

```text
AI_CALL_OFFLINE_ASR_PROVIDER=dashscope_qwen_filetrans
AI_CALL_OFFLINE_ASR_MODEL=qwen3-asr-flash-filetrans
AI_CALL_OFFLINE_ASR_LANGUAGE_HINTS=zh
```

回退到 Paraformer 时显式设置：

```text
AI_CALL_OFFLINE_ASR_PROVIDER=dashscope_paraformer
AI_CALL_OFFLINE_ASR_MODEL=paraformer-v2
```

## Provider 设计

新增 `DashScopeQwenFileTranscriptionAsrProvider`，复用现有 provider 的任务提交、轮询、
结果下载和句子解析流程，只覆盖 Qwen3 所需的提交载荷。

Qwen3 提交载荷：

```json
{
  "model": "qwen3-asr-flash-filetrans",
  "input": {
    "file_url": "https://oss.example.test/recordings/customer.ogg?signature=short-lived"
  },
  "parameters": {
    "channel_id": [0],
    "language": "zh",
    "enable_itn": true,
    "enable_words": true
  }
}
```

现有 Paraformer 继续使用：

```json
{
  "model": "paraformer-v2",
  "input": {
    "file_urls": [
      "https://oss.example.test/recordings/customer.ogg?signature=short-lived"
    ]
  },
  "parameters": {
    "language_hints": ["zh"]
  }
}
```

两者共享以下输出契约：

```python
OfflineAsrResult(
    task_id=...,
    transcription_url=...,
    segments=[
        OfflineAsrSegment(
            text=...,
            begin_time_ms=...,
            end_time_ms=...,
        )
    ],
)
```

Qwen3 任务状态中的结果地址位于 `output.result.transcription_url`，现有解析器需要兼容
该嵌套结构。下载后的 `transcripts[].sentences[]` 结构继续由现有句子解析逻辑处理。

## 数据流

```text
录音分轨完成
  -> 根据 AI_CALL_OFFLINE_ASR_PROVIDER 创建单一 provider
  -> 使用短时预签名 playUrl 提交 DashScope 文件转写
  -> 轮询异步任务
  -> 下载 transcription_url JSON
  -> 转为 OfflineAsrSegment
  -> 写入现有 ai_call_asr_job / ai_call_dialogue_segment
  -> 触发现有语义分析
```

`ai_call_asr_job.provider` 和 `model` 保存实际 provider/model，用于通话记录复核和回退后
区分历史结果。

## 异常和回退

1. API key 或录音 URL 为空：保持现有明确异常。
2. 提交、轮询、结果下载失败：保持现有 ASR job 失败状态和 `failure_message`。
3. Qwen3 返回成功但没有 `transcription_url`：任务按失败处理，不生成空白成功记录。
4. Qwen3 返回空句子：沿用现有空结果处理，不伪造文本。
5. 需要回退时修改 provider/model 配置并重启服务；历史 job 不修改。

## 测试与验收

1. 单元测试验证 Qwen3 使用单数 `file_url`，并提交语言、ITN、词级时间戳参数。
2. 单元测试验证 Qwen3 可解析 `output.result.transcription_url`。
3. 单元测试验证 Qwen3 `transcripts[].sentences[]` 被转换为带毫秒时间的分段。
4. 单元测试验证默认 provider/model 为 Qwen3。
5. 单元测试验证 Paraformer 请求载荷和配置回退保持可用。
6. 运行离线 ASR 相关测试及 AI Call 记录回归测试。
7. 重启 `19011` 后发起新通话，确认新 ASR job 的 provider/model 为 Qwen3，且“转人工、
   下面条、喝水、挂了吧”等口语能正确进入对话记录。

## 修改边界

当前工作树已有录音预签名 URL 相关未提交修改。本次必须保留该修改，因为 DashScope
文件转写要求能够访问录音 URL；实现只增量修改离线 ASR provider、启动选择、默认配置、
定向测试和本设计对应的实现计划，不覆盖其他未提交内容。
