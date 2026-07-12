# 2026-07-12 P1 VAD provider comparison

## 目的

本次只评估 WebRTC VAD 与 FSMN VAD 在现有真实客户分轨上的 detector 行为，
并与当前 authority fixture 回归分开解释。FSMN 仍为 offline/shadow 证据，
没有接入 19011 主决策链，也没有修改 runtime 阈值。

## 数据和命令

使用 19011 本地库中 6 通已有客户分轨。每通分别运行：

```bash
uv run --with funasr --with modelscope --with soundfile --with torch --with torchaudio \
  python tools/ai_call_p1_vad_provider_compare.py \
  --base-url http://127.0.0.1:19011 \
  --call-id <call_id> \
  --timeout-seconds 180
```

并用 `tools/ai_call_vad_shadow.py` 将 FSMN 窗口与离线 ASR 语音段对齐。

第二阶段使用导出器重新定位稳定声学起点，并运行 7 个正语音片段、32 个负片段。
负样本包括 6 个历史真实误停窗口，以及 continuous noise、impulse noise、AI echo、
background speech 四个可控 synthetic 样本。`background speech` 由本机 TTS 合成，
只用于暴露 VAD/provider 的权限边界，不计作独立真实客户样本。另加入 10 条
LibriSpeech dev-other 外部朗读语音，来源为 OpenSLR SLR12，License 为 CC BY 4.0，
用于模拟“电视/旁人说话但不代表客户授权插话”的 public corpus background speech。
注意：LibriSpeech 是英文语音，只能作为 source authority 边界样本，不能代表中文业务环境。
第三阶段加入 12 条 THCHS-30 `test-noise` 0dB 中文噪声语音，覆盖 car/cafe/white
三类噪声，来源为 OpenSLR SLR18，License 为 Apache License v.2.0。这些样本仍标为
`non_speech`，因为它们模拟的是“客户轨上出现外部中文人声/噪声人声”，不是已授权的近端客户插话。
第四阶段从 4 通历史真实中文业务通话客户分轨导出 6 条正语音片段，覆盖 `哪些AI啊`、
`你好好的好的`、`听一下，别说了`、`停一下停一下`、`别说了别说了，挂了吧`
和重复 `别说了`。导出音频仍保存在 `/private/tmp`，不提交录音本体：

```bash
uv run --with funasr --with modelscope --with soundfile --with torch --with torchaudio \
  python tools/ai_call_p1_vad_provider_compare.py \
  --benchmark-file docs/livekit-ai-outbound/p1-vad-benchmark.local.example.json \
  --timeout-seconds 180
```

旧 `ting` 短片因为无法定位稳定声学起点，没有纳入 benchmark。

## Detector 结果

| call | WebRTC candidates | FSMN candidates | FSMN ASR speech | FSMN slow |
| --- | ---: | ---: | ---: | ---: |
| `call_333059784225075200` | 32 | 5 | 4/4 | 0 |
| `call_333167639641690112` | 12 | 10 | 6/6 | 0 |
| `call_333790178132156416` | 9 | 5 | 4/4 | 0 |
| `call_333810711039770624` | 41 | 12 | 5/5 | 1 |
| `call_334205544210567168` | 10 | 9 | 7/7 | 1 |
| `call_334224153074712576` | 33 | 22 | 10/10 | 2 |
| **total** | **137** | **63** | **36/36** | **4** |

当前证据说明：

- FSMN 在这 6 通客户轨上没有漏掉离线 ASR 标出的 36 段语音。
- FSMN candidate 数约为 WebRTC 的 46%，表现得更保守。
- 36 段语音里有 4 段超过当前 detection lag 门槛，不能把“全检出”解释成“都能快速打断”。
- `call_333059784225075200` 还有 3 个 FSMN window 无法由现有事件解释，不能当作已证明的真人语音。

## Labeled benchmark

| provider | speech recall | within 600ms | lag p50 | lag p90 | false-positive samples | false-positive windows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| WebRTC | 13/13 | 12/13 | 160ms | 240ms | 28/32 | 64 |
| FSMN | 13/13 | 12/13 | 160ms | 240ms | 24/32 | 60 |
| WebRTC + FSMN agreement | 13/13 | 12/13 | 160ms | 240ms | 16/32 | 50 |

双证据使用 200ms candidate 对齐窗口。当前样本里 FSMN 没有增加多数正语音延迟，
并过滤了大部分 short/fan/continuous/impulse noise；加入真实中文业务正样本后，
三条 detector 路径都能检出 13/13，但 `real_zh_call_333517_bieshuole_gualeba`
在三条路径上都约 3.62s 后才出现 candidate，应作为独立慢停样本继续分析。
加入 THCHS-30 后，
WebRTC 与 FSMN 各自都对 11/12 条中文噪声人声触发 candidate，双证据降到 3/12。
这说明双证据对部分噪声人声有过滤价值，但不能把“检测到人声”升级成“可以停播”。
FSMN 和双证据仍会把一段 `opening_noise`、synthetic `ai_echo`、synthetic
`background_speech` 以及部分 public corpus 外部人声判成 speech candidate。

负样本总时长只有约 80.72 秒，因此 `47.572` / `44.599` / `37.166`
false-positive windows per minute 只用于同批样本横向比较，不能外推为产品误停率。
AI echo 和 background speech 的结果也证明，VAD provider 无法独立判断“这段人声是否来自 AI 播放、
旁人/电视，或是否代表客户真实插话”，authority echo guard / source authority
仍是必需层。

## Authority 结果

当前 authority fixture 回归为 `22/22`。完整 sample matrix 为 `32/49`，
其中 17 个失败均为历史 `api_history`，不等同于当前 authority replay 失败。

这两组数字不能直接合并成一个胜率：provider compare 只复放 detector，
authority fixture 才复放 candidate/defer/pre-stop 决策链。

`real_zh_call_333517_bieshuole_gualeba` 已追加 authority red fixture：
`docs/livekit-ai-outbound/reports/phase-e-p1-authority-red-fixtures-call_333517350307270656-2026-07-12.json`。
这个 fixture 不进入主 `fixture-only` gate；主 gate 仍保持 `22/22`。独立 fixture
已由 `missing_pre_stop` 修复为通过，当前结果为 `1/1`：

- 首个稳定 candidate 在 `5220ms`，对应 `sip_ai_playback_echo_deferred` /
  `awaiting_ai_playback_echo_guard`。当帧 uplink RMS 约 `-25.61dBFS`，
  AI playout RMS 约 `-16.14dBFS`，uplink 比 AI 低约 `9.47dB`。
- 第二段稳定 candidate 在 `6840ms`，AI 轨已降到约 `-29.88dBFS`，
  后续在 `6880ms` 通过 `ai_receded_compact_two_burst_turn` 证据 pre-stop。
  该路径要求两段累计约 `400ms`、RMS range `5.63dB`、max SNR `16.21dB`、
  当前段接近 `pre_stop_min_duration_ms`，并且 uplink 已明显高于 AI playout。

因此这个样本现在拆成两个独立瓶颈：detector 三路径共同晚到约 `3.62s`；
late candidate 之后的 authority 未授权问题已由更窄的 AI-receded 两段证据解决。
detector 起声慢已进一步拆到逐帧 reason span，见
`docs/livekit-ai-outbound/reports/2026-07-12-p1-detector-onset-diagnosis-call_333517350307270656.md`。
结论是：1600ms 起 WebRTC/FSMN 多次识别到人声，但片段被 `below_min_snr` /
`below_min_rms` 打断，无法连续积累到 candidate 所需时长，直到 `5220ms`
才首次形成 candidate。

## 结论

现有证据足以支持 FSMN 继续作为 shadow/offline 候选，但不足以支持进入主链：

- 小样本内 FSMN 在不损失 recall/lag 的前提下减少了噪声 candidate，是明确的正向信号。
- 32 个负样本中仍有 opening noise、AI echo、background speech 和中文噪声人声误判；加入 public corpus speech 后，FSMN 的噪声优势不能转化成“人声场景可授权停播”。
- 双证据在 THCHS-30 中文噪声人声上优于单 provider，但仍有误判，只能作为 evidence，不应直接作为停播 authority。
- 6 条真实中文业务正样本补齐后没有新增漏检，但暴露了一个三路径共同慢检的 call-end stop command；authority fixture 已能在 late candidate 之后授权停播，剩余问题仍是 detector 起点偏晚，不能靠调单个 authority 阈值解决。
- detector onset diagnosis 证明该慢检来自断续低 SNR/RMS 语音，不能据此直接降低全局 detector 阈值；如果后续优化，应先补真实中文电视/旁人/多人环境负样本，再评估断续多 burst 的 pre-candidate evidence。
- provider compare 的 `preStopCount` 是 detector replay 事件数，不是实际 authority 停播次数，不能用作产品误停率。

## 停止条件和下一步

本地已确认的 6 个历史误停窗口已经全部进入 labeled benchmark，不再继续搬运
`api_history`。本地集合也已覆盖 opening/mid-call fan、continuous、impulse、echo、一个 synthetic
background speech 边界样本、10 条 LibriSpeech public corpus background speech，
12 条 THCHS-30 中文 noisy background speech，以及 6 条真实中文业务正语音。

剩余数据缺口是真实业务通话里的中文电视/旁人/多人环境负样本，以及必要时的更大中文语料
或 MUSAN noise/music 扩展。`real_zh_call_333517_bieshuole_gualeba`
已经转成独立 authority fixture，并通过 `ai_receded_compact_two_burst_turn`
证据由 `missing_pre_stop` 修复为通过。后续如果继续修改 echo guard /
deferred episode 授权，必须保持这个 fixture 通过，并同时保持主 `fixture-only`
gate 为 `22/22`。它不应通过继续拨测或复制相邻窗口来凑数。
后续只需要在获得独立客户样本或公共语料后追加 JSON 条目并重跑现有命令，不需要再改 benchmark 工具。

只有扩充后的样本仍证明 FSMN 或双证据在 recall、误检和延迟上稳定优于当前路径，
才讨论让 FSMN 进入 authority 证据；当前保持 WebRTC + authority 不变。

`docs/livekit-ai-outbound/p1-vad-benchmark.local.example.json` 已加入
`benchmarkGates`，用于冻结当前 provider 基线：样本数、正负样本数、各 provider
检出数、600ms 内检出数、p90 延迟和误检窗口数都不能退化。没有新增真实中文负样本前，
不继续降低全局 detector 阈值，不把 FSMN 或双证据直接接入停播主链。

后续真实样本按 `docs/livekit-ai-outbound/p1-sip-barge-in-sample-intake.md`
记录即可：只需要 `call_id`、大概时间、期望停/不停/延迟确认和原因，录音剪辑与
replay 固化仍由离线工具完成。
