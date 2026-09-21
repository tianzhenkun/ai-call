# 2026-09-21 TURN/TLS 上线与隔离验收

## 范围与结论

- 本次按已确认范围，启用 118 的 LiveKit 内置 TURN/TLS，并在仅关联 118 的独立安全组开放 TURN 443 和直连媒体端口。未发布前后端坐席修复，未运行数据库迁移，未拨打真实号码。
- 补齐直连规则后，浏览器默认模式实际选择 UDP 直连，强制中继实际选择 TURN/TLS，两种路径均通过双向数据与合成音频验收。
- Python RTC SDK 1.1.10 的强制中继失败在本机重现；仅替换隔离目录中的 SDK 为 1.1.19 后，同一证书、同一服务器的强制中继及双向音频通过。当前项目虚拟环境和生产 SDK 均未升级，未关闭证书校验。

## 线上变更

| 对象 | 实际状态 |
| --- | --- |
| DNS | `turn.reach.lingchen-ai.com` → `118.25.125.221`，TTL 600 |
| 证书 | 腾讯云免费证书 `ay0yjyUE`；到期北京时间 2026-12-20 08:59:59 |
| 新安全组 | `sg-r89wgykk` / `reach-turn-tls-118`，3 条入站允许 `0.0.0.0/0 TCP:443`、`TCP:7881`、`UDP:50000-50100`，未新增出站规则 |
| 关联范围 | 仅 `ins-6dpudtt9` / `118.25.125.221`；保留原 `sg-e69n10ug`，没有修改原共享安全组 |
| LiveKit 镜像 | 保持 `ai-call-transfer/livekit-server:v1.13.1-amd64` |
| LiveKit 配置 | `turn.enabled=true`，TLS 443，`udp_port=0`，`external_tls=false` |
| 证书挂载 | `./runtime/turn:/etc/livekit/turn:ro`，目录 0700、私钥/证书 0600，root 所有 |
| 对外端口 | 新增 `443:443/tcp`，保留现有 7880 私网、7881、UDP 50000–50100 映射 |
| 生效时间 | LiveKit 容器启动于 2026-09-21 11:15:27 北京时间；服务就绪约 11:15:33 |

配置基于线上原文件生成，结构化比较确认除 TURN 段、证书挂载、443 映射、域名变量外，其他配置语义不变。同步更新了线上 LiveKit 模板和证书校验脚本，避免下次渲染撤销 TURN 配置。没有用本地整份 Compose 覆盖线上其他服务配置。

重建前连续两次检查：LiveKit 房间数 0、可运行外呼任务数 0。数据库有一条 8 月 11 日开始且没有对应房间的历史 `ending` 记录，本次没有改写这条数据。

生效命令限定为：

```bash
docker compose --env-file .env -f compose.yml up -d --no-deps --force-recreate --pull never --no-build livekit
```

部署前后比对 REACH 容器 ID 和启动时间，仅 LiveKit 变化。API、邮件 worker、SIP、Egress、FreeSWITCH、PostgreSQL、Redis、知识解析容器均未重建。最终 LiveKit `running=true`、`restart_count=0`，HTTP 健康检查返回 `OK`。

用户再次明确确认公网来源及端口范围后，于北京时间 11:44:11 在该独立安全组新增 TCP 7881、UDP 50000–50100 两条规则。未改共享安全组，未开放 7880、数据库或额外 SIP 端口；安全组即时生效，本次补齐规则没有重启任何服务。

## 验收证据

1. 本地 `tests/test_ai_call_turn_deployment.py`：13 passed。
2. 直接连接公网 IP 并使用 TURN 域名校验 TLS：公信链和主机名通过，TLS 1.3；按域名连接也通过，排除了仅 IP 直连成功的差异。
3. 公网 STUN Binding 成功；未带凭据的 TURN Allocate 返回 401，未形成匿名开放中继。
4. 使用现有 `livekit-client 2.20.2`，真实浏览器连接线上 WSS 和 LiveKit，仅创建两个短期授权测试参与方。音频来自 400/600 Hz 合成信号，不访问麦克风，不播放到用户扬声器，不涉及 SIP。

| 浏览器测试 | 客户端 0 解码样本 | 客户端 1 解码样本 | 对端音频频率实测 | 选中的路径 | 结论 |
| --- | ---: | ---: | --- | --- | --- |
| 强制 relay | 236160 | 240000 | 609.375 / 398.4375 Hz | 两端均 relay + TLS | PASS |
| 默认 all（补齐直连前） | 235680 | 240000 | 609.375 / 398.4375 Hz | 两端均 relay + TLS | PASS |
| 默认 all（补齐直连后） | 236160 | 240480 | 609.375 / 398.4375 Hz | 两端均 prflx + UDP，远端 host，无中继 | PASS |
| 强制 relay（补齐直连后） | 234720 | 240960 | 609.375 / 398.4375 Hz | 两端均 relay + TLS | PASS |

各轮均验证双向可靠数据包、非零解码波形、频率与对端匹配，以及实际 selected candidate pair 成功。TURN 地址为 `turns:turn.reach.lingchen-ai.com:443?transport=tcp`。候选中的 `localProtocol=udp` 描述中继分配的 UDP 媒体端，`relayProtocol=tls` 才是浏览器到 TURN 的传输方式。新增 TCP 7881 的公网 TCP 握手也已通过，但本次默认媒体选择的是 UDP，不将 TCP 握手记为 TCP 媒体验收。

诊断脚本最初没有消费接收音频；加入静音播放和 PCM 分析后可取得解码样本。静音情况下摘要 `totalAudioEnergy` 仍为 0，因此最终用实际解码样本、波形峰值和对端频率共同断言，不能用 RTP 包数代替音频验收。默认自动选路还等待选中候选从暂态进入成功状态，而非把单次瞬时状态当作最终结果。

所有临时房间已删除，最终房间数 0。本地诊断脚本位于被 Git 忽略的 `runtime/turn-enable-Az0HdH/`；不存在业务前端强制 relay 改动。

## Python SDK 隔离升级对照

通过官方 PyPI 将 `livekit==1.1.19` 安装至上述诊断目录中的 `sdk-1.1.19`，使用 `PYTHONPATH` 覆盖该包，复用原虚拟环境的其他依赖。没有修改 `.venv`、`pyproject.toml`、`uv.lock` 或生产容器。

| 测试项 | SDK 1.1.10 | SDK 1.1.19 |
| --- | --- | --- |
| 同一 TURN/TLS 强制中继 | 信令成功，媒体连接超时 | 两端连接成功 |
| 双向数据与合成音频 | 无法进入媒体阶段 | 双向数据通过，两端各 100 帧非零音频 |
| 实际选中的媒体路径 | 无成功候选 | 两端均 `RELAY` / `TRANSPORT_TLS` / `PAIR_SUCCEEDED` |

新版本已重复验证成功。诊断脚本原先错误地对 `AudioStream` 使用 `async with`，在首次连通后暴露；按 SDK 公开接口改为 `try/finally` 调用 `aclose()`，保持音频断言，并增加实际中继候选断言。业务代码没有这个上下文管理器用法。

新 SDK 隔离环境下运行现有 LiveKit、音频传输、语音 worker 生命周期相关回归，116 passed、327 deselected。包含 mock 的工程回归与真实隔离房间媒体验证分别记录，不将回归通过等同真实电话通过。复现命令：

```bash
.venv/bin/python runtime/turn-enable-Az0HdH/verify_rtc.py --mode relay
PYTHONPATH=runtime/turn-enable-Az0HdH/sdk-1.1.19 .venv/bin/python runtime/turn-enable-Az0HdH/verify_rtc.py --mode relay
PYTHONPATH=runtime/turn-enable-Az0HdH/sdk-1.1.19 .venv/bin/python -m pytest -q tests/test_ai_call_livekit_room_media.py tests/test_ai_call_runtime_livekit_provider.py tests/test_ai_call_voice_worker_lifecycle.py tests/test_ai_call_phase_a_core.py -k 'livekit or audio_transport or voice_worker'
```

## 验收边界与剩余项

- 补齐前，本机公网来源为 `120.225.64.189`，原直连端口白名单为 `120.225.64.161/32`，默认选择中继；新增独立媒体规则后实际选中 UDP 直连。此现场差异不能直接证明上次 178 通话的历史根因。
- Python RTC SDK 1.1.10：WSS 成功但 TURN/TLS 被客户端以 `unknown certificate authority` 拒绝；同一证书在系统 TLS 和真实浏览器通过。仅升级 SDK 后强制中继成功，将问题定位到旧原生 SDK 的证书信任兼容路径，而非需要关闭校验或更换当前服务器证书；具体缺失根证书未独立提取证实。参见 [LiveKit 问题 #1301](https://github.com/livekit/rust-sdks/issues/1301) 和 [上游修复 #1336](https://github.com/livekit/rust-sdks/pull/1336)。
- 本次 SDK 实测环境为 macOS arm64。1.1.19 是已通过本机隔离验证的升级候选，生产 Linux amd64 镜像尚未构建或升级，项目依赖未锁定到新版本；发布前仍需完成对应镜像的兼容验证。
- 本次不替代浏览器麦克风、真实扬声器、SIP 手机、坐席接管、挂断及录音验收。生产 AI 的信令地址为内部 `ws://livekit:7880`，没有强制中继；不能用本机强制中继失败直接断言生产 AI 通话同样失败。
- 坐席认领/接入状态、滚动与中文文案修复仍需按独立业务发布范围处理，不能因 TURN 完成而称为全部已上线。

## 备份、回滚与续期

线上备份：`/opt/ai-call/runtime/backups/turn-tls-20260921-9tWQ3b/before.tar`，root-only，包含原 Compose、`.env`、LiveKit 运行配置、LiveKit 模板和渲染脚本。备份含密钥，不上传或提交 Git。

需要回滚时，先确认没有活动通话，再在 `/opt/ai-call/runtime` 恢复该备份并仅重建 LiveKit。云侧如需撤销，仅解除 118 与新安全组的关联，不改原共享安全组。

证书未开启自动续费。应在 2026-12-20 前完成续签，替换证书后先执行 `scripts/render-configs.sh --check`，再在无活动通话窗口仅重建 LiveKit；仅替换 PEM 不会热加载。相关流程见 [部署说明](../../deploy/ai-call-118/README.md)。
