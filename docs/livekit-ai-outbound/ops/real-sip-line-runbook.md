# 真实 SIP 线路拨测 Runbook

最后更新：2026-06-09

## 1. 文档定位

本文档说明真实 SIP 线路拨测前后的操作边界、FreeSWITCH 停止/恢复要求、抓包工具使用和证据保存要求。

它对应 [../phases/phase-01-real-sip-entry.md](../phases/phase-01-real-sip-entry.md)，只用于真实电话拨测，不用于普通 Web 入口验证。

## 2. 什么时候需要停止 FreeSWITCH

如果 LiveKit SIP 要使用现有线路白名单对应的 `5080` 端口，而该端口当前被 FreeSWITCH 占用，则真实拨测前必须先停止 FreeSWITCH。

如果满足以下任一条件，不应为了测试而停止 FreeSWITCH：

1. 只是做浏览器 Web 入口验证。
2. LiveKit SIP 使用独立端口，且服务商白名单已放行该端口。
3. 测试不需要真实 SIP trunk。
4. 当前 FreeSWITCH 承载线上业务且没有明确维护窗口和回滚方案。

结论：

```text
Web 入口验证：不需要停止 FreeSWITCH
真实 SIP 拨测且抢占 5080：需要先停止 FreeSWITCH
真实 SIP 拨测但使用独立已放行端口：不需要停止 FreeSWITCH
```

## 3. 拨测前确认

拨测前必须确认：

1. 测试窗口已确认。
2. 被叫测试手机号已确认。
3. 被叫手机号只在受控环境使用，不写入仓库文档。
4. 当前服务器公网 IP 已在线路服务商白名单内。
5. LiveKit SIP signaling 端口已在线路服务商白名单内。
6. LiveKit SIP RTP 端口范围已在安全组 / 防火墙中放行。
7. 如果更换公网 IP、SIP 端口、主叫号或部署多节点，已提前让服务商重新开放白名单并调整线路配置。
8. FreeSWITCH 停止和恢复步骤已准备。
9. 抓包目录已准备。
10. 测试结束后恢复责任人明确。

## 4. 当前已知线路参数

```text
SIP proxy: 47.94.86.132:5089
主叫号码: 037123124845
传输协议: UDP
号码格式: 国内原始手机号，不加 +86 / 86 / 0 / 9
历史预期 codec: PCMA/8000
真实拨测 codec: PCMU/8000
DTMF: telephone-event / RFC2833，payload 101
RTP profile: RTP/AVP
```

当前测试服务器：

```text
公网 IP: 111.229.146.182
线路白名单规则: 绑定公网 IP + SIP signaling 端口
LiveKit Server HTTP/API: 7880
LiveKit Server TCP fallback / RTC: 7881
LiveKit ICE UDP: 50000-50100
LiveKit SIP signaling: 5080
LiveKit SIP RTP: 16384-16484
```

如果后续更换公网 IP、SIP signaling 端口、主叫号或部署多节点，需要服务商重新开放白名单并调整线路配置。

Redis：

```text
host: 118.89.137.44
port: 6379
database: 15
password: 通过 REDIS_PASSWORD 环境变量配置，不写入仓库文档
```

数据库：

```text
数据库类型: PostgreSQL
host: 118.89.137.44
port: 15432
database: recov_local
schema: public
username: postgres
password: 通过 DB_PASSWORD 环境变量配置，不写入仓库文档
JDBC URL: jdbc:postgresql://118.89.137.44:15432/recov_local
SSL / TLS 要求: 未确认
```

## 5. 停止 FreeSWITCH 前检查

检查端口占用：

```bash
ss -lntup | grep -E '(:5080|:7880|:7881)'
```

检查容器状态：

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
```

检查网关服务状态：

```bash
systemctl status recov-ten-gateway.service --no-pager
```

如果 `5080` 被 `sip_realtime_freeswitch` 占用，且本次 LiveKit SIP 必须使用 `5080`，才能进入停止步骤。

## 6. 停止 FreeSWITCH

优先使用固化脚本：

```bash
/opt/livekit-stage1/scripts/stop_freeswitch.sh
```

停止后必须确认：

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' | grep -E 'freeswitch|5080' || true
ss -lntup | grep ':5080' || true
```

要求：

1. `sip_realtime_freeswitch` 不再占用 `5080`。
2. 没有 LiveKit 旧容器残留占用 `5080`。
3. 如果停止脚本报错但端口已释放，必须记录原因，不能直接忽略。

## 7. 启动 LiveKit 测试服务

启动前确认 `.env` 中只保存变量，不把密钥写入文档：

```text
LIVEKIT_SIP_PORT=5080
LIVEKIT_SIP_RTP_PORT=16384-16484
SIP_PROVIDER_ADDRESS=47.94.86.132:5089
REDIS_PASSWORD=从受控环境读取
DB_PASSWORD=从受控环境读取
```

建议启动和检查顺序：

| 顺序 | 服务 | 检查点 |
|---:|---|---|
| 1 | PostgreSQL | 后端能连接测试库，Phase 00 表已存在 |
| 2 | Redis | LiveKit Server、LiveKit SIP、后端使用的 Redis 可达 |
| 3 | LiveKit Server | HTTP / API、WebSocket、RTC 端口可用 |
| 4 | LiveKit SIP Server | SIP signaling 端口和 RTP 范围监听成功 |
| 5 | `LingChenAiCallBase` 后端 | 健康检查通过，能连接 DB / Redis / LiveKit |
| 6 | Agent Worker | 能被 explicit dispatch 到指定 Room |
| 7 | Egress / 录音服务 | 完整验收需要；只做最小媒体拨测可暂缓 |
| 8 | OSS / `sys_oss` | 完整验收需要；只做最小媒体拨测可暂缓 |
| 9 | 抓包和日志工具 | `tcpdump`、`sngrep`、`tshark` 至少准备好 |

启动后检查：

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
ss -lntup | grep -E '(:5080|:7880|:7881)'
```

如果是完整 Phase 01 验收，还需要确认：

1. Egress 或等价录音服务可用。
2. OSS 活跃配置可用。
3. 录音文件后缀、MIME、上传大小限制满足 `.wav` 或 `.mp3`。
4. 通话后语义分析任务可运行。
5. Web 页面或查询工具能按 `call_id` 复盘。

## 8. 抓包工具使用

### 8.1 tcpdump

真实拨测建议保存 pcap：

```bash
mkdir -p /opt/livekit-stage1/state
tcpdump -i any -nn -s0 \
  -w /opt/livekit-stage1/state/real-call-$(date +%Y%m%d-%H%M%S).pcap \
  'host 47.94.86.132 or port 5080 or portrange 16384-16484'
```

说明：

1. `host 47.94.86.132` 用于抓服务商 SIP/RTP。
2. `port 5080` 用于抓本机 SIP signaling。
3. `portrange 16384-16484` 用于抓 LiveKit SIP RTP。
4. 如果 RTP 范围调整，抓包范围必须同步调整。

### 8.2 sngrep

查看 SIP 信令：

```bash
sngrep -d any port 5080 or host 47.94.86.132
```

重点看：

1. INVITE。
2. 100 Trying。
3. 180 Ringing 或 183 Session Progress。
4. 200 OK。
5. ACK。
6. BYE。
7. 4xx / 5xx 失败码。

### 8.3 tshark

从 pcap 中提取 SIP：

```bash
tshark -r /opt/livekit-stage1/state/{pcap文件名}.pcap -Y sip
```

查看 RTP 流：

```bash
tshark -r /opt/livekit-stage1/state/{pcap文件名}.pcap -q -z rtp,streams
```

### 8.4 jq

格式化 participant 或 API 返回：

```bash
jq . /opt/livekit-stage1/state/{participant文件名}.json
```

## 9. 拨测执行

最小拨测链路：

```text
创建 Room
  -> explicit dispatch Agent
  -> 创建 SIP Participant
  -> 手机震铃
  -> 手机接听
  -> Agent 播放固定话术
  -> 电话侧说话
  -> Agent 记录音频 / VAD / ASR
  -> 用户挂机
  -> 保存证据
```

Agent 固定话术：

```text
您好，这是一通 LiveKit 智能外呼测试。听到后请说“我听到了”。
```

测试人回复：

```text
我听到了。
```

## 10. 必须保存的证据

每次真实拨测至少保存：

```text
测试时间
测试人
call_id
room_name
outbound_trunk_id
livekit_sip_call_id
sip_call_id
sip_participant_id
agent_participant_id
协商 codec
接通耗时
结束原因
LiveKit SIP 日志路径
Agent 日志路径
pcap 路径
```

## 11. 测试结束后恢复

真实拨测结束后，必须停止 LiveKit 测试容器并恢复 FreeSWITCH。

检查并停止 LiveKit 测试容器：

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' | grep livekit
```

按实际容器名停止测试容器：

```bash
docker stop {livekit_sip_container} {livekit_server_container}
docker rm {livekit_sip_container} {livekit_server_container}
```

恢复 FreeSWITCH：

```bash
docker start sip_realtime_freeswitch
```

恢复后检查：

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' | grep freeswitch
ss -lntup | grep ':5080'
systemctl status recov-ten-gateway.service --no-pager
```

要求：

1. `sip_realtime_freeswitch` running / healthy。
2. `5080` 已由 FreeSWITCH 占用。
3. `recov-ten-gateway.service` active。
4. LiveKit 测试容器不再占用 `5080`。

## 12. 失败处理

如果手机不震铃：

1. 查 SIP INVITE 是否发出。
2. 查服务商是否返回 4xx / 5xx。
3. 查源 IP / 端口是否符合白名单。
4. 查主叫号和被叫号码格式。

如果手机接通但无声音：

1. 查 SDP answer 中 codec。
2. 查 RTP 是否双向。
3. 查 NAT 地址是否为公网 IP。
4. 查 RTP 端口是否被安全组放行。

如果电话侧听不到 Agent：

1. 查 Agent 是否加入 Room。
2. 查 Agent 是否发布 audio track。
3. 查 SIP Participant 是否订阅到音频。
4. 查 TTS 是否真的产生音频。

如果 Agent 听不到电话侧：

1. 查服务商 RTP 是否到达本机。
2. 查 LiveKit SIP 是否把 track 发布到 Room。
3. 查 Agent 是否订阅用户 track。
4. 查 VAD / ASR 输入是否有音频帧。

## 13. 注意事项

1. 不要在无维护窗口时停止承载线上业务的 FreeSWITCH。
2. 不要把完整被叫手机号写入仓库文档。
3. 不要把 Redis 密码、LiveKit secret、模型 API key 写入仓库文档。
4. 每次拨测都必须保存可复盘证据。
5. 如果服务恢复失败，先恢复 FreeSWITCH 和网关，再继续排查 LiveKit。
