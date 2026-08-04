# P1 本地联调基线

本文只记录当前 `19011` 上用于验证 SIP 外呼打断 P1 的本地链路。P2/P3、真实电话线路、供应商 AEC 暂不纳入这里。

## 目标

先固定测试环境，再评估 P1 打断效果。否则 `Linphone 不响铃`、`LiveKit SIP Participant 创建失败`、`AI 听不到人声`、`IP 变更` 这些环境问题会和打断策略混在一起，导致每次通话都像在排查新问题。

## 当前链路

```text
customer.html
-> 19011 AI Call API
-> 19011 专用 LiveKit Server
-> 19011 专用 LiveKit SIP service
-> freeswitch-local:5089
-> FreeSWITCH dialplan
-> Linphone 注册分机
```

Linphone 不是直接接 LiveKit。当前 LiveKit SIP 发起 outbound call 后，会把 SIP 呼叫打到 `freeswitch-local:5089`，再由 FreeSWITCH 呼到本机 Linphone。

## Linphone 对接方式

当前前端 `电话外呼` 的默认目标是 `本机软电话 Linphone`，默认被叫号码来自 `static/ai-call/customer.js`：

```text
19900001001
```

当前 FreeSWITCH 里对应的路由是：

```xml
<condition field="destination_number" expression="^19900001001$">
  <action application="ring_ready"/>
  <action application="bridge" data="${sofia_contact(*/1000)}"/>
</condition>
```

也就是说 Linphone 需要作为 SIP 用户 `1000` 注册到 FreeSWITCH。FreeSWITCH 拨号时按当前注册记录解析 Contact，不再把 dialplan 绑定到某个固定本机 IP。注册域仍通常是当前本机局域网 IP：

```text
1000@<当前本机 IP>
```

当前注册状态可用下面命令确认：

```bash
docker exec sip_realtime_freeswitch fs_cli -x 'sofia status profile internal reg'
```

期望看到：

```text
User:        1000@<当前本机 IP>
Agent:       Linphone-Desktop/...
Status:      Registered(UDP)
Ping-Status: Reachable
```

如果本机 IP 变化，比如从 `192.168.0.103` 变成其他地址，需要同步检查：

- Linphone 账号注册域
- FreeSWITCH 当前解析出来的 `${sofia_contact(*/1000)}`
- `env/.env.dev` 里的 `SIP_PUBLIC_IP` / `SIP_EXTERNAL_RTP_IP`
- 浏览器访问的 19011 地址

## 19011 隔离栈

19011 用专门的 LiveKit / Redis / Egress / SIP service，不和 19012 共用 LiveKit。19012 用于语义分析测试，不要因为 P1 联调随手停掉。

当前 19011 容器：

```text
ai-call-19011-livekit-server   7890 -> 7880, 7891 -> 7881, UDP 51000-51100
ai-call-19011-livekit-sip      UDP 15180, UDP 18384-18484
ai-call-19011-livekit-egress
ai-call-19011-livekit-redis
```

当前 FreeSWITCH 容器：

```text
sip_realtime_freeswitch
```

FreeSWITCH 同时接在 `livekit-egress_default` 网络上，并提供别名 `freeswitch-local`，所以 19011 的 LiveKit SIP 能访问：

```text
freeswitch-local:5089
```

## 本地文件配置

仓库中有这些本地配置文件。它们可能包含密钥，不提交真实文件，只提交 example：

```text
env/.env.dev
deploy/livekit-egress/livekit.19011.local.yaml
deploy/livekit-egress/egress.19011.local.yaml
deploy/livekit-egress/sip.19011.local.yaml
```

当前 `env/.env.dev` 中和 19011 相关的关键配置应该保持一致：

```text
LIVEKIT_URL=http://127.0.0.1:7890
SIP_PROXY=freeswitch-local:5089
SIP_SIGNALING_PORT=15180
SIP_RTP_RANGE=18384-18484
SIP_PUBLIC_IP=192.168.0.111
SIP_EXTERNAL_RTP_IP=192.168.0.111
LIVEKIT_RTC_TCP_PORT=7891
LIVEKIT_ICE_UDP_RANGE=51000-51100
```

`deploy/livekit-egress/*.19011.local.yaml` 里的 LiveKit API key / secret 必须和 `env/.env.dev` 一致。不要把真实 key / secret 写进文档或提交。

## 启动时环境变量

当前 19011 服务进程不是只靠 `.env.dev` 决定数据库和端口，启动时还传了本地覆盖：

```text
ENVIRONMENT=dev
SERVER_PORT=19011
DATABASE_TYPE=postgres
DATABASE_HOST=127.0.0.1
DATABASE_PORT=<ai-call-ed81-owner-19011-postgres 当前映射端口>
DATABASE_NAME=ai_call_owner_19011
DATABASE_USER=ai_call_owner_19011
DATABASE_PASSWORD=<本地安全配置，不提交>
REDIS_HOST=127.0.0.1
REDIS_PORT=<codex-ruoyi-redis-6379 当前映射端口>
REDIS_PASSWORD=<从本地 Redis 容器读取，不提交>
ROOT_PATH=
```

`DATABASE_PORT` 是容器启动时动态映射的，不要把当前端口固化到启动脚本。每次重启前用下面命令读取：

```bash
docker port ai-call-ed81-owner-19011-postgres 5432/tcp
```

当前 19011 使用独立容器 `ai-call-ed81-owner-19011-postgres`，不连接线上业务数据库；登录态和业务接口依赖本机容器 `codex-ruoyi-redis-6379`。数据库和 Redis 密码由启动脚本从对应本地容器读取，不写入本文或提交。

仓库内统一使用下面的受控启动入口。`--check` 只核对环境，不修改或重启服务；直接运行脚本时，如果 19011 已被占用会拒绝重复启动，不会终止现有进程：

```bash
tools/start_ai_call_19011.sh --check
tools/start_ai_call_19011.sh
```

`ENVIRONMENT=dev` 不能省略。配置加载器会根据 `ENVIRONMENT` 选择 `env/.env.dev`；如果重启时漏掉它，进程会使用默认配置，`AI_CALL_SIP_OUTBOUND_ENABLED` 会回落为 `false`，页面会报 `SIP 真实外呼未启用`。

启动入口为 Uvicorn 设置 10 秒 graceful shutdown 上限，避免 SSE 等长连接让重启无限等待；超过上限时 Uvicorn 会取消剩余请求并继续退出。

`ROOT_PATH` 在 19011 本地直连测试时应显式置空。否则 FastAPI 会按默认 `/ai-call-api/v1` root path 提供静态资源，客户页需要通过 `/ai-call-api/v1/static/ai-call/customer.html` 访问；本地拨测统一使用裸路径：

```text
http://127.0.0.1:19011/static/ai-call/customer.html
```

因此只用 `python main.py --env dev` 但没带这些环境变量时，可能连到错误数据库或直接启动失败。

当前服务进程可用下面命令确认：

```bash
lsof -nP -iTCP:19011 -sTCP:LISTEN
ps -p <pid> -o pid,ppid,etime,command
```

## 本地数据库配置

当前 19011 的权威数据库是隔离 PostgreSQL：

```text
container=ai-call-ed81-owner-19011-postgres
database=ai_call_owner_19011
user=ai_call_owner_19011
```

下面两份 SQLite 文件可能仍存在，但只是历史联调遗留，不是当前 19011 的运行数据库；重启时不要再指向它们：

```text
/tmp/ai_call_ed81_local.db
local_db/ai_call_ed81_local.copy.db
```

当前 PostgreSQL 包含 P1 联调需要的核心表：

```text
ai_call_record
ai_call_event
ai_call_recording
ai_call_recording_track
ai_call_prompt_profile
sys_oss
sys_oss_config
```

当前 `sys_oss_config` 有一条 active 配置：

```text
config_key=minio
bucket_name=recov
endpoint=81.68.166.109:9000
domain=https://oss.lingchen-ai.com
status=0
```

这条配置在隔离 PostgreSQL 里，录音上传会使用它。它会连接远端 OSS，但不会写线上业务数据库。注意不要泄露 `access_key` / `secret_key`。

当前外呼统一使用运行时全局打断开关，不再按业务场景单独授权：

```text
AI_CALL_BARGE_IN_ENABLED=true
AI_CALL_SIP_BARGE_IN_ENABLED=true
AI_CALL_SIP_BARGE_IN_FAST_STOP_ENABLED=true
```

新建通话后应确认 `effectiveConfig.bargeInEnabled=true`。如果全局开关关闭后仍发生打断，需要优先查 effective config 和事件日志，而不是继续调 VAD 阈值。

## 启动前检查

每次重新测试 P1 前，先做这些检查：

```bash
docker ps --format '{{.Names}}\t{{.Image}}\t{{.Ports}}' | grep -E '19011|freeswitch'
lsof -nP -iTCP:19011 -sTCP:LISTEN
docker port ai-call-ed81-owner-19011-postgres 5432/tcp
docker exec ai-call-ed81-owner-19011-postgres pg_isready -U ai_call_owner_19011 -d ai_call_owner_19011
docker exec ai-call-ed81-owner-19011-postgres psql -U ai_call_owner_19011 -d ai_call_owner_19011 -Atc "select count(*) from sys_oss_config where status='0';"
docker exec sip_realtime_freeswitch fs_cli -x 'sofia status profile internal reg'
```

期望结果：

- 19011 LiveKit / SIP / Egress / Redis 都在
- `sip_realtime_freeswitch` 在
- 19011 API 在监听
- 隔离 PostgreSQL 可连接且有 active OSS 配置
- 新建通话的 `effectiveConfig.bargeInEnabled=true`
- Linphone 注册 `1000@当前本机 IP` 且 Reachable

只有这条基线通过后，再分析 P1 打断慢、误打断、漏打断。
