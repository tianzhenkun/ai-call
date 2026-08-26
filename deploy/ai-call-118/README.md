# 118 AI Call 受控联调部署包

此目录只部署独立 AI Call 栈，不复用 UAT 的容器、网络、卷、PostgreSQL 或 Redis。

## 前置条件

1. WireGuard 已启动：81 为 `10.77.0.1`，118 为 `10.77.0.2`，Mac Linphone 为 `10.77.0.3`。
2. 118 已 `docker load` 媒体镜像包和 `ai-call-app-amd64-20260807.tar`。
3. 将此目录上传至 `/opt/ai-call/runtime`，复制 `.env.example` 为 `.env` 并填写全部 `REPLACE_WITH_...` 值。
4. `SECRET_KEY` 必须与 81 当前主系统的 JWT 签名密钥相同；这是复用登录态，不是复用主数据库。
5. OSS 必须是 S3 兼容对象存储，并允许 API 与 Egress 写入同一 bucket。

## 启动

```bash
cd /opt/ai-call/runtime
chmod 700 scripts/*.sh
./scripts/render-configs.sh
docker compose --env-file .env -f compose.yml config -q
docker compose --env-file .env -f compose.yml up -d
docker compose --env-file .env -f compose.yml ps
```

`init-db` 只会在独立数据库中创建当前代码的表，并写入一个名为 `ai-call-oss` 的 OSS 配置；它不会连接或修改任何 UAT 数据库。

升级已有数据库时，必须在更新 API 前备份数据库并执行提示词当前版本迁移：

```bash
docker compose -f compose.yml exec -T postgres sh -lc \
  'pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB"' > pre-prompt-current-version.sql
docker compose -f compose.yml exec -T postgres sh -lc \
  'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < phase-b4-prompt-current-version-postgres.sql
```

迁移文件来自 `docs/livekit-ai-outbound/sql/phase-b4-prompt-current-version-postgres.sql`，需随发布包上传到运行目录；迁移可重复执行。

## 网络与安全组

- 118：仅向受控 Mac 的公网 IP 开放 `51820/udp`、`7881/tcp`、`50000-50100/udp`；不要开放 `19011`、`7880`、`5432`、`6379`、`5089`、`8021`。
- 第三方 SIP 仅按其确认的源 IP 白名单开放 `5060/udp` 和 `10000-20000/udp`，公网地址为 `118.25.125.221`；不要向全网开放。
- LiveKit SIP 使用主机网络直接监听 `172.17.16.12`，Redis 仅映射到本机 `127.0.0.1:16379`，避免为 RTP 端口范围创建大量 Docker 代理进程。
- FreeSWITCH 的 `5060/udp` 和 `16384-16484/udp` 仍只绑定 WireGuard 地址，Linphone 通过 `10.77.0.2` 注册，账号为 `1000`。
- 81：在独立 `reach.lingchen-ai.com` 的 443 server 内代理 LiveKit WSS `/livekit/`，不开放 `7880/tcp`。
- 81 的 `/ai-call-agent-api/` 仅经 WireGuard 代理到 `10.77.0.2:19011`，SSE 已关闭缓冲并延长读取超时。

## 验收顺序

1. `docker compose ps` 中 API、LiveKit、Egress、SIP、FreeSWITCH 全部运行，`init-db` 成功退出。
2. Mac WireGuard 连通后，Linphone 使用 `sip:1000@10.77.0.2:5060` 注册成功。
3. 从 HTTPS 页面获取 Token，确认返回 `livekitUrl` 为 `wss://reach.lingchen-ai.com/livekit`。
4. 仅拨打白名单 `19900001001`，确认 Linphone 响铃、双向音频及主/分轨录音均写入独立 OSS bucket。

正式线路由 LiveKit SIP 直接连接第三方 SIP Provider；FreeSWITCH 只保留给本地 Linphone 联调。真实客户号码必须另行授权后才能拨打。
