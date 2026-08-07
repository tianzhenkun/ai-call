# 118 AI Call 受控联调部署包

此目录只部署独立 AI Call 栈，不复用 UAT 的容器、网络、卷、PostgreSQL 或 Redis；不会配置 SIP 中继或真实客户号码。

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

## 网络与安全组

- 118：仅向公网开放 `51820/udp`、`7881/tcp`、`50000-50100/udp`；不要开放 `19011`、`7880`、`5432`、`6379`、`5089`、`8021`。
- 118 的 `5060/udp` 和 `16384-16484/udp` 只绑定 WireGuard 地址，Linphone 通过 `10.77.0.2` 注册，账号为 `1000`。
- 81：在现有 Nginx 加载 `nginx/81-ai-call.conf`，并在安全组开放 `7880/tcp`。该端口只提供已签名 LiveKit Token 的 WSS，不提供 AI Call API。
- 81 的 `/ai-call-agent-api/` 仅经 WireGuard 代理到 `10.77.0.2:19011`，SSE 已关闭缓冲并延长读取超时。

## 验收顺序

1. `docker compose ps` 中 API、LiveKit、Egress、SIP、FreeSWITCH 全部运行，`init-db` 成功退出。
2. Mac WireGuard 连通后，Linphone 使用 `sip:1000@10.77.0.2:5060` 注册成功。
3. 从 HTTPS 页面获取 Token，确认返回 `livekitUrl` 为 `wss://recov.lingchen-ai.com:7880`。
4. 仅拨打白名单 `19900001001`，确认 Linphone 响铃、双向音频及主/分轨录音均写入独立 OSS bucket。

真实 SIP 中继、真实客户号码和 `/voice-api/` 不属于本部署包。
