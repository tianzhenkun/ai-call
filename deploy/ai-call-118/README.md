# 118 AI Call 受控联调部署包

此目录只部署独立 AI Call 栈，不复用 UAT 的容器、网络、卷、PostgreSQL 或 Redis。

## 前置条件

1. WireGuard 已启动：81 为 `10.77.0.1`，118 为 `10.77.0.2`，Mac Linphone 为 `10.77.0.3`。
2. 118 已 `docker load` 媒体镜像包和 `ai-call-app-amd64-20260807.tar`。
3. 将此目录上传至 `/opt/ai-call/runtime`，复制 `.env.example` 为 `.env` 并填写全部 `REPLACE_WITH_...` 值。
4. `SECRET_KEY` 必须与 81 当前主系统的 JWT 签名密钥相同；这是复用登录态，不是复用主数据库。
5. OSS 必须是 S3 兼容对象存储，并允许 API 与 Egress 写入同一 bucket。
6. 按下文配置 TURN 域名和受信任证书；`render-configs.sh` 会在写配置前校验证书，缺失时不会继续。

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

- 118：`51820/udp` 仅向受控 Mac 的公网 IP 开放；不要向公网开放 `19011`、`7880`、`5432`、`6379`、`5089`、`8021`。
- 坐席媒体使用 `7881/tcp`、`50000-50100/udp` 直连和 `443/tcp` TURN/TLS。动态公网坐席需在仅关联 118 的独立安全组内，经授权允许相应公网来源；不能沿用单台测试 Mac 的旧 IP 作为全部坐席的直连白名单，也不能修改多台主机共用的安全组来放宽范围。不额外开放 TURN/UDP 或 TURN relay 端口范围。
- 第三方 SIP 仅按其确认的源 IP 白名单开放 `5060/udp` 和 `10000-20000/udp`，公网地址为 `118.25.125.221`；不要向全网开放。
- LiveKit SIP 使用主机网络直接监听 `172.17.16.12`，Redis 仅映射到本机 `127.0.0.1:16379`，避免为 RTP 端口范围创建大量 Docker 代理进程。
- FreeSWITCH 的 `5060/udp` 和 `16384-16484/udp` 仍只绑定 WireGuard 地址，Linphone 通过 `10.77.0.2` 注册，账号为 `1000`。
- 81：在独立 `reach.lingchen-ai.com` 的 443 server 内代理 LiveKit WSS `/livekit/`，不开放 `7880/tcp`。
- 81 的 `/ai-call-agent-api/` 仅经 WireGuard 代理到 `10.77.0.2:19011`，SSE 已关闭缓冲并延长读取超时。

## TURN/TLS 配置与发布校验

复用 LiveKit v1.13.1 内置 TURN，保留 UDP 和 TCP 7881 直连。TLS 在 118 的 LiveKit 进程终止，不经过 81 的网站 HTTP `location`，也不启用 `external_tls`。浏览器使用 LiveKit 下发的短期中继凭据，不另设固定 TURN 密码，不在业务前端强制中继。

1. 为 TURN 准备独立域名，将 A 记录直接指向 118 的实际公网 IP，并填写 `.env` 的 `LIVEKIT_TURN_DOMAIN`。只填域名，不填协议或路径。不使用仅支持 HTTP 的 CDN 代理；未配置可用 IPv6 时不要发布 AAAA 记录。
2. 通过已有证书管理流程签发覆盖该域名的公信 CA 证书，包含完整中间证书链。放置 `runtime/turn/fullchain.pem` 和 `runtime/turn/privkey.pem`；私钥权限设为 `600`，不得提交 Git。现有网站证书只有覆盖 TURN 域名时才能复用。
3. 在部署主机检查 `443/tcp` 无其他进程或容器占用，确认云安全组与主机防火墙允许批准的坐席来源。81 的 HTTPS 443 与 118 的 TURN 443 是不同主机，不应改动 81 的网站监听。
4. 用下面的只读检查核验证书和部署模型。证书链不受信任、域名不匹配、私钥不匹配、有效期不足 7 天都会失败；失败前不会覆盖原运行配置。`--check` 不检查 DNS、端口占用或公网连通性，不能视为媒体验收通过。

```bash
./scripts/render-configs.sh --check
docker compose --env-file .env -f compose.yml config -q
```

取得发布授权后，在没有活动通话的维护窗口备份旧部署文件与 `runtime/livekit.yaml`，渲染配置，再仅重建 LiveKit：

```bash
./scripts/render-configs.sh
docker compose --env-file .env -f compose.yml up -d --no-deps --force-recreate livekit
docker compose --env-file .env -f compose.yml logs --tail 100 livekit
```

证书按目录只读挂载，续期时替换目录中的文件，不要替换成挂载范围外的软链接。LiveKit 在启动时加载证书，续期后先执行 `--check`，再在维护窗口重建 LiveKit；仅更新 PEM 文件不会自动热加载。回滚应恢复备份的 Compose、模板和运行配置，再只重建 LiveKit，不操作数据库或其他媒体服务。

上线后的验收必须包含：

- 从坐席所在网络验证 TURN 域名解析、TCP 443 和 TLS 证书链/主机名，不能只检查网页 HTTPS。
- 在正常网络验证实际选中直连候选、双向音频和录音；在隔离测试客户端强制 `relay` 或阻断直连，验证实际选中 `relay` 且通过 TURN/TLS 传输双向音频。强制中继仅用于验收，不作为生产客户端默认设置。
- 使用单通与三通排队场景验证临近期限认领、接入失败后下一通恢复。真实号码另行授权；出现异常先保留 ICE 候选、连接阶段、服务端时间线和媒体证据。

本地检查命令如下，需要 Docker Compose、OpenSSL 和已加载的部署版本 LiveKit 镜像。测试使用临时 CA 与随机本机端口，不接 SIP、不拨号、不复用运行中的 LiveKit/Redis；TLS/STUN 通过不等于公网中继或真实电话验收通过。

```bash
.venv/bin/python -m pytest -q --show-capture=no --tb=short tests/test_ai_call_turn_deployment.py
```

配置依据：[LiveKit 部署说明](https://docs.livekit.io/transport/self-hosting/deployment/)、[v1.13.1 配置字段](https://github.com/livekit/livekit/blob/v1.13.1/config-sample.yaml)。

## 验收顺序

1. `docker compose ps` 中 API、LiveKit、Egress、SIP、FreeSWITCH 全部运行，`init-db` 成功退出。
2. Mac WireGuard 连通后，Linphone 使用 `sip:1000@10.77.0.2:5060` 注册成功。
3. 从 HTTPS 页面获取 Token，确认返回 `livekitUrl` 为 `wss://reach.lingchen-ai.com/livekit`。
4. 仅拨打白名单 `19900001001`，确认 Linphone 响铃、双向音频及主/分轨录音均写入独立 OSS bucket。

正式线路由 LiveKit SIP 直接连接第三方 SIP Provider；FreeSWITCH 只保留给本地 Linphone 联调。真实客户号码必须另行授权后才能拨打。
