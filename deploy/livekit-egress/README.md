# LiveKit / Egress / SIP 本地自托管模板

当前录音实现依赖 LiveKit Egress；Phase E SIP 真实线路最小接入会额外依赖 LiveKit SIP service。Egress 和 SIP service 都不是 LiveKit Server 本体的一部分，必须和 LiveKit Server 使用同一个 Redis 才能收到对应任务。

## 使用方式

1. 停掉当前占用 `7880/7881/50000-50100` 的旧 LiveKit 容器。
2. 复制配置模板：

```bash
cp deploy/livekit-egress/livekit.yaml.example deploy/livekit-egress/livekit.yaml
cp deploy/livekit-egress/egress.yaml.example deploy/livekit-egress/egress.yaml
cp deploy/livekit-egress/sip.yaml.example deploy/livekit-egress/sip.yaml
```

3. 把 YAML 里的 `CHANGE_ME_*` 或示例 API Key / Secret 改成和 `env/.env.dev` 一致的 LiveKit API Key / Secret。
4. 如果不是本机浏览器访问，把 `docker-compose.yml` 里的 `NODE_IP=127.0.0.1` 改成浏览器/SIP 网关能访问到的 LiveKit 地址。
5. 如果要启用真实 SIP 线路，确认 `sip.yaml` 里的 `sip_port`、`rtp_port`、`use_external_ip` 和服务器公网 IP / 防火墙 / 供应商白名单一致。
6. 启动组件：

```bash
docker compose -f deploy/livekit-egress/docker-compose.yml up -d
```

7. 检查状态：

```bash
docker ps --filter name=ai-call-livekit
docker logs ai-call-livekit-egress --tail 80
docker logs ai-call-livekit-sip --tail 80
```

## 注意事项

1. `livekit.yaml`、`egress.yaml` 和 `sip.yaml` 可能包含密钥，已加入 `.gitignore`，不要提交。
2. 当前业务服务会在 `StartRoomCompositeEgress` 请求中传对象存储 S3 参数，所以 `egress.yaml` 不需要固定写存储密钥。
3. 本地 RoomComposite Egress 需要较高资源，官方建议每个 Egress 实例至少 4 CPU / 4 GB 内存。
4. 如果出现 `no response from servers`，优先确认 LiveKit Server 和 Egress 是否连接同一个 Redis。
5. Redis 只在 compose 内部网络暴露，不映射宿主机 `6379`，避免和本机已有 Redis 冲突。
6. SIP signaling 端口和 RTP 端口范围必须能被 SIP trunk provider 从公网访问；只接通但单向无声时优先检查 SDP 公网地址、RTP 端口、安全组和供应商白名单。
7. 当前模板服务镜像使用 `latest` 方便本地验证；生产环境应固定 LiveKit Server、Egress 和 SIP 镜像版本。
