# 2026-10-09 后端发布：客户发言审核可选

北京时间 17:14 左右，仅在 `110.42.223.109` 切换 REACH Python API。对应 Git 提交 `ca7c4724f855e8fd8b94037b52b30474df95283a`，镜像 `ai-call-transfer/api:reach-review-off-20261009T090456Z`（`sha256:6621e6fb95cb34cd773d6caeefab8e50e111c2e8c241b48f136a6b667e4765ef`）。前端、邮件 worker、知识解析服务和数据库结构未更新。

本次将 `AI_CALL_CUSTOMER_SPEECH_REVIEW_ENABLED` 默认设为 `False`；生产配置未覆盖该值，运行容器内核对为 `False`。因此逐轮模型发言审核及依赖它的背景声分句、连续离题判定暂不执行；空转写计时由无审核路径处理。

发布前 502 项相关回归通过，Ruff 和 `git diff --check` 通过；镜像内 3 个运行源码文件的 SHA-256 与 Git 提交一致。切换前活跃通话、待发起任务、SIP 预留、活跃转接和 LiveKit 房间均为 0；另有 1 条 `ending` 状态记录，没有对应活动房间，未做数据清理。

切换后 `ai-call-118-api-1` 使用上述镜像，`healthy`、重启数 0；容器内 `GET /ai-call/health` 返回 HTTP 200 和 `{"status":"ok"}`，Nacos 日志显示 `lingchen-reach-python` 新实例注册。主 Compose 与发布后的候选文件一致，且候选相对切换前仅修改 API 镜像。邮件 worker 和知识解析服务沿用原容器并保持 `healthy`。公网未登录的 `/dev-api` 健康探测返回 401，因此此次未完成带登录态的公网业务接口或真实 SIP 通话验收。

发布目录：`/opt/lingchen/deployments/reach-review-off-20261009T090456Z`。其中 `reach-compose.before.json` 是切换前 Compose，旧镜像 `sha256:bbd6b60a10a61c8d1e290c5a17250e23599f0f4038c6dd12ac3279624b408d18` 仍在；`ai-call-before.dump` 为切换前数据库备份，已通过 `pg_restore -l` 校验。两个文件权限均为 `0600`。本次无迁移，正常回退只需在再次确认没有活动通话后恢复 API 镜像和主 Compose：

```bash
release=/opt/lingchen/deployments/reach-review-off-20261009T090456Z
docker exec -i ai-call-118-api-1 python - < "$release/idle-check.py"
docker compose -p reach-migrated -f "$release/reach-compose.before.json" up -d --no-deps ai-call-118-api-1
# 确认旧 API healthy 后，再同步主 Compose。
install -m 600 "$release/reach-compose.before.json" /opt/lingchen-migration-20261002/reach-compose.json.next
mv -f /opt/lingchen-migration-20261002/reach-compose.json.next /opt/lingchen-migration-20261002/reach-compose.json
```
