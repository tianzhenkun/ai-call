# 转人工误挂断与接通状态修复发布记录

2026-09-21 北京时间 16:49 完成 API 切换，版本 `reach-call-fix-20260921T084006Z`。

## 源码与产物

- 已推送提交 `a5cb4c33ba9dc4676257d00688c84c894aaa1a2b`，分支 `codex/feat/reach-platform-integration`。
- 四个文件：agent_runner.py、attempt_reconciler.py 及两个对应测试。转人工停止或替换模型后，旧 provider 的迟到响应不再导致客户通话结束；当前模型真实错误仍保留终止逻辑。完整开始事实、接通时间与媒体证据成立后，外呼 attempt/target 同步 IN_CALL，避免退回 DIALING。
- API 镜像 `ai-call-transfer/api:reach-call-fix-20260921T084006Z`，基于 `reach-seat-20260921T051325Z` 叠加两处实现，无依赖或数据库迁移。
- 本地镜像索引摘要 `sha256:4de4dc12684bd38c603282815ac72930f55d806d458393bafe759a0f3a20658b`；导出包配置摘要与生产镜像 ID 为 `sha256:16a25dda58e07d15f7af60d6c399fcdf923f52deceb50565f6102f033acfe738`。二者为不同层级摘要，已核对包内配置。
- 发布目录 `/opt/lingchen/deployments/reach-call-fix-20260921T084006Z`，保留镜像包、release-manifest.json、changes.patch、SHA256SUMS、api.override.yml、idle-check.py 和 rollback.sh。

## 验证结果

- 交接四文件和补丁散列全部一致；重新执行相关回归 390 项通过、12 个警告，Ruff 与 diff 检查通过；提交后逐文件内容匹配候选清单。
- 切换前活动通话、待执行 START_CALL、活动 SIP 预留、活动转人工和 LiveKit 房间均为 0；保留一条历史 ending 记录，未改写数据。
- API healthy、重启 0，容器内健康接口返回 status=ok。生产端口绑定 10.77.0.2，主机 127.0.0.1 不可用；已改为容器内验证。
- Nacos 注册成功，线上坐席 bootstrap、pending 接口观察到 200。
- 容器内 agent_runner.py SHA256 `afaa4517e76ea1b0a185a6e2e81d202f3452657920c60618c8e858eb6a7ac9a0`；attempt_reconciler.py SHA256 `c1dfede4a503a35c86594fe90a25c6271a6ae99034e3e1deb6e00aa60bf32692`，均匹配提交和候选快照。
- 邮件 worker 沿用 seat 镜像，healthy、重启 0；其他容器 ID 均未变。生产 .env 与主 Compose 散列未变，前端未部署。
- 测试为隔离回归；本次未自动拨号，尚不能宣称真实转人工验收通过。158 历史挂机异常尚未定根因，本修复没有将其认定为已解决。

## 配置与回滚

沿用上一版四份 Compose 顺序，追加本发布目录 api.override.yml，仅覆盖 API 镜像。REACH_EMAIL_IMAGE 保持 `ai-call-transfer/api:reach-seat-20260921T051325Z`，防止邮件 worker 被隐式更新。基础设施、证书与密钥保持原状。

备份 `/opt/lingchen/backups/reach-call-fix-20260921T084006Z`，含原四份 Compose、运行环境、原 API 镜像 ID、配置散列和其他容器清单。无数据库结构或数据写入，不需要整库回滚。

回滚在确认无活动通话后运行本发布目录 rollback.sh，去掉本次第五份 API 覆盖，使用原四份 Compose 执行 `up -d --no-deps api`。原镜像配置摘要 `sha256:0633cbfb3019161a2fb2d2fec3fa005d0f31f52920ef7bbba6a59e8a40ecdfb7`。切回后再次检查 API 健康及 Nacos 注册。

## 授权 SIP 抓包

北京时间 2026-09-21 16:50:17 启动，预计 17:10:18 自动停止。服务器 `118.25.125.221`，目录 `/opt/lingchen/diagnostics/sip-hangup-20260921T085017Z`，权限 700，timeout PID 2613990 已验证存活，tcpdump 已监听。

覆盖所有来源的双向 UDP/TCP 5060、5089 信令；20 分钟限时，4 个 10 MB 轮转文件。不自动发起电话，原始包留在受限服务器目录，不提交 Git。后续以同一通话的信令与事件时间线分析 158 异常。
