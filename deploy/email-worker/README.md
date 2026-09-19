# 邮件进程托管与监测

## 生产发布入口

REACH 当前生产采用 Docker Compose 托管独立 worker。下次上线按[前后端上线操作手册](../../docs/email-module/production-runbook.md)执行；当次结果见[2026-09-18 发布记录](../../docs/email-module/production-deployed-20260918.md)。

`compose.production.yml` 与 `compose.credit.yml` 按顺序叠加在现有主 Compose 和平台覆盖文件之后。不要在同一生产数据库上再同时启动下面的 systemd worker；systemd 是另一种部署方式。

## Linux / systemd

提供三个 unit：`reach-email-worker.service`、`reach-email-health.service`、`reach-email-health.timer`。
上线时按实际部署目录、虚拟环境和非特权运行用户调整 unit；示例使用 `/opt/ai-call` 和 `reach` 用户。
`/etc/reach/email-worker.env` 必须与 API 使用同一数据库、凭据加密密钥、存储和模型配置，限制文件权限，不提交密钥。
先执行项目邮件数据库迁移，再将 unit 安装到 `/etc/systemd/system/`，执行：

```sh
systemctl daemon-reload
systemctl enable --now reach-email-worker.service reach-email-health.timer
systemctl status reach-email-worker.service
journalctl -u reach-email-health.service
```

进程退出后 15 秒重试。旧租约有效时新进程不会抢占，继续重试直到旧租约自然过期（最长约 120 秒，再加重启间隔及初始化时间）。
不清除租约、不把发送结果不明的邮件重新排队。发送中崩溃的邮件在恢复时进入待核对，避免盲目重复发送。

## 告警边界

- 健康检查每分钟执行，输出不含账号、邮件正文的 JSON；正常退出码 0，心跳过期或队列积压退出码 1，数据库错误非零退出。
- 无有效租约判定 `offline`。到期超过 5 分钟仍为 queued/retryable 的邮件判定 `backlog`，未来定时邮件不计入。
- Linux 检查异常会形成 failed unit 和 journal 记录；需由部署方将此服务失败接入现有值班告警渠道，未配置短信、邮件或即时通讯外发。
- 登录后的邮件页面每 15 秒检查，显示服务不可用、当前用户积压数或检查失败；恢复后自动消除提示。用户接口仅统计当前租户及当前用户的数据。
- 租约有 120 秒有效期，进程退出后不保证立即判离线；本指标证明租约有效，不代表每封邮件已送达。

## 本地联调

本机通过 `~/Library/LaunchAgents/com.lingchen.reach.email-worker.plist` 托管，KeepAlive 自动恢复，重试间隔 15 秒。
健康检查由 `com.lingchen.reach.email-health.plist` 每分钟执行。
入口为受限本地文件 `storage/email-verification/managed_worker.py`，沿用既有联调配置，plist 不包含密钥。
本地告警和进程日志位于 `storage/email-verification/managed-health.log` 和 `managed-worker.log`。
LaunchAgent 在用户登录后运行；本机休眠期间不能持续发送。线上应使用系统级服务。

暂停本地发送需 `launchctl bootout gui/$(id -u)/com.lingchen.reach.email-worker`，仅终止子进程会被自动拉起。
Linux unit 为交付配置，本次未登录线上服务器安装，不能视为线上验收完成。

## 2026-09-17 本地验证

- 本地空队列下暂停消费者超过租约有效期，健康检查输出 offline，非零退出；定时健康日志记录 offline。
- 真实登录页面显示发送服务暂不可用提示。终止暂停进程后 launchd 自动拉起新进程，健康检查恢复 ok，页面轮询自动消除告警。
- 演练前后的已接受人工回复 attempt_count 均为 1，sent_at 未变化；没有生成新测试邮件。
- 后台健康检查、权限隔离及 worker 回归共 34 项通过。发送中崩溃恢复为 unknown 的测试使用隔离测试数据库和模拟传输，不代表真实 SMTP 崩溃过程已完整复现。
- 前端类型检查、Biome、Ant Design 检查以及 ContentModal 9 项回归通过。CodeGraph 同步未执行成功：当前前端目录没有初始化索引。
