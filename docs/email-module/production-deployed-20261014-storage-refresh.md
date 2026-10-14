# 2026-10-14 邮件附件存储配置刷新发布

## 范围与来源

- 源码提交：`8ac67c4c65e45435c0e3d527d53f81303f3365ff`，分支 `codex/email-storage-refresh`，已推送。
- 发布目录：`110.42.223.109:/opt/lingchen/deployments/reach-email-storage-20261014-8ac67c4`。
- 基于 API、邮件 worker 各自正在运行的镜像增量构建，仅覆盖 `app/services/reach_email/worker.py`、`app/utils/minio_util.py`。
- 附件读取、收信附件保存前查询同库的当前启用 OSS 配置；缺少配置时明确失败。发送异常记录阶段、耗时、异常类型、存储 HTTP 状态及函数位置，不记录原始异常文本和敏感异常链。
- 没有 schema 变更；知识解析镜像、前端发布目标保持原值。原 Qwen 实验工作区未纳入发布。

## 事故证据与判断边界

人工回复 `c5637297541f454c899b70ce1986294f` 在 SMTP 提交前失败，状态为 `failed / PREPARE_SEND_FAILED`，发送次数为 0。

旧 worker 自 10 月 2 日启动后只初始化一次 OSS 配置；10 月 8 日配置备份中仍为旧存储地址。对同一附件进行只读对照：当前地址约 0.04 秒读取成功，旧地址约 30.047 秒触发 `httpx.ConnectTimeout`。这支持常驻进程保留旧地址的根因判断。原事故时间段没有保留下来的具体存储请求日志，因此历史异常类型仍不能直接证实。

## 发布与回滚资料

- 数据库备份 `ai-call-before.dump` 为 2,145,517 字节，通过 `pg_restore -l` 校验。
- 发布目录保存 `reach-compose.before.json`、`reach-compose.after.json`、`old-images.json`、`manifest.json`、`deployed.json` 和相关检查脚本、日志。
- 切换前活动通话、待发起命令、SIP 预留、活动转接、LiveKit 房间均为 0；1 条历史 `ending` 记录未作修改。
- 切换前发送中及到期待发邮件均为 0，运行中的邮件 AI 作业为 0。
- 旧 worker 停止后等待数据库租约自然到期；2026-10-14 00:30:12 UTC 检查剩余有效时间为 0，随后切换。未删除租约。
- 主 Compose 在服务健康、镜像和源码校验、Nacos 注册确认后原子更新。需要回滚时使用本目录的旧 Compose；邮件 worker 同样须遵守停止、自然到期、接管顺序。

| 服务 | 新镜像 ID |
| --- | --- |
| API | `sha256:6bdb49488cb904b05cde2fda087c7bf1c8847964837b2634f0d07fc304c57e9b` |
| 邮件 worker | `sha256:84922fae1fdac4f644e34144c819d36adc6b9ab9091b3885b16135fe56d4420e` |

## 验证结果

- `pytest tests/reach_email tests/test_minio_util.py -q`：168 passed，2 skipped。跳过的两项依赖未提供的本地 PostgreSQL 测试库；网络与 SMTP 故障测试使用隔离模拟。
- 相关四个文件 Ruff 检查与 `git diff --check` 通过。
- 镜像内 Python 语法检查、运行容器内两个源码文件 SHA-256 与提交一致；两个容器均 `healthy`，重启次数 0；API 健康接口 HTTP 200，启动日志确认 Nacos 注册成功。
- 新租约摘要 `d64ecbdf2f12194a` 已取代旧摘要 `7a3120cc6b665d0a`；两次检查有效期由 00:33:24 UTC 延长至 00:34:29 UTC，证明持续续租。
- 在新 worker 容器中，以旧地址构造内存存储对象，再调用真实 `load_attachments`：自动读取数据库当前配置，0.044 秒成功读取原附件 10,373 字节，原方法完成长度与 SHA-256 校验。
- 验证事务只读，SMTP 调用设为禁止；原消息仍为 `failed`、`attempt_count=0`、`sent_at=NULL`。未重发客户邮件，本次不代表客户邮件送达验收。

源码校验值：

```text
544c73c2620d6d563fece84fc39ac5468b8be6625ebbb34849818eb8862d9cbd  app/services/reach_email/worker.py
f1ef2f2f82d02feada134681a4e6b8223b87874fd3ee617296136602bf038405  app/utils/minio_util.py
```
