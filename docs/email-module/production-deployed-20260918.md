# REACH 邮件生产发布结果（2026-09-18）

状态：用户授权后完成生产部署与受控收发验收。本文更新发布准备文档及候选包中 `candidate_not_deployed` 的历史状态，不改写原候选包及其校验值。

下次上线请使用[可复用上线操作手册](production-runbook.md)；本文中的版本号、测试邮箱及回滚目标仅属于本次记录。

## 实际部署

- 后端服务器：`118.25.125.221`。
- 发布目录：`/opt/lingchen/deployments/reach-email-20260918`。
- API：`ai-call-118-api-1`；worker：`ai-call-118-email-worker-1`；均 healthy。
- 镜像标签：`ai-call-transfer/api:reach-email-20260918-candidate`。
- 本地 OCI index：`sha256:eb45e32d0d9a8ef4303d9620657f83d83b73cfef956b32ad68b7bcfa55c30eeb`。
- 归档 manifest.json 中 config digest 及生产 Docker image ID：`sha256:c8f917060a40fb9e342b44d770691b7fc63ea862ef2e4b474e451d77594abcf2`。两者属于不同镜像元数据层级；上传归档 SHA256 校验通过。
- 前端服务器：`81.68.166.109`，`/home/lingchen/web/ai-reach/current` 已原子切到 `releases/reach-email-20260918`。
- 公网首页与部署 index.html SHA256 一致：`6d7d5afe08edd3e6e0d65b4b96426d292fb8fb8fc2bcaa8387c5a693b2e86296`。
- 新目录额外保留旧版本的 110 个缺失哈希资源，支持已打开旧页面的延迟加载；不覆盖新文件及运行配置。浏览器曾命中旧页面资源，保留资源并刷新后个人中心正常。

当前 Compose 必须使用以下四份配置；不得只使用旧配置重建新 API，否则会缺少邮件和计费配置：

```bash
cd /opt/ai-call/runtime
export REACH_EMAIL_IMAGE=ai-call-transfer/api:reach-email-20260918-candidate
dc=(docker compose
  -f compose.yml
  -f /opt/lingchen/deployments/platform-integration-20260903T172008Z/deploy/reach-compose.override.yml
  -f /opt/lingchen/deployments/reach-email-20260918/deploy/reach-compose.email.yml
  -f /opt/lingchen/deployments/reach-email-20260918/deploy/compose.credit.yml)
"${dc[@]}" ps api email-worker
"${dc[@]}" exec -T email-worker python -m app.services.reach_email.health
```

## 配置、迁移与回退过程

- 切换前核实 LiveKit 房间及参与者均为 0。8 月遗留的 ending 记录保持原样。
- 备份目录：`/opt/lingchen/backups/reach-email-20260918`，包含数据库归档、原 Compose、原镜像 ID、运行环境文件及受限密钥备份。`pg_restore --list` 通过；不是完整恢复演练。
- 数据库备份 SHA256：`c42e19fd830de00b7a51144c6182c41609611073b30efd4f543098caa914c12b`。
- 创建 11 张 `reach_email_*` 表。邮件密钥位于 `/opt/ai-call/runtime/secrets/email`，目录 0700、文件 0400、属主 UID 10001；沿用用户确认的本地邮件 AI 密钥，生产单独生成凭据加密密钥。
- 首次启动新 API 发现缺少 `reach_credit_usage_outbox`，先恢复旧 API 并停 worker，未提前切换前端。检查了 69 个模型的表和字段存在性，只发现这一缺表；此检查不等同于完整约束/索引/数据迁移审计。
- 按仓库已有 `phase-k2-credit-metering-postgres.sql` 创建该表；当前后端正式外呼依赖计费接入，不能只迁移邮件表。
- 沿用生产平台已有 REACH 专用计费密钥，复制到 `/opt/ai-call/runtime/secrets/platform/reach-credit-metering-secret`，权限 0400、属主 UID 10001。新增 `compose.credit.yml` 保留原 JWT/Nacos 启动包装并加载该密钥。
- 计费网关为 `http://geo-uat-ruoyi-gateway:19090`；使用生产已有任务的用户 ID、租户 960001 调用签名资格接口，返回 `eligible=true`、`meteringEnabled=false`。未更改平台计费开关，未提交计费用量或拨打真实电话。
- 补齐后重新部署，API Nacos 注册成功，API 与 worker 均 healthy，worker 检查返回 `workerOnline=true, overdueCount=0, status=ok`。

## 验收结果

- 线上登录账号的邮件任务、发送记录、线索、邮箱授权、worker-health 接口均返回 200；原数据看板正常显示已有统计。
- 新建任务表单正确拒绝缺少名单及公司资料的提交，未保存无效草稿。
- 生产邮件 AI 适配器实际调用 `deepseek-v4-pro` 成功，返回非空主题及正文。使用不含客户信息的验收提示，未发送生成内容。
- 用户授权沿用本地两个邮箱；迁移到相同租户 960001、用户 2096000000000000203，保持邮箱权重、限额、收件同步位置，用生产密钥重新加密；未迁移本地客户名单/历史任务。
- `17866726638@163.com` 与 `18518968743@163.com` 在当前登录账号的授权列表可见，分别通过生产 SMTP/IMAP 检查。
- 验收任务 `上线验收-20260918-双邮箱互发`，ID `e4e61371d0bc4550b0f65f80c087825f`：仅两个上述邮箱，关闭自动跟进。通过应用 service 导入名单、创建及启动任务；将两条验收线索分别绑定到对方发件邮箱，实际发送由正常生产 worker 执行。
- 两封邮件均 SMTP accepted，随后 IMAP 均确认 received。初始收件保持未关联；没有把收件箱中的原始邮件误判为回复。
- XLSX 验收附件对象存储上传/读取哈希一致，两份实际收件附件与发件附件哈希及大小一致。第一次验收脚本误用了读取迭代器属性，事务回滚后更正为既有 body 接口；对象存储可能保留一个未关联的验收附件，不涉及客户文件。
- 另发送一封带标准 In-Reply-To/References 的受控测试回复，SMTP accepted，worker 同步后正确关联到上述任务及线索 `0f346d3b94b643959ed5445322525a39`。
- 页面显示任务“已结束 / 正常完成”，验收数据保留便于复核。共发出 2 封任务邮件及 1 封回复，未发往客户邮箱。
- 发送记录页面显示发送 2、已确认送达 1、回复 1。产品的送达统计采用回执/回复证据，不因本次在两个受控收件箱独立核实收件而人工改成 2。
- 最终观察窗口内无新增应用 ERROR；错误路径 `/health` 的单次 404 是验收脚本探测地址错误，实际健康地址为 `/ai-call/health`。

## 回滚

停止邮件 worker 后，用旧两份 Compose 仅重建 API；前端原子切回 `/home/lingchen/web/ai-reach/releases/web-package-ui-20260904T160925Z`。保留新表、凭据密钥及收发记录，不自动 DROP、不恢复整库、不重新排队已提交邮件。首次部署中的旧 API 回退已执行并确认 healthy。新镜像和旧镜像均保留。

该发布源于已验证候选源码快照，包含原本未提交改动；尚非通过 lingchen-release 完整 source-lock/release-plan 流程生成的正式归档。真实电话媒体链路、长期运行及未来定时邮件不在本次已完成验收结论内。
