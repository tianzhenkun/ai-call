# AI Call 录音目录公共读设计

## 背景

当前 AI Call 录音保存在 `recov` Bucket 的 `ai-call/recordings/` 对象前缀下。录音查询接口根据 `oss_id` 生成 15 分钟有效的 MinIO 预签名地址，前端将接口返回的 `playUrl` 交给浏览器原生 `<audio>` 播放。

本次需要为当前联调阶段提供可直接访问的 OSS 裸地址，同时避免把 `recov` Bucket 中的截图、文档和其他对象一并公开。

## 目标

1. 只允许匿名读取 `recov/ai-call/recordings/*`。
2. 通过显式配置控制录音接口返回裸地址还是预签名地址。
3. 配置关闭时保持现有私有录音行为。
4. 前端继续使用现有 `playUrl` 契约，无需修改。
5. 可以通过关闭配置并撤销 MinIO 策略完整回滚。

## 非目标

1. 不公开整个 `recov` Bucket。
2. 不允许匿名列举、上传、覆盖或删除对象。
3. 不修改录音生成、LiveKit Egress、转人工或离线 ASR 流程。
4. 不删除现有预签名能力。
5. 不修改非 `ai-call/recordings/` 前缀对象的访问方式。

## 方案

### 配置

新增布尔配置：

```text
AI_CALL_RECORDING_PUBLIC_READ=false
```

默认值必须为 `false`。本地联调环境显式设为 `true` 后，录音查询接口才允许返回 OSS 裸地址。生产环境未显式配置时继续返回预签名地址。

### 地址选择

录音查询仍通过现有接口：

```text
GET /ai-call/records/{callId}/recording
```

后端根据录音关联的 `sys_oss` 记录选择 `playUrl`：

1. `AI_CALL_RECORDING_PUBLIC_READ=true`；
2. OSS 记录存在；
3. 对象 `file_name` 等于 `ai-call/recordings/...` 范围内的路径；
4. OSS 记录存在非空 `url`。

四项全部满足时返回 `sys_oss.url`。任何一项不满足时，继续调用现有预签名逻辑。

前缀判断必须按路径边界执行，接受：

```text
ai-call/recordings/call-id.mp3
ai-call/recordings/tracks/call-id/customer.ogg
```

拒绝：

```text
ai-call/recordings-other/file.mp3
screenshots/file.png
documents/file.pdf
```

### MinIO 策略

只为以下资源授予匿名 `s3:GetObject`：

```text
arn:aws:s3:::recov/ai-call/recordings/*
```

策略不得包含 Bucket 级 `s3:ListBucket`，也不得包含写入和删除权限。设置后，AI Call 主混音及其 `tracks/` 子目录均可匿名读取，`recov` 中其他前缀仍保持私有。

### 前端

前端不改接口、不拼接 OSS 地址，也不保存长期 URL。`/ai-call/handoffs` 和 AI Call 测试页继续读取 `recording.playUrl` 并通过 `<audio>` 播放。

## 异常与回退

1. 开关开启但 MinIO 策略未生效：接口会返回裸地址，浏览器播放得到 `403`；部署步骤必须先设置策略并验证，再开启开关。
2. OSS 记录缺失或 `url` 为空：保持返回 `null` 或现有录音状态，不手工猜测地址。
3. 对象不在录音前缀：始终返回预签名地址。
4. 回滚：先将开关设为 `false` 并重启 19011，再撤销 MinIO 前缀公共读策略。

## 验证

1. 单元测试：默认配置返回带 `X-Amz-*` 的预签名地址。
2. 单元测试：公共读开启且对象位于录音前缀时返回无查询签名参数的 `sys_oss.url`。
3. 单元测试：公共读开启但对象位于其他前缀时仍返回预签名地址。
4. 接口验证：录音接口返回的 `playUrl` 不包含 `X-Amz-*`。
5. 对象验证：匿名请求 AI Call 录音返回 `200`，并支持浏览器音频 Range 请求。
6. 隔离验证：匿名请求 `recov` 中其他前缀对象仍返回 `403`。
7. 页面验证：`/ai-call/handoffs` 详情中的录音可正常播放。

## 修改边界

当前工作树已有预签名实现相关的未提交修改。本次实现保留这些修改作为默认回退，只在配置、录音地址选择和对应测试上做最小增量，不覆盖或提交其他已有修改。
