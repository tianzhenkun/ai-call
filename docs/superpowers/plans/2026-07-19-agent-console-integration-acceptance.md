# 通用浏览器坐席中心联调验收计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:executing-plans 执行本计划；每个场景保留后端状态、前端截图/日志和最终结论。

**目标：** 证明前后端组合后满足真实浏览器坐席的并发归属、媒体接通、异常恢复、快速话后处理和跟进闭环。

**架构：** 后端数据库状态、LiveKit Participant/Room、前端浏览器状态三方交叉验收。接口受理不等于业务完成，每个异步场景必须查询最终状态。

**技术栈：** FastAPI、PostgreSQL、LiveKit、React、Playwright/真实浏览器、pytest、Jest。

---

## 联调前门槛

- [ ] 后端计划任务 1～7 的定向测试和 ruff 全部通过。
- [ ] 前端计划任务 1～7 的 Jest、lint、build 全部通过。
- [ ] 前端固定后端接口契约提交号；状态枚举和错误码逐项一致。
- [ ] 准备两个不同登录用户、各自坐席档案和可重叠的 `scene_code`。
- [ ] 准备独立 LiveKit 测试 Room、测试客户和可安全拨打的回拨号码。
- [ ] 确认三个时限为 15/15/60 秒，浏览器通知权限和麦克风权限可分别模拟允许/拒绝。

### 场景 1：登录身份与在线状态

- [ ] 未登录用户访问工作台返回 401；已登录但无启用坐席档案的用户显示不可上线；V1 管理接口仅校验登录，因此任意已登录用户均可访问。
- [ ] 坐席只看到授权 `scene_code`；停用后不能认领；同账号双标签页只有一个 `console_session_id` 获得媒体控制权。
- [ ] 页面刷新和 SSE 断线后 bootstrap 恢复服务端状态；本地缓存不得覆盖服务端。

### 场景 2：公共池与原子抢单

- [ ] 两坐席同时点击同一 handoff，数据库仅一条 `accepted`，仅一名坐席获得 Token，另一端收到 `HANDOFF_ALREADY_CLAIMED`。
- [ ] 同一坐席同时认领两个 handoff，只有一个成功；坐席处于 `claiming/in_call/reconnecting/wrap_up_quick` 时不能认领。
- [ ] 请求进入后声音、桌面通知和页面红点生效；暂停或通话中不播放声音。

### 场景 3：首次接入、总等待和真实 connected

- [ ] 认领后未加入 Room，15 秒到期释放坐席；客户仍在 60 秒内则 handoff 回到 requested。
- [ ] 60 秒无人真实接通，handoff 变 expired，并且只创建一条 `handoff_unanswered` 跟进任务。
- [ ] 浏览器加入 Room 但未发布麦克风时不能写 connected；Participant 与麦克风就绪后才写 connected。
- [ ] AI 在等待和人工通话阶段保持挂起，客户只听到等待提示或人工声音，不出现 AI 与人工同时发声。

### 场景 4：断线重连与异常结束

- [ ] connected 后断网写 `reconnect_expires_at`，原坐席 15 秒内恢复并清空该字段。
- [ ] 超时后 handoff failed、通话异常结束并进入快速话后确认，不重新进入公共池。
- [ ] 管理端对仍有活动 Room 的坐席执行 release-stale 返回 `STALE_RELEASE_NOT_ALLOWED`。

### 场景 5：快速话后处理

- [ ] 通话结束只要求处理结果和是否跟进，摘要、录音和语义分析未完成不阻塞提交。
- [ ] 选择需要跟进时 ACW 与 follow-up 同事务成功；重复提交不产生重复任务。
- [ ] 提交后坐席立即 available；后到 AI 草稿不能覆盖人工已确认内容。

### 场景 6：人工未接回访与人工回拨

- [ ] 未接回访初始无负责人，两坐席同时认领仅一人成功，认领后不能转交。
- [ ] 回拨由服务端解析 contact_ref，前端网络请求和日志不出现明文号码；新 call_id 关联原任务且不启动 AI Runner。
- [ ] no_answer/busy/rejected 自动追加 attempt 并回 pending，不显示固定重试时间、不自动重拨。
- [ ] technical_failure 记录 error_message 且不计有效客户联系；客户明确预约才写 customer_callback_at。

### 场景 7：管理查询、录音与补偿

- [ ] 三个管理页面指标与数据库抽样一致；详情时间线能解释 requested/accepted/connected/ended。
- [ ] 录音处理中、成功、失败均正确展示；ACW 表不重复存储录音地址。
- [ ] 重复执行 reconcile 不产生重复终态、事件或跟进任务；所有异常操作有审计记录。

## 最终交付证据

- [ ] 保存后端测试输出、前端测试/构建输出、数据库状态快照和双浏览器抢单录屏或截图。
- [ ] 记录未通过项的 `call_id`、`handoff_id`、时间线和错误码，不用“偶现”替代证据。
- [ ] 只有以上场景全部 PASS，才把“浏览器坐席 V1”标记为联调完成；真实号码、生产 Trunk 和生产部署需另设上线门槛。
