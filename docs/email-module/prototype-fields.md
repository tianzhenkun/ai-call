# 邮件原型字段映射

2026-09-16 使用 CUA 只读核对 AI Studio 已打开的 [Remix Reach AI](https://aistudio.google.com/apps/5f0b33ee-a5e3-4c07-acec-688f417752c2)。未修改外部原型或发送邮件。

## 页面

原型为新建任务 / 邮件模板 / 历史记录 / 线索管理；按已确认设计调整为邮件任务 / 发送记录 / 线索管理。模板库不迁移，新建入口放任务列表。

## 发送记录

| 原型列 | REACH 字段 | 语义 |
| --- | --- | --- |
| 任务名称 | taskName | taskId 关联任务 |
| 发件邮箱 | senderEmail | 按任务与发件账号分组 |
| 发送邮件数 | sentCount | 已发送数量 |
| 成功送达数 | deliveredCount | 无最终送达回执时为 null，显示 —，不能以 SMTP 接受数替代 |
| 收到回复数 | repliedCount | 点击进入该任务线索管理 |

接口 GET /reach-api/v1/email/messages/summary；分页 items / total / page / pageSize。acceptedCount 仅为 SMTP 接受证据，不等于最终送达。

## 线索管理

任务筛选，分类为有意向 / 持续跟进 / 低价值（interested / following / low_value）。表列：所属任务 taskName、客户名称 customerName、客户邮箱 email、最新跟进状态 latestStatus、往来信息 conversationId、操作（回复/划转）。人工分类不改变跟进队列。

## 往来弹窗

原型顶部客户名称、联系人、邮箱和最新跟进状态，下方按时间展示来往邮件序列：客户来信、首封及追加跟进；每项包含时间、主题、发件人和正文。底部撰写新回复。REACH 展示真实 messages 的 direction/kind/createdAt/subject/fromEmail/toEmail/html/status/attachmentIds。HTML 使用 sandbox iframe + 清洗 + CSP 禁止外部图片。回复与主动跟进返回入队结果，不能显示已送达。

## 平台菜单登记

前端注册 `/reach/email`，薄入口 `src/pages/reach/email/index.tsx`。平台需登记 REACH 产品菜单“邮件管理”（组件 reach/email/index），分配给相应角色；原始 getRouters 树仍为 allowlist，不放宽前端鉴权。邮箱管理通过此页面“管理邮箱账号”入口和底部账号菜单“账号授权”入口（跳转 `/reach/email?accounts=1`）共用同一管理组件，不调用 Sales 邮件接口。本次不修改 Java 代码或菜单数据库。
