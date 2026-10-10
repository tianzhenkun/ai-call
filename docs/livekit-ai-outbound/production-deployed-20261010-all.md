# 2026-10-10 REACH 前后端发布记录

北京时间约 12:35，在 `110.42.223.109` 发布本次已提交的前后端改动。后端提交 `b3cb215fe1a123184c54bb108cad0fd096c5c587`，前端提交 `cf1e51f28ae4272335247f958a0ea5f26778ac54`。本次后端变更为 ASR 失败后的重问、迟到转写处理，以及第 15 轮后的补答与告别流程；前端变更为任务创建页头部和 Agent Workbench 宽屏布局。没有数据库 schema 变更，邮件 worker 与知识解析服务的代码和镜像均未更新。

两个 Git 提交已分别推送到各自远端分支。发布前，后端相关回归 503 项、Ruff 通过；前端相关页面测试 28 项、TypeScript、Biome 和正式构建通过。发布包含 409 个前端文件及逐文件 SHA-256 清单。后端镜像从当时生产镜像增量构建；旧快照哈希与 Git 基线一致，新镜像内两个修改文件的 SHA-256 与新提交一致。

发布目录 `/opt/lingchen/deployments/reach-all-20261010T043225Z`，前端目录 `/home/lingchen/web/ai-reach/releases/reach-all-20261010T043225Z`。API 镜像为 `ai-call-transfer/api:reach-all-20261010T043225Z`，image ID `sha256:8e808783588500f0c7a04f4466eb4111327d6597b776c77bdeda1df717dbc36a`。

切换前保存 `reach-compose.before.json`、`reach-compose.after.json` 和 `ai-call-before.dump`，数据库归档大小 1,435,006 字节，已通过 `pg_restore -l` 检查。两份 Compose 经结构比较，仅 API 镜像字段不同。活动通话、待发起命令、SIP 预留、活跃转接及 LiveKit 房间均为 0；另有一条历史 `ending` 记录，没有活动房间，未清理。

新 API 容器 `ai-call-118-api-1` 已 `healthy`，重启数 0，内部 `/ai-call/health` 返回 `{"status":"ok"}`；日志显示 `lingchen-reach-python` 在 Nacos 重新注册。主 Compose `/opt/lingchen-migration-20261002/reach-compose.json` 与发布后的候选文件一致。邮件 worker、知识解析容器继续 `healthy`。

前端 `current` 已指向新目录，沿用旧版 `runtime-config.js`，其 SHA-256 与原目录一致；另保留 378 个旧哈希资源。首次切换后公网曾返回 HTTP 403：新目录由 `umask 077` 下的脚本生成，目录 `0700`、文件 `0600`，Nginx 用户无法读取。仅调整本次新前端目录为目录 `0755`、文件 `0644` 后，域名首页恢复，公网 HTML 与新 `index.html` 的 SHA-256 同为 `4e7a7d4de8211e56e7a1e3553234cc8e8e8ae25df6be057e8d36924f3ec3e1ab`；260 个公开 JS/CSS 和关键页面资源逐一核对通过。此次短暂 403 的实际访问影响没有独立统计，不以最终 200 掩盖该故障。

本次证据覆盖文件、容器、健康、Nacos 注册及公开静态资源。没有带登录态的生产业务页面/API 验收，也没有真实 SIP 电话的响铃、接通、双向媒体、打断和录音验收。

回滚材料仍在上述发布目录。再次确认没有活动通话后，用 `reach-compose.before.json` 恢复旧 API 镜像 `sha256:6621e6fb95cb34cd773d6caeefab8e50e111c2e8c241b48f136a6b667e4765ef` 并确认健康，再将主 Compose 原子恢复；前端按 `previous-target.txt` 原子指回 `/home/lingchen/web/ai-reach/releases/reach-all-20261008T101338Z` 并验证域名哈希。本次无数据库迁移，通常不需要还原数据。下次发布请按[现行上线手册](../email-module/production-runbook.md)重新核对现场。
