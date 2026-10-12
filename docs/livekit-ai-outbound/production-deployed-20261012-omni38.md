# 2026-10-12 Omni3.8 Flash 发布记录

状态：已部署到生产 API 与 REACH 前端。服务健康、Nacos 注册和公网静态资源检查通过；本次未发起真实 SIP 通话。

## 发布范围

- 发布编号：`reach-omni38-20261012T073318Z`。
- 后端提交：`badcf788f8cba274bd11744ae8f50d3b4c1ac4cb`；前端提交：`48a385e01e0147e8e98f9543e4cef890b4798ef0`，均位于 `codex/release-omni38-20261012` 分支。
- 从线上 Omni Plus / Flash 对照版本增量发布。新增 `qwen3.8-omni-flash-realtime` 会话模型、业务空间连接地址校验、音色限制及 REACH 任务和浏览器通话入口；默认模型继续为 `qwen3.5-omni-plus-realtime`。
- 生产 Compose 仅替换 API 镜像并为该服务增加 `DASHSCOPE_WORKSPACE_REALTIME_URL`；不涉及数据库迁移、其他 worker、公共 Java 或 SIP 栈。

## 验证与现场证据

- 本地后端相关 632 项测试、前端 31 项测试、TypeScript 检查和正式构建通过；Python Ruff `F,E9` 与前端修改文件的 Biome 检查通过。
- 发布包 SHA-256：`f8a557d0e0a21c4a043fee6a753eac2c386a5193b95f18ef68eb60304a529f23`，服务器上传后复核一致。
- 服务器：`110.42.223.109`。发布目录：`/opt/lingchen/deployments/reach-omni38-20261012T073318Z`。部署前数据库备份 `ai-call-before.dump` 为 1,721,777 字节，`pg_restore -l` 校验通过；旧 Compose、镜像和前端目标保存在该目录。
- 切换前活动通话、待启动命令、SIP 预留、转接和 LiveKit 房间均为 0；已有 1 条 `ending` 记录保留。
- API 镜像 `ai-call-transfer/api:reach-omni38-20261012T073318Z`，ID `sha256:b63c093a0ba5864249c7b622da39569bcfa78484030743ae3e4f2a2ddfeafcc6`；容器健康、重启数 0，内部健康接口返回 200，`lingchen-reach-python` 已注册到生产 Nacos。邮件与知识解析 worker 保持健康且镜像和容器 ID 未变。
- 容器内验证 Omni3.8 Flash 配置可用、默认模型仍是 Omni3.5 Plus。业务空间地址和 API 密钥仅校验指纹与是否生效，不记录其内容。
- 前端 `current` 指向 `/home/lingchen/web/ai-reach/releases/reach-omni38-20261012T073318Z`。保留生产 `runtime-config.js` 和 393 个旧 JS/CSS 资源；259 个公网页面及静态资源的哈希检查通过，独立复核公网首页与发布清单一致。
- 前端切换前发现构建清单中的 `runtime-config.js` 与生产运行配置不同。发布脚本改为单独核对保留的生产运行配置，随后完成切换；应用代码和构建产物未改动。

## 回滚与验收边界

回滚前确认没有正在使用 Omni3.8 Flash 的新通话或待执行任务。旧 API Compose、旧镜像、前端软链接目标和数据库备份位于上述发布目录。本次无数据库迁移，常规回滚只需恢复旧 API Compose 与前端软链接。

以上检查确认部署与静态链路，不代表 Omni3.8 Flash 的真实电话体验已验收。首次电话对照仍需使用受控号码和一致的话术、知识、音色及线路，检查双向媒体、打断、工具、事实依据、转接、结束和录音。
