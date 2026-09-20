# REACH 2026-09-20 任务编辑更新发布记录

版本：`reach-email-20260920T051012Z`；入口：<https://reach.lingchen-ai.com/>。

## Git 与范围

- 前端提交：`07e5d3e43aa25eabc3924f71ab2d0ec33b80236e`，已推送 `codex/release-reach-email-20260919`，未合入 main。新增直接编辑任务入口，编辑与新建共用内联设置表单，保留原名单和正文。
- 后端仓库文档提交：`1cc1918cad1031d1a48f6d5a765a86bb051431ea`，已推送 `codex/feat/reach-platform-integration`，同步设计文档。
- 后端应用源码、依赖与线上构建提交 `745ebd4af5faa6d20ce5aeb05549bd3d2c57f276` 完全一致，本次沿用镜像 `ai-call-transfer/api:reach-email-20260920T035101Z`，没有重建或重启 API/worker，没有执行数据库迁移。
- 保留工作区 AGENTS.md 修改及本地 storage、浏览器临时文件，未提交凭据。

## 验证与部署

- 任务页面 28 项测试通过；Biome、TypeScript、Ant Design lint 和生产构建通过。Ant Design lint 仍有既存的 18 项弃用及 1 项性能提示。
- 生产 runtime-config.js 沿用现有配置，通过当前解析器。构建前端工作区干净，产物绑定上述前端提交。
- API/worker 均 healthy，各自 233 个应用源码文件哈希匹配原发布清单；worker 健康命令返回 `workerOnline=true`、`overdueCount=0`、`status=ok`。
- 后端镜像 config digest：`sha256:2d198eea6c5d9b01572847c41523f9b5a475d8eda8bd6b5910cfe18548103330`。
- 前端服务器 `81.68.166.109`，新目录 `/home/lingchen/web/ai-reach/releases/reach-email-20260920T051012Z`。逐文件校验通过，补齐 122 个旧哈希资源后原子切换 current。
- 公网首页 SHA256：`8cd373e3e486a94326aa88910ea4b954672d47f7f1fbd7e6870560a672608ca8`，与新文件一致。
- 从前端服务器通过线上 HTTPS 域名逐一读取 367 个 JS/CSS/HTML 文件，全部哈希匹配本次产物。
- 本次没有操作真实业务任务，未将组件测试及静态资源核验当作登录态保存验收。

## 产物与回滚

本地忽略目录 `build/reach-email-20260920T051012Z/` 保存前端包、manifest.json、SHA256SUMS 和核验脚本。前端服务器 releases 下保留对应 `.tar.gz`、`.manifest.json` 和 `.previous-target.txt`。

上一版前端目录为 `/home/lingchen/web/ai-reach/releases/reach-email-20260920T035101Z`，需要回滚时通过临时软链接和 `mv -Tf` 原子切回。后端、数据库及全部密钥未变，无需后端回滚或数据库恢复。

通用步骤见 [上线操作手册](production-runbook.md)。
