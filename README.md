# LingChen AI Call Base

智能外呼独立后端基座，裁剪自 `LingChenAdmin` 的通用工程能力。

## 当前定位

本项目只保留智能外呼需要的基础设施：

1. FastAPI 应用入口。
2. 数据库、Redis、日志、异常、中间件。
3. 鉴权、用户、字典基础能力。
4. `system/oss` 和 `sys_oss` 文件索引能力。
5. `ai_call` 智能外呼模块占位路由。

旧催收、诉讼、资产包、画像、RPA、文档处理和原 call_record 业务已从基座中移除。

## 目录说明

```text
app/api/v1/ai_call/      智能外呼模块，Phase 00 从这里开始实现
app/api/v1/system/       基座系统能力
app/core/                数据库、Redis、异常、日志、中间件
app/common/              通用响应、枚举、常量
app/config/              配置加载
app/utils/               通用工具
docs/                    保留的通用开发和日志规范
env/                     环境变量示例，不包含真实密钥
```

## 启动方式

先按实际环境复制示例配置：

```bash
cp env/.env.dev.example env/.env.dev
```

再修改数据库、Redis、JWT 等配置后启动：

```bash
uv run python main.py run --env dev
```

## 当前健康检查

```text
GET /common/health
GET /ai-call/health
```

## 下一步

按 `docs/livekit-ai-outbound/phases/phase-00-web-business-loop.md` 中的 Phase 00 方案，实现 Web 版商业闭环。
