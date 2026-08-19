# 118 Knowledge Parser 集成

本目录中的 `compose.knowledge-parser.yml` 是现有 118 `compose.yml` 的附加文件，只增加独立 PPTX 解析进程和 API 的 Unix Socket 挂载，不复制整套 118 部署。

构建 amd64 离线镜像：

```bash
docker build --platform linux/amd64 \
  -f deploy/ai-call-118/Dockerfile.knowledge-parser \
  -t ai-call-transfer/knowledge-parser:20260819 .
```

部署前先备份数据库，并将
`docs/livekit-ai-outbound/sql/phase-j1-knowledge-lexical-postgres.sql`
上传到运行目录后执行：

```bash
docker compose -f compose.yml exec -T postgres sh -lc \
  'pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB"' > pre-knowledge.sql
docker compose -f compose.yml exec -T postgres sh -lc \
  'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < phase-j1-knowledge-lexical-postgres.sql
```

随后验证配置，只重建解析器和 API，不重启现有媒体服务：

```bash
docker compose --env-file .env \
  -f compose.yml \
  -f compose.knowledge-parser.yml config -q

docker compose --env-file .env \
  -f compose.yml \
  -f compose.knowledge-parser.yml up -d --no-deps knowledge-parser api
```

解析器固定为非 root、只读根文件系统、`network_mode: none`、`cap_drop: ALL`，不加载 `.env`、数据库、COS、Redis 或 DashScope 凭证。API 只通过共享 Unix Socket 传递只读文件描述符；解析器不挂载知识原文件目录。单次解析在进程内强制 25 秒截止，早于 API 的 30 秒等待上限。

当前只启用 `pptx-ooxml-stdlib-v1`。前端仍保持 TXT/Markdown 白名单；只有在 118 合并部署并完成相同镜像的现场冒烟后，才开放 PPTX 选择入口。
