# 118 Knowledge Parser 集成

本目录中的 `compose.knowledge-parser.yml` 是现有 118 `compose.yml` 的附加文件，只增加独立 PPTX、DOCX、文本型 PDF 解析进程和 API 的 Unix Socket 挂载，不复制整套 118 部署。

构建 amd64 离线镜像：

```bash
docker build --platform linux/amd64 \
  -f deploy/ai-call-118/Dockerfile.knowledge-parser \
  -t ai-call-transfer/knowledge-parser:20260820-docx-pdf .
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

解析器包含 `pptx-ooxml-stdlib-v1`、`docx-ooxml-stdlib-v1` 和 `pdf-pypdf-6.16.1-v1`。前端当前只开放已经在 118 验收的 PPTX；DOCX、文本型 PDF 必须使用同一镜像完成 118 现场冒烟后再开放。
