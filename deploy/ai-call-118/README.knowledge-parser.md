# 118 Knowledge Parser 集成

现有 118 `compose.yml` 叠加 `compose.knowledge-parser.yml` 后，知识库任务通过 Unix Socket 将 DOCX 和 PDF 交给独立进程解析。该进程和任务服务使用同一份 `lingchen-knowledge` 公共组件。

构建 amd64 离线镜像：

```bash
docker build --platform linux/amd64 \
  -f deploy/ai-call-118/Dockerfile.knowledge-parser \
  -t ai-call-transfer/knowledge-parser:lingchen-knowledge-0.8.4 .
```

部署前先备份数据库，将 `docs/livekit-ai-outbound/sql/` 下的基础脚本和资料统计增量脚本上传到运行目录，依次执行。

```bash
docker compose -f compose.yml exec -T postgres sh -lc \
  'pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB"' > pre-knowledge.sql
docker compose -f compose.yml exec -T postgres sh -lc \
  'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < phase-j1-knowledge-lexical-postgres.sql
docker compose -f compose.yml exec -T postgres sh -lc \
  'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < knowledge-library-metrics-postgres.sql
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

公共组件保留 Word 段落编号和 PDF 页码。扫描版 PDF 由解析器识别后，任务服务按公共组件的文字识别流程处理。数据库脚本可重复执行，为既有知识版本增加供应商任务编号、请求键与轮次等字段。

在运行知识库任务的服务上配置 `AI_CALL_KNOWLEDGE_OCR_API_KEY` 用于图片和扫描 PDF 的 PaddleOCR 识别。音视频转写使用 `ASR_API_KEY`，也可以复用 `DASHSCOPE_API_KEY`。FFmpeg 提取的临时音轨需要使用 DashScope 可访问的 HTTPS 对象存储地址。

场景提示词页的「从知识库一键提取」调用 `deepseek-flash`。在运行 Reach API 的服务上设置 `AI_CALL_KNOWLEDGE_DEEPSEEK_API_KEY_FILE` 指向只读密钥文件，或设置 `AI_CALL_KNOWLEDGE_DEEPSEEK_API_KEY`，两者选其一。

生产环境默认使用 `/run/ai-call-parser/parser.sock`，可通过 `AI_CALL_KNOWLEDGE_PARSER_SOCKET` 指定实际挂载路径。开发环境未配置该路径时，公共组件在有超时限制的本机子进程中解析文档。
