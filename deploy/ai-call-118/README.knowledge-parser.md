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

场景提示词页的开场白、场景提示词生成和优化，以及「从知识库一键提取」均调用 `deepseek-flash`。在运行 Reach API 的服务上设置 `AI_CALL_KNOWLEDGE_DEEPSEEK_API_KEY_FILE` 指向只读密钥文件，或设置 `AI_CALL_KNOWLEDGE_DEEPSEEK_API_KEY`，两者选其一。

已有数据库开启新增场景的保存前提取时，先应用 `docs/livekit-ai-outbound/sql/knowledge-summary-unsaved-scene-postgres.sql`。提取审计在场景未创建时保留来源版本，场景 ID 为空。

生产环境默认使用 `/run/ai-call-parser/parser.sock`，可通过 `AI_CALL_KNOWLEDGE_PARSER_SOCKET` 指定实际挂载路径。开发环境未配置该路径时，公共组件在有超时限制的本机子进程中解析文档。

### 外呼口语检索

升级已有数据库时，先执行 `docs/livekit-ai-outbound/sql/knowledge-spoken-retrieval-postgres.sql`。启用口语检索的 API、jobs 和实时通话进程使用相同的 `AI_CALL_KNOWLEDGE_EMBEDDING_DIR` 配置。将 BGE-small-zh-v1.5 的 `model_optimized.onnx` 和 `tokenizer.json` 放入该只读目录，启动时会核对文件 SHA-256。模型文件取自 [固定的模型版本](https://huggingface.co/Qdrant/bge-small-zh-v1.5/tree/46fbe35fd4374a00fee7de77dfddaeb6dd6a2c59)。

jobs 开启 `AI_CALL_KNOWLEDGE_WORKER_ENABLED`，并配置上文的 DeepSeek 凭证。资料解析就绪后，worker 整理产品章节、常见问法及对应的正文，同时补齐已有可用资料。整理失败会延迟重试。关联资料全部整理好后即可新建外呼任务。任务固定资料版本和检索方式。已有词法检索任务仍按原快照运行。

实时通话中由 Qwen 判断是否需要查询。查询同时携带客户问法和业务关键词，在本机匹配，返回相应产品的原文、文件和页码。多产品场景缺少具体产品时先向客户问清楚。
