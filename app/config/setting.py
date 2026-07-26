import os
import warnings
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote_plus

from pydantic_settings import BaseSettings, SettingsConfigDict

from app.common.enums import EnvironmentEnum
from app.config.path_conf import BASE_DIR, ENV_DIR


class Settings(BaseSettings):
    """系统配置类"""

    model_config = SettingsConfigDict(
        env_file=ENV_DIR / f".env.{os.getenv('ENVIRONMENT')}",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True,  # 区分大小写
    )

    # ================================================= #
    # ******************* 项目环境 ****************** #
    # ================================================= #
    ENVIRONMENT: EnvironmentEnum = EnvironmentEnum.DEV

    # ================================================= #
    # ******************* 服务器配置 ****************** #
    # ================================================= #
    SERVER_HOST: str = "0.0.0.0"  # 允许访问的IP地址
    SERVER_PORT: int = 19010  # 服务端口

    # ================================================= #
    # ******************* API文档配置 ****************** #
    # ================================================= #
    DEBUG: bool = True  # 调试模式
    TITLE: str = "LingChen AI Call"  # 文档标题
    VERSION: str = "0.1.0"  # 版本号
    DESCRIPTION: str = (
        "智能外呼独立后端基座，基于 FastAPI、SQLAlchemy、Redis 和 sys_oss 能力建设。"  # 文档描述
    )
    SUMMARY: str = "接口汇总"  # 文档概述
    DOCS_URL: str = "/docs"  # Swagger UI路径
    ROOT_PATH: str = "/ai-call-api/v1"  # API路由前缀

    # ================================================= #
    # ******************** 日志配置 ******************** #
    # ================================================= #
    LOGGER_LEVEL: str = "DEBUG"  # 日志级别

    # ================================================= #
    # ******************** 跨域配置 ******************** #
    # ================================================= #
    CORS_ORIGIN_ENABLE: bool = True  # 是否启用跨域
    ALLOW_ORIGINS: list[str] = ["*"]  # 允许的域名列表
    ALLOW_METHODS: list[str] = ["*"]  # 允许的HTTP方法
    ALLOW_HEADERS: list[str] = ["*"]  # 允许的请求头
    ALLOW_CREDENTIALS: bool = True  # 是否允许携带cookie
    CORS_EXPOSE_HEADERS: list[str] = ["X-Request-ID"]

    # ================================================= #
    # ******************* 登录认证配置 ****************** #
    # ================================================= #
    JWT_ENABLE: bool = True  # 是否启用JWT认证
    SECRET_KEY: str = "abcdefghijklmnopqrstuvwxyz"  # JWT密钥
    ALGORITHM: str = "HS256"  # JWT算法
    TOKEN_TYPE: str = "bearer"  # token类型
    TOKEN_REQUEST_PATH_EXCLUDE: list[str] = ["ai-call-api/v1/auth/login"]  # JWT路由白名单

    # ================================================= #
    # ******************** 数据库配置 ******************* #
    # ================================================= #
    SQL_DB_ENABLE: bool = True  # 是否启用数据库
    DATABASE_ECHO: bool | Literal["debug"] = False  # 是否显示SQL日志
    ECHO_POOL: bool | Literal["debug"] = False  # 是否显示连接池日志
    POOL_SIZE: int = 10  # 连接池大小
    MAX_OVERFLOW: int = 20  # 最大溢出连接数
    POOL_TIMEOUT: int = 30  # 连接超时时间(秒)
    POOL_RECYCLE: int = 1800  # 连接回收时间(秒)
    POOL_USE_LIFO: bool = True  # 是否使用LIFO连接池
    POOL_PRE_PING: bool = True  # 是否开启连接预检
    FUTURE: bool = True  # 是否使用SQLAlchemy 2.0特性
    AUTOCOMMIT: bool = False  # 是否自动提交
    AUTOFETCH: bool = False  # 是否自动刷新
    EXPIRE_ON_COMMIT: bool = False  # 是否在提交时过期

    # MySQL/PostgreSQL数据库连接
    DATABASE_TYPE: Literal["mysql", "postgres", "sqlite"] = "postgres"
    DATABASE_HOST: str = "localhost"
    DATABASE_PORT: int = 5432
    DATABASE_USER: str = "postgres"
    DATABASE_PASSWORD: str = "ServBay.dev"
    DATABASE_NAME: str = "lingchen_ai_call"

    # ================================================= #
    # ******************** Redis配置 ******************* #
    # ================================================= #
    REDIS_ENABLE: bool = True  # 是否启用Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB_NAME: int = 1
    REDIS_USER: str = ""
    REDIS_PASSWORD: str = ""

    # ================================================= #
    # ******************* LiveKit配置 ****************** #
    # ================================================= #
    LIVEKIT_URL: str = ""
    LIVEKIT_API_KEY: str = ""
    LIVEKIT_API_SECRET: str = ""
    LIVEKIT_BROWSER_TOKEN_TTL_SECONDS: int = 600
    LIVEKIT_RTC_TCP_PORT: int = 7881
    LIVEKIT_ICE_UDP_RANGE: str = "50000-50100"

    # ================================================= #
    # ******************** SIP配置 ********************* #
    # ================================================= #
    AI_CALL_SIP_OUTBOUND_ENABLED: bool = False
    AI_CALL_SIP_ALLOWED_CALLEE_PREFIXES: str = ""
    AI_CALL_SIP_DEFAULT_RINGING_TIMEOUT_SECONDS: int = 45
    AI_CALL_SIP_MAX_RINGING_TIMEOUT_SECONDS: int = 120
    AI_CALL_SIP_MAX_CALL_DURATION_SECONDS: int = 600
    LIVEKIT_SIP_OUTBOUND_TRUNK_ID: str = ""
    LIVEKIT_SIP_OUTBOUND_TRUNK_HOSTNAME: str = ""
    LIVEKIT_SIP_OUTBOUND_DESTINATION_COUNTRY: str = "CN"
    LIVEKIT_SIP_AUTH_USERNAME: str = ""
    LIVEKIT_SIP_AUTH_PASSWORD: str = ""
    SIP_PROXY: str = ""
    SIP_CALLER_NUMBER: str = ""
    SIP_SIGNALING_PORT: int = 5080
    SIP_RTP_RANGE: str = "16384-16484"
    SIP_PUBLIC_IP: str = ""
    SIP_USE_EXTERNAL_IP: bool = True
    AI_CALL_BARGE_IN_ENABLED: bool = True
    AI_CALL_SIP_BARGE_IN_ENABLED: bool = True
    AI_CALL_SIP_BARGE_IN_MIN_RMS_DBFS: float = -35.0
    AI_CALL_SIP_BARGE_IN_MIN_SPEECH_DURATION_MS: int = 220
    AI_CALL_SIP_BARGE_IN_HOLD_TIMEOUT_SECONDS: float = 5.0
    AI_CALL_SIP_BARGE_IN_FAST_STOP_ENABLED: bool = False
    AI_CALL_SIP_BARGE_IN_RMS_THRESHOLD_DBFS: float = -36.0
    AI_CALL_SIP_BARGE_IN_SNR_THRESHOLD_DB: float = 10.0
    AI_CALL_SIP_BARGE_IN_VAD_VOICED_DURATION_MS: int = 120
    AI_CALL_SIP_BARGE_IN_CANDIDATE_MIN_DURATION_MS: int = 180
    AI_CALL_SIP_BARGE_IN_PRE_STOP_MIN_DURATION_MS: int = 240
    AI_CALL_SIP_BARGE_IN_SHORT_SPEECH_MIN_DURATION_MS: int = 180
    AI_CALL_SIP_BARGE_IN_IMPULSE_NOISE_MAX_DURATION_MS: int = 120
    AI_CALL_SIP_BARGE_IN_CLEAN_WINDOW_MS: int = 300
    AI_CALL_SIP_BARGE_IN_MAX_HOLD_MS: int = 500
    AI_CALL_SIP_BARGE_IN_ECHO_TAIL_WINDOW_MS: int = 500
    AI_CALL_SIP_BARGE_IN_RECOVERY_SILENCE_MS: int = 600
    AI_CALL_SIP_BARGE_IN_RECOVERY_MAX_PER_TURN: int = 1
    AI_CALL_SIP_VAD_SHADOW_ENABLED: bool = False
    AI_CALL_SIP_VAD_SHADOW_DETECTOR: Literal["webrtc", "fsmn", "webrtc+fsmn"] = "webrtc"
    AI_CALL_SIP_VAD_SHADOW_FSMN_MODEL: str = "iic/speech_fsmn_vad_zh-cn-16k-common-pytorch"
    AI_CALL_SIP_VAD_SHADOW_FSMN_ENDPOINT: str = ""
    AI_CALL_SIP_VAD_SHADOW_FSMN_TIMEOUT_SECONDS: float = 0.2
    AI_CALL_SIP_VAD_SHADOW_QUEUE_SIZE: int = 50

    # ================================================= #
    # ***************** AI Provider配置 **************** #
    # ================================================= #
    DASHSCOPE_API_KEY: str = ""
    DASHSCOPE_REGION: str = "cn-beijing"
    DASHSCOPE_BASE_URL: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    DASHSCOPE_WEBSOCKET_URL: str = "wss://dashscope.aliyuncs.com/api-ws/v1/inference"
    DASHSCOPE_REALTIME_URL: str = "wss://dashscope.aliyuncs.com/api-ws/v1/realtime"

    QWEN_REALTIME_MODEL: str = "qwen3.5-omni-plus-realtime"
    QWEN_REALTIME_VOICE: str = "Tina"
    QWEN_REALTIME_TURN_DETECTION_TYPE: str = "server_vad"
    QWEN_REALTIME_VAD_THRESHOLD: float = 0.5
    QWEN_REALTIME_VAD_SILENCE_DURATION_MS: int = 800

    AI_CALL_DEFAULT_PROMPT: str = "你是一个电话外呼助手，回答要简短自然。"
    AI_CALL_STANDALONE_ENABLE: bool = False
    AI_CALL_OPENING_MESSAGE: str = "您好，我是灵宸智能助手，请问现在方便简单沟通一下吗？"
    AI_CALL_EVENT_STORE: Literal["memory", "jsonl"] = "memory"
    AI_CALL_RECORDING_ENABLED: bool = True
    AI_CALL_PARTICIPANT_RECORDING_ENABLED: bool = True
    AI_CALL_RECORDING_FORMAT: Literal["MP4", "OGG", "MP3"] = "MP3"
    AI_CALL_PARTICIPANT_RECORDING_FORMAT: Literal["MP4", "OGG", "MP3"] = "OGG"
    AI_CALL_RECORDING_OBJECT_PREFIX: str = "ai-call/recordings"
    AI_CALL_EGRESS_TIMEOUT_SECONDS: float = 2.0
    AI_CALL_EGRESS_STOP_TIMEOUT_SECONDS: float = 10.0
    AI_CALL_RECORDING_VERIFY_DEADLINE_SECONDS: int = 900
    AI_CALL_RECORDING_RECONCILE_ENABLED: bool = True
    AI_CALL_RECORDING_RECONCILE_INTERVAL_SECONDS: float = 5.0
    AI_CALL_RECORDING_RECONCILE_BATCH_SIZE: int = 50
    AI_CALL_OFFLINE_ASR_ENABLED: bool = True
    AI_CALL_OFFLINE_ASR_PROVIDER: Literal[
        "dashscope_paraformer",
        "dashscope_qwen_filetrans",
    ] = "dashscope_qwen_filetrans"
    AI_CALL_OFFLINE_ASR_MODEL: str = "qwen3-asr-flash-filetrans"
    AI_CALL_OFFLINE_ASR_LANGUAGE_HINTS: str = "zh"
    AI_CALL_OFFLINE_ASR_TIMEOUT_SECONDS: float = 300.0
    AI_CALL_OFFLINE_ASR_POLL_INTERVAL_SECONDS: float = 2.0
    AI_CALL_OFFLINE_ASR_QUEUE_MAX_SIZE: int = 1000
    AI_CALL_SEMANTIC_ANALYSIS_ENABLED: bool = True
    AI_CALL_SEMANTIC_ANALYSIS_MODEL: str = ""
    AI_CALL_SEMANTIC_ANALYSIS_TIMEOUT_SECONDS: float = 30.0
    AI_CALL_SEMANTIC_ANALYSIS_QUEUE_MAX_SIZE: int = 1000
    AI_CALL_USER_TURN_STABILITY_DELAY_SECONDS: float = 0.35
    AI_CALL_HANDOFF_WAITING_PROMPT_AUDIO_PATH: str | None = str(
        BASE_DIR / "static/ai-call/audio/handoff-waiting.wav"
    )
    AI_CALL_HANDOFF_WAITING_TONE_ENABLED: bool = True
    AI_CALL_HANDOFF_WAITING_TONE_AUDIO_PATH: str = str(
        BASE_DIR / "static/ai-call/audio/handoff-ringback.wav"
    )
    AI_CALL_HANDOFF_WAITING_TONE_INTERVAL_SECONDS: float = 0.0
    AI_CALL_HANDOFF_UNAVAILABLE_PROMPT_AUDIO_PATH: str | None = str(
        BASE_DIR / "static/ai-call/audio/handoff-unavailable.wav"
    )
    AI_CALL_HANDOFF_UNAVAILABLE_PROMPT_TEXT: str = (
        "当前暂时没有人工接入，我先帮您记录需求，稍后安排顾问联系您。"
    )
    AI_CALL_HANDOFF_TIMEOUT_SECONDS: int = 30
    AI_CALL_AGENT_CLAIM_CONNECT_TIMEOUT_SECONDS: int = 15
    AI_CALL_AGENT_RECONNECT_GRACE_SECONDS: int = 15
    AI_CALL_HANDOFF_TOTAL_WAIT_SECONDS: int = 60
    AI_CALL_HANDOFF_EXCEPTION_CLOSE_ENABLED: bool = True
    AI_CALL_HANDOFF_PROMPT_CONSTRAINT_ENABLED: bool = True
    AI_CALL_HANDOFF_AUTO_TRIGGER_ENABLED: bool = True
    AI_CALL_HANDOFF_CUSTOMER_INTENT_ENABLED: bool = True
    AI_CALL_HANDOFF_SYSTEM_RULE_ENABLED: bool = False
    AI_CALL_HANDOFF_INTENT_THRESHOLD: float = 0.8
    AI_CALL_HANDOFF_INTENT_TIMEOUT_SECONDS: float = 1.0
    AI_CALL_PROMPT_RESOLVE_TIMEOUT_SECONDS: float = 2.0
    AI_CALL_DEBUG_PROMPT_OVERRIDE_ENABLED: bool = False
    AI_CALL_COLLECTION_POSTGRES_DSN: str = ""
    AI_CALL_COLLECTION_POSTGRES_TIMEOUT_SECONDS: float = 2.0

    WEB_AUDIO_ECHO_CANCELLATION: bool = True
    WEB_AUDIO_NOISE_SUPPRESSION: bool = True
    WEB_AUDIO_AUTO_GAIN_CONTROL: bool = True

    ASR_PROVIDER: str = ""
    ASR_MODEL: str = ""
    ASR_API_KEY: str = ""

    LLM_PROVIDER: str = ""
    LLM_BASE_URL: str = ""
    LLM_API_KEY: str = ""
    LLM_MODEL: str = ""

    TTS_PROVIDER: str = ""
    TTS_MODEL: str = ""
    TTS_API_KEY: str = ""
    TTS_VOICE: str = ""

    POST_ANALYSIS_MODEL: str = ""

    # ================================================= #
    # ******************** 验证码配置 ******************* #
    # ================================================= #
    CAPTCHA_ENABLE: bool = True  # 是否启用验证码
    CAPTCHA_EXPIRE_SECONDS: int = 60 * 1  # 验证码过期时间(秒) 1分钟
    CAPTCHA_FONT_SIZE: int = 40  # 字体大小
    CAPTCHA_FONT_PATH: str = "static/assets/font/Arial.ttf"  # 字体路径

    # ================================================= #
    # ********************* 日志配置 ******************* #
    # ================================================= #
    OPERATION_LOG_RECORD: bool = True  # 是否记录操作日志
    IGNORE_OPERATION_FUNCTION: list[str] = ["get_captcha_for_login"]  # 忽略记录的函数
    OPERATION_RECORD_METHOD: list[str] = [
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "HEAD",
        "OPTIONS",
    ]  # 需要记录的请求方法

    # ================================================= #
    # ******************* Gzip压缩配置 ******************* #
    # ================================================= #
    GZIP_ENABLE: bool = True  # 是否启用Gzip
    GZIP_MIN_SIZE: int = 1000  # 最小压缩大小(字节)
    GZIP_COMPRESS_LEVEL: int = 9  # 压缩级别(1-9)

    # ================================================= #
    # ***************** 静态文件配置 ***************** #
    # ================================================= #
    STATIC_ENABLE: bool = True  # 是否启用静态文件
    STATIC_URL: str = "/static"  # 访问路由
    STATIC_DIR: str = "static"  # 目录名
    STATIC_ROOT: Path = BASE_DIR.joinpath(STATIC_DIR)  # 绝对路径

    # ================================================= #
    # ***************** 动态文件配置 ***************** #
    # ================================================= #
    UPLOAD_FILE_PATH: Path = Path("static/upload")  # 上传目录
    UPLOAD_MACHINE: str = "A"  # 上传机器标识
    ALLOWED_EXTENSIONS: list[str] = [  # 允许的文件类型
        ".gif",
        ".jpg",
        ".jpeg",
        ".png",
        ".ico",
        ".svg",
        ".xls",
        ".xlsx",
        ".wav",
        ".mp3",
    ]
    MAX_FILE_SIZE: int = 10 * 1024 * 1024  # 最大文件大小(10MB)

    # ================================================= #
    # ***************** Swagger配置 ***************** #
    # ================================================= #
    SWAGGER_CSS_URL: str = "static/swagger/swagger-ui/swagger-ui.css"
    SWAGGER_JS_URL: str = "static/swagger/swagger-ui/swagger-ui-bundle.js"
    FAVICON_URL: str = "static/swagger/favicon.png"

    # ================================================= #
    # ******************* 重构配置 ******************* #
    # ================================================= #
    @property
    def MIDDLEWARE_LIST(self) -> list[str | None]:
        """获取项目根目录"""
        # 中间件列表
        MIDDLEWARES: list[str | None] = [
            "app.core.middlewares.CustomCORSMiddleware" if self.CORS_ORIGIN_ENABLE else None,
            "app.core.middlewares.RequestLogMiddleware" if self.OPERATION_LOG_RECORD else None,
            "app.core.middlewares.CustomGZipMiddleware" if self.GZIP_ENABLE else None,
        ]
        return MIDDLEWARES

    @property
    def EVENT_LIST(self) -> list[str | None]:
        """获取事件列表"""
        EVENTS: list[str | None] = [
            "app.core.database.redis_connect" if self.REDIS_ENABLE else None,
        ]
        return EVENTS

    @property
    def EFFECTIVE_ASR_API_KEY(self) -> str:
        """获取 ASR 实际使用的 API key"""
        return self.ASR_API_KEY or self.DASHSCOPE_API_KEY

    @property
    def EFFECTIVE_LLM_API_KEY(self) -> str:
        """获取 LLM 实际使用的 API key"""
        return self.LLM_API_KEY or self.DASHSCOPE_API_KEY

    @property
    def EFFECTIVE_TTS_API_KEY(self) -> str:
        """获取 TTS 实际使用的 API key"""
        return self.TTS_API_KEY or self.DASHSCOPE_API_KEY

    @property
    def EFFECTIVE_POST_ANALYSIS_API_KEY(self) -> str:
        """获取通话后语义分析实际使用的 API key"""
        return self.DASHSCOPE_API_KEY

    @property
    def ASYNC_DB_URI(self) -> str:
        """获取异步数据库连接"""
        if self.DATABASE_TYPE not in ("mysql", "postgres", "sqlite"):
            raise ValueError(
                f"数据库驱动不支持: {self.DATABASE_TYPE}, 异步数据库请选择 mysql、postgres、sqlite"
            )
        db_connect: str = ""
        if self.DATABASE_TYPE == "mysql":
            db_connect = f"mysql+asyncmy://{self.DATABASE_USER}:{quote_plus(self.DATABASE_PASSWORD)}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}?charset=utf8mb4"
        elif self.DATABASE_TYPE == "postgres":
            db_connect = f"postgresql+asyncpg://{self.DATABASE_USER}:{quote_plus(self.DATABASE_PASSWORD)}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}"
        else:
            db_connect = f"sqlite+aiosqlite:///{self.DATABASE_NAME}.db"
        return db_connect

    @property
    def DB_URI(self) -> str:
        """获取同步数据库连接"""
        if self.DATABASE_TYPE not in ("mysql", "postgres", "sqlite"):
            raise ValueError(
                f"数据库驱动不支持: {self.DATABASE_TYPE}, 同步数据库请选择 mysql、postgres、sqlite"
            )
        db_connect: str = ""
        if self.DATABASE_TYPE == "mysql":
            db_connect = f"mysql+pymysql://{self.DATABASE_USER}:{quote_plus(self.DATABASE_PASSWORD)}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}?charset=utf8mb4"
        elif self.DATABASE_TYPE == "postgres":
            db_connect = f"postgresql+psycopg://{self.DATABASE_USER}:{quote_plus(self.DATABASE_PASSWORD)}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}"
        else:
            db_connect = f"sqlite:///{self.DATABASE_NAME}.db"
        return db_connect

    @property
    def REDIS_URI(self) -> str:
        """获取Redis连接"""
        return f"redis://{self.REDIS_USER}:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB_NAME}"

    @property
    def FASTAPI_CONFIG(self) -> dict[str, Any]:
        """获取FastAPI应用属性"""
        return {
            "debug": self.DEBUG,
            "title": self.TITLE,
            "version": self.VERSION,
            "description": self.DESCRIPTION,
            "summary": self.SUMMARY,
            "docs_url": None,
            "redoc_url": None,
            "root_path": "" if self.AI_CALL_STANDALONE_ENABLE else self.ROOT_PATH,
            "responses": {
                200: {"description": "成功"},
                400: {"description": "请求参数错误"},
                401: {"description": "未认证"},
                403: {"description": "未授权"},
                404: {"description": "资源不存在"},
                422: {"description": "请求参数验证错误"},
                500: {"description": "服务器内部错误"},
            },
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """获取配置实例"""
    return Settings()


settings = get_settings()

if "AI_CALL_HANDOFF_TIMEOUT_SECONDS" in os.environ:
    warnings.warn(
        "AI_CALL_HANDOFF_TIMEOUT_SECONDS is deprecated; use "
        "AI_CALL_HANDOFF_TOTAL_WAIT_SECONDS instead",
        DeprecationWarning,
        stacklevel=2,
    )
