import asyncio
import json
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.system.auth.schema import AuthSchema
from app.common.constant import RET
from app.core.exceptions import CustomException
from app.core.logger import log
from app.utils.id_util import generate_snowflake_id
from app.utils.minio_util import MinioUtil

from .crud import OssCRUD
from .schema import OssOutSchema, OssUrlOutSchema


class OssService:
    """OSS对象存储服务层"""

    _active_config: dict | None = None

    @classmethod
    async def init_active_config(cls) -> None:
        """启动时初始化活跃 OSS 配置，避免每次上传都查询数据库"""
        from sqlalchemy import text

        from app.core.database import async_db_session

        async with async_db_session() as session:
            result = await session.execute(
                text("SELECT * FROM sys_oss_config WHERE status = '0' LIMIT 1")
            )
            row = result.mappings().first()
            cls._active_config = dict(row) if row else None
        config_key = cls._active_config.get("config_key") if cls._active_config else None
        log.info(f"✅ OSS配置初始化完成: {config_key}")

    @classmethod
    async def get_by_oss_id_service(cls, auth: AuthSchema, oss_id: int) -> dict:
        """
        根据oss_id获取OSS对象完整信息

        参数:
        - auth (AuthSchema): 认证信息模型
        - oss_id (int): 对象存储主键

        返回:
        - dict: OSS对象完整信息

        异常:
        - CustomException: OSS对象不存在时抛出
        """
        oss_obj = await OssCRUD(auth).get_by_oss_id_crud(oss_id=oss_id)
        if not oss_obj:
            raise CustomException(msg=f"OSS对象不存在: {oss_id}")
        return OssOutSchema.model_validate(oss_obj).model_dump(by_alias=True)

    @classmethod
    async def get_url_by_oss_id_service(cls, auth: AuthSchema, oss_id: int) -> dict:
        """
        根据oss_id获取URL信息（简化版，常用场景）

        参数:
        - auth (AuthSchema): 认证信息模型
        - oss_id (int): 对象存储主键

        返回:
        - dict: 包含ossId、url、originalName、fileSuffix的信息

        异常:
        - CustomException: OSS对象不存在时抛出
        """
        oss_obj = await OssCRUD(auth).get_url_by_oss_id_crud(oss_id=oss_id)
        if not oss_obj:
            raise CustomException(msg=f"OSS对象不存在: {oss_id}")
        return OssUrlOutSchema.model_validate(oss_obj).model_dump(by_alias=True)

    @classmethod
    async def get_presigned_url_by_oss_id_service(
        cls,
        auth: AuthSchema,
        oss_id: int,
        *,
        expires_seconds: int = 900,
    ) -> str | None:
        """返回可短时访问的对象地址，供浏览器播放和外部任务拉取。"""
        oss_obj = await OssCRUD(auth).get_by_oss_id_crud(oss_id=oss_id)
        if not oss_obj:
            return None
        if oss_obj.get("service") != "minio":
            return str(oss_obj.get("url") or "") or None
        config = cls._active_config
        object_name = str(oss_obj.get("file_name") or "").strip()
        if not config or not object_name:
            return str(oss_obj.get("url") or "") or None
        return MinioUtil.presigned_get_url(
            config,
            object_name,
            expires_seconds=expires_seconds,
        )

    @classmethod
    async def get_list_by_oss_ids_service(cls, auth: AuthSchema, oss_ids: list[int]) -> list[dict]:
        """
        根据oss_id列表批量获取OSS对象完整信息

        参数:
        - auth (AuthSchema): 认证信息模型
        - oss_ids (list[int]): 对象存储主键列表

        返回:
        - list[dict]: OSS对象完整信息列表
        """
        if not oss_ids:
            return []
        oss_list = await OssCRUD(auth).get_list_by_oss_ids_crud(oss_ids=oss_ids)
        return [OssOutSchema.model_validate(obj).model_dump(by_alias=True) for obj in oss_list]

    @classmethod
    async def get_url_list_by_oss_ids_service(
        cls, auth: AuthSchema, oss_ids: list[int]
    ) -> list[dict]:
        """
        根据oss_id列表批量获取URL信息（简化版，常用场景）

        参数:
        - auth (AuthSchema): 认证信息模型
        - oss_ids (list[int]): 对象存储主键列表

        返回:
        - list[dict]: 包含ossId、url、originalName、fileSuffix的信息列表
        """
        if not oss_ids:
            return []
        oss_list = await OssCRUD(auth).get_url_list_by_oss_ids_crud(oss_ids=oss_ids)
        return [OssUrlOutSchema.model_validate(obj).model_dump(by_alias=True) for obj in oss_list]

    @classmethod
    async def upload_service(
        cls,
        auth: AuthSchema,
        data: bytes,
        original_filename: str,
        content_type: str = "application/octet-stream",
    ) -> int:
        """
        上传文件到 MinIO 并写入 sys_oss 记录。

        参数:
        - auth: 认证信息（提供 DB 会话和用户上下文）
        - data: 文件二进制内容
        - original_filename: 原始文件名
        - content_type: MIME 类型，默认 application/octet-stream

        返回:
        - int: 新建 sys_oss 记录的 oss_id（雪花ID）

        异常:
        - CustomException: 无可用 OSS 配置或上传失败时抛出
        """
        config = cls._active_config
        if not config:
            raise CustomException(
                msg="未找到可用的OSS配置，请检查 sys_oss_config 表中 status='0' 的记录",
                code=RET.SERVERERR.code,
            )

        url, object_name = MinioUtil.upload(config, data, original_filename, content_type)

        create_dept = getattr(auth.user, "dept_id", None) if auth.user else None
        oss = await OssCRUD(auth).create({
            "oss_id": generate_snowflake_id(),
            "file_name": object_name,
            "original_name": original_filename,
            "file_suffix": Path(original_filename).suffix.lower(),
            "url": url,
            "ext1": json.dumps({"fileSize": len(data), "contentType": content_type}),
            "create_dept": create_dept,
            "service": "minio",
        })
        return oss.oss_id

    @classmethod
    async def upload_committed_service(
        cls,
        auth: AuthSchema,
        data: bytes,
        original_filename: str,
        content_type: str = "application/octet-stream",
    ) -> int:
        """
        独立事务上传文件并写入 sys_oss。

        失败现场截图等证据需要在业务失败事务回滚后仍可访问，不能复用外层会话。
        """
        from app.core.database import async_db_session

        async with async_db_session() as db:
            detached_auth = AuthSchema(
                user=auth.user,
                check_data_scope=auth.check_data_scope,
                db=db,
            )
            oss_id = await cls.upload_service(
                auth=detached_auth,
                data=data,
                original_filename=original_filename,
                content_type=content_type,
            )
            await db.commit()
            return oss_id

    @classmethod
    async def register_existing_object_service(
        cls,
        db: AsyncSession,
        *,
        object_name: str,
        original_filename: str,
        content_type: str,
        file_size: int | None = None,
    ) -> int:
        """
        登记已由外部组件写入对象存储的文件。

        LiveKit Egress 直接写桶，业务服务不再二次下载上传，只补 sys_oss 索引。
        """
        config = cls._active_config
        if not config:
            raise CustomException(
                msg="未找到可用的OSS配置，请检查 sys_oss_config 表中 status='0' 的记录",
                code=RET.SERVERERR.code,
            )

        if not file_size or file_size <= 0:
            file_size = await cls.resolve_existing_object_size(config, object_name)

        oss_id = generate_snowflake_id()
        url = cls.build_object_url(config, object_name)
        auth = AuthSchema(user=None, check_data_scope=False, db=db)
        oss = await OssCRUD(auth).create({
            "oss_id": oss_id,
            "file_name": object_name,
            "original_name": original_filename,
            "file_suffix": Path(original_filename).suffix.lower(),
            "url": url,
            "ext1": json.dumps(
                {
                    "fileSize": file_size,
                    "contentType": content_type,
                    "registeredFrom": "livekit_egress",
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            "create_dept": None,
            "service": "minio",
        })
        return oss.oss_id

    @classmethod
    def active_config(cls) -> dict | None:
        return dict(cls._active_config) if cls._active_config else None

    @staticmethod
    def build_object_url(config: dict, object_name: str) -> str:
        return MinioUtil._build_url(config, object_name)

    @classmethod
    async def resolve_existing_object_size(cls, config: dict, object_name: str) -> int | None:
        last_error: Exception | None = None
        for attempt in range(5):
            try:
                size = await MinioUtil.head_object_size(config, object_name)
                if size is not None:
                    return size
            except Exception as exc:
                last_error = exc
            if attempt < 4:
                await asyncio.sleep(0.25 * (attempt + 1))

        if last_error is not None:
            log.warning(
                "获取已存在OSS对象大小失败: objectName={}, errorType={}, message={}",
                object_name,
                type(last_error).__name__,
                str(last_error),
            )
        return None
