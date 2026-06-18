import json

from redis.asyncio.client import Redis

from app.api.v1.system.auth.schema import AuthSchema
from app.common.enums import RedisInitKeyConfig
from app.core.database import async_db_session
from app.core.exceptions import CustomException
from app.core.logger import log
from app.core.redis_crud import RedisCURD

from .crud import DictDataCRUD
from .schema import DictDataOutSchema


class DictDataService:
    """
    字典数据管理模块服务层（简化版 - 只读查询）
    """

    @classmethod
    async def get_obj_list_service(
        cls,
        auth: AuthSchema,
        dict_type: str | None = None,
    ) -> list[dict]:
        """
        获取数据字典数据列表

        参数:
        - auth (AuthSchema): 认证信息模型
        - dict_type (str | None): 字典类型过滤

        返回:
        - list[dict]: 数据字典数据详情字典列表
        """
        obj_list = await DictDataCRUD(auth).get_obj_list_crud(
            dict_type=dict_type,
        )
        return [DictDataOutSchema.model_validate(obj).model_dump(by_alias=True) for obj in obj_list]

    @classmethod
    async def get_obj_list_by_dict_type_service(
        cls, auth: AuthSchema, dict_type: str
    ) -> list[dict]:
        """
        根据字典类型获取字典数据列表

        参数:
        - auth (AuthSchema): 认证信息模型
        - dict_type (str): 字典类型

        返回:
        - list[dict]: 数据字典数据详情字典列表
        """
        obj_list = await DictDataCRUD(auth).get_obj_list_by_dict_type_crud(dict_type=dict_type)
        return [DictDataOutSchema.model_validate(obj).model_dump(by_alias=True) for obj in obj_list]

    @classmethod
    async def get_dict_label_by_value(
        cls, auth: AuthSchema, dict_type: str, dict_value: str
    ) -> str | None:
        """
        根据字典类型和字典值获取字典标签

        参数:
        - auth (AuthSchema): 认证信息模型
        - dict_type (str): 字典类型
        - dict_value (str): 字典值

        返回:
        - str | None: 字典标签
        """
        return await DictDataCRUD(auth).get_dict_label_by_value(
            dict_type=dict_type, dict_value=dict_value
        )

    @classmethod
    async def get_dict_value_by_label(
        cls, auth: AuthSchema, dict_type: str, dict_label: str
    ) -> str | None:
        """
        根据字典类型和字典标签获取字典值

        参数:
        - auth (AuthSchema): 认证信息模型
        - dict_type (str): 字典类型
        - dict_label (str): 字典标签

        返回:
        - str | None: 字典值
        """
        return await DictDataCRUD(auth).get_dict_value_by_label(
            dict_type=dict_type, dict_label=dict_label
        )

    @classmethod
    async def batch_get_dict_labels_by_values(
        cls, auth: AuthSchema, dict_type: str, dict_values: list[str]
    ) -> dict[str, str]:
        """
        批量根据字典值获取字典标签

        参数:
        - auth (AuthSchema): 认证信息模型
        - dict_type (str): 字典类型
        - dict_values (list[str]): 字典值列表

        返回:
        - dict[str, str]: {dict_value: dict_label} 映射
        """
        return await DictDataCRUD(auth).batch_get_dict_labels_by_values(
            dict_type=dict_type, dict_values=dict_values
        )

    @classmethod
    async def batch_get_dict_values_by_labels(
        cls, auth: AuthSchema, dict_type: str, dict_labels: list[str]
    ) -> dict[str, str]:
        """
        批量根据字典标签获取字典值

        参数:
        - auth (AuthSchema): 认证信息模型
        - dict_type (str): 字典类型
        - dict_labels (list[str]): 字典标签列表

        返回:
        - dict[str, str]: {dict_label: dict_value} 映射
        """
        return await DictDataCRUD(auth).batch_get_dict_values_by_labels(
            dict_type=dict_type, dict_labels=dict_labels
        )

    @classmethod
    async def init_dict_service(cls, redis: Redis, force: bool = False) -> None:
        """
        应用初始化: 获取所有字典类型对应的字典数据信息并缓存

        参数:
        - redis (Redis): Redis客户端
        - force (bool): 是否强制刷新缓存，默认 False

        返回:
        - None
        """
        try:
            if not force:
                existing_keys = await RedisCURD(redis).get_keys(
                    f"{RedisInitKeyConfig.SYSTEM_DICT.key}:*"
                )
                if existing_keys:
                    log.info(
                        f"✅ Redis 字典缓存已存在，跳过初始化（共 {len(existing_keys)} 个类型）"
                    )
                    return

            async with async_db_session() as session:
                async with session.begin():
                    auth = AuthSchema(db=session, check_data_scope=False)
                    crud = DictDataCRUD(auth)
                    obj_list = await crud.get_obj_list_crud()

                    if not obj_list:
                        log.warning("未找到任何字典数据")
                        return

                    dict_type_map: dict[str, list] = {}
                    for obj in obj_list:
                        dict_type = obj.get("dict_type")
                        if dict_type not in dict_type_map:
                            dict_type_map[dict_type] = []
                        dict_type_map[dict_type].append(
                            DictDataOutSchema.model_validate(obj).model_dump(by_alias=True)
                        )

                    for dict_type, data_list in dict_type_map.items():
                        try:
                            redis_key = f"{RedisInitKeyConfig.SYSTEM_DICT.key}:{dict_type}"
                            value = json.dumps(data_list, ensure_ascii=False)
                            await RedisCURD(redis).set(key=redis_key, value=value)
                        except Exception as e:
                            log.error(f"❌ 初始化字典数据失败 [{dict_type}]: {e}")

                    log.info(f"✅ 字典数据初始化完成，共 {len(dict_type_map)} 个类型")

        except Exception as e:
            log.error(f"字典初始化过程发生错误: {e}")
            raise CustomException(msg=f"字典数据初始化失败: {e!s}")

    @classmethod
    async def get_init_dict_service(cls, redis: Redis, dict_type: str) -> list[dict]:
        """
        从缓存获取字典数据列表信息

        参数:
        - redis (Redis): Redis客户端
        - dict_type (str): 字典类型

        返回:
        - list[dict]: 字典数据列表
        """
        try:
            redis_key = f"{RedisInitKeyConfig.SYSTEM_DICT.key}:{dict_type}"
            obj_list_dict = await RedisCURD(redis).get(redis_key)

            if obj_list_dict:
                if isinstance(obj_list_dict, str):
                    try:
                        return json.loads(obj_list_dict)
                    except json.JSONDecodeError:
                        log.warning(f"字典数据反序列化失败，尝试重新初始化缓存: {dict_type}")
                elif isinstance(obj_list_dict, list):
                    return obj_list_dict

            await cls.init_dict_service(redis)
            obj_list_dict = await RedisCURD(redis).get(redis_key)
            if not obj_list_dict:
                return []

            if isinstance(obj_list_dict, str):
                try:
                    return json.loads(obj_list_dict)
                except json.JSONDecodeError:
                    return []
            return obj_list_dict
        except CustomException:
            raise
        except Exception as e:
            log.error(f"获取字典缓存失败: {e!s}")
            raise CustomException(msg=f"获取字典数据失败: {e!s}")
