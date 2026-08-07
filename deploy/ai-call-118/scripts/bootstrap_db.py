import asyncio
import os

from sqlalchemy import select

# Register every table needed by the independent AI Call database before create_all.
from app.api.v1.ai_call import model as _ai_call_model  # noqa: F401
from app.api.v1.ai_call.outbound import model as _outbound_model  # noqa: F401
from app.api.v1.ai_call.outbound import rule_task_model as _rule_task_model  # noqa: F401
from app.api.v1.ai_call.outbound import sip_line_model as _sip_line_model  # noqa: F401
from app.api.v1.ai_call.voice import model as _voice_model  # noqa: F401
from app.api.v1.system.dict import model as _dict_model  # noqa: F401
from app.api.v1.system.oss import model as _oss_model  # noqa: F401
from app.api.v1.system.oss_config.model import OssConfigModel
from app.api.v1.system.user import model as _user_model  # noqa: F401
from app.core.database import async_db_session, create_tables
from app.services.ai_call.runtime_control import models as _runtime_models  # noqa: F401
from app.utils.id_util import generate_snowflake_id


def required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or value.startswith("REPLACE_WITH_"):
        raise RuntimeError(f"{name} must be configured before startup")
    return value


async def seed_oss_config() -> None:
    values = {
        "endpoint": required("AI_CALL_OSS_ENDPOINT"),
        "access_key": required("AI_CALL_OSS_ACCESS_KEY"),
        "secret_key": required("AI_CALL_OSS_SECRET_KEY"),
        "bucket_name": required("AI_CALL_OSS_BUCKET"),
        "region": os.getenv("AI_CALL_OSS_REGION", "ap-shanghai").strip(),
        "prefix": os.getenv("AI_CALL_OSS_PREFIX", "ai-call").strip(),
        "is_https": "Y" if os.getenv("AI_CALL_OSS_HTTPS", "true").lower() == "true" else "N",
        "config_key": "ai-call-oss",
        "status": "0",
        "tenant_id": "000000",
        "access_policy": "1",
    }
    async with async_db_session() as session:
        active = await session.scalar(
            select(OssConfigModel).where(OssConfigModel.config_key == "ai-call-oss")
        )
        if active is None:
            active = OssConfigModel(oss_config_id=generate_snowflake_id(), **values)
            session.add(active)
        else:
            for key, value in values.items():
                setattr(active, key, value)
        await session.commit()


async def main() -> None:
    await create_tables()
    await seed_oss_config()


asyncio.run(main())
