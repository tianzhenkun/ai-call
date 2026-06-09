from fastapi import APIRouter

from app.common.response import ResponseSchema as ResponseSchema

from .auth.controller import AuthRouter
from .dict.controller import DictRouter
from .oss.controller import OssRouter
from .user.controller import UserRouter

system_router = APIRouter(prefix="/system")

system_router.include_router(AuthRouter)
system_router.include_router(DictRouter)
system_router.include_router(OssRouter)
system_router.include_router(UserRouter)
