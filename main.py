import os
from typing import Annotated

import typer
import uvicorn
from fastapi import FastAPI

from app.common.enums import EnvironmentEnum

fastapiadmin_cli = typer.Typer()


def create_app() -> FastAPI:
    """创建 FastAPI 应用实例"""
    from app.config.setting import settings
    from app.plugin.init_app import (
        lifespan,
        register_exceptions,
        register_files,
        register_middlewares,
        register_routers,
        reset_api_docs,
    )

    # 创建FastAPI应用
    app = FastAPI(**settings.FASTAPI_CONFIG, lifespan=lifespan)

    from app.core.logger import setup_logging

    # 初始化日志
    setup_logging()
    # 注册各种组件
    register_exceptions(app)
    # 注册中间件
    register_middlewares(app)
    # 注册路由
    register_routers(app)
    # 注册静态文件
    register_files(app)
    # 重设API文档
    reset_api_docs(app)

    return app


# typer.Option是非必填；typer.Argument是必填
@fastapiadmin_cli.command(
    name="run",
    help="启动 LingChen AI Call 服务，运行 python main.py --env=local/dev/prod，不加参数默认 dev 环境",
)
def run(
    env: Annotated[
        EnvironmentEnum, typer.Option("--env", help="运行环境 (local, dev, prod)")
    ] = EnvironmentEnum.DEV,
) -> None:
    """启动FastAPI服务"""

    try:
        # 设置环境变量
        os.environ["ENVIRONMENT"] = env.value
        typer.echo("项目启动中...")

        # 清除配置缓存，确保重新加载配置
        from app.config.setting import get_settings

        get_settings.cache_clear()
        settings = get_settings()

        from app.core.logger import setup_logging

        setup_logging()

        # 显示启动横幅
        from app.utils.banner import worship

        worship(env.value)

        # 启动uvicorn服务
        uvicorn.run(
            app="main:create_app",
            host=settings.SERVER_HOST,
            port=settings.SERVER_PORT,
            reload=env in {EnvironmentEnum.LOCAL, EnvironmentEnum.DEV},
            factory=True,
            log_config=None,
        )
    except Exception:
        raise
    finally:
        from app.core.logger import cleanup_logging

        cleanup_logging()


if __name__ == "__main__":
    fastapiadmin_cli()
