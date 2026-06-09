from app.core.logger import log


def worship(env: str) -> None:
    """输出项目启动信息。"""
    log.info(f"LingChen AI Call starting, environment={env}")
