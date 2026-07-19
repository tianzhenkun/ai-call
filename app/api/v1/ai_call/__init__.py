from .agent_console_controller import AgentAdminRouter, AgentConsoleRouter
from .controller import AiCallRouter

AiCallRouter.include_router(AgentConsoleRouter)
AiCallRouter.include_router(AgentAdminRouter)

__all__ = ["AiCallRouter"]
