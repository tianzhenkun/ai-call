from .agent_console_controller import AgentAdminRouter, AgentConsoleRouter
from .controller import AiCallRouter
from .outbound import OutboundValidationRouter

AiCallRouter.include_router(AgentConsoleRouter)
AiCallRouter.include_router(AgentAdminRouter)
AiCallRouter.include_router(OutboundValidationRouter)

__all__ = ["AiCallRouter"]
