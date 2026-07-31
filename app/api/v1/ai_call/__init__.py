from .agent_console_controller import AgentAdminRouter, AgentConsoleRouter
from .controller import AiCallRouter
from .outbound import OutboundValidationRouter
from .statistics_controller import OutboundStatisticsRouter
from .voice import VoiceRouter

AiCallRouter.include_router(AgentConsoleRouter)
AiCallRouter.include_router(AgentAdminRouter)
AiCallRouter.include_router(OutboundValidationRouter)
AiCallRouter.include_router(OutboundStatisticsRouter)
AiCallRouter.include_router(VoiceRouter)

__all__ = ["AiCallRouter"]
