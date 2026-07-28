from .controller import OutboundValidationRouter
from .rule_task_controller import OutboundRuleTaskRouter

OutboundValidationRouter.include_router(OutboundRuleTaskRouter)

__all__ = ["OutboundValidationRouter", "OutboundRuleTaskRouter"]
