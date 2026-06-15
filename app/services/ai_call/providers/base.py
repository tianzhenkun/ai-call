from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class ProviderEvent:
    type: str
    payload: dict[str, Any] = field(default_factory=dict)
