from __future__ import annotations

from enum import StrEnum


class ProcessRole(StrEnum):
    API = "api"
    RUNTIME = "runtime"
    DISPATCHER = "dispatcher"
    OUTBOUND = "outbound"
    JOBS = "jobs"
    LEGACY_RUNTIME = "legacy_runtime"


class OwnerCommandEntry(StrEnum):
    WEB = "web"
    DIRECT_SIP = "direct_sip"
    OUTBOUND = "outbound"


class CommandStatus(StrEnum):
    PENDING = "PENDING"
    DISPATCHING = "DISPATCHING"
    PUBLISHED = "PUBLISHED"
    PROCESSING = "PROCESSING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    DEAD = "DEAD"
    SUPERSEDED = "SUPERSEDED"
    CANCELED = "CANCELED"


class EffectStatus(StrEnum):
    PENDING = "PENDING"
    APPLYING = "APPLYING"
    APPLIED = "APPLIED"
    RECONCILE_REQUIRED = "RECONCILE_REQUIRED"
    FAILED = "FAILED"
