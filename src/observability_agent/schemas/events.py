from datetime import UTC, datetime
from enum import StrEnum
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import ConfigDict, Field, model_validator

from observability_agent.schemas.base import CamelModel


def utc_now() -> datetime:
    return datetime.now(UTC)


class DiagnosisStep(StrEnum):
    RECEIVED = "RECEIVED"
    LOADING_CONTEXT = "LOADING_CONTEXT"
    PLANNING = "PLANNING"
    ANALYZING_LOGS = "ANALYZING_LOGS"
    VERIFYING_EVIDENCE = "VERIFYING_EVIDENCE"
    BUILDING_REPORT = "BUILDING_REPORT"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class DiagnosisRequestedEvent(CamelModel):
    # Wire contracts are forward compatible: Java may add optional v1 fields.
    model_config = ConfigDict(extra="ignore")
    event_id: str
    task_id: str = Field(min_length=1)
    incident_id: str = Field(min_length=1)
    version: int = Field(ge=1)
    requested_at: datetime

    @model_validator(mode="after")
    def require_utc_timestamp(self):
        if self.requested_at.tzinfo is None or self.requested_at.utcoffset() != UTC.utcoffset(None):
            raise ValueError("requestedAt must be a UTC timestamp")
        return self


def stable_event_id(task_id: str, version: int, event_type: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"observability:{task_id}:{version}:{event_type}"))


class DiagnosisProgressEvent(CamelModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    task_id: str
    incident_id: str
    version: int
    sequence: int = Field(ge=1)
    step: DiagnosisStep
    progress: int = Field(ge=0, le=100)
    message: str
    occurred_at: datetime = Field(default_factory=utc_now)


class DiagnosisCompletedEvent(CamelModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    task_id: str
    incident_id: str
    version: int
    root_cause: str
    confidence: float = Field(ge=0, le=1)
    evidence: list[str]
    recommendations: list[str]
    tool_calls: list[str]
    completed_at: datetime = Field(default_factory=utc_now)
    error: str | None = None

    @model_validator(mode="after")
    def validate_success_or_failure(self):
        if self.completed_at.tzinfo is None:
            raise ValueError("completedAt must include a timezone")
        if self.error is None and not self.root_cause.strip():
            raise ValueError("rootCause is required for a successful result")
        return self


class DiagnosisFailedEvent(CamelModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    task_id: str
    incident_id: str
    version: int
    error_code: str
    error_message: str
    retryable: bool
    occurred_at: datetime = Field(default_factory=utc_now)
