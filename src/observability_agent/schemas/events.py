from datetime import UTC, datetime
from enum import StrEnum
from uuid import uuid4

from pydantic import Field

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
    event_id: str
    task_id: str = Field(min_length=1)
    incident_id: str = Field(min_length=1)
    version: int = Field(ge=1)
    requested_at: datetime


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


class DiagnosisFailedEvent(CamelModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    task_id: str
    incident_id: str
    version: int
    error_code: str
    error_message: str
    retryable: bool
    occurred_at: datetime = Field(default_factory=utc_now)
