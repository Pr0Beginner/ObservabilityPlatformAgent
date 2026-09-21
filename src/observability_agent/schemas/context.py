from datetime import datetime

from pydantic import Field

from observability_agent.schemas.base import CamelModel


class LogEvidence(CamelModel):
    id: str
    timestamp: datetime
    level: str
    trace_id: str | None = None
    message: str
    fingerprint: str
    span_id: str | None = None
    parent_span_id: str | None = None
    request_id: str | None = None
    operation: str | None = None
    span_kind: str | None = None
    status_code: int | None = None
    success: bool | None = None
    error_code: str | None = None
    duration_ms: int | None = None


class IncidentContext(CamelModel):
    incident_id: str
    title: str
    service: str
    environment: str
    severity: str
    status: str
    fingerprint: str
    logs: list[LogEvidence]
    related_trace_ids: list[str] = Field(default_factory=list)
    incident_type: str = ""
    operation: str | None = None
    dimension: str | None = None
    current_value: float | None = None
    baseline_value: float | None = None
