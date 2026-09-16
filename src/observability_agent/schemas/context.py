from datetime import datetime

from observability_agent.schemas.base import CamelModel


class LogEvidence(CamelModel):
    id: str
    timestamp: datetime
    level: str
    trace_id: str | None = None
    message: str
    fingerprint: str


class IncidentContext(CamelModel):
    incident_id: str
    title: str
    service: str
    environment: str
    severity: str
    status: str
    fingerprint: str
    logs: list[LogEvidence]
