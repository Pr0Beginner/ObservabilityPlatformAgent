from pydantic import Field

from observability_agent.schemas.base import CamelModel


class EvidenceReference(CamelModel):
    log_id: str = Field(min_length=1)
    reason: str = Field(min_length=1, max_length=500)


class ModelDiagnosis(CamelModel):
    root_cause: str = Field(min_length=1, max_length=1000)
    confidence: float = Field(ge=0, le=1)
    evidence: list[EvidenceReference] = Field(default_factory=list, max_length=20)
    recommendations: list[str] = Field(default_factory=list, max_length=10)


class DiagnosisReport(CamelModel):
    root_cause: str
    confidence: float = Field(ge=0, le=1)
    evidence: list[EvidenceReference]
    recommendations: list[str]
    tool_calls: list[str]
