from typing import NotRequired, TypedDict

from observability_agent.schemas.context import IncidentContext
from observability_agent.schemas.events import DiagnosisRequestedEvent
from observability_agent.schemas.report import DiagnosisReport, ModelDiagnosis


class DiagnosisState(TypedDict):
    request: DiagnosisRequestedEvent
    context: NotRequired[IncidentContext]
    analysis_plan: NotRequired[list[str]]
    model_diagnosis: NotRequired[ModelDiagnosis]
    verified_diagnosis: NotRequired[ModelDiagnosis]
    report: NotRequired[DiagnosisReport]
    tool_calls: list[str]
    errors: list[str]
