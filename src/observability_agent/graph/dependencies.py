from collections.abc import Callable
from dataclasses import dataclass

from observability_agent.clients.deepseek_client import DiagnosisAnalyzer
from observability_agent.schemas.events import DiagnosisRequestedEvent, DiagnosisStep
from observability_agent.tools.get_incident_context import GetIncidentContextTool

ProgressCallback = Callable[[DiagnosisRequestedEvent, DiagnosisStep, int, str], None]


@dataclass(frozen=True)
class GraphDependencies:
    context_tool: GetIncidentContextTool
    analyzer: DiagnosisAnalyzer
    emit_progress: ProgressCallback
