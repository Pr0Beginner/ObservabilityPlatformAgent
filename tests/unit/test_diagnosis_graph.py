from datetime import UTC, datetime

from observability_agent.graph.builder import build_diagnosis_graph
from observability_agent.graph.dependencies import GraphDependencies
from observability_agent.schemas.context import IncidentContext, LogEvidence
from observability_agent.schemas.events import DiagnosisRequestedEvent
from observability_agent.schemas.report import EvidenceReference, ModelDiagnosis
from observability_agent.tools.get_incident_context import GetIncidentContextTool


class FakeContextClient:
    def get_incident_context(self, incident_id: str, log_limit: int) -> IncidentContext:
        return IncidentContext(
            incident_id=incident_id,
            title="Database timeout",
            service="order-service",
            environment="test",
            severity="P2",
            status="OPEN",
            fingerprint="fp-1",
            logs=[
                LogEvidence(
                    id="log-1",
                    timestamp=datetime.now(UTC),
                    level="ERROR",
                    trace_id="trace-1",
                    message="connection acquisition timeout",
                    fingerprint="fp-1",
                )
            ],
        )


class FakeAnalyzer:
    def analyze(self, context: IncidentContext, analysis_plan: list[str]) -> ModelDiagnosis:
        assert analysis_plan
        return ModelDiagnosis(
            root_cause="数据库连接池耗尽",
            confidence=0.85,
            evidence=[EvidenceReference(log_id="log-1", reason="连接获取超时")],
            recommendations=["检查连接泄漏"],
        )


def test_graph_builds_grounded_report() -> None:
    progress = []
    dependencies = GraphDependencies(
        context_tool=GetIncidentContextTool(FakeContextClient(), 100),
        analyzer=FakeAnalyzer(),
        emit_progress=lambda request, step, percent, message: progress.append(
            (step, percent, message)
        ),
    )
    graph = build_diagnosis_graph(dependencies)
    request = DiagnosisRequestedEvent(
        event_id="evt-1",
        task_id="task-1",
        incident_id="incident-1",
        version=1,
        requested_at=datetime.now(UTC),
    )

    result = graph.invoke({"request": request, "tool_calls": [], "errors": []})

    assert result["report"].root_cause == "数据库连接池耗尽"
    assert result["report"].evidence[0].log_id == "log-1"
    assert result["tool_calls"] == ["get_incident_context(incidentId=incident-1, logLimit=100)"]
    assert len(progress) == 6
