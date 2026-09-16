from typing import Any, Protocol

from observability_agent.schemas.events import DiagnosisRequestedEvent, DiagnosisStep
from observability_agent.schemas.report import DiagnosisReport
from observability_agent.services.progress_reporter import KafkaProgressReporter


class CompiledDiagnosisGraph(Protocol):
    def invoke(self, input: dict[str, Any]) -> dict[str, Any]: ...


class DiagnosisRunner:
    def __init__(
        self,
        graph: CompiledDiagnosisGraph,
        progress_reporter: KafkaProgressReporter,
    ) -> None:
        self._graph = graph
        self._progress_reporter = progress_reporter

    def run(self, request: DiagnosisRequestedEvent) -> DiagnosisReport:
        try:
            result = self._graph.invoke(
                {
                    "request": request,
                    "tool_calls": [],
                    "errors": [],
                }
            )
            report = result["report"]
            if not isinstance(report, DiagnosisReport):
                report = DiagnosisReport.model_validate(report)
            self._progress_reporter.publish(
                request,
                DiagnosisStep.COMPLETED,
                100,
                "诊断报告已生成",
            )
            return report
        finally:
            self._progress_reporter.finish(request.task_id)
