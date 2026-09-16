from observability_agent.graph.dependencies import GraphDependencies
from observability_agent.graph.state import DiagnosisState
from observability_agent.schemas.events import DiagnosisStep
from observability_agent.schemas.report import DiagnosisReport


def build_report(state: DiagnosisState, dependencies: GraphDependencies) -> dict:
    request = state["request"]
    dependencies.emit_progress(
        request,
        DiagnosisStep.BUILDING_REPORT,
        90,
        "正在生成结构化诊断报告",
    )
    diagnosis = state["verified_diagnosis"]
    report = DiagnosisReport(
        root_cause=diagnosis.root_cause,
        confidence=diagnosis.confidence,
        evidence=diagnosis.evidence,
        recommendations=diagnosis.recommendations,
        tool_calls=state["tool_calls"],
    )
    return {"report": report}
