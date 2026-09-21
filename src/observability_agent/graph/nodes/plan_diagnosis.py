from observability_agent.analysis.facts import build_analysis_plan
from observability_agent.graph.dependencies import GraphDependencies
from observability_agent.graph.state import DiagnosisState
from observability_agent.schemas.events import DiagnosisStep


def plan_diagnosis(state: DiagnosisState, dependencies: GraphDependencies) -> dict:
    request = state["request"]
    context = state["context"]
    plan = build_analysis_plan(context)
    dependencies.emit_progress(
        request,
        DiagnosisStep.PLANNING,
        35,
        "已生成日志诊断计划",
    )
    return {"analysis_plan": plan}
