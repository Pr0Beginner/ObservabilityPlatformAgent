from observability_agent.graph.dependencies import GraphDependencies
from observability_agent.graph.state import DiagnosisState
from observability_agent.schemas.events import DiagnosisStep


def validate_task(state: DiagnosisState, dependencies: GraphDependencies) -> dict:
    request = state["request"]
    dependencies.emit_progress(request, DiagnosisStep.RECEIVED, 5, "已接收诊断任务")
    return {"errors": [], "tool_calls": []}
