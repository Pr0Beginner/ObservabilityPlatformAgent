from observability_agent.graph.dependencies import GraphDependencies
from observability_agent.graph.state import DiagnosisState
from observability_agent.schemas.events import DiagnosisStep


def load_context(state: DiagnosisState, dependencies: GraphDependencies) -> dict:
    request = state["request"]
    dependencies.emit_progress(
        request,
        DiagnosisStep.LOADING_CONTEXT,
        20,
        "正在加载故障上下文和相关日志",
    )
    context = dependencies.context_tool.invoke(request.incident_id)
    return {
        "context": context,
        "tool_calls": [dependencies.context_tool.describe_call(request.incident_id)],
    }
