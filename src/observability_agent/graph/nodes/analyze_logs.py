from observability_agent.graph.dependencies import GraphDependencies
from observability_agent.graph.state import DiagnosisState
from observability_agent.schemas.events import DiagnosisStep


def analyze_logs(state: DiagnosisState, dependencies: GraphDependencies) -> dict:
    request = state["request"]
    dependencies.emit_progress(
        request,
        DiagnosisStep.ANALYZING_LOGS,
        55,
        f"正在分析 {len(state['context'].logs)} 条相关日志",
    )
    diagnosis = dependencies.analyzer.analyze(state["context"], state["analysis_plan"])
    return {"model_diagnosis": diagnosis}
