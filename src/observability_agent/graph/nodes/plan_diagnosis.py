from observability_agent.graph.dependencies import GraphDependencies
from observability_agent.graph.state import DiagnosisState
from observability_agent.schemas.events import DiagnosisStep


def plan_diagnosis(state: DiagnosisState, dependencies: GraphDependencies) -> dict:
    request = state["request"]
    context = state["context"]
    error_count = sum(log.level in {"ERROR", "FATAL"} for log in context.logs)
    trace_count = len({log.trace_id for log in context.logs if log.trace_id})
    plan = [
        f"按时间顺序分析 {len(context.logs)} 条日志，其中 {error_count} 条为 ERROR/FATAL",
        f"检查异常指纹 {context.fingerprint} 是否与候选根因一致",
        f"结合 {trace_count} 个 Trace ID 判断错误传播关系",
        "只引用真实日志 ID，并生成可执行但不自动执行的建议",
    ]
    dependencies.emit_progress(
        request,
        DiagnosisStep.PLANNING,
        35,
        "已生成日志诊断计划",
    )
    return {"analysis_plan": plan}
