from observability_agent.graph.dependencies import GraphDependencies
from observability_agent.graph.state import DiagnosisState
from observability_agent.schemas.events import DiagnosisStep
from observability_agent.schemas.report import ModelDiagnosis


def insufficient_evidence(state: DiagnosisState, dependencies: GraphDependencies) -> dict:
    dependencies.emit_progress(
        state["request"],
        DiagnosisStep.VERIFYING_EVIDENCE,
        75,
        "未查询到可用于诊断的日志",
    )
    return {
        "verified_diagnosis": ModelDiagnosis(
            root_cause="证据不足，无法确定根因",
            confidence=0,
            evidence=[],
            recommendations=["确认故障时间范围内的日志已经完成采集和索引"],
        )
    }
