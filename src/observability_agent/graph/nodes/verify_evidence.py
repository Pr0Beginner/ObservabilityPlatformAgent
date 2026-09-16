from observability_agent.graph.dependencies import GraphDependencies
from observability_agent.graph.state import DiagnosisState
from observability_agent.schemas.events import DiagnosisStep
from observability_agent.schemas.report import ModelDiagnosis


def verify_model_diagnosis(state: DiagnosisState) -> ModelDiagnosis:
    context = state["context"]
    diagnosis = state["model_diagnosis"]
    valid_log_ids = {log.id for log in context.logs}
    seen: set[str] = set()
    valid_evidence = []
    for evidence in diagnosis.evidence:
        if evidence.log_id in valid_log_ids and evidence.log_id not in seen:
            valid_evidence.append(evidence)
            seen.add(evidence.log_id)

    if not valid_evidence:
        return ModelDiagnosis(
            root_cause="证据不足，无法确定根因",
            confidence=min(diagnosis.confidence, 0.3),
            evidence=[],
            recommendations=diagnosis.recommendations,
        )

    return ModelDiagnosis(
        root_cause=diagnosis.root_cause,
        confidence=diagnosis.confidence,
        evidence=valid_evidence,
        recommendations=diagnosis.recommendations,
    )


def verify_evidence(state: DiagnosisState, dependencies: GraphDependencies) -> dict:
    request = state["request"]
    dependencies.emit_progress(
        request,
        DiagnosisStep.VERIFYING_EVIDENCE,
        75,
        "正在校验日志证据",
    )
    return {"verified_diagnosis": verify_model_diagnosis(state)}
