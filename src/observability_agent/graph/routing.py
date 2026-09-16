from typing import Literal

from observability_agent.graph.state import DiagnosisState


def route_after_context(
    state: DiagnosisState,
) -> Literal["plan_diagnosis", "insufficient_evidence"]:
    return "plan_diagnosis" if state["context"].logs else "insufficient_evidence"
