from datetime import UTC, datetime

from observability_agent.graph.nodes.verify_evidence import verify_model_diagnosis
from observability_agent.schemas.context import IncidentContext, LogEvidence
from observability_agent.schemas.report import EvidenceReference, ModelDiagnosis


def context_with_one_log() -> IncidentContext:
    return IncidentContext(
        incident_id="incident-1",
        title="Database timeout",
        service="order-service",
        environment="test",
        severity="P2",
        status="OPEN",
        fingerprint="fp-1",
        logs=[
            LogEvidence(
                id="log-1",
                timestamp=datetime.now(UTC),
                level="ERROR",
                message="connection timeout",
                fingerprint="fp-1",
            )
        ],
    )


def test_unknown_log_references_are_removed() -> None:
    state = {
        "context": context_with_one_log(),
        "model_diagnosis": ModelDiagnosis(
            root_cause="database unavailable",
            confidence=0.9,
            evidence=[EvidenceReference(log_id="made-up", reason="not real")],
            recommendations=["check database"],
        ),
    }

    verified = verify_model_diagnosis(state)  # type: ignore[arg-type]

    assert verified.root_cause == "证据不足，无法确定根因"
    assert verified.confidence == 0.3
    assert verified.evidence == []


def test_real_log_reference_is_kept() -> None:
    state = {
        "context": context_with_one_log(),
        "model_diagnosis": ModelDiagnosis(
            root_cause="database unavailable",
            confidence=0.8,
            evidence=[EvidenceReference(log_id="log-1", reason="connection timeout")],
            recommendations=[],
        ),
    }

    verified = verify_model_diagnosis(state)  # type: ignore[arg-type]

    assert verified.root_cause == "database unavailable"
    assert verified.evidence[0].log_id == "log-1"
