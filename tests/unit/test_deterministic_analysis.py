from datetime import UTC, datetime, timedelta

from observability_agent.analysis.facts import build_analysis_plan, find_first_failure
from observability_agent.schemas.context import IncidentContext, LogEvidence


def test_failure_strategy_selects_earliest_root_failure_span() -> None:
    now = datetime.now(UTC)
    downstream = LogEvidence(
        id="downstream",
        timestamp=now,
        level="ERROR",
        trace_id="trace",
        message="db timeout",
        fingerprint="fp",
        span_id="child",
        parent_span_id="root",
        operation="SELECT",
        success=False,
        error_code="DB_TIMEOUT",
    )
    propagated = LogEvidence(
        id="upstream",
        timestamp=now + timedelta(milliseconds=10),
        level="ERROR",
        trace_id="trace",
        message="request failed",
        fingerprint="fp",
        span_id="root",
        operation="POST /orders",
        success=False,
        status_code=500,
    )
    context = IncidentContext(
        incident_id="i",
        title="failure spike",
        service="orders",
        environment="test",
        severity="P1",
        status="OPEN",
        fingerprint="fp",
        logs=[propagated, downstream],
        incident_type="FAILURE_RATE_SPIKE",
        current_value=0.5,
        baseline_value=0.01,
    )

    assert find_first_failure(context.logs).id == "downstream"  # type: ignore[union-attr]
    plan = build_analysis_plan(context)
    assert "最早失败 Span" in plan[0]
    assert any("logId=downstream" in item for item in plan)
