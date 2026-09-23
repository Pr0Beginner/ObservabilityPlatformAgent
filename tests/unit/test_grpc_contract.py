from datetime import UTC, datetime

from observability_agent.clients.java_grpc_client import JavaGrpcClient
from observability_agent.generated import incident_context_pb2


def test_python_descriptor_matches_java_proto() -> None:
    incident_fields = {
        field.name: field.number
        for field in incident_context_pb2.IncidentContextResponse.DESCRIPTOR.fields
    }
    log_fields = {
        field.name: field.number for field in incident_context_pb2.LogEvidence.DESCRIPTOR.fields
    }
    assert incident_fields == {
        "incident_id": 1,
        "title": 2,
        "service": 3,
        "environment": 4,
        "severity": 5,
        "status": 6,
        "fingerprint": 7,
        "logs": 8,
        "related_trace_ids": 9,
        "incident_type": 10,
        "operation": 11,
        "dimension": 12,
        "current_value": 13,
        "baseline_value": 14,
    }
    assert log_fields == {
        "id": 1,
        "timestamp": 2,
        "level": 3,
        "trace_id": 4,
        "message": 5,
        "fingerprint": 6,
        "span_id": 7,
        "parent_span_id": 8,
        "request_id": 9,
        "operation": 10,
        "span_kind": 11,
        "status_code": 12,
        "success": 13,
        "error_code": 14,
        "duration_ms": 15,
    }


class FakeStub:
    def GetIncidentContext(self, request, timeout):  # noqa: N802, ANN001
        assert request.incident_id == "incident-1"
        return incident_context_pb2.IncidentContextResponse(
            incident_id="incident-1",
            title="failure spike",
            service="orders",
            environment="test",
            severity="P1",
            status="OPEN",
            fingerprint="fp",
            related_trace_ids=["trace-1"],
            incident_type="FAILURE_RATE_SPIKE",
            operation="POST /orders",
            dimension="service",
            current_value=0.0,
            baseline_value=0.01,
            logs=[
                incident_context_pb2.LogEvidence(
                    id="log-1",
                    timestamp=datetime.now(UTC).isoformat(),
                    level="ERROR",
                    trace_id="trace-1",
                    message="downstream failed",
                    fingerprint="fp",
                    span_id="span-2",
                    parent_span_id="span-1",
                    request_id="req-1",
                    operation="reserve",
                    span_kind="CLIENT",
                    status_code=0,
                    success=False,
                    error_code="TIMEOUT",
                    duration_ms=0,
                )
            ],
        )


def test_grpc_mapping_preserves_optional_zero_and_false() -> None:
    client = JavaGrpcClient.__new__(JavaGrpcClient)
    client._stub = FakeStub()  # type: ignore[attr-defined]
    client._timeout_seconds = 1  # type: ignore[attr-defined]

    context = client.get_incident_context("incident-1", 100)

    assert context.current_value == 0.0
    assert context.logs[0].status_code == 0
    assert context.logs[0].success is False
    assert context.logs[0].duration_ms == 0
    assert context.logs[0].parent_span_id == "span-1"
