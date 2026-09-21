from datetime import datetime

import grpc

from observability_agent.generated import incident_context_pb2, incident_context_pb2_grpc
from observability_agent.schemas.context import IncidentContext, LogEvidence


class JavaGrpcClient:
    def __init__(self, target: str, timeout_seconds: float) -> None:
        self._channel = grpc.insecure_channel(target)
        self._stub = incident_context_pb2_grpc.IncidentContextServiceStub(self._channel)
        self._timeout_seconds = timeout_seconds

    def get_incident_context(self, incident_id: str, log_limit: int) -> IncidentContext:
        request = incident_context_pb2.IncidentContextRequest(
            incident_id=incident_id,
            log_limit=log_limit,
        )
        response = self._stub.GetIncidentContext(request, timeout=self._timeout_seconds)
        return IncidentContext(
            incident_id=response.incident_id,
            title=response.title,
            service=response.service,
            environment=response.environment,
            severity=response.severity,
            status=response.status,
            fingerprint=response.fingerprint,
            related_trace_ids=list(response.related_trace_ids),
            incident_type=response.incident_type,
            operation=response.operation or None,
            dimension=response.dimension or None,
            current_value=(response.current_value if response.HasField("current_value") else None),
            baseline_value=(
                response.baseline_value if response.HasField("baseline_value") else None
            ),
            logs=[
                LogEvidence(
                    id=log.id,
                    timestamp=datetime.fromisoformat(log.timestamp.replace("Z", "+00:00")),
                    level=log.level,
                    trace_id=log.trace_id or None,
                    message=log.message,
                    fingerprint=log.fingerprint,
                    span_id=log.span_id or None,
                    parent_span_id=log.parent_span_id or None,
                    request_id=log.request_id or None,
                    operation=log.operation or None,
                    span_kind=log.span_kind or None,
                    status_code=(log.status_code if log.HasField("status_code") else None),
                    success=log.success if log.HasField("success") else None,
                    error_code=log.error_code or None,
                    duration_ms=(log.duration_ms if log.HasField("duration_ms") else None),
                )
                for log in response.logs
            ],
        )

    def close(self) -> None:
        self._channel.close()
