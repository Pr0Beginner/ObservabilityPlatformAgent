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
            logs=[
                LogEvidence(
                    id=log.id,
                    timestamp=datetime.fromisoformat(log.timestamp.replace("Z", "+00:00")),
                    level=log.level,
                    trace_id=log.trace_id or None,
                    message=log.message,
                    fingerprint=log.fingerprint,
                )
                for log in response.logs
            ],
        )

    def close(self) -> None:
        self._channel.close()
