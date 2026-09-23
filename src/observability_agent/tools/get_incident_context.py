from typing import Protocol

from observability_agent.schemas.context import IncidentContext


class IncidentContextClient(Protocol):
    def get_incident_context(self, incident_id: str, log_limit: int) -> IncidentContext: ...


class GetIncidentContextTool:
    name = "get_incident_context"

    def __init__(self, client: IncidentContextClient, log_limit: int) -> None:
        self._client = client
        self._log_limit = log_limit

    def invoke(self, incident_id: str) -> IncidentContext:
        return self._client.get_incident_context(incident_id, self._log_limit)

    def describe_call(self, incident_id: str) -> str:
        return f"{self.name}(incidentId={incident_id}, logLimit={self._log_limit})"
