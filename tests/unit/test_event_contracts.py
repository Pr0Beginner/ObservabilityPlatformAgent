import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from observability_agent.schemas.events import DiagnosisCompletedEvent, DiagnosisRequestedEvent


def test_java_camel_case_event_is_accepted() -> None:
    event = DiagnosisRequestedEvent.model_validate(
        {
            "eventId": "evt-1",
            "taskId": "task-1",
            "incidentId": "incident-1",
            "version": 1,
            "requestedAt": "2026-09-16T10:00:00Z",
        }
    )

    assert event.task_id == "task-1"
    assert event.model_dump(by_alias=True)["incidentId"] == "incident-1"


def test_request_allows_future_fields_but_requires_utc() -> None:
    data = {
        "eventId": "evt-1",
        "taskId": "task-1",
        "incidentId": "incident-1",
        "version": 1,
        "requestedAt": "2026-09-16T10:00:00Z",
        "futureField": True,
    }
    assert DiagnosisRequestedEvent.model_validate(data).task_id == "task-1"
    data["requestedAt"] = "2026-09-16T10:00:00+08:00"
    with pytest.raises(ValidationError):
        DiagnosisRequestedEvent.model_validate(data)


def test_completed_event_matches_java_field_contract() -> None:
    event = DiagnosisCompletedEvent(
        event_id="evt",
        task_id="task",
        incident_id="incident",
        version=1,
        root_cause="root",
        confidence=0.8,
        evidence=[],
        recommendations=[],
        tool_calls=[],
        completed_at=datetime.now(UTC),
        error=None,
    )
    payload = json.loads(event.model_dump_json(by_alias=True, exclude_none=False))
    assert set(payload) == {
        "eventId",
        "taskId",
        "incidentId",
        "version",
        "rootCause",
        "confidence",
        "evidence",
        "recommendations",
        "toolCalls",
        "completedAt",
        "error",
    }
    assert payload["error"] is None
    assert all(payload[name] is not None for name in ("evidence", "recommendations", "toolCalls"))


def test_contract_files_are_executable_json_schemas() -> None:
    root = Path(__file__).parents[2] / "contracts" / "events"
    for path in root.glob("*.json"):
        schema = json.loads(path.read_text(encoding="utf-8"))
        assert schema["$schema"].endswith("2020-12/schema")
        assert schema["type"] == "object"
        assert schema["required"]
