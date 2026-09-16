from observability_agent.schemas.events import DiagnosisRequestedEvent


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
