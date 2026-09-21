from observability_agent.core.config import Settings
from observability_agent.schemas.events import DiagnosisRequestedEvent, DiagnosisStep
from observability_agent.services.progress_reporter import KafkaProgressReporter


class FakePublisher:
    def __init__(self) -> None:
        self.messages = []

    def publish(self, topic, event, key):  # noqa: ANN001
        self.messages.append((topic, event, key))


def test_progress_events_are_disabled_by_default() -> None:
    settings = Settings()
    publisher = FakePublisher()
    reporter = KafkaProgressReporter(
        publisher,  # type: ignore[arg-type]
        settings.kafka_diagnosis_progress_topic,
        enabled=settings.kafka_progress_enabled,
    )
    request = DiagnosisRequestedEvent.model_validate(
        {
            "eventId": "event-1",
            "taskId": "task-1",
            "incidentId": "incident-1",
            "version": 1,
            "requestedAt": "2026-09-21T00:00:00Z",
        }
    )

    reporter.publish(request, DiagnosisStep.RECEIVED, 1, "received")

    assert settings.kafka_progress_enabled is False
    assert publisher.messages == []
