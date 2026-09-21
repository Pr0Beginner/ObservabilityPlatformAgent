from datetime import UTC, datetime

import pytest

from observability_agent.core.config import Settings
from observability_agent.messaging.diagnosis_worker import KafkaDiagnosisWorker
from observability_agent.messaging.kafka_publisher import KafkaEventPublisher
from observability_agent.persistence.idempotency_store import SqliteIdempotencyStore
from observability_agent.schemas.report import DiagnosisReport


class FakeRunner:
    def __init__(self) -> None:
        self.calls = 0

    def run(self, request):  # noqa: ANN001
        self.calls += 1
        return DiagnosisReport(
            root_cause="database timeout", confidence=0.8, evidence=[],
            recommendations=["inspect pool"], tool_calls=["get_incident_context(...)"],
        )


class FailingRunner:
    def run(self, request):  # noqa: ANN001
        raise TimeoutError("model timed out")


class FakePublisher:
    serialize = staticmethod(KafkaEventPublisher.serialize)

    def __init__(self, fail_flush: bool = False) -> None:
        self.messages = []
        self.fail_flush = fail_flush

    def publish_raw(self, topic, payload, key):  # noqa: ANN001
        self.messages.append((topic, payload, key))

    def flush(self):
        if self.fail_flush:
            raise RuntimeError("broker unavailable")


class FakeConsumer:
    def __init__(self) -> None:
        self.commits = 0

    def commit(self, message, asynchronous):  # noqa: ANN001
        self.commits += 1


class FakeMessage:
    def value(self):
        return (
            b'{"eventId":"e","taskId":"t","incidentId":"i","version":1,'
            b'"requestedAt":"2026-09-16T10:00:00Z"}'
        )


def test_redelivery_reuses_result_after_delivery_failure(tmp_path) -> None:
    db = tmp_path / "agent.db"
    runner = FakeRunner()
    failing_publisher = FakePublisher(fail_flush=True)
    worker = KafkaDiagnosisWorker(
        Settings(), runner, failing_publisher, SqliteIdempotencyStore(db, 60)  # type: ignore[arg-type]
    )
    first_consumer = FakeConsumer()

    with pytest.raises(RuntimeError, match="broker unavailable"):
        worker._handle_message(first_consumer, FakeMessage())  # type: ignore[arg-type]
    assert first_consumer.commits == 0
    assert runner.calls == 1

    recovered_runner = FakeRunner()
    recovered_publisher = FakePublisher()
    recovered_worker = KafkaDiagnosisWorker(
        Settings(), recovered_runner, recovered_publisher,
        SqliteIdempotencyStore(db, 60),  # type: ignore[arg-type]
    )
    second_consumer = FakeConsumer()
    recovered_worker._handle_message(second_consumer, FakeMessage())  # type: ignore[arg-type]

    assert recovered_runner.calls == 0
    assert second_consumer.commits == 1
    assert recovered_publisher.messages[0][1] == failing_publisher.messages[0][1]
    assert datetime.fromisoformat(
        __import__("json").loads(recovered_publisher.messages[0][1])["completedAt"].replace(
            "Z", "+00:00"
        )
    ).tzinfo == UTC


def test_failed_topic_is_not_published_by_default(tmp_path) -> None:
    settings = Settings()
    publisher = FakePublisher()
    worker = KafkaDiagnosisWorker(
        settings,
        FailingRunner(),  # type: ignore[arg-type]
        publisher,  # type: ignore[arg-type]
        SqliteIdempotencyStore(tmp_path / "agent.db", 60),
    )
    consumer = FakeConsumer()

    worker._handle_message(consumer, FakeMessage())  # type: ignore[arg-type]

    assert settings.kafka_failed_enabled is False
    assert [message[0] for message in publisher.messages] == [
        settings.kafka_diagnosis_completed_topic
    ]
    assert consumer.commits == 1
