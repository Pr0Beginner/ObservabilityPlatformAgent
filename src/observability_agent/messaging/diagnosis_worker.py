import json
import threading
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from pydantic import ValidationError

from observability_agent.core.config import Settings
from observability_agent.messaging.kafka_publisher import KafkaEventPublisher
from observability_agent.persistence.idempotency_store import SqliteIdempotencyStore
from observability_agent.schemas.events import (
    DiagnosisCompletedEvent,
    DiagnosisFailedEvent,
    DiagnosisRequestedEvent,
)
from observability_agent.services.diagnosis_runner import DiagnosisRunner


class KafkaDiagnosisWorker:
    def __init__(
        self,
        settings: Settings,
        runner: DiagnosisRunner,
        publisher: KafkaEventPublisher,
        idempotency_store: SqliteIdempotencyStore,
    ) -> None:
        self._settings = settings
        self._runner = runner
        self._publisher = publisher
        self._idempotency_store = idempotency_store
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._logger = structlog.get_logger(__name__)

    @property
    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.is_alive:
            return
        self._thread = threading.Thread(
            target=self._consume_loop,
            name="diagnosis-kafka-consumer",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 15) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _new_consumer(self) -> Consumer:
        return Consumer(
            {
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "group.id": self._settings.kafka_group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "max.poll.interval.ms": 300_000,
            }
        )

    def _consume_loop(self) -> None:
        consumer = self._new_consumer()
        consumer.subscribe([self._settings.kafka_diagnosis_requested_topic])
        self._logger.info(
            "diagnosis_worker_started",
            topic=self._settings.kafka_diagnosis_requested_topic,
        )
        try:
            while not self._stop_event.is_set():
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())
                self._handle_message(consumer, message)
        except Exception:
            self._logger.exception("diagnosis_worker_stopped_unexpectedly")
        finally:
            consumer.close()
            self._logger.info("diagnosis_worker_stopped")

    def _handle_message(self, consumer: Consumer, message: Message) -> None:
        raw_payload = message.value().decode("utf-8")
        try:
            request = DiagnosisRequestedEvent.model_validate_json(raw_payload)
        except (ValidationError, ValueError) as error:
            self._publish_dlq(raw_payload, str(error))
            self._publisher.flush()
            consumer.commit(message=message, asynchronous=False)
            return

        if not self._idempotency_store.claim(request.task_id, request.version):
            self._logger.info(
                "diagnosis_duplicate_skipped",
                task_id=request.task_id,
                version=request.version,
            )
            consumer.commit(message=message, asynchronous=False)
            return

        try:
            report = self._runner.run(request)
            completed = DiagnosisCompletedEvent(
                task_id=request.task_id,
                incident_id=request.incident_id,
                version=request.version,
                root_cause=report.root_cause,
                confidence=report.confidence,
                evidence=[f"{item.log_id}: {item.reason}" for item in report.evidence],
                recommendations=report.recommendations,
                tool_calls=report.tool_calls,
            )
            self._publisher.publish(
                self._settings.kafka_diagnosis_completed_topic,
                completed,
                key=request.task_id,
            )
            self._publisher.flush()
            self._idempotency_store.mark_completed(request.task_id, request.version)
        except Exception as error:
            self._handle_failure(request, error)
        consumer.commit(message=message, asynchronous=False)

    def _handle_failure(self, request: DiagnosisRequestedEvent, error: Exception) -> None:
        error_message = str(error) or error.__class__.__name__
        self._logger.exception(
            "diagnosis_failed",
            task_id=request.task_id,
            incident_id=request.incident_id,
        )
        failed = DiagnosisFailedEvent(
            task_id=request.task_id,
            incident_id=request.incident_id,
            version=request.version,
            error_code=error.__class__.__name__.upper(),
            error_message=error_message,
            retryable=True,
        )
        self._publisher.publish(
            self._settings.kafka_diagnosis_failed_topic,
            failed,
            key=request.task_id,
        )
        # The current Java service consumes only diagnosis.completed.v1. Publishing the
        # same failure there allows it to move the task to FAILED until it gains a
        # dedicated diagnosis.failed.v1 consumer.
        compatibility_event = DiagnosisCompletedEvent(
            task_id=request.task_id,
            incident_id=request.incident_id,
            version=request.version,
            root_cause="",
            confidence=0,
            evidence=[],
            recommendations=[],
            tool_calls=[],
            error=error_message,
        )
        self._publisher.publish(
            self._settings.kafka_diagnosis_completed_topic,
            compatibility_event,
            key=request.task_id,
        )
        self._publisher.flush()
        self._idempotency_store.mark_failed(request.task_id, request.version, error_message)

    def _publish_dlq(self, raw_payload: str, error: str) -> None:
        payload: dict[str, Any] = {
            "payload": self._safe_json(raw_payload),
            "error": error,
        }
        self._publisher.publish(
            self._settings.kafka_diagnosis_dlq_topic,
            payload,
            key="invalid-diagnosis-request",
        )

    @staticmethod
    def _safe_json(payload: str) -> Any:
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return payload
