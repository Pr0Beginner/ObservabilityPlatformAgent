import json
import threading
from datetime import UTC, datetime
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from pydantic import ValidationError

from observability_agent.core.config import Settings
from observability_agent.messaging.kafka_publisher import KafkaEventPublisher
from observability_agent.messaging.kafka_topics import KafkaTopicProvisioner
from observability_agent.persistence.idempotency_store import ClaimStatus, SqliteIdempotencyStore
from observability_agent.schemas.events import (
    DiagnosisCompletedEvent,
    DiagnosisFailedEvent,
    DiagnosisRequestedEvent,
    stable_event_id,
)
from observability_agent.services.diagnosis_runner import DiagnosisRunner


class KafkaDiagnosisWorker:
    def __init__(
        self,
        settings: Settings,
        runner: DiagnosisRunner,
        publisher: KafkaEventPublisher,
        idempotency_store: SqliteIdempotencyStore,
        topic_provisioner: KafkaTopicProvisioner | None = None,
    ) -> None:
        self._settings = settings
        self._runner = runner
        self._publisher = publisher
        self._idempotency_store = idempotency_store
        self._topic_provisioner = topic_provisioner or KafkaTopicProvisioner(
            settings.kafka_bootstrap_servers,
            settings.kafka_dlq_partitions,
            settings.kafka_dlq_replication_factor,
            settings.kafka_admin_timeout_seconds,
        )
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
            target=self._consume_loop, name="diagnosis-kafka-consumer", daemon=True
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
                "max.poll.interval.ms": self._settings.kafka_max_poll_interval_ms,
                "session.timeout.ms": self._settings.kafka_session_timeout_ms,
            }
        )

    def _consume_loop(self) -> None:
        backoff = 1.0
        while not self._stop_event.is_set():
            consumer: Consumer | None = None
            try:
                self._topic_provisioner.ensure_topic(
                    self._settings.kafka_diagnosis_dlq_topic
                )
                consumer = self._new_consumer()
                consumer.subscribe([self._settings.kafka_diagnosis_requested_topic])
                self._logger.info(
                    "diagnosis_worker_started",
                    topic=self._settings.kafka_diagnosis_requested_topic,
                )
                backoff = 1.0
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
                self._logger.exception("diagnosis_worker_cycle_failed", retry_in_seconds=backoff)
                self._stop_event.wait(backoff)
                backoff = min(backoff * 2, 30.0)
            finally:
                if consumer is not None:
                    consumer.close()
        self._logger.info("diagnosis_worker_stopped")

    def _handle_message(self, consumer: Consumer, message: Message) -> None:
        raw_payload = message.value().decode("utf-8")
        try:
            request = DiagnosisRequestedEvent.model_validate_json(raw_payload)
        except (ValidationError, ValueError) as error:
            self._publish_dlq(message, raw_payload, error)
            self._publisher.flush()
            consumer.commit(message=message, asynchronous=False)
            return

        claim = self._idempotency_store.acquire(request.task_id, request.version)
        if claim.status == ClaimStatus.RESULT_READY:
            self._publish_stored_result(request, claim.completed_payload, claim.failed_payload)
            consumer.commit(message=message, asynchronous=False)
            return

        try:
            report = self._runner.run(request)
        except Exception as error:
            self._store_and_publish_failure(request, error)
        else:
            completed = DiagnosisCompletedEvent(
                event_id=stable_event_id(request.task_id, request.version, "completed"),
                task_id=request.task_id,
                incident_id=request.incident_id,
                version=request.version,
                root_cause=report.root_cause,
                confidence=report.confidence,
                evidence=[f"{item.log_id}: {item.reason}" for item in report.evidence],
                recommendations=report.recommendations,
                tool_calls=report.tool_calls,
            )
            self._store_and_publish(request, completed)

        consumer.commit(message=message, asynchronous=False)

    def _store_and_publish(
        self, request: DiagnosisRequestedEvent, completed: DiagnosisCompletedEvent
    ) -> None:
        payload = self._publisher.serialize(completed)
        self._idempotency_store.save_result(request.task_id, request.version, payload)
        self._publish_stored_result(request, payload, None)

    def _store_and_publish_failure(
        self, request: DiagnosisRequestedEvent, error: Exception
    ) -> None:
        error_message = str(error) or error.__class__.__name__
        error_code, retryable = self._classify_error(error)
        self._logger.exception(
            "diagnosis_failed", task_id=request.task_id, incident_id=request.incident_id
        )
        failed = DiagnosisFailedEvent(
            event_id=stable_event_id(request.task_id, request.version, "failed"),
            task_id=request.task_id,
            incident_id=request.incident_id,
            version=request.version,
            error_code=error_code,
            error_message=error_message,
            retryable=retryable,
        )
        compatibility = DiagnosisCompletedEvent(
            event_id=stable_event_id(request.task_id, request.version, "completed-failure"),
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
        completed_payload = self._publisher.serialize(compatibility)
        failed_payload = self._publisher.serialize(failed)
        self._idempotency_store.save_result(
            request.task_id,
            request.version,
            completed_payload,
            failed_payload,
            error_message,
        )
        self._publish_stored_result(request, completed_payload, failed_payload)

    def _publish_stored_result(
        self,
        request: DiagnosisRequestedEvent,
        completed_payload: str | None,
        failed_payload: str | None,
    ) -> None:
        if completed_payload is None:
            raise RuntimeError("stored diagnosis result has no Java-compatible payload")
        # failed.v1 is reserved for future Java support. The completed compatibility
        # event is the authoritative state transition and must always be delivered.
        if failed_payload is not None and self._settings.kafka_failed_enabled:
            self._publisher.publish_raw(
                self._settings.kafka_diagnosis_failed_topic, failed_payload, request.task_id
            )
        self._publisher.publish_raw(
            self._settings.kafka_diagnosis_completed_topic, completed_payload, request.task_id
        )
        self._publisher.flush()
        self._idempotency_store.mark_published(request.task_id, request.version)

    @staticmethod
    def _classify_error(error: Exception) -> tuple[str, bool]:
        import grpc
        from openai import APIConnectionError, APITimeoutError, RateLimitError

        if isinstance(error, grpc.RpcError):
            code = error.code()
            retryable = code in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}
            return f"GRPC_{code.name}", retryable
        if isinstance(error, (TimeoutError, ConnectionError)):
            return "UPSTREAM_TEMPORARY_FAILURE", True
        if isinstance(error, RateLimitError):
            return "MODEL_RATE_LIMITED", True
        if isinstance(error, APITimeoutError):
            return "MODEL_TIMEOUT", True
        if isinstance(error, APIConnectionError):
            return "MODEL_UNAVAILABLE", True
        if isinstance(error, (ValidationError, ValueError)):
            return "INVALID_DIAGNOSIS_DATA", False
        return "DIAGNOSIS_EXECUTION_FAILED", False

    def _publish_dlq(self, message: Message, raw_payload: str, error: Exception) -> None:
        payload: dict[str, Any] = {
            "sourceTopic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
            "key": message.key().decode("utf-8", errors="replace") if message.key() else None,
            "errorCode": "INVALID_REQUEST",
            "errorType": error.__class__.__name__,
            "errorMessage": str(error),
            "occurredAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "payload": self._safe_json(raw_payload),
        }
        self._publisher.publish(
            self._settings.kafka_diagnosis_dlq_topic, payload, key="invalid-diagnosis-request"
        )

    @staticmethod
    def _safe_json(payload: str) -> Any:
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return payload
