import threading
from collections import defaultdict

from observability_agent.messaging.kafka_publisher import KafkaEventPublisher
from observability_agent.schemas.events import (
    DiagnosisProgressEvent,
    DiagnosisRequestedEvent,
    DiagnosisStep,
)


class KafkaProgressReporter:
    def __init__(self, publisher: KafkaEventPublisher, topic: str) -> None:
        self._publisher = publisher
        self._topic = topic
        self._sequences: dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def publish(
        self,
        request: DiagnosisRequestedEvent,
        step: DiagnosisStep,
        progress: int,
        message: str,
    ) -> None:
        with self._lock:
            self._sequences[request.task_id] += 1
            sequence = self._sequences[request.task_id]
        event = DiagnosisProgressEvent(
            task_id=request.task_id,
            incident_id=request.incident_id,
            version=request.version,
            sequence=sequence,
            step=step,
            progress=progress,
            message=message,
        )
        self._publisher.publish(self._topic, event, key=request.task_id)

    def finish(self, task_id: str) -> None:
        with self._lock:
            self._sequences.pop(task_id, None)
