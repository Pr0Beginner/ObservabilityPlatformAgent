import json
from typing import Any

import structlog
from confluent_kafka import Producer
from pydantic import BaseModel


class KafkaEventPublisher:
    def __init__(self, bootstrap_servers: str) -> None:
        self._logger = structlog.get_logger(__name__)
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "snappy",
            }
        )
        self._delivery_errors: list[Exception] = []

    @staticmethod
    def serialize(event: BaseModel | dict[str, Any]) -> str:
        if isinstance(event, BaseModel):
            return event.model_dump_json(by_alias=True, exclude_none=False)
        return json.dumps(event, ensure_ascii=False, default=str)

    def publish(self, topic: str, event: BaseModel | dict[str, Any], key: str) -> None:
        self.publish_raw(topic, self.serialize(event), key)

    def publish_raw(self, topic: str, payload: str, key: str) -> None:
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=payload.encode("utf-8"),
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10) -> None:
        remaining = self._producer.flush(timeout)
        if remaining:
            raise TimeoutError(f"{remaining} Kafka messages were not delivered")
        if self._delivery_errors:
            errors = self._delivery_errors.copy()
            self._delivery_errors.clear()
            raise RuntimeError(f"Kafka delivery failed: {errors[0]}")

    def _on_delivery(self, error, message) -> None:
        if error is not None:
            self._delivery_errors.append(RuntimeError(str(error)))
            self._logger.error(
                "kafka_delivery_failed",
                topic=message.topic(),
                error=str(error),
            )
