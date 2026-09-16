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

    def publish(self, topic: str, event: BaseModel | dict[str, Any], key: str) -> None:
        if isinstance(event, BaseModel):
            payload = event.model_dump_json(by_alias=True, exclude_none=False)
        else:
            payload = json.dumps(event, ensure_ascii=False, default=str)
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

    def _on_delivery(self, error, message) -> None:
        if error is not None:
            self._logger.error(
                "kafka_delivery_failed",
                topic=message.topic(),
                error=str(error),
            )
