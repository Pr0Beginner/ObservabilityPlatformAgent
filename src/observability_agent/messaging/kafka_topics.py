from typing import Any

import structlog
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


class KafkaTopicProvisioner:
    """Create Agent-owned Kafka topics without relying on broker auto-creation."""

    def __init__(
        self,
        bootstrap_servers: str,
        partitions: int,
        replication_factor: int,
        timeout_seconds: float,
        admin_client: Any | None = None,
    ) -> None:
        self._admin_client = admin_client or AdminClient(
            {"bootstrap.servers": bootstrap_servers}
        )
        self._partitions = partitions
        self._replication_factor = replication_factor
        self._timeout_seconds = timeout_seconds
        self._logger = structlog.get_logger(__name__)

    def ensure_topic(self, topic: str) -> None:
        new_topic = NewTopic(
            topic,
            num_partitions=self._partitions,
            replication_factor=self._replication_factor,
        )
        future = self._admin_client.create_topics(
            [new_topic],
            operation_timeout=self._timeout_seconds,
        )[topic]
        try:
            future.result(timeout=self._timeout_seconds)
        except KafkaException as error:
            kafka_error = error.args[0] if error.args else None
            if isinstance(kafka_error, KafkaError) and (
                kafka_error.code() == KafkaError.TOPIC_ALREADY_EXISTS
            ):
                self._logger.debug("kafka_topic_already_exists", topic=topic)
                return
            raise
        self._logger.info("kafka_topic_created", topic=topic)
