from confluent_kafka import KafkaError, KafkaException

from observability_agent.messaging.kafka_topics import KafkaTopicProvisioner


class FakeFuture:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.timeout = None

    def result(self, timeout):  # noqa: ANN001
        self.timeout = timeout
        if self.error is not None:
            raise self.error


class FakeAdminClient:
    def __init__(self, future: FakeFuture) -> None:
        self.future = future
        self.topics = []
        self.operation_timeout = None

    def create_topics(self, topics, operation_timeout):  # noqa: ANN001
        self.topics = topics
        self.operation_timeout = operation_timeout
        return {topics[0].topic: self.future}


def test_dlq_topic_is_created_explicitly() -> None:
    future = FakeFuture()
    admin = FakeAdminClient(future)
    provisioner = KafkaTopicProvisioner(
        "localhost:29092",
        partitions=3,
        replication_factor=1,
        timeout_seconds=7,
        admin_client=admin,
    )

    provisioner.ensure_topic("diagnosis.requested.v1.dlq")

    assert admin.topics[0].topic == "diagnosis.requested.v1.dlq"
    assert admin.topics[0].num_partitions == 3
    assert admin.topics[0].replication_factor == 1
    assert admin.operation_timeout == 7
    assert future.timeout == 7


def test_existing_dlq_topic_is_accepted() -> None:
    future = FakeFuture(KafkaException(KafkaError(KafkaError.TOPIC_ALREADY_EXISTS)))
    provisioner = KafkaTopicProvisioner(
        "localhost:29092",
        partitions=3,
        replication_factor=1,
        timeout_seconds=7,
        admin_client=FakeAdminClient(future),
    )

    provisioner.ensure_topic("diagnosis.requested.v1.dlq")
