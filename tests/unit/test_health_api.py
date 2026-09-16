from fastapi.testclient import TestClient

from observability_agent.api.application import create_app
from observability_agent.core.config import Settings


def test_health_is_ready_when_kafka_is_disabled() -> None:
    app = create_app(Settings(kafka_enabled=False))

    with TestClient(app) as client:
        assert client.get("/health/live").json() == {"status": "UP"}
        assert client.get("/health/ready").json() == {"status": "UP"}
