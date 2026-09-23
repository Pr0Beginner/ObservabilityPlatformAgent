import base64
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from observability_agent.api.application import create_app
from observability_agent.clients.deepseek_client import DeepSeekDiagnosisAnalyzer
from observability_agent.core.config import Settings
from observability_agent.core.sensitive_data import redact_sensitive
from observability_agent.messaging.diagnosis_worker import KafkaDiagnosisWorker
from observability_agent.schemas.context import IncidentContext, LogEvidence
from observability_agent.schemas.report import ModelDiagnosis


@pytest.mark.parametrize("raw", [b"\xff\x80", None, b"not-json", b"{}"])
def test_invalid_messages_reach_dlq_before_offset_commit(raw):
    consumer, publisher, runner, store = Mock(), Mock(), Mock(), Mock()
    order = Mock()
    order.attach_mock(publisher, "publisher")
    order.attach_mock(consumer, "consumer")
    worker = KafkaDiagnosisWorker(Settings(), runner, publisher, store, Mock())
    message = Mock()
    message.value.return_value = raw
    message.key.return_value = b"key"
    worker._handle_message(consumer, message)

    assert [call[0] for call in order.mock_calls] == [
        "publisher.publish",
        "publisher.flush",
        "consumer.commit",
    ]
    runner.run.assert_not_called()
    store.acquire.assert_not_called()
    payload = publisher.publish.call_args.args[1]["payload"]
    if raw is None:
        assert payload is None
    elif raw == b"\xff\x80":
        assert payload["encoding"] == "base64"
        assert base64.b64decode(payload["data"]) == raw


def test_failed_dlq_delivery_does_not_acknowledge_source_message():
    consumer, publisher = Mock(), Mock()
    publisher.flush.side_effect = TimeoutError("DLQ unavailable")
    worker = KafkaDiagnosisWorker(Settings(), Mock(), publisher, Mock(), Mock())
    message = Mock()
    message.value.return_value = None
    message.key.return_value = None
    with pytest.raises(TimeoutError):
        worker._handle_message(consumer, message)
    consumer.commit.assert_not_called()


@pytest.mark.parametrize("ready,status", [(False, 503), (True, 200)])
def test_readiness_uses_delivery_readiness_and_http_status(ready, status):
    runtime = SimpleNamespace(
        worker=SimpleNamespace(is_alive=True, is_ready=ready), start=lambda: None, stop=lambda: None
    )
    app = create_app(Settings(kafka_enabled=True), runtime_factory=lambda settings: runtime)
    with TestClient(app) as client:
        assert client.get("/health/ready").status_code == status
        assert client.get("/health/live").status_code == 200


def test_worker_requires_broker_and_assignment_and_loses_readiness_on_errors():
    worker = KafkaDiagnosisWorker(Settings(), Mock(), Mock(), Mock(), Mock())
    worker._thread = Mock()
    worker._thread.is_alive.return_value = True
    assert not worker.is_ready
    worker._on_assign(Mock(), [Mock()])
    assert not worker.is_ready
    consumer = Mock()
    consumer.list_topics.return_value = SimpleNamespace(
        topics={"diagnosis.requested.v1": SimpleNamespace(error=None)}
    )
    worker._probe_connection(consumer)
    assert worker.is_ready
    worker._on_kafka_error(Mock())
    assert not worker.is_ready
    worker._probe_connection(consumer)
    assert worker.is_ready
    worker._on_revoke(consumer, [])
    assert not worker.is_ready


def test_model_input_redacts_json_fragments_nested_fields_and_metadata():
    secret = "synthetic-private-value"
    message = json.dumps(
        {
            "password": secret,
            "items": [{"api_key": secret}],
            "message": json.dumps({"Authorization": "Bearer " + secret}),
        }
    )
    redacted = redact_sensitive(message)
    assert secret not in redacted
    assert json.loads(redacted)["password"] == "[REDACTED]"
    assert secret not in redact_sensitive('prefix {"password":"' + secret + '"}')
    analyzer = object.__new__(DeepSeekDiagnosisAnalyzer)
    analyzer._system_prompt = "test"
    analyzer._model = Mock()
    analyzer._model.invoke.return_value = ModelDiagnosis(root_cause="test", confidence=0.1)
    context = IncidentContext(
        incident_id="i",
        title="token=" + secret,
        service="orders",
        environment="test",
        severity="P1",
        status="OPEN",
        fingerprint="fp",
        logs=[
            LogEvidence(
                id="log",
                timestamp=datetime.now(UTC),
                level="ERROR",
                message=message,
                fingerprint="fp",
            )
        ],
    )
    analyzer.analyze(context, ["password=" + secret])
    assert secret not in analyzer._model.invoke.call_args.args[0][1].content


def test_context_budget_keeps_valid_json_instead_of_slicing_a_log():
    analyzer = object.__new__(DeepSeekDiagnosisAnalyzer)
    analyzer._system_prompt = "test"
    analyzer._model = Mock()
    analyzer._model.invoke.return_value = ModelDiagnosis(root_cause="test", confidence=0.1)
    logs = [
        LogEvidence(
            id=str(index),
            timestamp=datetime.now(UTC),
            level="ERROR",
            message="x" * 2000,
            fingerprint="fp",
        )
        for index in range(100)
    ]
    context = IncidentContext(
        incident_id="i",
        title="test",
        service="s",
        environment="test",
        severity="P1",
        status="OPEN",
        fingerprint="fp",
        logs=logs,
    )
    analyzer.analyze(context, [])
    content = analyzer._model.invoke.call_args.args[0][1].content
    payload = content.split("故障上下文：\n", 1)[1]
    assert len(payload) <= 60000
    assert 0 < len(json.loads(payload)["logs"]) < 100
