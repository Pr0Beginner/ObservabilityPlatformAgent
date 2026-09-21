from observability_agent.clients.deepseek_client import redact_sensitive


def test_sensitive_values_are_redacted_from_log_messages() -> None:
    message = "Authorization: Bearer secret-token api_key=my-key password=hunter2"

    redacted = redact_sensitive(message)

    assert "secret-token" not in redacted
    assert "my-key" not in redacted
    assert "hunter2" not in redacted
    assert redacted.count("[REDACTED]") == 3
