class DiagnosisError(RuntimeError):
    """Base exception for failures that should become diagnosis failure events."""


class InvalidDiagnosisRequest(DiagnosisError):
    """The Kafka payload is valid JSON but not a valid diagnosis request."""


class ModelConfigurationError(DiagnosisError):
    """DeepSeek is not configured for an analysis run."""
