from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    environment: str = "local"
    log_level: str = "INFO"

    kafka_enabled: bool = True
    kafka_bootstrap_servers: str = "localhost:29092"
    kafka_group_id: str = "observability-diagnosis-agent"
    kafka_diagnosis_requested_topic: str = "diagnosis.requested.v1"
    kafka_diagnosis_progress_topic: str = "diagnosis.progress.v1"
    kafka_diagnosis_completed_topic: str = "diagnosis.completed.v1"
    kafka_diagnosis_failed_topic: str = "diagnosis.failed.v1"
    kafka_diagnosis_dlq_topic: str = "diagnosis.requested.v1.dlq"

    java_grpc_target: str = "localhost:9090"
    java_grpc_timeout_seconds: float = Field(default=10, gt=0)
    incident_log_limit: int = Field(default=100, ge=1, le=200)

    deepseek_api_key: SecretStr | None = None
    deepseek_base_url: str = "https://api.deepseek.com"
    deepseek_model: str = "deepseek-chat"
    deepseek_timeout_seconds: float = Field(default=45, gt=0)
    deepseek_max_retries: int = Field(default=2, ge=0, le=5)

    idempotency_db_path: Path = Path(".data/agent.db")
    idempotency_stale_after_seconds: int = Field(default=300, ge=30)

    health_host: str = "0.0.0.0"
    health_port: int = Field(default=8090, ge=1, le=65535)


@lru_cache
def get_settings() -> Settings:
    return Settings()
