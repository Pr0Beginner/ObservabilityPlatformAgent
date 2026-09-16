import uvicorn

from observability_agent.api.application import create_app
from observability_agent.core.config import get_settings
from observability_agent.core.logging import configure_logging

settings = get_settings()
configure_logging(settings.log_level)
app = create_app(settings)


def run() -> None:
    uvicorn.run(
        "observability_agent.main:app",
        host=settings.health_host,
        port=settings.health_port,
        log_config=None,
    )


if __name__ == "__main__":
    run()
