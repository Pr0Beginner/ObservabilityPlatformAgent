from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI

from observability_agent.bootstrap import AgentRuntime, build_runtime
from observability_agent.core.config import Settings, get_settings

RuntimeFactory = Callable[[Settings], AgentRuntime]


def create_app(
    settings: Settings | None = None,
    runtime_factory: RuntimeFactory = build_runtime,
) -> FastAPI:
    resolved_settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        runtime: AgentRuntime | None = None
        if resolved_settings.kafka_enabled:
            runtime = runtime_factory(resolved_settings)
            runtime.start()
        app.state.runtime = runtime
        yield
        if runtime is not None:
            runtime.stop()

    app = FastAPI(
        title="ObservabilityPlatform Diagnosis Agent",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.runtime = None

    @app.get("/health/live")
    def liveness() -> dict[str, str]:
        return {"status": "UP"}

    @app.get("/health/ready")
    def readiness() -> dict[str, str]:
        runtime = app.state.runtime
        ready = not resolved_settings.kafka_enabled or (
            runtime is not None and runtime.worker.is_alive
        )
        return {"status": "UP" if ready else "DOWN"}

    return app
