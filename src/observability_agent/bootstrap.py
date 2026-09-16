from dataclasses import dataclass

from observability_agent.clients.deepseek_client import DeepSeekDiagnosisAnalyzer
from observability_agent.clients.java_grpc_client import JavaGrpcClient
from observability_agent.core.config import Settings
from observability_agent.graph.builder import build_diagnosis_graph
from observability_agent.graph.dependencies import GraphDependencies
from observability_agent.messaging.diagnosis_worker import KafkaDiagnosisWorker
from observability_agent.messaging.kafka_publisher import KafkaEventPublisher
from observability_agent.persistence.idempotency_store import SqliteIdempotencyStore
from observability_agent.services.diagnosis_runner import DiagnosisRunner
from observability_agent.services.progress_reporter import KafkaProgressReporter
from observability_agent.tools.get_incident_context import GetIncidentContextTool


@dataclass
class AgentRuntime:
    worker: KafkaDiagnosisWorker
    publisher: KafkaEventPublisher
    grpc_client: JavaGrpcClient

    def start(self) -> None:
        self.worker.start()

    def stop(self) -> None:
        self.worker.stop()
        self.publisher.flush()
        self.grpc_client.close()


def build_runtime(settings: Settings) -> AgentRuntime:
    publisher = KafkaEventPublisher(settings.kafka_bootstrap_servers)
    progress_reporter = KafkaProgressReporter(
        publisher,
        settings.kafka_diagnosis_progress_topic,
    )
    grpc_client = JavaGrpcClient(
        settings.java_grpc_target,
        settings.java_grpc_timeout_seconds,
    )
    context_tool = GetIncidentContextTool(grpc_client, settings.incident_log_limit)
    analyzer = DeepSeekDiagnosisAnalyzer(settings)
    dependencies = GraphDependencies(
        context_tool=context_tool,
        analyzer=analyzer,
        emit_progress=progress_reporter.publish,
    )
    graph = build_diagnosis_graph(dependencies)
    runner = DiagnosisRunner(graph, progress_reporter)
    idempotency_store = SqliteIdempotencyStore(
        settings.idempotency_db_path,
        settings.idempotency_stale_after_seconds,
    )
    worker = KafkaDiagnosisWorker(settings, runner, publisher, idempotency_store)
    return AgentRuntime(worker=worker, publisher=publisher, grpc_client=grpc_client)
