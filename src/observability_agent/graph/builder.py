from functools import partial

from langgraph.graph import END, START, StateGraph

from observability_agent.graph.dependencies import GraphDependencies
from observability_agent.graph.nodes.analyze_logs import analyze_logs
from observability_agent.graph.nodes.build_report import build_report
from observability_agent.graph.nodes.insufficient_evidence import insufficient_evidence
from observability_agent.graph.nodes.load_context import load_context
from observability_agent.graph.nodes.plan_diagnosis import plan_diagnosis
from observability_agent.graph.nodes.validate_task import validate_task
from observability_agent.graph.nodes.verify_evidence import verify_evidence
from observability_agent.graph.routing import route_after_context
from observability_agent.graph.state import DiagnosisState


def build_diagnosis_graph(dependencies: GraphDependencies):
    graph = StateGraph(DiagnosisState)
    graph.add_node("validate_task", partial(validate_task, dependencies=dependencies))
    graph.add_node("load_context", partial(load_context, dependencies=dependencies))
    graph.add_node("plan_diagnosis", partial(plan_diagnosis, dependencies=dependencies))
    graph.add_node("analyze_logs", partial(analyze_logs, dependencies=dependencies))
    graph.add_node("verify_evidence", partial(verify_evidence, dependencies=dependencies))
    graph.add_node(
        "insufficient_evidence",
        partial(insufficient_evidence, dependencies=dependencies),
    )
    graph.add_node("build_report", partial(build_report, dependencies=dependencies))

    graph.add_edge(START, "validate_task")
    graph.add_edge("validate_task", "load_context")
    graph.add_conditional_edges("load_context", route_after_context)
    graph.add_edge("plan_diagnosis", "analyze_logs")
    graph.add_edge("analyze_logs", "verify_evidence")
    graph.add_edge("verify_evidence", "build_report")
    graph.add_edge("insufficient_evidence", "build_report")
    graph.add_edge("build_report", END)
    return graph.compile()
