from observability_agent.schemas.context import IncidentContext, LogEvidence


def build_analysis_plan(context: IncidentContext) -> list[str]:
    errors = [log for log in context.logs if log.level.upper() in {"ERROR", "FATAL"}]
    trace_ids = {log.trace_id for log in context.logs if log.trace_id}
    strategies = {
        "REPEATED_ERROR": "核对错误指纹、首次/末次出现时间、重复次数和共同 Trace",
        "ERROR_CODE_COUNT_SPIKE": "比较错误码当前计数与基线，定位集中接口和代表 Trace",
        "ERROR_CODE_RATE_SPIKE": "比较错误码占比与基线，并区分分子增长和流量变化",
        "FAILURE_RATE_SPIKE": "定位每条 Trace 的最早失败 Span 和向上游传播路径",
    }
    strategy = strategies.get(
        context.incident_type,
        "按时间、错误码、接口和 Trace 父子关系提取候选根因",
    )
    plan = [
        f"诊断策略[{context.incident_type or 'UNKNOWN'}]：{strategy}",
        (
            f"指标事实：dimension={context.dimension or 'unknown'}, "
            f"current={context.current_value}, baseline={context.baseline_value}, "
            f"operation={context.operation or 'unknown'}"
        ),
        f"按时间顺序分析 {len(context.logs)} 条日志，其中 {len(errors)} 条为 ERROR/FATAL",
        f"结合 {len(trace_ids)} 个 Trace ID 判断错误传播关系",
    ]
    first_failure = find_first_failure(context.logs)
    if first_failure is not None:
        plan.append(
            "确定性 Trace 候选："
            f"logId={first_failure.id}, traceId={first_failure.trace_id}, "
            f"spanId={first_failure.span_id}, parentSpanId={first_failure.parent_span_id}, "
            f"serviceOperation={first_failure.operation}, errorCode={first_failure.error_code}, "
            f"statusCode={first_failure.status_code}"
        )
    plan.append("只引用真实日志 ID，并生成可执行但不自动执行的建议")
    return plan


def find_first_failure(logs: list[LogEvidence]) -> LogEvidence | None:
    failures = [
        log
        for log in logs
        if log.success is False
        or log.level.upper() in {"ERROR", "FATAL"}
        or (log.status_code is not None and log.status_code >= 500)
        or bool(log.error_code)
    ]
    if not failures:
        return None
    # A failing leaf is more likely to be the origin than its propagated failing parent.
    failed_parent_ids = {log.parent_span_id for log in failures if log.parent_span_id}
    return min(
        failures,
        key=lambda log: (log.span_id in failed_parent_ids, log.timestamp),
    )
