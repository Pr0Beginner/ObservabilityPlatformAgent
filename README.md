# ObservabilityPlatform Diagnosis Agent

Python 诊断执行器。它从 Kafka 接收 Java 平台创建的诊断任务，通过 gRPC 获取受控故障上下文，使用 LangGraph 和 DeepSeek 生成有日志证据的诊断报告，再通过 Kafka 返回进度和结果。

## MVP 架构

```text
Java ── diagnosis.requested.v1 ──> Kafka ──> Python Agent
                                                   │
                                                   ├─ gRPC 获取故障上下文
                                                   ├─ LangGraph 编排
                                                   └─ DeepSeek 日志分析
Java <── progress/completed/failed ── Kafka <───────┘
  │
  └─ SSE 推送给浏览器
```

SSE 由 Java 对前端提供。Agent 只发布进度事件，不保存 Incident，也不直接访问 Java 的 PostgreSQL 或 OpenSearch。

## LangGraph 流程

```text
validate_task
→ load_context
→ 有日志：plan_diagnosis → analyze_logs → verify_evidence
→ 无日志：insufficient_evidence
→ build_report
```

当前 Java 契约只提供 `GetIncidentContext`，因此 MVP 只实现这一项真实工具。增加日志搜索、上下文和 Trace RPC 后，可以继续在 `tools/` 和图节点中扩展。

## Kafka Topics

| Topic | 方向 | 用途 |
| --- | --- | --- |
| `diagnosis.requested.v1` | Java → Agent | 创建诊断任务 |
| `diagnosis.progress.v1` | Agent → Java | SSE 所需的任务进度 |
| `diagnosis.completed.v1` | Agent → Java | 成功报告；当前兼容失败状态更新 |
| `diagnosis.failed.v1` | Agent → Java | 结构化失败事件 |
| `diagnosis.requested.v1.dlq` | Agent → Kafka | 无法解析的请求 |

同一任务的 Kafka key 固定为 `taskId`，保证该任务的进度和结果在同一分区内有序。消费端通过 `taskId + version` 做持久化幂等。

## 本地运行

要求 Python 3.12。

```powershell
Copy-Item .env.example .env
# 编辑 .env，填入 DEEPSEEK_API_KEY
uv sync --extra dev --python 3.12
uv run python -m grpc_tools.protoc `
  -Icontracts/proto `
  --python_out=src/observability_agent/generated `
  --grpc_python_out=src/observability_agent/generated `
  contracts/proto/incident_context.proto
uv run observability-agent
```

默认地址：

- Kafka：`localhost:29092`
- Java gRPC：`localhost:9090`
- Agent 健康检查：`http://localhost:8090/health/live`

## 测试与检查

```powershell
uv run pytest
uv run ruff check .
```

## 安全边界

- 模型引用的日志 ID 必须出现在 gRPC 工具结果中。
- 无有效证据时根因固定为“证据不足，无法确定根因”，置信度不超过 `0.3`。
- Agent 只生成建议，不执行重启、扩容、回滚或数据库修改。
- SSE 只展示步骤、进度和结果摘要，不输出模型思维过程。
