# ObservabilityPlatform Diagnosis Agent

Python 诊断执行器：消费 Java 创建的诊断任务，通过 Java gRPC 获取受控故障上下文，使用确定性 Trace 分析、LangGraph 和 DeepSeek 生成有真实日志引用的报告，再通过 Kafka 返回结果。

## MVP 职责边界

```text
Java -- diagnosis.requested.v1 --> Kafka --> Agent
  ^                                      |  gRPC GetIncidentContext
  |                                      |  deterministic facts + LLM
  +----- diagnosis.completed.v1 ---------+
```

- Java 是 Incident、日志和诊断任务的事实来源，负责创建/取消/超时/重试任务及保存报告。
- Agent 不访问 MySQL 或 Elasticsearch，不修改 Incident，不执行重启、扩容、回滚或数据库变更。
- Java 当前只消费 `diagnosis.completed.v1`。失败时 Agent 发送兼容的 completed 事件（`error` 非空）；`progress` 和独立 `failed` 是预留 Topic，默认关闭，可通过环境变量显式启用。
- gRPC 当前为明文且无身份认证，只适用于本地或受控内网。

## 已实现闭环

- Agent proto 与 Java `src/main/proto/incident_context.proto` 字段及编号一致，并正确区分 optional 字段缺失与合法的 `0`/`false`。
- Kafka wire request 允许 v1 向后兼容新增字段；完成事件保持 Java 所需 camelCase、非 null 数组、UTC 时间和置信度范围。
- SQLite 以 `taskId + version` 保存执行租约和完整结果 payload。进程重启可接管遗留的 `PROCESSING`；模型完成后先保存 `RESULT_READY`，重投只发布相同 eventId 和 payload，不再次调用模型。
- 只有 completed 兼容事件收到 Kafka delivery ack 后才提交请求 offset。消费循环遇到 Kafka 异常会退避重连。
- Agent 启动消费循环前通过 Kafka Admin API 幂等创建自己负责的请求 DLQ，不依赖 Broker 自动创建 Topic。
- 支持 `REPEATED_ERROR`、`ERROR_CODE_COUNT_SPIKE`、`ERROR_CODE_RATE_SPIKE`、`FAILURE_RATE_SPIKE` 的差异化计划，并从 span/parentSpan、时间、状态码和错误码提取最早失败候选。
- 模型引用的 `logId` 必须存在；无有效证据时固定输出“证据不足，无法确定根因”，置信度不超过 0.3。

SQLite 方案只支持单 Agent 实例。生产多实例需要共享幂等/outbox 存储或重构为 Kafka 原生事务流程。

## Topic 与地址

| Topic | 方向 | 当前状态 |
| --- | --- | --- |
| `diagnosis.requested.v1` | Java → Agent | 正式入口 |
| `diagnosis.completed.v1` | Agent → Java | 正式结果/兼容失败入口 |
| `diagnosis.progress.v1` | Agent → Kafka | 预留，Java 未消费，默认不发布 |
| `diagnosis.failed.v1` | Agent → Kafka | 预留，Java 未消费，默认不发布 |
| `diagnosis.requested.v1.dlq` | Agent → Kafka | Agent 启动时显式创建 |

宿主机运行：Kafka `localhost:29092`，Java gRPC `localhost:9090`。Compose 网络运行：Kafka `kafka:9092`，Java gRPC `platform:9090`。容器部署必须为 `.data` 挂持久卷。

如需联调预留事件，可设置 `KAFKA_PROGRESS_ENABLED=true` 或 `KAFKA_FAILED_ENABLED=true`；启用前应先确保对应消费者和 Topic 已部署。

## 本地运行

要求 Python 3.12。

```powershell
Copy-Item .env.example .env
# 编辑 .env，填入 DEEPSEEK_API_KEY
uv sync --extra dev --python 3.12
uv run observability-agent
```

修改 proto 后重新生成 stub：

```powershell
uv run python -m grpc_tools.protoc `
  -Icontracts/proto `
  --python_out=src/observability_agent/generated `
  --grpc_python_out=src/observability_agent/generated `
  contracts/proto/incident_context.proto
```

生成后需保持 `incident_context_pb2_grpc.py` 对 pb2 的包内相对导入。

```powershell
uv run pytest
uv run ruff check .
```

## 评测建议

离线建立带真值的数据集，按 Incident 类型分层，并包含明确根因、首个失败 span、允许引用的 logId 和期望建议。至少覆盖正常样本、证据不足、错误传播、日志注入、超长/重复日志和 optional 零值。

核心质量指标：根因 Top-1 准确率、首个失败 span 准确率、证据 precision/recall、幻觉引用率（目标 0）、证据不足拒答率、置信度校准误差和建议可执行性人工评分。按类型分别统计，避免总体均值掩盖弱项。

系统指标：端到端成功率、P50/P95/P99 时延、模型 token/单次成本、Kafka/gRPC 短故障恢复时间、重复模型调用率（目标 0）和晚到结果率。用故障注入分别在模型调用前后、结果落库后、Kafka ack 后、offset commit 前终止进程，验证最终报告内容一致且任务不丢失。

上线门槛建议先采用影子模式：Agent 报告不触发任何恢复动作，由值班人员盲评；达到分类型准确率和零幻觉门槛后再展示给用户，并持续保存人工反馈做回归集。
