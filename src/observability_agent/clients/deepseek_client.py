from pathlib import Path
from typing import Protocol

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from observability_agent.core.config import Settings
from observability_agent.core.exceptions import ModelConfigurationError
from observability_agent.schemas.context import IncidentContext
from observability_agent.schemas.report import ModelDiagnosis


class DiagnosisAnalyzer(Protocol):
    def analyze(self, context: IncidentContext, analysis_plan: list[str]) -> ModelDiagnosis: ...


class DeepSeekDiagnosisAnalyzer:
    def __init__(self, settings: Settings) -> None:
        if settings.deepseek_api_key is None or not settings.deepseek_api_key.get_secret_value():
            raise ModelConfigurationError("DEEPSEEK_API_KEY is required")
        model = ChatOpenAI(
            model=settings.deepseek_model,
            api_key=settings.deepseek_api_key,
            base_url=settings.deepseek_base_url,
            timeout=settings.deepseek_timeout_seconds,
            max_retries=settings.deepseek_max_retries,
            temperature=0,
        )
        self._model = model.with_structured_output(ModelDiagnosis, method="json_mode")
        prompt_path = Path(__file__).parents[1] / "prompts" / "log_analyst.md"
        self._system_prompt = prompt_path.read_text(encoding="utf-8")

    def analyze(self, context: IncidentContext, analysis_plan: list[str]) -> ModelDiagnosis:
        payload = context.model_dump_json(by_alias=True, indent=2)
        plan = "\n".join(f"{index}. {item}" for index, item in enumerate(analysis_plan, 1))
        result = self._model.invoke(
            [
                SystemMessage(content=self._system_prompt),
                HumanMessage(
                    content=(
                        "请严格输出 JSON。\n"
                        f"分析计划：\n{plan}\n\n"
                        f"故障上下文：\n{payload}"
                    )
                ),
            ]
        )
        if isinstance(result, ModelDiagnosis):
            return result
        return ModelDiagnosis.model_validate(result)
