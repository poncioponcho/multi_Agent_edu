"""
LLM 客户端封装 -- 统一调用 OpenAI API。

支持：
- 分级 System Prompt（对应苏格拉底教学策略）
- 异步调用
- API 异常时自动 fallback 到模板回复

面试要点：
- 为什么抽象 LLMClient：便于切换模型、统一管理异常、方便测试 mock
- fallback 策略：生产环境不能因 LLM 故障导致服务不可用
"""

import logging
from typing import Literal

from openai import AsyncOpenAI

from config.settings import settings

logger = logging.getLogger(__name__)


class LLMClient:
    """统一 LLM 调用客户端。"""

    def __init__(self) -> None:
        self._client: AsyncOpenAI | None = None
        if settings.openai_api_key and settings.openai_api_key not in (
            "",
            "your-openai-api-key-here",
        ):
            # 支持自定义 base_url（DeepSeek 等 OpenAI 兼容端点），未配置时走 OpenAI 官方
            self._client = AsyncOpenAI(
                api_key=settings.openai_api_key,
                base_url=settings.openai_base_url or None,
            )
            logger.info(
                "LLMClient initialized with model=%s, base_url=%s",
                settings.openai_model,
                settings.openai_base_url or "https://api.openai.com/v1",
            )
        else:
            logger.warning("LLMClient: no valid API key, will use template fallback")

    @property
    def is_available(self) -> bool:
        return self._client is not None

    async def chat(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.7,
    ) -> str:
        """
        调用 LLM 生成回复。

        如果 LLM 不可用或调用失败，返回空字符串，
        调用方应使用模板作为 fallback。
        """
        if not self._client:
            return ""

        # 推理模型（DeepSeek v4 等）思考 token 计入输出预算且不可控，
        # 通过 extra_body 禁用/限制思考；不支持该参数的 provider 留空即可
        extra: dict = {}
        if settings.llm_reasoning_effort:
            extra["extra_body"] = {"reasoning_effort": settings.llm_reasoning_effort}

        try:
            response = await self._client.chat.completions.create(
                model=settings.openai_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=temperature,
                # 推理模型的思考 token 也计入输出预算，512 会被推理耗尽导致
                # content 为空（静默降级模板），故放宽
                max_tokens=2048,
                **extra,
            )
            content = response.choices[0].message.content or ""
            logger.info("[LLM] tokens=%s", response.usage.total_tokens if response.usage else "?")
            return content.strip()
        except Exception:
            logger.exception("[LLM] API call failed, will fallback to template")
            return ""

    # ── 预置的 System Prompt ──

    SOCRATIC_SYSTEM_PROMPTS = {
        "beginner": (
            "你是一位耐心的数学导师。学生刚刚开始学习这个知识点。\n"
            "请用最简单的语言和生活化的类比帮助理解。\n"
            "不要直接给答案，而是通过提问引导学生自己发现。"
        ),
        "developing": (
            "你是一位苏格拉底式的数学导师。学生正在学习中，需要引导。\n"
            "请通过提问引导学生思考：\n"
            "1. 问学生已经知道哪些相关知识\n"
            "2. 引导学生发现问题的关键步骤\n"
            "3. 当学生卡住时，给一个关键提示而非答案"
        ),
        "proficient": (
            "你是一位挑战型的数学导师。学生已经比较熟练。\n"
            "请提出更深层的思考问题，引导学生发现知识点之间的联系，给出变式题目。"
        ),
        "mastered": (
            "你是一位高级数学导师。学生已掌握此知识点。\n"
            "请引导学生总结方法论，布置综合挑战题，鼓励费曼学习法。"
        ),
    }

    HINT_SYSTEM_PROMPTS = {
        "metacognitive": (
            "你是一位教学提示专家。请给出一个元认知层面的暗示，\n"
            "引导学生反思自己的思考过程，但不要直接给答案或解题步骤。"
        ),
        "scaffolding": (
            "你是一位教学提示专家。请给出一个脚手架式的引导提示，\n"
            "给出关键概念或一个中间步骤的提示，但不要给出完整答案。"
        ),
        "targeted": (
            "你是一位教学提示专家。学生已经多次尝试失败，\n"
            "请给出具体的解题思路和步骤，但鼓励学生自己重新做一遍。"
        ),
    }

    # 输出格式约束：前端无 LaTeX 渲染时保证公式可读性
    MATH_FORMAT_RULE = (
        "\n\n输出格式要求：数学表达式一律使用纯文本写法，"
        "例如 2x+3=7、x = (-b ± √(b²-4ac)) / (2a)、y ≥ 3。"
        "禁止使用 LaTeX 标记（如 \\( \\) \\[ \\] $ \\frac \\sqrt 等）。"
    )

    async def socratic_teach(
        self,
        level: Literal["beginner", "developing", "proficient", "mastered"],
        context: str,
    ) -> str:
        """苏格拉底式教学：根据学生水平选择 System Prompt。"""
        system = self.SOCRATIC_SYSTEM_PROMPTS.get(level, self.SOCRATIC_SYSTEM_PROMPTS["beginner"])
        return await self.chat(system + self.MATH_FORMAT_RULE, context, temperature=0.7)

    async def generate_hint(
        self,
        level: Literal["metacognitive", "scaffolding", "targeted"],
        context: str,
    ) -> str:
        """分级提示生成。"""
        system = self.HINT_SYSTEM_PROMPTS.get(level, self.HINT_SYSTEM_PROMPTS["metacognitive"])
        return await self.chat(system + self.MATH_FORMAT_RULE, context, temperature=0.6)
