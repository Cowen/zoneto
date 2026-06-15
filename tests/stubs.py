"""Test doubles for the narrator's Pydantic AI agents.

These build real ``pydantic_ai.Agent`` objects backed by ``TestModel`` /
``FunctionModel`` so no network call is made and no API key is required. They
replace the old hand-rolled ``FakeLLMClient`` / ``CapturingFakeLLMClient``.
"""

from __future__ import annotations

from pydantic_ai import Agent
from pydantic_ai.messages import (
    ModelMessage,
    ModelResponse,
    ToolCallPart,
    UserPromptPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.models.test import TestModel

from zoneto.llm.agents import NarratorAgents
from zoneto.llm.schemas import NarrationResult


def stub_eval_agent(
    summary: str = "Summary.", confidence: int = 50
) -> Agent[None, NarrationResult]:
    """An evaluation agent that always returns a fixed NarrationResult."""
    output = NarrationResult(summary_md=summary, confidence=confidence)
    return Agent(
        TestModel(custom_output_args=output),
        output_type=NarrationResult,
    )


def stub_question_agent(answer: str = "Answer.") -> Agent[None, str]:
    """A question agent that always returns fixed answer text."""
    return Agent(TestModel(custom_output_text=answer), output_type=str)


def stub_narrator_agents(
    summary: str = "Summary.", confidence: int = 50, answer: str = "Answer."
) -> NarratorAgents:
    """A full NarratorAgents bundle for wiring into ``app.state.narrator``."""
    return NarratorAgents(
        evaluation=stub_eval_agent(summary, confidence),
        question=stub_question_agent(answer),
    )


def capturing_eval_agent(
    summary: str = "Summary.", confidence: int = 60
) -> tuple[Agent[None, NarrationResult], dict[str, str]]:
    """An evaluation agent that records the user prompt it was called with.

    Returns ``(agent, captured)`` where ``captured["prompt"]`` holds the last
    user-prompt string after a run — the equivalent of the old
    ``CapturingFakeLLMClient.last_messages[0]["content"]``.
    """
    captured: dict[str, str] = {}

    def fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        for msg in reversed(messages):
            for part in msg.parts:
                if isinstance(part, UserPromptPart) and isinstance(part.content, str):
                    captured["prompt"] = part.content
                    break
            else:
                continue
            break
        tool = info.output_tools[0]
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=tool.name,
                    args={"summary_md": summary, "confidence": confidence},
                )
            ]
        )

    return Agent(FunctionModel(fn), output_type=NarrationResult), captured
