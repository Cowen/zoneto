"""Construct Pydantic AI agents from declarative config.

The narrator's large system prompt stays in ``zoneto.api.narrator`` (it is
coupled to the formatter functions); this module only wires the configured
model + runtime params into ``pydantic_ai.Agent`` instances with typed output.

Models are built with an explicit ``AnthropicProvider`` so construction
tolerates a missing ``ANTHROPIC_API_KEY`` (tests override the model and never
issue a real request); production supplies the real key via the environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from pydantic_ai import Agent
from pydantic_ai.models.anthropic import AnthropicModel
from pydantic_ai.providers.anthropic import AnthropicProvider
from pydantic_ai.settings import ModelSettings

from zoneto.llm.config import AgentConfig, AgentsConfig, load_agents_config
from zoneto.llm.schemas import NarrationResult

EVALUATION_AGENT = "narrator_evaluation"
QUESTION_AGENT = "narrator_question"

_PLACEHOLDER_KEY = "placeholder-no-anthropic-key"


@dataclass(frozen=True)
class NarratorAgents:
    """The agents the application calls, with typed outputs."""

    evaluation: Agent[None, NarrationResult]
    question: Agent[None, str]


def _build_model(cfg: AgentConfig) -> AnthropicModel:
    model_id = (
        cfg.model.split(":", 1)[1] if cfg.model.startswith("anthropic:") else cfg.model
    )
    api_key = os.environ.get("ANTHROPIC_API_KEY") or _PLACEHOLDER_KEY
    return AnthropicModel(model_id, provider=AnthropicProvider(api_key=api_key))


def _settings(cfg: AgentConfig) -> ModelSettings:
    settings: ModelSettings = {"max_tokens": cfg.max_tokens}
    if cfg.temperature is not None:
        settings["temperature"] = cfg.temperature
    return settings


def build_agents(config: AgentsConfig) -> NarratorAgents:
    """Build the narrator agents from a loaded config."""
    from zoneto.api.narrator import (
        _SYSTEM_PROMPT,  # noqa: PLC0415 — avoids import cycle
    )

    eval_cfg = config[EVALUATION_AGENT]
    question_cfg = config[QUESTION_AGENT]
    evaluation = Agent[None, NarrationResult](
        _build_model(eval_cfg),
        output_type=NarrationResult,
        system_prompt=_SYSTEM_PROMPT,
        model_settings=_settings(eval_cfg),
        retries=eval_cfg.retries,
    )
    question = Agent[None, str](
        _build_model(question_cfg),
        output_type=str,
        system_prompt=_SYSTEM_PROMPT,
        model_settings=_settings(question_cfg),
        retries=question_cfg.retries,
    )
    return NarratorAgents(evaluation=evaluation, question=question)


def make_narrator_agents() -> NarratorAgents:
    """Load config from the packaged TOML (or env override) and build agents."""
    return build_agents(load_agents_config())
