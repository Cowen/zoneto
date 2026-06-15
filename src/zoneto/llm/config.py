"""Declarative LLM agent configuration loaded from TOML.

Mirrors the lightweight pydantic-model style of ``zoneto.models.CKANConfig``.
Agents are declared in ``agents.toml`` (shipped inside the package) and read via
stdlib ``tomllib`` — no extra dependency. Resolution order for the config file:

  1. an explicit ``path`` argument, else
  2. the ``ZONETO_AGENTS_CONFIG`` env var, else
  3. the packaged ``zoneto/llm/agents.toml``.

If ``ZONETO_LLM_MODEL`` is set it overrides the ``model`` of every agent, so the
documented deploy knob keeps working.
"""

from __future__ import annotations

import os
import tomllib
from importlib import resources
from pathlib import Path

from pydantic import BaseModel


class AgentConfig(BaseModel):
    """Model and runtime parameters for a single LLM agent."""

    model: str
    max_tokens: int
    retries: int = 2
    temperature: float | None = None


class AgentsConfig(BaseModel):
    """A named collection of agent configs (one per declared agent)."""

    agents: dict[str, AgentConfig]

    def __getitem__(self, name: str) -> AgentConfig:
        return self.agents[name]


def _read_toml_bytes(path: Path | None) -> bytes:
    if path is not None:
        return path.read_bytes()
    env = os.environ.get("ZONETO_AGENTS_CONFIG")
    if env:
        return Path(env).read_bytes()
    return (resources.files("zoneto.llm") / "agents.toml").read_bytes()


def load_agents_config(path: Path | None = None) -> AgentsConfig:
    """Load and validate the agent declarations.

    Args:
        path: Explicit TOML path. When ``None``, falls back to
            ``ZONETO_AGENTS_CONFIG`` and then the packaged default.
    """
    data = tomllib.loads(_read_toml_bytes(path).decode("utf-8"))
    model_override = os.environ.get("ZONETO_LLM_MODEL")
    agents: dict[str, AgentConfig] = {}
    for name, raw in data.items():
        cfg = AgentConfig(**raw)
        if model_override:
            cfg = cfg.model_copy(update={"model": model_override})
        agents[name] = cfg
    return AgentsConfig(agents=agents)
