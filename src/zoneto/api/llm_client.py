"""LLM client abstraction for the narrator layer."""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Protocol, runtime_checkable


@runtime_checkable
class LLMClient(Protocol):
    """Minimal interface for an LLM completion backend."""

    def complete(
        self, system: str, messages: list[dict[str, str]], max_tokens: int
    ) -> str: ...

    def stream(
        self, system: str, messages: list[dict[str, str]], max_tokens: int
    ) -> Iterator[str]: ...


class AnthropicClient:
    """Anthropic API client using the official SDK.

    Provider and model are controlled via environment variables so the
    deployment can swap without code changes:
      ZONETO_LLM_PROVIDER  (default: anthropic — currently the only real impl)
      ZONETO_LLM_MODEL     (default: claude-haiku-4-5-20251001)

    Args:
        api_key: Anthropic API key. Falls back to ANTHROPIC_API_KEY env var.
        model: Model ID override. Falls back to ZONETO_LLM_MODEL env var.
    """

    _DEFAULT_MODEL = "claude-haiku-4-5-20251001"

    def __init__(
        self,
        api_key: str | None = None,
        model: str | None = None,
    ) -> None:
        import anthropic  # noqa: PLC0415 — lazy to avoid import at module level

        resolved_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        self._client = anthropic.Anthropic(api_key=resolved_key)
        self._model = (
            model
            or os.environ.get("ZONETO_LLM_MODEL", self._DEFAULT_MODEL)
        )

    def complete(
        self, system: str, messages: list[dict[str, str]], max_tokens: int
    ) -> str:
        """Return the full completion as a string."""
        response = self._client.messages.create(
            model=self._model,
            max_tokens=max_tokens,
            system=system,
            messages=messages,  # type: ignore[arg-type]
        )
        block = response.content[0]
        return block.text if hasattr(block, "text") else ""

    def stream(
        self, system: str, messages: list[dict[str, str]], max_tokens: int
    ) -> Iterator[str]:
        """Yield text delta chunks as they arrive."""
        with self._client.messages.stream(
            model=self._model,
            max_tokens=max_tokens,
            system=system,
            messages=messages,  # type: ignore[arg-type]
        ) as stream:
            yield from stream.text_stream


class FakeLLMClient:
    """Deterministic stand-in for tests — returns fixed responses."""

    def __init__(self, response: str = "Fake LLM response.") -> None:
        self._response = response

    def complete(
        self, system: str, messages: list[dict[str, str]], max_tokens: int
    ) -> str:
        return self._response

    def stream(
        self, system: str, messages: list[dict[str, str]], max_tokens: int
    ) -> Iterator[str]:
        yield self._response


def make_llm_client() -> AnthropicClient:
    """Construct the configured LLM client from environment variables.

    Only Anthropic is supported in v1. Provider selection via
    ZONETO_LLM_PROVIDER is a no-op stub for future extensibility.
    """
    return AnthropicClient()
