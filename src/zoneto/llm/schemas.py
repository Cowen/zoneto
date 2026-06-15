"""Typed input/output schemas for LLM agent calls."""

from __future__ import annotations

from pydantic import BaseModel, Field


class NarrationResult(BaseModel):
    """Structured output of the narrator evaluation agent.

    Replaces the legacy free-text ``CONFIDENCE: <n>`` trailing line: the model
    now returns the markdown summary and the confidence score as typed fields.
    The deterministic ``_apply_confidence_overrides`` clamp is applied to
    ``confidence`` afterwards by the caller.
    """

    summary_md: str = Field(
        description=(
            "Concise compliance summary in plain markdown (4–8 sentences). "
            "Explains the violations, the likely planning path, and relevant "
            "by-law context. Does NOT contain a CONFIDENCE line."
        )
    )
    confidence: int = Field(
        ge=0,
        le=100,
        description=(
            "Approval-likelihood score 0–100, assigned via the three-step rule "
            "in the system prompt (extreme-violation cap, base band, ±8 adjust)."
        ),
    )
