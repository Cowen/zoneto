"""LLM prompt composer and narration layer for bylaw evaluation."""

from __future__ import annotations

import re
from typing import Any

from zoneto.analytics.bylaw_index import Chunk
from zoneto.analytics.compliance import Violation
from zoneto.analytics.extract import ProjectFeatures
from zoneto.api.llm_client import LLMClient

_SYSTEM_PROMPT = """\
You are a Toronto urban planning assistant that helps evaluate development
proposals against By-law 569-2013 (City of Toronto Zoning By-law).

STRICT RULES you must always follow:
1. Use ONLY the context provided to you. Do not invent by-law sections,
   numbers, rules, or outcomes that are not in the provided context.
2. When citing, use the form: By-law 569-2013, §10.20.40.10(1)
3. The VIOLATIONS LIST is authoritative. You must not soften, re-interpret,
   or contradict it. You may explain it.
4. If a question cannot be answered from the provided context, say:
   "I cannot answer this from the available context. Please consult
   Toronto City Planning at toronto.ca/city-planning or call 311."
5. Be concise and precise. Use plain language where possible.
6. At the very end of your compliance summary, on its own line, write:
   CONFIDENCE: <0-100>
   where the number is your overall assessment of the proposal's likelihood
   of passing development review as submitted (100 = near-certain as-of-right
   approval, 0 = categorically prohibited or no realistic path forward).
   Base the score on the violations, zone context, and retrieved sections.
   This line must be the absolute last line of your response.
"""


def _parse_confidence(raw: str) -> tuple[str, int | None]:
    """Extract a CONFIDENCE: <n> line from LLM output, scanning backward.

    Returns (summary_markdown, score) where score is None if the line was absent.
    Score is clamped to [0, 100]. Scanning backward (rather than checking only
    the last line) makes extraction robust when the LLM appends a trailing note
    or caveat after the CONFIDENCE line.
    """
    lines = raw.splitlines()
    while lines and not lines[-1].strip():
        lines.pop()
    score: int | None = None
    for i in range(len(lines) - 1, -1, -1):
        m = re.match(r"^\s*CONFIDENCE:\s*(-?\d+)\s*$", lines[i], re.I)
        if m:
            score = min(100, max(0, int(m.group(1))))
            lines.pop(i)
            break
    # Strip trailing blank lines after removing score line
    while lines and not lines[-1].strip():
        lines.pop()
    return "\n".join(lines), score


def _format_violations(violations: list[Violation]) -> str:
    if not violations:
        return "No violations detected from available structured data."
    lines = []
    for v in violations:
        lines.append(
            f"- [{v.severity.value.upper()}] {v.rule_id}: "
            f"{v.observed} (allowed: {v.allowed})\n"
            f"  Reference: {v.section_ref}"
        )
    return "\n".join(lines)


def _format_chunks(chunks: list[Chunk]) -> str:
    if not chunks:
        return "No relevant by-law sections retrieved."
    parts = []
    for c in chunks:
        header = f"[§{c.section_number} {c.section_title} — {c.source_file}]"
        parts.append(f"{header}\n{c.text[:600]}")
    return "\n\n---\n\n".join(parts)


def _format_site(site: dict[str, Any]) -> str:
    zone = site.get("zoning_class") or "unknown"
    use_cat = site.get("permitted_use_category") or "unknown"
    flags = []
    if site.get("in_heritage_register"):
        flags.append("Heritage Register")
    if site.get("in_heritage_district"):
        flags.append("Heritage Conservation District")
    if site.get("in_mtsa"):
        flags.append("Major Transit Station Area (MTSA)")
    if site.get("in_secondary_plan"):
        flags.append(f"Secondary Plan: {site.get('secondary_plan_name')}")
    if site.get("zoning_holding"):
        flags.append("Holding (H) symbol")
    limits = []
    if site.get("zoning_max_storeys"):
        limits.append(f"max {site['zoning_max_storeys']} storeys")
    if site.get("zoning_max_height_m"):
        limits.append(f"max {site['zoning_max_height_m']}m height")
    if site.get("zoning_max_units"):
        limits.append(f"max {site['zoning_max_units']} units")
    if site.get("zoning_max_density"):
        limits.append(f"max FSI {site['zoning_max_density']}")
    return (
        f"Zone: {zone} | Permitted use category: {use_cat}\n"
        f"Limits: {', '.join(limits) or 'not available from structured data'}\n"
        f"Flags: {', '.join(flags) or 'none'}"
    )


def _format_extracted(extracted: ProjectFeatures) -> str:
    parts = []
    if extracted.proposed_storeys is not None:
        parts.append(f"{extracted.proposed_storeys} storeys")
    if extracted.proposed_units is not None:
        parts.append(f"{extracted.proposed_units} units")
    if extracted.proposed_use:
        parts.append(f"proposed use: {extracted.proposed_use}")
    if extracted.has_ground_floor_retail:
        parts.append("ground-floor retail")
    return ", ".join(parts) if parts else "no structured features extracted"


def narrate_evaluation(
    site: dict[str, Any],
    extracted: ProjectFeatures,
    violations: list[Violation],
    chunks: list[Chunk],
    llm_client: LLMClient,
) -> tuple[str, int | None]:
    """Generate a markdown compliance summary and confidence score.

    The LLM receives structured context and is constrained to narrate only
    what the rule engine and retrieval system found — it cannot invent rules.
    The trailing CONFIDENCE: <n> line is parsed out and returned separately.

    Returns:
        (summary_markdown, confidence_score) where confidence_score is 0–100
        or None if the LLM did not emit the expected line.
    """
    user_content = f"""\
## Site context
{_format_site(site)}

## Extracted project features
{_format_extracted(extracted)}

## Compliance violations (authoritative — do not contradict)
{_format_violations(violations)}

## Relevant By-law 569-2013 sections (retrieved by semantic search)
{_format_chunks(chunks)}

---

Write a concise compliance summary (3–6 sentences) in plain markdown. Explain:
1. What the violations mean in practical terms for the applicant.
2. What path forward is most likely (as-of-right adjustment, minor variance,
   or rezoning), citing the specific violations above.
3. Any important context from the retrieved by-law sections above.

Do not repeat the violations verbatim — explain them in plain language.
Do not invent information not present in the context above.
End with a CONFIDENCE line as required by the system rules.
"""
    raw = llm_client.complete(
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
        max_tokens=650,
    )
    return _parse_confidence(raw)


def narrate_question(
    question: str,
    site: dict[str, Any],
    extracted: ProjectFeatures,
    violations: list[Violation],
    retrieved_chunks: list[Chunk],
    history: list[dict[str, str]],
    llm_client: LLMClient,
) -> str:
    """Answer a follow-up question about the evaluated project.

    Constructs a bounded context from site facts, violations, and retrieved
    sections, then calls the LLM with the full chat history.

    Returns the answer string (caller is responsible for streaming if needed).
    """
    context_block = f"""\
## Site context
{_format_site(site)}

## Extracted project features
{_format_extracted(extracted)}

## Active violations
{_format_violations(violations)}

## Relevant By-law sections for this question
{_format_chunks(retrieved_chunks)}
"""
    messages: list[dict[str, str]] = [
        {"role": "user", "content": context_block},
        {"role": "assistant", "content": "Context noted. What is your question?"},
        *history,
        {"role": "user", "content": question},
    ]
    return llm_client.complete(
        system=_SYSTEM_PROMPT,
        messages=messages,
        max_tokens=400,
    )
