"""LLM prompt composer and narration layer for bylaw evaluation."""

from __future__ import annotations

import math
import re

from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    UserPromptPart,
)

from zoneto.analytics import planning_act
from zoneto.analytics.bylaw_index import Chunk
from zoneto.analytics.compliance import Severity, Violation, effective_height_m
from zoneto.analytics.extract import ProjectFeatures
from zoneto.analytics.use_classifier import use_matches_zone
from zoneto.api.desc_similarity import DescriptionSimilarity
from zoneto.api.site_context import SiteContext
from zoneto.llm.schemas import NarrationResult


class CommunityBenefitsContext(BaseModel):
    """Section 37 community-benefit aggregates over a recent year window."""

    n_comps: int
    median_monetary: float
    p25_monetary: float
    p75_monetary: float
    common_benefit_types: list[str] = []


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
6. You must return two things: a markdown compliance summary (`summary_md`)
   and a `confidence` score from 0 to 100. The confidence is your assessment
   of the proposal's likelihood of obtaining planning approval (through
   whatever process is required):
     90–100: as-of-right or near-certain approval — no rezoning needed
     70–89:  strong likelihood — well-supported rezoning, backed by direct
             comparable approvals (especially same-zone) or good zone fit
     50–69:  probable — rezoning required with solid precedent in similar
             zones or corridors; no fundamental incompatibilities
     30–49:  uncertain — rezoning required with mixed signals, data gaps,
             or modest violation severity with limited comparable support
     10–29:  low probability — extreme violations (e.g. units proposed 10x
             zoning limit), no same-zone approved precedent, poor zone fit
     0–9:    effectively prohibited — categorically excluded by by-law
   IMPORTANT CALIBRATION RULES:
   - OZ/SA applications by definition require rezoning. NEEDS_REZONING
     violations are expected and do NOT by themselves lower confidence.
     Instead they confirm that an OZ application is the correct path.
   - MANDATORY CAP — EXTREME VIOLATIONS: If ANY violation shows proposed
     value ≥ 3× the zone limit (e.g. 17 storeys proposed, 3 allowed =
     5.7×; or 258 units proposed, 4 allowed = 64×), you MUST set
     confidence ≤ 30. Set confidence 10–30 based on violation severity.
     Violations whose observed value is inferred (height estimated at
     3m/storey) and FSI/density violations count for this check too.
     Exception — approved same-zone precedent: if the comparable
     application outcomes include a same-zone OZ that is Council-approved
     with similarity ≥ 90%, the cap does NOT apply. Proceed to Step 2.
     The approved OZ is direct evidence this violation profile is
     approvable in this zone. Treat it as a strong upward signal in Step 3.
   - Cross-zone comparables: a comparable from a different zone type does
     NOT establish same-zone precedent. A CR-zone approval does not
     support an RM-zone proposal (or vice versa).
   STEP-BY-STEP CONFIDENCE ASSIGNMENT (follow exactly, in order):
   Step 1 — Extreme violation check:
     If ANY violation is ≥ 3× zone limit → confidence 10–30. STOP.
     Exception: same-zone OZ approved at ≥ 90% similarity → skip cap,
     proceed to Step 2 and apply approved comparable as +8 in Step 3.
   Step 2 — Base band from zone/violation profile (pick ONE):
     CRITICAL: INFORMATIONAL violations (heritage register, MTSA flag, zoning
     exception notes) are context — NOT compliance failures. They do NOT lower
     the base band. Only NEEDS_VARIANCE and NEEDS_REZONING violations count.
     A. Zero STRUCTURAL violations (NEEDS_VARIANCE / NEEDS_REZONING) + compatible
        zone use + at least one numeric limit VERIFIED → base band 70–80.
        "Compatible" means the Use compatibility line shows "permitted".
        "Verified" means the Limits line states a numeric limit (storeys,
        height, units, or FSI) that an extracted project feature respects.
        INFORMATIONAL violations are fine here.
     B. Zero structural violations + compatible use, but NO numeric limit could
        be checked against the proposal (Limits line says "not available from
        structured data", or no comparable feature was extracted) → 55–65.
        Zero violations here means "nothing was checkable", not "as-of-right" —
        state the missing limits as a data gap in the prose.
     C. Zero structural violations + mismatched or unclear zone use → 55–65.
     D. Structural violations present (one or more NEEDS_VARIANCE or
        NEEDS_REZONING) → 35–55.
   Step 3 — Adjust within the base band ONLY (max ±8 total):
     Up to +8: MTSA, secondary plan, or site-specific exception.
     Up to -8: comparable appeal rate ≥ 25%, or heritage district.
     The floor of the Step 2 band is an absolute floor. Appeal rate
     and data gaps CANNOT move the score below that floor.
     DATA GAPS ARE NOT PENALTIES — mention them as caveats in the
     prose summary but do NOT subtract from the confidence score.
   - The "Statutory process & appeal route (Planning Act)" section is
     CONTEXT for the prose only. Requiring an OPA/ZBA, a minor variance,
     or having an OLT appeal route does NOT by itself lower confidence —
     it just names the correct path. Do NOT adjust confidence for it.
   The confidence is a separate numeric field — do NOT write it inside the
   summary text.
"""


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


_OPA_RE = re.compile(r"official plan amendment|\bOPA\b", re.I)

# Plain-language labels for the magnitude_band ordinal classes (retrieval_eval).
_MAGNITUDE_LABELS = {
    "small": "low-rise",
    "medium": "mid-rise",
    "large": "high-rise",
    "xlarge": "tower",
}


def _format_statutory_process(
    violations: list[Violation],
    extracted: ProjectFeatures,
    description_similarity: DescriptionSimilarity | None = None,
    description: str | None = None,
) -> str:
    """Render the Planning Act process/appeal/timeline block for the prompt.

    Derives the primary provincial process from the violation severities (who
    decides, appeal route, statutory non-decision clock, and how to read a
    comparable appeal rate post-Bill 23/185) and then lists any ORTHOGONAL
    processes the proposal also triggers (site plan, subdivision, consent,
    rental replacement). This is CONTEXT only — it never changes CONFIDENCE.
    """
    path = planning_act.path_for_violations(violations)
    # Prefer the same-zone appeal rate: the pooled rate is computed over a
    # text-similar comp set that is only weakly same-zone (~19% concordance, see
    # scripts/comps_eval.py), so it must not drive the appeal-risk framing. Fall
    # back to the pooled rate only when no same-zone rate is available.
    appeal_rate = None
    if description_similarity:
        appeal_rate = description_similarity.zone_matched_appeal_rate
        if appeal_rate is None:
            appeal_rate = description_similarity.appeal_rate
    # OZ filings can be a standalone ZBA (90-day clock) or combined with an OPA
    # (120). An OPA is signalled by either an explicit mention in the description or
    # an Official-Plan non-conformity (op_use_nonconforming) — the latter *is* the
    # statutory trigger for an OPA — so the rezoning line shows the right floor.
    op_nonconforming = any(v.rule_id == "op_use_nonconforming" for v in violations)
    is_combined = (
        bool(description and _OPA_RE.search(description)) or op_nonconforming or None
    )
    text = planning_act.format_statutory_context(
        path, comparable_appeal_rate=appeal_rate, is_combined=is_combined
    )
    extras = [
        planning_act.ADDITIONAL_PROCESS[k].process_label
        + f" ({planning_act.ADDITIONAL_PROCESS[k].act_section})"
        for k in planning_act.additional_processes(extracted)
    ]
    if extras:
        text += (
            " Likely also required (orthogonal to zoning): " + "; ".join(extras) + "."
        )
    return text


def _format_chunks(chunks: list[Chunk]) -> str:
    if not chunks:
        return "No relevant by-law sections retrieved."
    parts = []
    for c in chunks:
        header = f"[§{c.section_number} {c.section_title} — {c.source_file}]"
        parts.append(f"{header}\n{c.text}")
    return "\n\n---\n\n".join(parts)


def _format_site(site: SiteContext, proposed_use: str | None = None) -> str:
    zone = site.zoning_class or "unknown"
    use_cat = site.permitted_use_category or "unknown"
    match = use_matches_zone(proposed_use, site.permitted_use_category)
    if match == 1:
        use_compat = f"permitted ('{proposed_use}' is allowed in this zone)"
    elif match == 0:
        use_compat = (
            f"NOT permitted ('{proposed_use}' violates zone use category"
            " — see violations)"
        )
    else:
        use_compat = "unknown (insufficient data to determine compatibility)"
    flags = []
    if site.in_heritage_register:
        flags.append("Heritage Register")
    if site.in_heritage_district:
        flags.append("Heritage Conservation District")
    if site.in_mtsa:
        flags.append("Major Transit Station Area (MTSA)")
    if site.in_secondary_plan:
        flags.append(f"Secondary Plan: {site.secondary_plan_name}")
    if site.zoning_holding:
        flags.append("Holding (H) symbol")
    if site.zoning_exception:
        exc_no = site.zoning_exception_no
        exc_label = f"No. {exc_no}" if exc_no else "site-specific"
        flags.append(f"Site-specific Zoning Exception ({exc_label})")
    limits = []
    if site.zoning_max_storeys:
        limits.append(f"max {site.zoning_max_storeys} storeys")
    if site.zoning_max_height_m:
        limits.append(f"max {site.zoning_max_height_m}m height")
    if site.zoning_max_units:
        limits.append(f"max {site.zoning_max_units} units")
    if site.zoning_max_density:
        limits.append(f"max FSI {site.zoning_max_density}")
    return (
        f"Zone: {zone} | Permitted use category: {use_cat}\n"
        f"Use compatibility: {use_compat}\n"
        f"Limits: {', '.join(limits) or 'not available from structured data'}\n"
        f"Flags: {', '.join(flags) or 'none'}"
    )


def _format_extracted(extracted: ProjectFeatures) -> str:
    parts = []
    if extracted.proposed_storeys is not None:
        parts.append(f"{extracted.proposed_storeys} storeys")
    if getattr(extracted, "proposed_height_m", None) is not None:
        parts.append(f"{extracted.proposed_height_m}m height")
    if extracted.proposed_units is not None:
        parts.append(f"{extracted.proposed_units} units")
    if extracted.proposed_use:
        parts.append(f"proposed use: {extracted.proposed_use}")
    if extracted.has_ground_floor_retail:
        parts.append("ground-floor retail")
    if getattr(extracted, "building_type", None):
        parts.append(f"building type: {extracted.building_type}")
    return ", ".join(parts) if parts else "no structured features extracted"


def _format_description_similarity(
    sim: DescriptionSimilarity | None,
    *,
    site_zoning_class: str | None = None,
) -> str:
    """Format description similarity context for the LLM prompt.

    Returns empty string when sim is None or n_similar==0 (no useful signal).

    Approval rates (overall, zone-matched, zone base) are intentionally excluded:
    dev_approved labels only cover ~9.6% of applications (those that reached an
    AIC decision milestone), creating ~97% survivorship bias across all zones.
    Appeal rate is the honest signal and is surfaced instead.

    Outcome for the single best comparable is surfaced when its zone matches the
    query site's zone (or is unknown). Cross-zone comparables show zone only.
    """
    if not sim:
        return ""
    n = sim.n_similar
    if not n:
        return ""
    parts = [
        f"Description similarity analysis found {n} comparable OZ/SA applications "
        "with similar project descriptions."
    ]
    # Surface the closest comparable's zone and — when known — its outcome.
    # Outcome for a single high-similarity match is a direct data point, not an
    # aggregate rate, so it is not subject to the same survivorship bias as
    # overall approval rates. Only surface when:
    #   - similarity >= 0.90, AND
    #   - comparable zone matches query site zone (or is unknown)
    # Cross-zone comparables show zone only (no outcome), since the outcome
    # from a different zone type is not directly applicable.
    top_matches = sim.top_matches
    if top_matches:
        best = top_matches[0]
        if best.similarity >= 0.90:
            app_type = best.application_type or "OZ"
            best_zone = best.zoning_class
            zone_note = f" (zoning: {best_zone})" if best_zone else ""
            # Only surface outcome when zone matches or is unknown (no cross-zone noise)
            zone_matches = (
                best_zone is None
                or site_zoning_class is None
                or best_zone == site_zoning_class
            )
            if zone_matches:
                approved = best.dev_approved
                appealed = best.dev_appealed
                if approved == 1:
                    outcome: str | None = "Council-approved"
                    if appealed == 0:
                        outcome += ", not appealed"
                elif approved == 0:
                    outcome = "not approved"
                else:
                    outcome = None
                outcome_note = f" — outcome: {outcome}" if outcome else ""
            else:
                outcome_note = ""
            parts.append(
                f"The closest comparable is {app_type}{zone_note} "
                f"with similarity {best.similarity:.0%}{outcome_note}. "
                "Use the zone of this comparable to judge cross-zone relevance."
            )
    # Appeal rate is the differentiable signal — not subject to survivorship bias.
    appeal_rate = sim.appeal_rate
    if appeal_rate is not None:
        pct = round(appeal_rate * 100)
        parts.append(
            f"Across all {n} comparables, the appeal rate is {pct}%. "
            "A low appeal rate suggests low adversarial risk; "
            "a high rate suggests elevated OLT exposure."
        )
    # Zone-matched analysis: count + appeal rate for same-zone comparables.
    # Zone-matched appeal rate is more targeted than overall — filters to the
    # same zone type, removing cross-zone noise. Not subject to survivorship bias.
    zone_n = sim.zone_matched_n_similar
    if zone_n is not None:
        if zone_n == 0:
            parts.append(
                "Zone-matched analysis: no description-similar applications in the "
                "same zoning category were found. The comparables above are from "
                "different zone types — they do not establish same-zone precedent."
            )
        else:
            zm_appeal = sim.zone_matched_appeal_rate
            zm_type = sim.zone_matched_application_type
            zm_mag = sim.zone_matched_magnitude
            if zm_appeal is not None:
                zm_pct = round(zm_appeal * 100)
                # Build the comparability qualifier from whichever axes were
                # applied: always zone, plus process type and/or built-form scale.
                type_word = f"{zm_type} " if zm_type else ""
                quals = ["zone"]
                if zm_type:
                    quals.append("process type")
                if zm_mag:
                    quals.append(f"built-form scale ({_MAGNITUDE_LABELS.get(zm_mag)})")
                if len(quals) == 1:
                    qual_phrase = quals[0]
                else:
                    qual_phrase = ", ".join(quals[:-1]) + " and " + quals[-1]
                parts.append(
                    f"Zone-matched analysis: {zone_n} similar {type_word}"
                    f"application(s) in the same {qual_phrase}, appeal rate "
                    f"{zm_pct}% (more representative than the overall rate)."
                )
            else:
                parts.append(
                    f"Zone-matched analysis: {zone_n} similar application(s) "
                    "found in the same zoning category."
                )
    return " ".join(parts)


def _format_community_benefits(cb: CommunityBenefitsContext | None) -> str:
    if not cb:
        return ""
    n = cb.n_comps
    median = cb.median_monetary
    p25 = cb.p25_monetary
    p75 = cb.p75_monetary
    types = ", ".join(cb.common_benefit_types or []) or "various"
    return (
        f"Section 37 Community Benefits — based on {n} comparable Toronto rezonings:\n"
        f"  Median contribution: ${median:,.0f}  (range: ${p25:,.0f}–${p75:,.0f})\n"
        f"  Common benefits: {types}"
    )


def _format_data_gaps(data_gaps: list[str]) -> str:
    if not data_gaps:
        return ""
    lines = [
        "The following information is unavailable from open data and limits "
        "the completeness of this assessment:"
    ]
    for gap in data_gaps:
        lines.append(f"- {gap}")
    return "\n".join(lines)


_SAME_SITE_RADIUS_M = 250.0


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Approximate great-circle distance in metres."""
    r = 6_371_000.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = math.sin(d_lat / 2) ** 2 + (
        math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(d_lon / 2) ** 2
    )
    return r * 2 * math.asin(math.sqrt(a))


def _has_approved_precedent(
    description_similarity: DescriptionSimilarity | None,
    site_zone: str | None,
) -> bool:
    """True when top_matches has a same-zone Council-approved OZ at ≥ 90% similarity
    AND the comparable is within _SAME_SITE_RADIUS_M of the query location.

    This is the escape hatch for the extreme-violation cap: if the description
    is a near-identical match to an application that was actually approved at the
    same site in the same zone, the violation magnitude is not a reliable rejection
    signal. Zone-unknown comparables (either side None) are accepted.

    Distance gate: when query_lat/query_lon are in the similarity dict AND the
    comparable has lat/lon, the comparable must be within _SAME_SITE_RADIUS_M.
    If query coords are absent (caller didn't pass them), distance is not checked.
    If comparable coords are absent, the match is rejected (cannot confirm proximity).
    """
    if not description_similarity:
        return False
    query_lat: float | None = description_similarity.query_lat
    query_lon: float | None = description_similarity.query_lon
    for m in description_similarity.top_matches:
        if m.similarity < 0.90:
            break  # list is sorted desc — no point continuing
        if m.dev_approved != 1:
            continue
        comp_zone = m.zoning_class
        zone_matches = comp_zone is None or site_zone is None or comp_zone == site_zone
        if not zone_matches:
            continue
        if query_lat is not None and query_lon is not None:
            comp_lat: float | None = m.lat
            comp_lon: float | None = m.lon
            if comp_lat is None or comp_lon is None:
                continue  # comparable not geocoded — can't confirm proximity
            if (
                _haversine_m(query_lat, query_lon, comp_lat, comp_lon)
                > _SAME_SITE_RADIUS_M
            ):
                continue
        return True
    return False


def _limits_verified(extracted: ProjectFeatures, site: SiteContext) -> bool:
    """True when at least one encoded zoning limit was checked against the proposal.

    Zero violations only means "as-of-right" when something was actually
    verified: an extracted value (storeys, height — stated or storeys-inferred,
    units, FSI) paired with an encoded limit. A zone polygon with all-null
    limits verifies nothing, no matter what the proposal says.
    """
    pairs = (
        (extracted.proposed_storeys, site.zoning_max_storeys),
        (effective_height_m(extracted), site.zoning_max_height_m),
        (extracted.proposed_units, site.zoning_max_units),
        (extracted.proposed_fsi, site.zoning_max_density),
    )
    return any(prop is not None and limit for prop, limit in pairs)


def _apply_confidence_overrides(
    score: int,
    violations: list[Violation],
    site: SiteContext,
    extracted: ProjectFeatures,
    description_similarity: DescriptionSimilarity | None = None,
) -> int:
    """Apply deterministic floor/cap rules after LLM scoring.

    Floors (applied first):
      - Use is permitted in zone + no structural violations → min 70 when at
        least one encoded limit was verified against the proposal (as-of-right
        path); min 55 when no limit could be verified (compatible use, limits
        unknown — refusal drivers may be invisible). Informational violations
        (heritage, MTSA, exceptions) do not block these floors.
      - No violations + mixed/commercial-residential category string → same
        70/55 floor (fallback for zone categories not in _ZONE_ALLOWED).
      - Same-zone Council-approved comparable at ≥ 90% similarity → min 55.

    Cap (skipped when approved precedent exists):
      - Proposed ≥ 3× zoning limit (storeys, units, height, or FSI) → max 30.
        Height uses the stated metres or a 3m/storey inference from storeys.
        Exemption: if a same-zone approved comparable matches at ≥ 90%,
        the cap does not apply — the precedent overrides the ratio rule.
    """
    structural = [v for v in violations if v.severity != Severity.INFORMATIONAL]
    as_of_right_floor = 70 if _limits_verified(extracted, site) else 55
    if (
        use_matches_zone(extracted.proposed_use, site.permitted_use_category) == 1
        and not structural
    ):
        score = max(score, as_of_right_floor)

    permitted = (site.permitted_use_category or "").lower()
    if not violations and any(
        k in permitted for k in ("mixed", "commercial residential")
    ):
        score = max(score, as_of_right_floor)

    has_precedent = _has_approved_precedent(description_similarity, site.zoning_class)
    if has_precedent:
        score = max(score, 55)

    max_storeys = site.zoning_max_storeys or 0
    max_units = site.zoning_max_units or 0
    max_height = site.zoning_max_height_m or 0
    max_fsi = site.zoning_max_density or 0
    prop_storeys = extracted.proposed_storeys or 0
    prop_units = extracted.proposed_units or 0
    prop_height = effective_height_m(extracted) or 0
    prop_fsi = extracted.proposed_fsi or 0
    extreme = (
        (max_storeys and prop_storeys / max_storeys >= 3.0)
        or (max_units and prop_units / max_units >= 3.0)
        or (max_height and prop_height / max_height >= 3.0)
        or (max_fsi and prop_fsi / max_fsi >= 3.0)
    )
    if extreme and not has_precedent:
        score = min(score, 30)

    return score


def narrate_evaluation(
    site: SiteContext,
    extracted: ProjectFeatures,
    violations: list[Violation],
    chunks: list[Chunk],
    agent: Agent[None, NarrationResult],
    *,
    description: str | None = None,
    data_gaps: list[str] | None = None,
    description_similarity: DescriptionSimilarity | None = None,
    community_benefits: CommunityBenefitsContext | None = None,
) -> tuple[str, int]:
    """Generate a markdown compliance summary and confidence score.

    The agent receives structured context and is constrained to narrate only
    what the rule engine and retrieval system found — it cannot invent rules.
    It returns a typed :class:`NarrationResult`; the confidence is then passed
    through the deterministic ``_apply_confidence_overrides`` clamp.

    Returns:
        (summary_markdown, confidence_score) where confidence_score is 0–100.
    """
    gaps_section = _format_data_gaps(data_gaps or [])
    site_zone = site.zoning_class
    statutory_section = _format_statutory_process(
        violations, extracted, description_similarity, description
    )
    sim_section = _format_description_similarity(
        description_similarity, site_zoning_class=site_zone
    )
    cb_section = _format_community_benefits(community_benefits)
    desc_section = f"\n## Project description\n{description}" if description else ""
    user_content = f"""\
## Site context
{_format_site(site, proposed_use=extracted.proposed_use)}

## Extracted project features
{_format_extracted(extracted)}{desc_section}

## Compliance violations (authoritative — do not contradict)
{_format_violations(violations)}

## Statutory process & appeal route (Planning Act) — context only, not scored
{statutory_section}

## Relevant By-law 569-2013 sections (retrieved by semantic search)
{_format_chunks(chunks)}
{("" if not sim_section else ("\n## Comparable application outcomes\n" + sim_section))}
{
        (
            ""
            if not cb_section
            else ("\n## Section 37 Community Benefits Context\n" + cb_section)
        )
    }
{
        (
            ""
            if not gaps_section
            else (
                "\n## Known data gaps (do not speculate beyond these)\n" + gaps_section
            )
        )
    }

---

Write a concise compliance summary (4–8 sentences maximum) in plain markdown. Explain:
1. What the violations mean in practical terms for the applicant.
2. What path forward is most likely (as-of-right adjustment, minor variance,
   or rezoning), grounded in the Statutory process & appeal route section above —
   name the Planning Act process, who decides, and the appeal route/timeline.
3. Any important context from the retrieved by-law sections above.
4. If comparable application outcomes are listed above, factor the appeal rate
   of similar projects into your confidence assessment.
5. If data gaps are listed above, note that the assessment is limited by those
   gaps and recommend the applicant supply the missing information.

Do not repeat the violations verbatim — explain them in plain language.
Do not invent information not present in the context above.

When setting the `confidence` field, follow the three-step rule from the system prompt:
Step 1: Check each violation ratio (including inferred-height and FSI violations).
        If ANY exceeds 3×: set confidence 10–30, stop.
        Exception: if a same-zone OZ comparable appears with similarity ≥ 90%
        AND Council-approved status, skip the cap — proceed to Steps 2 and 3
        and apply the approved comparable as a +8 upward adjustment in Step 3.
Step 2: Determine base band (70–80 for zero STRUCTURAL violations + compatible zone
        + at least one verified numeric limit; 55–65 for zero structural violations
        with no checkable limit OR a mismatched zone; 35–55 for structural
        violations present). INFORMATIONAL violations never lower the band.
Step 3: Adjust within the base band (±8 max) for MTSA/overlay/appeal rate.
        Appeal rate CANNOT push the score below the Step 2 floor.
Remember: data gaps are caveats, NOT violations. Do not penalise for missing data
beyond the band choice in Step 2.
VERIFY before writing CONFIDENCE:
  - If Use compatibility says "permitted" AND there are no NEEDS_VARIANCE or
    NEEDS_REZONING violations: your score MUST be ≥ 70 when the Limits line shows
    a numeric limit the extracted features respect, and ≥ 55 (band 55–65) when no
    limit could be checked. INFORMATIONAL violations (heritage notes, MTSA flags,
    exception notes) do NOT count against this.

Return the summary in `summary_md` and the score in `confidence`.
"""
    result = agent.run_sync(user_content)
    score = _apply_confidence_overrides(
        result.output.confidence, violations, site, extracted, description_similarity
    )
    return result.output.summary_md, score


def narrate_question(
    question: str,
    site: SiteContext,
    extracted: ProjectFeatures,
    violations: list[Violation],
    retrieved_chunks: list[Chunk],
    history: list[dict[str, str]],
    agent: Agent[None, str],
) -> str:
    """Answer a follow-up question about the evaluated project.

    Constructs a bounded context from site facts, violations, and retrieved
    sections, then calls the agent with the full chat history.

    Returns the answer string.
    """
    context_block = f"""\
## Site context
{_format_site(site, proposed_use=extracted.proposed_use)}

## Extracted project features
{_format_extracted(extracted)}

## Active violations
{_format_violations(violations)}

## Relevant By-law sections for this question
{_format_chunks(retrieved_chunks)}
"""
    ack = "Context noted. What is your question?"
    message_history: list[ModelMessage] = [
        ModelRequest(parts=[UserPromptPart(content=context_block)]),
        ModelResponse(parts=[TextPart(content=ack)]),
    ]
    for turn in history:
        if turn.get("role") == "assistant":
            message_history.append(
                ModelResponse(parts=[TextPart(content=turn.get("content", ""))])
            )
        else:
            message_history.append(
                ModelRequest(parts=[UserPromptPart(content=turn.get("content", ""))])
            )
    result = agent.run_sync(question, message_history=message_history)
    return result.output
