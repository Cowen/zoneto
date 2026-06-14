"""Deterministic Planning Act (R.S.O. 1990, c. P.13) reference layer.

Encodes the *provincial* statutory framework that governs Toronto development
applications: which process applies, who decides, what the appeal route is, and
the statutory clock to a non-decision appeal. By-law 569-2013 (handled in
``compliance.py``) is the *municipal* layer; this module is the provincial one.

This is a framing/context layer only. It never feeds the 0-100 confidence number
— see ``specs/2026-06-13-planning-act-integration.md``. The narrator cites it,
``compliance.py`` single-sources the minor-variance tests from it, and
``score.py`` uses it to give every application type a defensible statutory
timeline baseline (the OZ/SA-only survival model cannot).

No network, no I/O — a static table in the spirit of ``compliance._PROHIBITED``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from zoneto.analytics.compliance import Severity, Violation
from zoneto.analytics.extract import ProjectFeatures

# Statutory values reflect the Planning Act as amended through Bill 185 (Cutting
# Red Tape to Build More Homes Act, 2024), carrying the non-decision appeal
# windows introduced by Bill 108/109 (OPA 120 days, ZBA 90 days / 120 if combined
# with an OPA, plan of subdivision 120 days). Third-party appeals of OPA/ZBA were
# removed by Bill 23 (2022) and the removal was cemented/broadened by Bill 185
# (Cutting Red Tape to Build More Homes Act, 2024, royal assent 2024-06-06): only
# the applicant landowner, the Minister/approval authority, and narrow "specified
# persons" (NAV Canada, airport authorities, certain EPA/aggregate holders) may
# appeal. The non-decision day counts (ZBA 90, combined OPA+ZBA 120, minor
# variance appeal 20) were verified 2026-06-13 against Ontario's Citizens' Guide
# to Land Use Planning (ontario.ca/document/citizens-guide-land-use-planning).
# The e-Laws consolidation (ontario.ca/laws/statute/90p13) is JS-rendered and
# could not be machine-fetched; the site-plan (SA) appeal status — which Bill 185
# changed — still warrants a primary-source check. See
# specs/2026-06-13-planning-act-integration.md.
_SOURCE_NOTE = (
    "Planning Act R.S.O. 1990, c. P.13, as amended through Bill 185 (2024); "
    "day counts verified 2026-06-13 vs Ontario's Citizens' Guide to Land Use "
    "Planning"
)


# Planning Act s.45(1): the four tests a Committee of Adjustment must be
# satisfied of before granting a minor variance. They are qualitative — there is
# no statutory percentage threshold for what counts as "minor".
MINOR_VARIANCE_TESTS: tuple[str, ...] = (
    "maintains the general intent and purpose of the Official Plan",
    "maintains the general intent and purpose of the Zoning By-law",
    "is desirable for the appropriate development or use of the land",
    "is minor",
)


@dataclass(frozen=True)
class StatutoryProcess:
    """The Planning Act process a proposal must run, with its appeal exposure."""

    act_section: str  # e.g. "Planning Act s.34 (s.22 if an OPA is also required)"
    process_label: str  # human-readable, e.g. "Zoning By-law Amendment (rezoning)"
    decider: str  # who makes the decision
    appeal_body: str | None  # "Ontario Land Tribunal" or None
    non_decision_appeal_days: int | None  # clock to an applicant non-decision appeal
    third_party_appeal: bool  # can non-applicants appeal? (mostly False post-Bill 23)
    notes: str


# Required process keyed by the compliance verdict the narrator can derive from
# the violation list (see path_for_violations). This is what the *narrator* uses,
# since /evaluate has no stored application_type — only a proposal vs. a zone.
PROCESS_BY_PATH: dict[str, StatutoryProcess] = {
    "as_of_right": StatutoryProcess(
        act_section="Planning Act — no amendment required",
        process_label="As-of-right (building permit only)",
        decider="Toronto Building (Chief Building Official)",
        appeal_body=None,
        non_decision_appeal_days=None,
        third_party_appeal=False,
        notes=(
            "No Planning Act approval is engaged — the proposal conforms to the "
            "in-force zoning. Site Plan Approval (s.41) may still apply, and a "
            "building permit is required."
        ),
    ),
    "minor_variance": StatutoryProcess(
        act_section="Planning Act s.45",
        process_label="Minor Variance",
        decider="Committee of Adjustment",
        appeal_body="Ontario Land Tribunal",
        # COA must hold a hearing — no non-decision clock. s.45 decisions remain
        # appealable by any party who appeared (s.45(12)).
        non_decision_appeal_days=None,
        third_party_appeal=True,
        notes=(
            "The Committee of Adjustment weighs the four s.45(1) tests; there is no "
            "statutory percentage that makes a variance 'minor'. Its decision may be "
            "appealed to the Ontario Land Tribunal within 20 days (s.45(12)) by the "
            "applicant or any party who appeared at the hearing."
        ),
    ),
    "rezoning": StatutoryProcess(
        act_section="Planning Act s.34 (and s.22 where an OPA is also required)",
        process_label="Zoning By-law Amendment (rezoning), often with an OPA",
        decider="City Council",
        appeal_body="Ontario Land Tribunal",
        # 90 days for a standalone ZBA; 120 when combined with an OPA.
        non_decision_appeal_days=120,
        third_party_appeal=False,  # Bill 23/185 removed third-party OPA/ZBA appeals
        notes=(
            "Council decides. An applicant may appeal Council's non-decision to the "
            "Ontario Land Tribunal after 90 days for a standalone ZBA (s.34(11)) or "
            "120 days when an OPA is also required (s.22(7)). Since Bill 23 (2022), "
            "cemented by Bill 185 (2024), third parties (residents, community groups) "
            "can no longer appeal an OPA or ZBA — only the applicant landowner, the "
            "Minister/approval authority, and narrow 'specified persons' (NAV Canada, "
            "airport authorities, certain EPA/aggregate holders) retain appeal rights."
        ),
    ),
    "prohibited": StatutoryProcess(
        act_section="Planning Act — no available approval pathway",
        process_label="Prohibited use",
        decider="n/a",
        appeal_body=None,
        non_decision_appeal_days=None,
        third_party_appeal=False,
        notes=(
            "The use is absolutely excluded from all zones (By-law 569-2013 "
            "§60.20.20.10(1)); no rezoning or variance under the Planning Act can "
            "permit it."
        ),
    ),
}


# Statutory process keyed by Toronto AIC application_type code. Used by the batch
# score path to give every type — including CD/SB/PL, which the OZ/SA-only
# survival model leaves null — a deterministic statutory timeline baseline.
APPLICATION_TYPE_PROCESS: dict[str, StatutoryProcess] = {
    # Official Plan and/or Zoning By-law Amendment.
    "OZ": PROCESS_BY_PATH["rezoning"],
    # Site Plan Approval (Site Plan Control). Bill 185 (2024) removed the applicant
    # non-decision appeal for site plan that Bill 109 had introduced; encoded as no
    # OLT appeal here — re-confirm against the current s.41 consolidation.
    "SA": StatutoryProcess(
        act_section="Planning Act s.41",
        process_label="Site Plan Approval (Site Plan Control)",
        decider="City Planning (delegated approval authority)",
        appeal_body=None,
        non_decision_appeal_days=None,
        third_party_appeal=False,
        notes=(
            "Site Plan Control governs the technical/design details of an otherwise "
            "permitted use. Bill 185 (2024) removed the third-party and applicant "
            "non-decision appeal rights for site plan — re-confirm under the current "
            "s.41 consolidation."
        ),
    ),
    # Draft Plan of Subdivision.
    "SB": StatutoryProcess(
        act_section="Planning Act s.51",
        process_label="Draft Plan of Subdivision",
        decider="City Council (approval authority)",
        appeal_body="Ontario Land Tribunal",
        non_decision_appeal_days=120,  # s.51(34)
        third_party_appeal=False,  # Bill 23 narrowed subdivision appeal rights
        notes=(
            "An applicant may appeal a non-decision to the Ontario Land Tribunal "
            "after 120 days (s.51(34))."
        ),
    ),
    # Draft Plan of Condominium (Planning Act s.51 as applied via the Condominium Act).
    "CD": StatutoryProcess(
        act_section="Planning Act s.51 (via the Condominium Act)",
        process_label="Draft Plan of Condominium",
        decider="City Planning (approval authority)",
        appeal_body="Ontario Land Tribunal",
        non_decision_appeal_days=120,
        third_party_appeal=False,
        notes=(
            "Plan of condominium approval follows the s.51 subdivision process; a "
            "standard (non-conversion) condominium is typically procedural where the "
            "built form is already approved."
        ),
    ),
    # Part Lot Control Exemption.
    "PL": StatutoryProcess(
        act_section="Planning Act s.50",
        process_label="Part Lot Control Exemption",
        decider="City Council (by by-law)",
        appeal_body=None,
        non_decision_appeal_days=None,
        third_party_appeal=False,
        notes=(
            "Part Lot Control is lifted by a Council by-law to allow lot lines within "
            "a registered plan to be redrawn (e.g. to convey townhouse units). It is "
            "not appealable to the Ontario Land Tribunal and is usually a short, "
            "administrative step."
        ),
    ),
    # Minor Variance — Committee of Adjustment under s.45.
    "MV": PROCESS_BY_PATH["minor_variance"],
    # Consent to sever / land severance under s.53 (also the Committee of
    # Adjustment). No non-decision appeal clock; the decision is appealable to the
    # OLT within 20 days (s.53(19), (27)).
    "CO": StatutoryProcess(
        act_section="Planning Act s.53",
        process_label="Consent to Sever (land severance)",
        decider="Committee of Adjustment",
        appeal_body="Ontario Land Tribunal",
        non_decision_appeal_days=None,
        third_party_appeal=True,  # consent decisions remain appealable by any party
        notes=(
            "A consent creates new lots without a plan of subdivision. The Committee "
            "of Adjustment decides; its decision may be appealed to the Ontario Land "
            "Tribunal within 20 days (s.53)."
        ),
    ),
    # TLAB (Toronto Local Appeal Body) is an appeal forum for minor variance and
    # consent decisions, not an application type — intentionally left unmapped so
    # statutory_timeline_days returns None for it.
}


# Processes that are ORTHOGONAL to the zoning-envelope path: a proposal can be
# fully as-of-right on zoning and still trigger these. The zoning path answers
# "does this exceed the envelope?"; these answer "what else does the Planning Act
# (and Toronto's rental-replacement regime) require?". Detected by deterministic
# heuristics in additional_processes() and reported as "likely/may apply".
_RENTAL_REPLACEMENT = StatutoryProcess(
    act_section="City of Toronto Act s.111; Toronto Municipal Code Ch. 667",
    process_label="Rental housing demolition / replacement",
    decider="City Council",
    appeal_body=None,
    non_decision_appeal_days=None,
    third_party_appeal=False,
    notes=(
        "Demolishing six or more rental units triggers rental replacement: the "
        "units must be replaced at similar rents and tenants given relocation "
        "rights. Often a project-defining cost — confirm the existing rental "
        "tenure before proceeding."
    ),
)

ADDITIONAL_PROCESS: dict[str, StatutoryProcess] = {
    "site_plan": APPLICATION_TYPE_PROCESS["SA"],
    "subdivision": APPLICATION_TYPE_PROCESS["SB"],
    "condominium": APPLICATION_TYPE_PROCESS["CD"],
    "consent": APPLICATION_TYPE_PROCESS["CO"],
    "part_lot_control": APPLICATION_TYPE_PROCESS["PL"],
    "rental_replacement": _RENTAL_REPLACEMENT,
}

_SUBDIVISION_RE = re.compile(r"\bsubdivision\b|draft plan of subdivision", re.I)
_PART_LOT_RE = re.compile(r"part[- ]lot control", re.I)
_CONSENT_RE = re.compile(r"consent to sever|land severance|\bseverance\b", re.I)
_CONDO_RE = re.compile(r"condominium|\bcondo\b", re.I)
_RENTAL_REPLACE_RE = re.compile(
    r"rental replacement|replace\w*\s+(?:the\s+)?\w*\s*rental|"
    r"demol\w+\s+(?:\w+\s+){0,4}rental|rental\s+(?:\w+\s+){0,2}demol\w+",
    re.I,
)
# Proposed uses / building types that almost always need Site Plan Approval.
_SPA_USES = {"commercial", "mixed_use", "employment", "institutional"}
_SPA_BUILDING_TYPES = {"apartment", "mixed", "townhouse"}


def path_for_violations(violations: list[Violation]) -> str:
    """Derive the required Planning Act process from a compliance violation list.

    Precedence mirrors the severity ordering already used in the narrator's
    ``_apply_confidence_overrides``: a prohibited use trumps everything, then any
    NEEDS_REZONING means a rezoning, then any NEEDS_VARIANCE means a minor
    variance, otherwise the proposal is as-of-right. Returns a key into
    ``PROCESS_BY_PATH``.
    """
    if any(v.rule_id == "prohibited_use" for v in violations):
        return "prohibited"
    severities = {v.severity for v in violations}
    if Severity.NEEDS_REZONING in severities:
        return "rezoning"
    if Severity.NEEDS_VARIANCE in severities:
        return "minor_variance"
    return "as_of_right"


def _needs_site_plan(extracted: ProjectFeatures) -> bool:
    """Heuristic: Site Plan Control applies to most development beyond a small
    house. Conservative — we say "likely", not "required"."""
    if (extracted.proposed_units or 0) >= 4:
        return True
    if (extracted.proposed_storeys or 0) >= 4:
        return True
    if extracted.proposed_use in _SPA_USES:
        return True
    return extracted.building_type in _SPA_BUILDING_TYPES


def additional_processes(extracted: ProjectFeatures) -> list[str]:
    """Planning Act (and rental-replacement) processes triggered *orthogonally*
    to the zoning path, detected deterministically from the proposal.

    Returns keys into ``ADDITIONAL_PROCESS``. These are advisory ("likely/may
    apply") and never affect the confidence number — they widen the process
    picture beyond "does it bust the zoning envelope?".
    """
    desc = extracted.description or ""
    out: list[str] = []
    if _needs_site_plan(extracted):
        out.append("site_plan")
    if _SUBDIVISION_RE.search(desc):
        out.append("subdivision")
    if _CONDO_RE.search(desc):
        out.append("condominium")
    if _CONSENT_RE.search(desc):
        out.append("consent")
    if _PART_LOT_RE.search(desc):
        out.append("part_lot_control")
    if _RENTAL_REPLACE_RE.search(desc):
        out.append("rental_replacement")
    return out


def statutory_processes(
    violations: list[Violation],
    extracted: ProjectFeatures,
) -> list[tuple[str, StatutoryProcess]]:
    """Full set of triggered processes: the primary zoning path first, then any
    orthogonal processes (site plan, subdivision, consent, …).

    Returns (key, StatutoryProcess) pairs, deduplicated by process_label so a
    rezoning that also reads as a condominium isn't double-counted.
    """
    primary = path_for_violations(violations)
    pairs: list[tuple[str, StatutoryProcess]] = [(primary, PROCESS_BY_PATH[primary])]
    seen = {PROCESS_BY_PATH[primary].process_label}
    for key in additional_processes(extracted):
        proc = ADDITIONAL_PROCESS[key]
        if proc.process_label in seen:
            continue
        seen.add(proc.process_label)
        pairs.append((key, proc))
    return pairs


def rezoning_appeal_days(is_combined: bool | None) -> int:
    """Non-decision appeal clock for a rezoning: 90 for a standalone ZBA, 120
    when an OPA is also required (s.34(11) / s.22(7)).

    ``is_combined`` None means unknown — defaults to 120, the conservative
    (longer) figure, so the statutory floor is never understated.
    """
    return 90 if is_combined is False else 120


def statutory_timeline_days(
    application_type: str | None,
    *,
    is_combined: bool | None = None,
) -> int | None:
    """Statutory non-decision appeal window (days) for a Toronto AIC type code.

    This is the *statutory floor* — the clock after which an applicant may appeal
    Council/approval-authority inaction to the Ontario Land Tribunal — NOT a
    prediction of how long a decision actually takes (that is the survival model's
    empirical p50). Returns None when the type is unknown or has no non-decision
    appeal clock (e.g. site plan, part lot control).

    For OZ, ``is_combined`` selects the standalone-ZBA (90) vs OPA-combined (120)
    clock; a Toronto OZ filing can be either, so the caller passes the
    is_combined_application signal where it has one.
    """
    if not application_type:
        return None
    code = application_type.strip().upper()
    if code == "OZ":
        return rezoning_appeal_days(is_combined)
    process = APPLICATION_TYPE_PROCESS.get(code)
    return process.non_decision_appeal_days if process else None


def format_statutory_context(
    path: str,
    *,
    comparable_appeal_rate: float | None = None,
    is_combined: bool | None = None,
) -> str:
    """Render the Planning Act process block for the narrator prompt.

    ``path`` is a key into ``PROCESS_BY_PATH`` (see ``path_for_violations``).
    For a rezoning, ``is_combined`` selects the 90-day (standalone ZBA) vs
    120-day (OPA-combined) non-decision clock. When a comparable appeal rate is
    supplied, a line interprets it in light of the post-Bill 23/185 appeal regime
    so the narrator does not read historical third-party appeals as forward risk
    that no longer exists.
    """
    proc = PROCESS_BY_PATH.get(path)
    if proc is None:
        return ""
    lines = [
        f"Required process: {proc.process_label} ({proc.act_section}).",
        f"Decided by: {proc.decider}.",
    ]
    if proc.appeal_body:
        days = (
            rezoning_appeal_days(is_combined)
            if path == "rezoning"
            else proc.non_decision_appeal_days
        )
        if days is not None:
            lines.append(
                f"Appeal route: {proc.appeal_body}. An applicant may appeal a "
                f"non-decision after ~{days} days "
                "(statutory floor, not an expected timeline)."
            )
        else:
            lines.append(f"Appeal route: {proc.appeal_body}.")
    else:
        lines.append("Appeal route: none under the Planning Act.")
    lines.append(proc.notes)
    if comparable_appeal_rate is not None and path == "rezoning":
        pct = round(comparable_appeal_rate * 100)
        lines.append(
            f"Note on the {pct}% comparable appeal rate: since Bill 23/185 removed "
            "third-party OPA/ZBA appeals, most remaining appeals are applicant-led "
            "(appealing a refusal or non-decision) and resolve via OLT settlement — "
            "read this as OLT/settlement exposure, not third-party opposition risk."
        )
    return " ".join(lines)
