"""Deterministic compliance rule engine for By-law 569-2013."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from zoneto.analytics.extract import ProjectFeatures
from zoneto.analytics.use_classifier import (
    op_use_matches_designation,
    use_matches_zone,
)

if TYPE_CHECKING:
    from zoneto.api.site_context import SiteContext

# By-law 569-2013 §60.20.20.10(1): uses explicitly excluded from all zones,
# including Employment Industrial. These cannot be permitted through rezoning.
_PROHIBITED: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\brefiner(y|ies)\b", re.I), "crude petroleum or coal refinery"),
    (re.compile(r"\bpetroleum\b", re.I), "crude petroleum refinery"),
    (re.compile(r"\bsmelter?\b|\bsmelting\b", re.I), "smelting of metallic ores"),
    (re.compile(r"\bfoundr(y|ies)\b", re.I), "metal foundry"),
    (re.compile(r"\bquarr(y|ies)\b", re.I), "quarry"),
    (
        re.compile(r"\b(coal|ore)\s+(mine|mining)\b|\bcoal\s+mine\b", re.I),
        "coal mine",
    ),
    (re.compile(r"\babattoir\b", re.I), "abattoir"),
    (re.compile(r"\bslaughterhouse\b", re.I), "slaughterhouse (abattoir)"),
    (re.compile(r"\basphalt\s+plant\b", re.I), "asphalt plant"),
    (re.compile(r"\bcement\s+plant\b", re.I), "cement plant"),
    (re.compile(r"\brendering\s+plant\b", re.I), "rendering plant"),
    (
        re.compile(r"\bexplosives?\s+(manufactur|plant|facilit)\b", re.I),
        "explosives manufacturing",
    ),
]


# Conservative storeys -> metres conversion for proposals that state height only
# in storeys. Toronto residential floor-to-floor is typically 3.0-3.3m; 3.0 keeps
# the inference a lower bound so it never overstates a violation.
STOREY_HEIGHT_M = 3.0


def effective_height_m(extracted: ProjectFeatures) -> float | None:
    """Stated height in metres, else a conservative inference from storeys.

    Returns None when neither a height nor a storey count was extracted.
    """
    if extracted.proposed_height_m is not None:
        return extracted.proposed_height_m
    if extracted.proposed_storeys is not None:
        return extracted.proposed_storeys * STOREY_HEIGHT_M
    return None


class Severity(Enum):
    """How much the rule violation deviates from as-of-right permissions."""

    INFORMATIONAL = "informational"  # not a violation, just context
    NEEDS_VARIANCE = "needs_variance"  # minor variance (COA)
    NEEDS_REZONING = "needs_rezoning"  # official plan amendment / rezoning (OZ)


@dataclass
class Violation:
    """A single compliance finding from the rule engine."""

    rule_id: str
    section_ref: str
    observed: str
    allowed: str
    severity: Severity
    suggested_remedy: str


def _planning_act_ref(severity: Severity) -> str:
    """Provincial-statute suffix for a violation's municipal ``section_ref``.

    By-law 569-2013 is the municipal layer; the Planning Act is the provincial
    process that resolves the violation. A variance is granted under s.45; a
    rezoning under s.34 (with s.22 where an OPA is also required).
    """
    if severity == Severity.NEEDS_VARIANCE:
        return "; Planning Act s.45 (minor variance)"
    if severity == Severity.NEEDS_REZONING:
        return "; Planning Act s.34 (zoning by-law amendment) / s.22 (OPA)"
    return ""


def _minor_variance_note() -> str:
    """Plain-language statement of the Planning Act s.45(1) minor-variance tests.

    Replaces the legally false "up to 10% deviation is typically considered
    minor" heuristic: there is no statutory percentage that makes a variance
    "minor" — eligibility turns on four qualitative tests, and Toronto Committee
    of Adjustment panels routinely grant larger variances and refuse smaller
    ones. Single-sourced from ``planning_act.MINOR_VARIANCE_TESTS`` via a
    deferred import (planning_act imports Severity/Violation from this module).
    """
    from zoneto.analytics.planning_act import MINOR_VARIANCE_TESTS

    tests = "; ".join(MINOR_VARIANCE_TESTS)
    return (
        "Whether the Committee of Adjustment grants this turns on the four "
        f"Planning Act s.45(1) tests (the variance: {tests}) — there is no fixed "
        "percentage that makes a variance 'minor'."
    )


def check_compliance(
    extracted: ProjectFeatures,
    site: SiteContext,
    *,
    exception_text: str | None = None,
    site_address: str | None = None,
) -> list[Violation]:
    """Run deterministic compliance checks against site zoning limits.

    Compares extracted project features against structured limits from
    lookup_site_context(). The rule engine is the authoritative compliance
    verdict; the LLM narrator phrases it but does not override it.

    Args:
        extracted: ProjectFeatures from extract_project_features().
        site: SiteContext from lookup_site_context() carrying fields such as
            zoning_class, zoning_max_storeys, zoning_max_units, zoning_max_density,
            permitted_use_category, in_heritage_register, in_heritage_district,
            in_mtsa, zoning_exception, zoning_holding.
        exception_text: Verbatim text of the site's exception schedule (retrieved
            from the bylaw index by the caller). When provided, the exception
            finding quotes the actual provisions and reconciles them against the
            other violations rather than punting back to the user.
        site_address: The subject site's street address. Used to resolve which of
            the exception's address-scoped prevailing by-laws actually apply here.

    Returns:
        List of Violation findings, empty when no issues detected.
    """
    violations: list[Violation] = []

    violations.extend(_check_prohibited_uses(extracted))
    violations.extend(_check_storeys(extracted, site))
    violations.extend(_check_height_m(extracted, site))
    violations.extend(_check_height_inferred(extracted, site))
    violations.extend(_check_units(extracted, site))
    violations.extend(_check_fsi(extracted, site))
    violations.extend(_check_unit_limit_advisory(extracted, site))
    violations.extend(_check_use(extracted, site))
    violations.extend(_check_op_conformity(extracted, site))
    violations.extend(_check_heritage(site))
    violations.extend(_check_mtsa(site))
    violations.extend(_check_trca(site))
    violations.extend(_check_greenbelt(site))
    violations.extend(_check_holding(site))
    # The exception check reconciles against everything flagged before it, so it
    # runs last and receives a snapshot of the prior findings.
    violations.extend(
        _check_exception(site, list(violations), exception_text, site_address)
    )

    return violations


def _check_prohibited_uses(extracted: ProjectFeatures) -> list[Violation]:
    """Check description against By-law §60.20.20.10(1) absolutely prohibited uses.

    These uses cannot be permitted in Toronto through rezoning — they are
    explicitly excluded from all zones including Employment Industrial.
    """
    if not extracted.description:
        return []
    for pattern, label in _PROHIBITED:
        if pattern.search(extracted.description):
            return [
                Violation(
                    rule_id="prohibited_use",
                    section_ref=(
                        "By-law 569-2013 §60.20.20.10(1) — absolutely prohibited uses"
                    ),
                    observed=f"proposed use matches a prohibited type: {label}",
                    allowed=(
                        "this use is explicitly excluded from all zones in Toronto, "
                        "including Employment Industrial — it cannot be permitted "
                        "through rezoning or variance"
                    ),
                    severity=Severity.NEEDS_REZONING,
                    suggested_remedy=(
                        "This use is absolutely prohibited under By-law 569-2013 "
                        f"§60.20.20.10(1) ({label}). No rezoning or variance can "
                        "permit it. Consider an alternative use type or contact "
                        "Toronto City Planning for guidance."
                    ),
                )
            ]
    return []


def _check_storeys(extracted: ProjectFeatures, site: SiteContext) -> list[Violation]:
    max_storeys = site.zoning_max_storeys
    proposed = extracted.proposed_storeys
    if proposed is None or not max_storeys or max_storeys <= 0:
        return []
    if proposed <= max_storeys:
        return []

    excess = proposed - max_storeys
    if excess <= 2:
        severity = Severity.NEEDS_VARIANCE
        remedy = (
            f"Reduce to {max_storeys} storeys to be as-of-right, or apply to "
            f"the Committee of Adjustment for a minor variance. "
            f"{_minor_variance_note()}"
        )
    else:
        severity = Severity.NEEDS_REZONING
        remedy = (
            f"Reduce to {max_storeys} storeys to be as-of-right, or file "
            "an Official Plan Amendment / Rezoning (OZ) application to "
            "increase the permitted height. Comparable OZ approvals nearby "
            "can inform the feasibility."
        )

    return [
        Violation(
            rule_id="storeys_exceed_max",
            section_ref=(
                "By-law 569-2013 — zone-specific building height requirements"
                + _planning_act_ref(severity)
            ),
            observed=f"{proposed} storeys proposed",
            allowed=f"max {max_storeys} storeys",
            severity=severity,
            suggested_remedy=remedy,
        )
    ]


def _check_height_m(extracted: ProjectFeatures, site: SiteContext) -> list[Violation]:
    max_height = site.zoning_max_height_m
    proposed = extracted.proposed_height_m
    if proposed is None or not max_height or max_height <= 0:
        return []
    if proposed <= max_height:
        return []

    excess_pct = (proposed - max_height) / max_height
    if excess_pct <= 0.10:
        severity = Severity.NEEDS_VARIANCE
        remedy = (
            f"Reduce to {max_height}m to be as-of-right, or apply to the "
            f"Committee of Adjustment for a minor variance. {_minor_variance_note()}"
        )
    else:
        severity = Severity.NEEDS_REZONING
        remedy = (
            f"Reduce to {max_height}m to be as-of-right, or file an Official "
            "Plan Amendment / Rezoning (OZ) application to increase the permitted "
            "height. Comparable OZ approvals nearby can inform feasibility."
        )

    return [
        Violation(
            rule_id="height_exceeds_max",
            section_ref=(
                "By-law 569-2013 — zone-specific height overlay (HT_HEIGHT)"
                + _planning_act_ref(severity)
            ),
            observed=f"{proposed}m proposed",
            allowed=f"max {max_height}m",
            severity=severity,
            suggested_remedy=remedy,
        )
    ]


def _check_height_inferred(
    extracted: ProjectFeatures, site: SiteContext
) -> list[Violation]:
    """Check the height limit against a storeys-derived height estimate.

    Many descriptions state height only in storeys ("a 73-storey tower") while
    the zone encodes only a metre limit — leaving the proposal invisible to
    _check_height_m. Only fires when no explicit height was stated AND the zone
    has no storey limit (otherwise _check_storeys already covers the dimension),
    and only when the estimate exceeds the limit by >25% — beyond both the
    minor-variance threshold and the slack in the 3m/storey assumption.
    """
    max_height = site.zoning_max_height_m
    if max_height is None:
        return []
    if extracted.proposed_height_m is not None:
        return []
    if site.zoning_max_storeys is not None:
        return []
    storeys = extracted.proposed_storeys
    if storeys is None:
        return []
    inferred = storeys * STOREY_HEIGHT_M
    if inferred <= max_height * 1.25:
        return []

    return [
        Violation(
            rule_id="height_exceeds_max_inferred",
            section_ref=(
                "By-law 569-2013 — zone-specific height overlay (HT_HEIGHT)"
                + _planning_act_ref(Severity.NEEDS_REZONING)
            ),
            observed=(
                f"≈{inferred:g}m proposed (inferred from {storeys} storeys "
                f"at {STOREY_HEIGHT_M:g}m/storey)"
            ),
            allowed=f"max {max_height}m",
            severity=Severity.NEEDS_REZONING,
            suggested_remedy=(
                f"At a conservative {STOREY_HEIGHT_M:g}m per storey, {storeys} "
                f"storeys is well past the {max_height}m zone limit. File an "
                "Official Plan Amendment / Rezoning (OZ) application, or state "
                "the proposed height in metres if the estimate is wrong."
            ),
        )
    ]


def _check_units(extracted: ProjectFeatures, site: SiteContext) -> list[Violation]:
    max_units = site.zoning_max_units
    proposed = extracted.proposed_units
    if proposed is None or not max_units or max_units <= 0:
        return []
    if proposed <= max_units:
        return []

    return [
        Violation(
            rule_id="units_exceed_max",
            section_ref=(
                "By-law 569-2013 — zone-specific lot requirements (UNITS)"
                + _planning_act_ref(Severity.NEEDS_REZONING)
            ),
            observed=f"{proposed} units proposed",
            allowed=f"max {max_units} units",
            severity=Severity.NEEDS_REZONING,
            suggested_remedy=(
                f"Reduce to {max_units} units to be as-of-right, or apply "
                "for an Official Plan Amendment / Rezoning to increase the "
                "permitted density. Note: the UNITS field in the by-law may "
                "be further constrained by FSI (density) limits."
            ),
        )
    ]


def _check_fsi(extracted: ProjectFeatures, site: SiteContext) -> list[Violation]:
    max_density = site.zoning_max_density
    proposed = extracted.proposed_fsi
    if proposed is None or not max_density or max_density <= 0:
        return []
    if proposed <= max_density:
        return []

    excess_pct = (proposed - max_density) / max_density
    if excess_pct <= 0.10:
        severity = Severity.NEEDS_VARIANCE
        remedy = (
            f"Reduce gross floor area to FSI {max_density} to be as-of-right, "
            f"or apply to the Committee of Adjustment for a minor variance. "
            f"{_minor_variance_note()}"
        )
    else:
        severity = Severity.NEEDS_REZONING
        remedy = (
            f"Reduce gross floor area to FSI {max_density} to be as-of-right, "
            "or file an Official Plan Amendment / Rezoning (OZ) application to "
            "increase the permitted density. Comparable OZ approvals nearby "
            "can inform feasibility."
        )

    return [
        Violation(
            rule_id="fsi_exceeds_max",
            section_ref=(
                "By-law 569-2013 — zone-specific density (DENSITY/FSI)"
                + _planning_act_ref(severity)
            ),
            observed=f"FSI {proposed:g} proposed",
            allowed=f"max FSI {max_density:g}",
            severity=severity,
            suggested_remedy=remedy,
        )
    ]


def _check_unit_limit_advisory(
    extracted: ProjectFeatures, site: SiteContext
) -> list[Violation]:
    """Warn when a zone has a very low unit ceiling and no explicit count was given.

    When proposed_units is known, _check_units handles it explicitly. This rule
    catches proposals where the description omits a unit count but the zone's low
    ceiling is clearly a barrier (e.g. "14-storey rental" in a 4-unit RM zone).
    Only fires when max_units <= 6 and the proposed use is residential or mixed.
    """
    max_units = site.zoning_max_units
    if not max_units or max_units <= 0 or max_units > 6:
        return []
    if extracted.proposed_units is not None:
        return []
    if extracted.proposed_use not in ("residential", "mixed_use"):
        return []
    return [
        Violation(
            rule_id="unit_limit_advisory",
            section_ref=(
                "By-law 569-2013 — zone-specific lot requirements (UNITS)"
                + _planning_act_ref(Severity.NEEDS_REZONING)
            ),
            observed=(
                f"proposed use is {extracted.proposed_use}; "
                f"zone permits a maximum of {max_units} unit(s)"
            ),
            allowed=f"max {max_units} unit(s) as-of-right",
            severity=Severity.NEEDS_REZONING,
            suggested_remedy=(
                f"This zone limits residential development to {max_units} unit(s) "
                "as-of-right. Any multi-unit proposal exceeding this requires an "
                "Official Plan Amendment / Rezoning (OZ). Confirm your unit count "
                "and apply accordingly."
            ),
        )
    ]


def _check_use(extracted: ProjectFeatures, site: SiteContext) -> list[Violation]:
    proposed_use = extracted.proposed_use
    permitted_category = site.permitted_use_category
    match = use_matches_zone(proposed_use, permitted_category)
    if match is None or match == 1:
        return []

    return [
        Violation(
            rule_id="use_not_permitted",
            section_ref=(
                "By-law 569-2013 — zone-specific permitted uses "
                "(Chapter 10–90, principal permitted uses)"
                + _planning_act_ref(Severity.NEEDS_REZONING)
            ),
            observed=f"proposed use: {proposed_use}",
            allowed=f"permitted category for this zone: {permitted_category}",
            severity=Severity.NEEDS_REZONING,
            suggested_remedy=(
                "A rezoning (OZ) application is required to introduce a "
                f"non-permitted use. Alternatively, consider whether your "
                "proposal can be characterised within the permitted "
                f"category ({permitted_category}) to reduce approval risk."
            ),
        )
    ]


def _check_op_conformity(
    extracted: ProjectFeatures, site: SiteContext
) -> list[Violation]:
    """Flag a proposed use that does not conform to the Official Plan designation.

    The provincial counterpart to ``_check_use`` (which checks the municipal zoning
    permitted-use category). A non-conforming use implicates an Official Plan
    Amendment (Planning Act s.22) on top of any rezoning, since by s.24 a zoning
    by-law must conform to the Official Plan — the practical effect is the longer
    combined OPA+ZBA process and its 120-day non-decision clock.

    Severity is INFORMATIONAL: the designation comes from an interim, deep-learning-
    derived reconstruction (see analytics/reference.py), so it widens the process
    picture and refines the narrator's combined-application read **without** moving
    the confidence number on possibly-imprecise data. Promote to NEEDS_REZONING once
    an authoritative City designation layer replaces the interim source.
    """
    proposed_use = extracted.proposed_use
    designation = site.op_land_use_designation
    match = op_use_matches_designation(proposed_use, designation)
    if match is None or match == 1:
        return []

    return [
        Violation(
            rule_id="op_use_nonconforming",
            section_ref=(
                f"Official Plan land-use designation ({designation}); "
                "Planning Act s.24 (conformity) / s.22 (Official Plan Amendment)"
            ),
            observed=f"proposed use: {proposed_use}",
            allowed=f"Official Plan designation for this site: {designation}",
            severity=Severity.INFORMATIONAL,
            suggested_remedy=(
                f"The site's Official Plan designation ({designation}) does not "
                f"contemplate a {proposed_use} use. Beyond a Zoning By-law Amendment, "
                "an Official Plan Amendment (Planning Act s.22) is likely required so "
                "the rezoning conforms to the Plan (s.24) — this is the longer "
                "combined OPA + rezoning process (120-day non-decision clock). Confirm "
                "the designation against the authoritative Official Plan map schedule."
            ),
        )
    ]


def _check_heritage(site: SiteContext) -> list[Violation]:
    violations = []

    if site.in_heritage_register:
        violations.append(
            Violation(
                rule_id="heritage_register",
                section_ref="Ontario Heritage Act; Toronto Heritage Register",
                observed="site is on the Toronto Heritage Register",
                allowed="alterations require Heritage Impact Assessment (HIA)",
                severity=Severity.INFORMATIONAL,
                suggested_remedy=(
                    "A Heritage Impact Assessment (HIA) is required for "
                    "any alteration. Engage a heritage consultant early. "
                    "Design should conserve the heritage attributes of the "
                    "registered property or adjacent listed resources."
                ),
            )
        )

    if site.in_heritage_district:
        violations.append(
            Violation(
                rule_id="heritage_district",
                section_ref=(
                    "Ontario Heritage Act s.42; Toronto Heritage Conservation "
                    "District Plans"
                ),
                observed="site is within a Heritage Conservation District",
                allowed=(
                    "alterations require Heritage Permit under district plan guidelines"
                ),
                severity=Severity.INFORMATIONAL,
                suggested_remedy=(
                    "Heritage Conservation District permits are required for "
                    "exterior alterations. Review the applicable district plan "
                    "for design guidelines before finalising massing or "
                    "materials. Approval timelines are typically 60–120 days."
                ),
            )
        )

    return violations


def _check_mtsa(site: SiteContext) -> list[Violation]:
    if not site.in_mtsa:
        return []
    return [
        Violation(
            rule_id="mtsa_relaxation",
            section_ref=(
                "By-law 569-2013 as amended; Provincial Planning Statement 2023 "
                "§2.2 (Major Transit Station Areas)"
            ),
            observed="site is within a Major Transit Station Area (MTSA)",
            allowed=(
                "MTSA designation may override base-zone height/density limits; "
                "as-of-right permissions are typically more permissive"
            ),
            severity=Severity.INFORMATIONAL,
            suggested_remedy=(
                "Verify the applicable MTSA schedule for this site. "
                "Many MTSA zones have higher as-of-right permissions than "
                "the base zone; some height violations flagged above may "
                "not apply if the MTSA-specific schedule is more permissive."
            ),
        )
    ]


def _check_trca(site: SiteContext) -> list[Violation]:
    if not site.in_trca_regulated_area:
        return []
    return [
        Violation(
            rule_id="trca_regulated",
            section_ref="TRCA O. Reg. 41/24 (Conservation Authorities Act)",
            observed="site is within a TRCA regulated area",
            allowed="development within regulated areas requires a TRCA permit",
            severity=Severity.INFORMATIONAL,
            suggested_remedy=(
                "A TRCA permit under O. Reg. 41/24 is required before site plan "
                "submission. Engage TRCA early — permit review can take 3–6 months "
                "and may require floodplain studies or other technical reports."
            ),
        )
    ]


def _check_greenbelt(site: SiteContext) -> list[Violation]:
    if not site.in_greenbelt:
        return []
    return [
        Violation(
            rule_id="greenbelt",
            section_ref="Ontario Greenbelt Plan, 2017 (O. Reg. 59/05)",
            observed="site is within the Ontario Greenbelt",
            allowed=(
                "the Greenbelt Plan prohibits most forms of urban residential "
                "or commercial development"
            ),
            severity=Severity.INFORMATIONAL,
            suggested_remedy=(
                "Confirm the proposed use is permitted under the Greenbelt Plan "
                "with the Ministry of Municipal Affairs and Housing before "
                "proceeding. Most urban residential and commercial development "
                "is not permitted in the Greenbelt."
            ),
        )
    ]


def _check_holding(site: SiteContext) -> list[Violation]:
    if not site.zoning_holding:
        return []
    return [
        Violation(
            rule_id="holding_provision",
            section_ref=(
                "By-law 569-2013 — Holding (H) symbol; "
                "Planning Act s.36 (removal of holding symbol)"
            ),
            observed="site has a Holding (H) symbol on its zoning",
            allowed="development requires removal of the H symbol first",
            severity=Severity.NEEDS_REZONING,
            suggested_remedy=(
                "A holding symbol removal application must be approved by "
                "City Council before any development permit can be issued. "
                "The conditions for removal are specified in the zoning by-law "
                "or the applicable Official Plan policy."
            ),
        )
    ]


# Which exception-schedule dimension would relax each curable base-zone
# violation. Only these rule_ids participate in reconciliation; INFORMATIONAL
# context flags (heritage, MTSA, …) are never "cured" by an exception, and
# prohibited_use is absolute (no exception can permit it).
_VIOLATION_DIMENSION: dict[str, str] = {
    "storeys_exceed_max": "height",
    "height_exceeds_max": "height",
    "height_exceeds_max_inferred": "height",
    "units_exceed_max": "density",
    "unit_limit_advisory": "density",
    "fsi_exceeds_max": "density",
    "use_not_permitted": "use",
}

_DIMENSION_LABELS: dict[str, str] = {
    "height": "height/storeys",
    "density": "density (units/FSI)",
    "use": "permitted use",
}

# Keyword signatures used to decide whether a readable provision touches a
# dimension. Deliberately conservative: a miss yields "not modified" (the safe,
# common case) and a hit yields a hedged "appears to" — the verbatim provision
# is always shown so the expert makes the final call.
_DIMENSION_SIGNATURES: dict[str, re.Pattern[str]] = {
    "height": re.compile(r"\b(height|storey|storeys|angular plane)\b", re.I),
    "density": re.compile(
        r"gross floor area|floor space index|\bFSI\b|\bdensity\b|dwelling unit", re.I
    ),
    "use": re.compile(
        r"\bpermitted\b|building type|apartment building|mixed[- ]use|\buse\b", re.I
    ),
}

# A lettered provision item: "(A) ...text..." up to the next letter or end.
_LETTER_ITEM_RE = re.compile(r"\(([A-Z])\)\s+(.*?)(?=\n\s*\([A-Z]\)|\Z)", re.S)
# By-law / former-municipality references inside the prevailing block.
_BYLAW_REF_RE = re.compile(r"By-law[s]?\s+[\d][\d\-]*(?:\([A-Z]+\))?", re.I)

# Street-type suffixes -> canonical abbreviation, for address normalization.
_STREET_SUFFIXES: dict[str, str] = {
    "road": "rd",
    "rd": "rd",
    "avenue": "ave",
    "ave": "ave",
    "street": "st",
    "st": "st",
    "boulevard": "blvd",
    "blvd": "blvd",
    "drive": "dr",
    "dr": "dr",
    "crescent": "cres",
    "cres": "cres",
    "court": "crt",
    "crt": "crt",
    "ct": "crt",
    "lane": "lane",
    "place": "pl",
    "pl": "pl",
    "way": "way",
    "terrace": "terr",
    "trail": "trail",
    "square": "sq",
    "sq": "sq",
    "gardens": "gdns",
    "grove": "grv",
    "parkway": "pkwy",
    "pkwy": "pkwy",
    "circle": "cir",
    "cir": "cir",
    "row": "row",
    "path": "path",
    "gate": "gate",
    "mews": "mews",
    "hill": "hill",
    "park": "park",
}
_DIRECTIONS: dict[str, str] = {
    "east": "e",
    "west": "w",
    "north": "n",
    "south": "s",
    "e": "e",
    "w": "w",
    "n": "n",
    "s": "s",
}
# Longest-first so multi-char suffixes win over their abbreviations.
_SUFFIX_KEYS: list[str] = list(_STREET_SUFFIXES)
_SUFFIX_KEYS.sort(key=len, reverse=True)
_SUFFIX_ALT = "|".join(re.escape(s) for s in _SUFFIX_KEYS)
# A municipal address: one or more street numbers on a single named street, e.g.
# "1500 Weston Road" or "601, 603 and 605 Oakwood Avenue". Requires a street-type
# suffix so by-law numbers ("By-law 1268-2009") and section refs never match.
_ADDRESS_RE = re.compile(
    r"(?P<nums>\d{1,6}(?:\s*,\s*\d{1,6}|\s+and\s+\d{1,6})*)\s+"
    r"(?P<street>[A-Za-z][A-Za-z.'-]+(?:\s+[A-Za-z][A-Za-z.'-]+){0,2}?)\s+"
    r"(?P<suffix>" + _SUFFIX_ALT + r")\b"
    r"(?:\s+(?P<dir>East|West|North|South|E|W|N|S)\b)?",
    re.I,
)
# Concise prevailing reference for display: a former-by-law section, or a by-law
# number (optionally prefixed with the former municipality).
_PREV_REF_RE = re.compile(
    r"Section\s+\d+\s*\([^)]*\)\s*\d+[A-Za-z()]*\s+of\s+former[^.;,]*?By-law\s+[\d-]+"
    r"|(?:Former City of [A-Za-z]+(?:\s+[A-Za-z]+)?\s+)?[Bb]y-?laws?\s+\d[\d-]*"
    r"(?:\([A-Z]+\))?",
    re.I,
)


@dataclass(frozen=True)
class _Address:
    """A parsed municipal address. Equality/scope is judged on (number, street)."""

    number: int
    street: str  # normalized, e.g. "weston rd" / "queen st e"
    display: str  # original-cased, e.g. "1500 Weston Road"


def _parse_addresses(text: str) -> list[_Address]:
    """Extract municipal addresses from free text (one per street number).

    A street-type suffix is required, so by-law numbers and section references
    are never mistaken for addresses. "601, 603 and 605 Oakwood Avenue" yields
    three addresses sharing the normalized street "oakwood ave".
    """
    out: list[_Address] = []
    for m in _ADDRESS_RE.finditer(text):
        street_raw = re.sub(r"\s+", " ", m.group("street")).strip()
        suffix_raw = m.group("suffix")
        suffix_norm = _STREET_SUFFIXES.get(suffix_raw.lower(), suffix_raw.lower())
        dir_raw = m.group("dir") or ""
        dir_norm = _DIRECTIONS.get(dir_raw.lower(), dir_raw.lower())
        street_key = " ".join(
            p for p in (street_raw.lower(), suffix_norm, dir_norm) if p
        )
        for num in re.findall(r"\d{1,6}", m.group("nums")):
            display = " ".join(p for p in (num, street_raw, suffix_raw, dir_raw) if p)
            out.append(_Address(int(num), street_key, display))
    return out


def _address_applies(subject: list[_Address], scope: list[_Address]) -> bool | None:
    """Does the subject site fall within an address-scoped clause?

    Returns True on an exact (number, street) match, False on a confident
    mismatch, and None when either side could not be parsed — the conservative
    case, where the caller must keep the honest "confirm" caveat rather than
    claim the clause does not apply.
    """
    if not subject or not scope:
        return None
    scope_keys = {(a.number, a.street) for a in scope}
    return any((a.number, a.street) in scope_keys for a in subject)


@dataclass
class _PrevailingItem:
    """One entry in an exception's Prevailing By-laws / Sections block."""

    reference: str  # concise display label (by-law no. or section ref)
    scope_addresses: list[_Address]  # lands the entry is scoped to ([] = unscoped)


@dataclass
class _ExceptionDigest:
    """Parsed contents of an exception schedule's verbatim text."""

    site_specific: list[str]  # readable site-specific provisions
    prevailing_items: list[_PrevailingItem]  # imported by-law/section entries
    has_prevailing: bool


def _clean_provision(text: str) -> str:
    """Collapse the PDF line-wrapping inside a single provision item."""
    return re.sub(r"\s+", " ", text).strip()


def _extract_items(section: str) -> list[str]:
    """Pull cleaned lettered items from a provisions section, dropping noise."""
    items: list[str] = []
    for _letter, body in _LETTER_ITEM_RE.findall(section):
        cleaned = _clean_provision(body)
        # Strip trailing office-consolidation page-footer noise.
        cleaned = re.split(r"\bBy-law 569-2013 as amended\b", cleaned)[0].strip()
        if cleaned and "none apply" not in cleaned.lower():
            items.append(cleaned)
    return items


def _prevailing_reference(item: str) -> str:
    """A concise display label for a prevailing entry (section or by-law ref)."""
    refs: list[str] = []
    for r in _PREV_REF_RE.findall(item):
        cleaned = _clean_provision(r)
        if cleaned and cleaned not in refs:
            refs.append(cleaned)
    return "; ".join(refs) if refs else _clean_provision(item)[:90]


def _parse_prevailing_items(prev_section: str) -> list[_PrevailingItem]:
    """Parse each lettered entry in the Prevailing block into ref + scope."""
    items: list[_PrevailingItem] = []
    for _letter, body in _LETTER_ITEM_RE.findall(prev_section):
        cleaned = _clean_provision(body)
        # Strip office-consolidation footer noise, then rejoin numbers split
        # across the PDF line-wrap ("1268- 2009" -> "1268-2009").
        cleaned = re.split(r"\bBy-law 569-2013 as amended\b", cleaned)[0].strip()
        cleaned = re.sub(r"(\d)-\s+(\d)", r"\1-\2", cleaned)
        if not cleaned or "none apply" in cleaned.lower():
            continue
        items.append(
            _PrevailingItem(
                reference=_prevailing_reference(cleaned),
                scope_addresses=_parse_addresses(cleaned),
            )
        )
    return items


def _parse_exception(text: str) -> _ExceptionDigest:
    """Split exception text into readable provisions vs. unreadable imports."""
    m_prev = re.search(r"Prevailing By-laws and Prevailing Sections\s*:?", text, re.I)
    if m_prev:
        ss_section = text[: m_prev.start()]
        prev_section = text[m_prev.end() :]
    else:
        ss_section, prev_section = text, ""

    m_ss = re.search(r"Site Specific Provisions\s*:?", ss_section, re.I)
    ss_body = ss_section[m_ss.end() :] if m_ss else ""
    site_specific = [] if "none apply" in ss_body.lower() else _extract_items(ss_body)

    has_prevailing = bool(prev_section) and "none apply" not in prev_section.lower()
    prevailing_items = _parse_prevailing_items(prev_section) if has_prevailing else []
    return _ExceptionDigest(site_specific, prevailing_items, has_prevailing)


def _reconcile_violations(
    exc_label: str,
    digest: _ExceptionDigest,
    prior_violations: list[Violation],
) -> list[str]:
    """Reconciliation lines ONLY where the exception text touches a violation.

    For each curable violation, checks whether a readable site-specific provision
    actually mentions that dimension (height/density/use). A keyword hit -> one
    hedged "appears to modify — confirm" line, quoting the provision. A miss emits
    nothing: an exception about lot frontage has no bearing on a storeys or units
    violation, and manufacturing a "this violation stands" line against every
    unrelated violation is noise, not review. Absence of a relaxation is the
    default — the violation already stands as its own separate finding.
    """
    lines: list[str] = []
    for v in prior_violations:
        dim = _VIOLATION_DIMENSION.get(v.rule_id)
        if dim is None:
            continue
        signature = _DIMENSION_SIGNATURES[dim]
        matches = [p for p in digest.site_specific if signature.search(p)]
        if not matches:
            continue
        label = _DIMENSION_LABELS[dim]
        quoted = "; ".join(f'"{m}"' for m in matches)
        lines.append(
            f"  • {v.rule_id} ({label}): {exc_label} appears to modify this "
            f"dimension — confirm whether it relaxes the limit your proposal "
            f"exceeds: {quoted}"
        )
    return lines


def _render_prevailing(
    exc_label: str,
    items: list[_PrevailingItem],
    site_address: str | None,
) -> str:
    """Per-clause applicability verdict for the Prevailing By-laws block.

    Applicability is judged by ADDRESS SCOPE only — we read each clause's
    scoping ("On the lands known as 1500 Weston Road, …"), never the by-law's
    content (which is not in our corpus). So we say "does not apply" only when a
    clause is scoped to a different parcel; we never claim a clause's substance
    has been cleared. Falls back to the honest blanket caveat if nothing parsed.
    """
    subject = _parse_addresses(site_address or "")
    site_disp = subject[0].display if subject else (site_address or "this site")
    gap = "its text is not in our corpus"

    lines: list[str] = []
    for it in items:
        if not it.scope_addresses:
            lines.append(
                f"  • {it.reference} — not address-scoped, so it applies to this "
                f"parcel; {gap}, confirm its provisions."
            )
            continue
        scope_disp = ", ".join(dict.fromkeys(a.display for a in it.scope_addresses))
        verdict = _address_applies(subject, it.scope_addresses)
        if verdict is False:
            lines.append(
                f"  • {it.reference} — scoped to {scope_disp}; does not apply to "
                f"{site_disp} (a different parcel)."
            )
        elif verdict is True:
            lines.append(
                f"  • {it.reference} — scoped to lands that include {site_disp}; "
                f"applies — {gap}, confirm its provisions."
            )
        else:
            lines.append(
                f"  • {it.reference} — scoped to specific lands ({scope_disp}); "
                f"we cannot confirm from the address whether {site_disp} is among "
                f"them; {gap}, confirm."
            )

    if not lines:
        refs = ", ".join(it.reference for it in items)
        refs_note = f" (e.g. {refs})" if refs else ""
        return (
            f"Caveat: {exc_label} also imports prevailing by-laws/sections"
            f"{refs_note} that are not in our corpus — some are scoped to specific "
            "addresses. Confirm whether any apply to this site; they could modify "
            "standards we could not read."
        )
    return (
        "Prevailing by-laws & sections (applicability judged by address scope; "
        "their text is not in our corpus):\n" + "\n".join(lines)
    )


def _check_exception(
    site: SiteContext,
    prior_violations: list[Violation],
    exception_text: str | None,
    site_address: str | None = None,
) -> list[Violation]:
    if not site.zoning_exception:
        return []
    exc_no = site.zoning_exception_no
    exc_label = f"Exception No. {exc_no}" if exc_no else "a site-specific exception"
    section_ref = f"By-law 569-2013 — {exc_label}"

    # No schedule text available: be honest about OUR gap rather than punting
    # the review back to the expert.
    if not exception_text:
        return [
            Violation(
                rule_id="zoning_exception",
                section_ref=section_ref,
                observed=f"site has {exc_label} modifying base zone standards",
                allowed=(
                    "site-specific exception schedules may permit uses, heights, "
                    "or densities that differ from the base zone limits above"
                ),
                severity=Severity.INFORMATIONAL,
                suggested_remedy=(
                    f"{exc_label} applies to this site, but its schedule text was "
                    "not available to retrieve, so its provisions could not be "
                    "read here. Consult the exception schedule in By-law 569-2013 "
                    "directly."
                ),
            )
        ]

    digest = _parse_exception(exception_text)

    parts: list[str] = []
    if digest.site_specific:
        provisions = "\n".join(f"  • {p}" for p in digest.site_specific)
        parts.append(f"{exc_label} site-specific provisions:\n{provisions}")
    else:
        parts.append(
            f"{exc_label} carries no readable site-specific provisions "
            "(its schedule modifies the base zone only through the prevailing "
            "by-laws/sections below)."
        )

    # Only surfaced when a readable provision actually bears on a flagged
    # violation; an exception unrelated to the violations adds no line here.
    reconciliation = _reconcile_violations(exc_label, digest, prior_violations)
    if reconciliation:
        parts.append(
            "Bearing on the violations flagged above:\n" + "\n".join(reconciliation)
        )

    if digest.has_prevailing:
        parts.append(
            _render_prevailing(exc_label, digest.prevailing_items, site_address)
        )

    return [
        Violation(
            rule_id="zoning_exception",
            section_ref=section_ref,
            observed=(
                f"site has {exc_label}: "
                f"{len(digest.site_specific)} readable site-specific provision(s)"
                + (
                    f", {len(digest.prevailing_items)} prevailing by-law/section "
                    "import(s)"
                    if digest.has_prevailing
                    else ""
                )
            ),
            allowed=(
                "site-specific exception schedules may modify the base zone "
                "limits shown above; its readable provisions are quoted below"
            ),
            severity=Severity.INFORMATIONAL,
            suggested_remedy="\n".join(parts),
        )
    ]
