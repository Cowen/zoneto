"""Deterministic compliance rule engine for By-law 569-2013."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from zoneto.analytics.extract import ProjectFeatures
from zoneto.analytics.use_classifier import use_matches_zone


class Severity(Enum):
    """How much the rule violation deviates from as-of-right permissions."""

    INFORMATIONAL = "informational"        # not a violation, just context
    NEEDS_VARIANCE = "needs_variance"      # minor variance (COA)
    NEEDS_REZONING = "needs_rezoning"      # official plan amendment / rezoning (OZ)


@dataclass
class Violation:
    """A single compliance finding from the rule engine."""

    rule_id: str
    section_ref: str
    observed: str
    allowed: str
    severity: Severity
    suggested_remedy: str


def check_compliance(
    extracted: ProjectFeatures,
    site: dict[str, Any],
) -> list[Violation]:
    """Run deterministic compliance checks against site zoning limits.

    Compares extracted project features against structured limits from
    lookup_site_context(). The rule engine is the authoritative compliance
    verdict; the LLM narrator phrases it but does not override it.

    Args:
        extracted: ProjectFeatures from extract_project_features().
        site: Site context dict from lookup_site_context() with keys such as
            zoning_class, zoning_max_storeys, zoning_max_units, zoning_max_density,
            permitted_use_category, in_heritage_register, in_heritage_district,
            in_mtsa, zoning_exception, zoning_holding.

    Returns:
        List of Violation findings, empty when no issues detected.
    """
    violations: list[Violation] = []

    violations.extend(_check_storeys(extracted, site))
    violations.extend(_check_units(extracted, site))
    violations.extend(_check_use(extracted, site))
    violations.extend(_check_heritage(site))
    violations.extend(_check_mtsa(site))
    violations.extend(_check_holding(site))

    return violations


def _check_storeys(
    extracted: ProjectFeatures, site: dict[str, Any]
) -> list[Violation]:
    max_storeys = site.get("zoning_max_storeys")
    proposed = extracted.proposed_storeys
    if proposed is None or max_storeys is None:
        return []
    if proposed <= max_storeys:
        return []

    excess = proposed - max_storeys
    if excess <= 2:
        severity = Severity.NEEDS_VARIANCE
        remedy = (
            f"Reduce to {max_storeys} storeys to be as-of-right, or apply "
            "to the Committee of Adjustment for a minor variance (up to 10% "
            "deviation is typically considered minor)."
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
            section_ref="By-law 569-2013 — zone-specific building height requirements",
            observed=f"{proposed} storeys proposed",
            allowed=f"max {max_storeys} storeys",
            severity=severity,
            suggested_remedy=remedy,
        )
    ]


def _check_units(
    extracted: ProjectFeatures, site: dict[str, Any]
) -> list[Violation]:
    max_units = site.get("zoning_max_units")
    proposed = extracted.proposed_units
    if proposed is None or max_units is None:
        return []
    if proposed <= max_units:
        return []

    return [
        Violation(
            rule_id="units_exceed_max",
            section_ref=(
                "By-law 569-2013 — zone-specific lot requirements (UNITS)"
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


def _check_use(
    extracted: ProjectFeatures, site: dict[str, Any]
) -> list[Violation]:
    proposed_use = extracted.proposed_use
    permitted_category = site.get("permitted_use_category")
    match = use_matches_zone(proposed_use, permitted_category)
    if match is None or match == 1:
        return []

    return [
        Violation(
            rule_id="use_not_permitted",
            section_ref=(
                "By-law 569-2013 — zone-specific permitted uses "
                "(Chapter 10–90, principal permitted uses)"
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


def _check_heritage(site: dict[str, Any]) -> list[Violation]:
    violations = []

    if site.get("in_heritage_register"):
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

    if site.get("in_heritage_district"):
        violations.append(
            Violation(
                rule_id="heritage_district",
                section_ref=(
                    "Ontario Heritage Act s.42; Toronto Heritage Conservation "
                    "District Plans"
                ),
                observed="site is within a Heritage Conservation District",
                allowed=(
                    "alterations require Heritage Permit under district plan "
                    "guidelines"
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


def _check_mtsa(site: dict[str, Any]) -> list[Violation]:
    if not site.get("in_mtsa"):
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


def _check_holding(site: dict[str, Any]) -> list[Violation]:
    if not site.get("zoning_holding"):
        return []
    return [
        Violation(
            rule_id="holding_provision",
            section_ref="By-law 569-2013 — Holding (H) symbol",
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
