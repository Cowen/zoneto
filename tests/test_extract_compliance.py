"""Tests for project feature extraction and compliance rule engine."""

from __future__ import annotations

import pytest

from zoneto.analytics.compliance import (
    Severity,
    Violation,
    _address_applies,
    _parse_addresses,
    check_compliance,
)
from zoneto.analytics.extract import ProjectFeatures, extract_project_features
from zoneto.analytics.use_classifier import classify_use
from zoneto.api.site_context import SiteContext

# ---------------------------------------------------------------------------
# extract_project_features
# ---------------------------------------------------------------------------


class TestExtractProjectFeatures:
    def test_extracts_storeys(self) -> None:
        """Given: Description with storey count.
        When: Extracting features.
        Then: proposed_storeys is populated."""
        result = extract_project_features("A 12-storey residential building.")
        assert result.proposed_storeys == 12

    def test_extracts_storeys_alternate_spelling(self) -> None:
        """Given: Description using 'story' spelling.
        When: Extracting features.
        Then: proposed_storeys is still extracted."""
        result = extract_project_features("14 story apartment tower.")
        assert result.proposed_storeys == 14

    def test_extracts_units(self) -> None:
        """Given: Description with unit count.
        When: Extracting features.
        Then: proposed_units is populated."""
        result = extract_project_features("Proposes 180 dwelling units.")
        assert result.proposed_units == 180

    def test_extracts_units_without_dwelling(self) -> None:
        """Given: Description with 'units' but no 'dwelling'.
        When: Extracting features.
        Then: proposed_units is still extracted."""
        result = extract_project_features("120 units of rental housing.")
        assert result.proposed_units == 120

    def test_extracts_residential_use(self) -> None:
        """Given: Clearly residential description.
        When: Extracting features.
        Then: proposed_use is 'residential'."""
        result = extract_project_features(
            "A condominium building with 80 residential units."
        )
        assert result.proposed_use == "residential"

    def test_extracts_mixed_use(self) -> None:
        """Given: Description with ground-floor retail phrase.
        When: Extracting features.
        Then: proposed_use is 'mixed_use' and has_ground_floor_retail is True."""
        result = extract_project_features(
            "12-storey building with ground-floor retail and 100 dwelling units above."
        )
        assert result.proposed_use == "mixed_use"
        assert result.has_ground_floor_retail is True

    def test_none_description_returns_empty_features(self) -> None:
        """Given: None description.
        When: Extracting features.
        Then: All fields are None or False."""
        result = extract_project_features(None)
        assert result.proposed_storeys is None
        assert result.proposed_units is None
        assert result.proposed_use is None
        assert result.has_ground_floor_retail is False

    def test_empty_description_returns_empty_features(self) -> None:
        """Given: Empty string description.
        When: Extracting features.
        Then: All fields are None or False."""
        result = extract_project_features("")
        assert result.proposed_storeys is None
        assert result.proposed_units is None

    def test_no_ground_floor_retail_when_absent(self) -> None:
        """Given: Purely residential description.
        When: Extracting features.
        Then: has_ground_floor_retail is False."""
        result = extract_project_features(
            "A 6-storey apartment building with 40 units."
        )
        assert result.has_ground_floor_retail is False


# ---------------------------------------------------------------------------
# check_compliance — storeys
# ---------------------------------------------------------------------------


class TestComplianceStoreys:
    def _site(self, **kwargs: object) -> SiteContext:
        base: dict = {
            "zoning_max_storeys": 8,
            "zoning_max_units": None,
            "zoning_max_density": None,
            "permitted_use_category": "Residential",
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_mtsa": 0,
            "zoning_holding": 0,
        }
        base.update(kwargs)
        return SiteContext.model_validate(base)

    def test_no_violation_when_within_limit(self) -> None:
        """Given: Proposed storeys equal to max.
        When: Checking compliance.
        Then: No storey violation returned."""
        extracted = ProjectFeatures(
            proposed_storeys=8,
            proposed_units=None,
            proposed_use="residential",
            has_ground_floor_retail=False,
        )
        violations = check_compliance(extracted, self._site())
        rule_ids = [v.rule_id for v in violations]
        assert "storeys_exceed_max" not in rule_ids

    def test_needs_variance_for_small_excess(self) -> None:
        """Given: Proposed storeys exceed max by 2.
        When: Checking compliance.
        Then: Violation with NEEDS_VARIANCE severity."""
        extracted = ProjectFeatures(
            proposed_storeys=10,
            proposed_units=None,
            proposed_use="residential",
            has_ground_floor_retail=False,
        )
        violations = check_compliance(extracted, self._site())
        storey_vs = [v for v in violations if v.rule_id == "storeys_exceed_max"]
        assert len(storey_vs) == 1
        assert storey_vs[0].severity == Severity.NEEDS_VARIANCE

    def test_needs_rezoning_for_large_excess(self) -> None:
        """Given: Proposed storeys far exceed max.
        When: Checking compliance.
        Then: Violation with NEEDS_REZONING severity."""
        extracted = ProjectFeatures(
            proposed_storeys=20,
            proposed_units=None,
            proposed_use="residential",
            has_ground_floor_retail=False,
        )
        violations = check_compliance(extracted, self._site())
        storey_vs = [v for v in violations if v.rule_id == "storeys_exceed_max"]
        assert len(storey_vs) == 1
        assert storey_vs[0].severity == Severity.NEEDS_REZONING

    def test_no_violation_when_max_unknown(self) -> None:
        """Given: Site has no max storey limit.
        When: Checking compliance.
        Then: No storey violation (can't evaluate without limit)."""
        extracted = ProjectFeatures(
            proposed_storeys=25,
            proposed_units=None,
            proposed_use="residential",
            has_ground_floor_retail=False,
        )
        violations = check_compliance(extracted, self._site(zoning_max_storeys=None))
        rule_ids = [v.rule_id for v in violations]
        assert "storeys_exceed_max" not in rule_ids


# ---------------------------------------------------------------------------
# check_compliance — units
# ---------------------------------------------------------------------------


class TestComplianceUnits:
    def _site(self, **kwargs: object) -> SiteContext:
        base: dict = {
            "zoning_max_storeys": None,
            "zoning_max_units": 50,
            "zoning_max_density": None,
            "permitted_use_category": "Residential",
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_mtsa": 0,
            "zoning_holding": 0,
        }
        base.update(kwargs)
        return SiteContext.model_validate(base)

    def test_no_violation_when_within_limit(self) -> None:
        """Given: Proposed units equal to max.
        When: Checking compliance.
        Then: No units violation."""
        extracted = ProjectFeatures(
            proposed_storeys=None,
            proposed_units=50,
            proposed_use="residential",
            has_ground_floor_retail=False,
        )
        violations = check_compliance(extracted, self._site())
        rule_ids = [v.rule_id for v in violations]
        assert "units_exceed_max" not in rule_ids

    def test_violation_when_exceeds_limit(self) -> None:
        """Given: Proposed units exceed max.
        When: Checking compliance.
        Then: Violation with NEEDS_REZONING severity."""
        extracted = ProjectFeatures(
            proposed_storeys=None,
            proposed_units=120,
            proposed_use="residential",
            has_ground_floor_retail=False,
        )
        violations = check_compliance(extracted, self._site())
        unit_vs = [v for v in violations if v.rule_id == "units_exceed_max"]
        assert len(unit_vs) == 1
        assert unit_vs[0].severity == Severity.NEEDS_REZONING


# ---------------------------------------------------------------------------
# check_compliance — use
# ---------------------------------------------------------------------------


class TestComplianceUse:
    def _site(self, **kwargs: object) -> SiteContext:
        base: dict = {
            "zoning_max_storeys": None,
            "zoning_max_units": None,
            "zoning_max_density": None,
            "permitted_use_category": "Residential",
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_mtsa": 0,
            "zoning_holding": 0,
        }
        base.update(kwargs)
        return SiteContext.model_validate(base)

    def test_no_violation_for_permitted_use(self) -> None:
        """Given: Proposed use matches zone category.
        When: Checking compliance.
        Then: No use violation."""
        extracted = ProjectFeatures(
            proposed_storeys=None,
            proposed_units=None,
            proposed_use="residential",
            has_ground_floor_retail=False,
        )
        violations = check_compliance(extracted, self._site())
        rule_ids = [v.rule_id for v in violations]
        assert "use_not_permitted" not in rule_ids

    def test_violation_for_non_permitted_use(self) -> None:
        """Given: Employment use proposed in Residential zone.
        When: Checking compliance.
        Then: use_not_permitted violation with NEEDS_REZONING."""
        extracted = ProjectFeatures(
            proposed_storeys=None,
            proposed_units=None,
            proposed_use="employment",
            has_ground_floor_retail=False,
        )
        violations = check_compliance(extracted, self._site())
        use_vs = [v for v in violations if v.rule_id == "use_not_permitted"]
        assert len(use_vs) == 1
        assert use_vs[0].severity == Severity.NEEDS_REZONING

    def test_no_violation_when_use_unknown(self) -> None:
        """Given: Proposed use cannot be determined.
        When: Checking compliance.
        Then: No use violation (can't evaluate without signal)."""
        extracted = ProjectFeatures(
            proposed_storeys=None,
            proposed_units=None,
            proposed_use=None,
            has_ground_floor_retail=False,
        )
        violations = check_compliance(extracted, self._site())
        rule_ids = [v.rule_id for v in violations]
        assert "use_not_permitted" not in rule_ids


# ---------------------------------------------------------------------------
# check_compliance — Official Plan conformity (s.24 / s.22)
# ---------------------------------------------------------------------------


class TestComplianceOpConformity:
    def _site(self, designation: str | None, **kwargs: object) -> SiteContext:
        base: dict = {
            "zoning_max_storeys": None,
            "zoning_max_units": None,
            "zoning_max_density": None,
            "permitted_use_category": None,
            "op_land_use_designation": designation,
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_mtsa": 0,
            "zoning_holding": 0,
        }
        base.update(kwargs)
        return SiteContext.model_validate(base)

    def test_nonconforming_use_flags_informational(self) -> None:
        """Given: residential proposed on a Core Employment Areas site.
        When: checking compliance.
        Then: op_use_nonconforming violation at INFORMATIONAL severity (does not
        move the confidence number while on the interim source)."""
        extracted = ProjectFeatures(
            proposed_storeys=None,
            proposed_units=None,
            proposed_use="residential",
            has_ground_floor_retail=False,
        )
        violations = check_compliance(extracted, self._site("Core Employment Areas"))
        op_vs = [v for v in violations if v.rule_id == "op_use_nonconforming"]
        assert len(op_vs) == 1
        assert op_vs[0].severity == Severity.INFORMATIONAL
        assert "s.22" in op_vs[0].section_ref

    def test_conforming_use_no_violation(self) -> None:
        """Residential in Neighbourhoods → no OP conformity violation."""
        extracted = ProjectFeatures(
            proposed_storeys=None,
            proposed_units=None,
            proposed_use="residential",
            has_ground_floor_retail=False,
        )
        violations = check_compliance(extracted, self._site("Neighbourhoods"))
        assert "op_use_nonconforming" not in [v.rule_id for v in violations]

    def test_no_violation_when_designation_unknown(self) -> None:
        """No designation (layer absent / off-coverage) → no OP violation."""
        extracted = ProjectFeatures(
            proposed_storeys=None,
            proposed_units=None,
            proposed_use="residential",
            has_ground_floor_retail=False,
        )
        violations = check_compliance(extracted, self._site(None))
        assert "op_use_nonconforming" not in [v.rule_id for v in violations]


# ---------------------------------------------------------------------------
# check_compliance — unit limit advisory (low max_units, no explicit count)
# ---------------------------------------------------------------------------


class TestComplianceUnitLimitAdvisory:
    def _site(self, max_units: int | None, **kwargs: object) -> SiteContext:
        base: dict = {
            "zoning_max_storeys": None,
            "zoning_max_units": max_units,
            "zoning_max_density": None,
            "permitted_use_category": "Residential",
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_mtsa": 0,
            "zoning_holding": 0,
        }
        base.update(kwargs)
        return SiteContext.model_validate(base)

    def test_advisory_fires_for_low_limit_with_no_count(self) -> None:
        """Given: Residential proposal with no explicit unit count in a 4-unit zone.
        When: Checking compliance.
        Then: unit_limit_advisory violation fires at NEEDS_REZONING severity."""
        extracted = ProjectFeatures(
            proposed_storeys=14,
            proposed_units=None,
            proposed_use="residential",
            has_ground_floor_retail=False,
            description="A 14-story purpose-built rental.",
        )
        violations = check_compliance(extracted, self._site(max_units=4))
        rule_ids = [v.rule_id for v in violations]
        assert "unit_limit_advisory" in rule_ids
        v = next(v for v in violations if v.rule_id == "unit_limit_advisory")
        assert v.severity == Severity.NEEDS_REZONING

    def test_advisory_fires_for_mixed_use_with_low_limit(self) -> None:
        """Given: Mixed-use proposal with no unit count, zone limits to 6 units.
        When: Checking compliance.
        Then: unit_limit_advisory violation fires."""
        extracted = ProjectFeatures(
            proposed_storeys=8,
            proposed_units=None,
            proposed_use="mixed_use",
            has_ground_floor_retail=True,
            description="A mixed-use building with retail and rental apartments above.",
        )
        violations = check_compliance(extracted, self._site(max_units=6))
        rule_ids = [v.rule_id for v in violations]
        assert "unit_limit_advisory" in rule_ids

    def test_advisory_suppressed_when_units_known(self) -> None:
        """Given: Proposal with explicit unit count (handled by units_exceed_max).
        When: Checking compliance.
        Then: unit_limit_advisory is NOT fired — the explicit check handles it."""
        extracted = ProjectFeatures(
            proposed_storeys=None,
            proposed_units=120,
            proposed_use="residential",
            has_ground_floor_retail=False,
            description="A residential building with 120 units.",
        )
        violations = check_compliance(extracted, self._site(max_units=4))
        rule_ids = [v.rule_id for v in violations]
        assert "unit_limit_advisory" not in rule_ids
        assert "units_exceed_max" in rule_ids

    def test_advisory_suppressed_when_limit_is_high(self) -> None:
        """Given: Zone has a generous unit limit (> 6).
        When: Checking compliance.
        Then: No advisory — the limit is not notably restrictive."""
        extracted = ProjectFeatures(
            proposed_storeys=None,
            proposed_units=None,
            proposed_use="residential",
            has_ground_floor_retail=False,
            description="A 6-storey residential building.",
        )
        violations = check_compliance(extracted, self._site(max_units=50))
        rule_ids = [v.rule_id for v in violations]
        assert "unit_limit_advisory" not in rule_ids

    def test_advisory_suppressed_for_non_residential_use(self) -> None:
        """Given: Employment use in a zone with a 4-unit limit.
        When: Checking compliance.
        Then: No unit_limit_advisory (unit limits are irrelevant to non-residential)."""
        extracted = ProjectFeatures(
            proposed_storeys=None,
            proposed_units=None,
            proposed_use="employment",
            has_ground_floor_retail=False,
            description="A warehouse facility.",
        )
        violations = check_compliance(extracted, self._site(max_units=4))
        rule_ids = [v.rule_id for v in violations]
        assert "unit_limit_advisory" not in rule_ids


# ---------------------------------------------------------------------------
# check_compliance — heritage, MTSA, holding
# ---------------------------------------------------------------------------


class TestComplianceContextualFlags:
    def _site(self, **kwargs: object) -> SiteContext:
        base: dict = {
            "zoning_max_storeys": None,
            "zoning_max_units": None,
            "zoning_max_density": None,
            "permitted_use_category": None,
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_mtsa": 0,
            "zoning_holding": 0,
        }
        base.update(kwargs)
        return SiteContext.model_validate(base)

    def test_heritage_register_flag(self) -> None:
        """Given: Site is on the Heritage Register.
        When: Checking compliance.
        Then: heritage_register informational violation returned."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(extracted, self._site(in_heritage_register=1))
        rule_ids = [v.rule_id for v in violations]
        assert "heritage_register" in rule_ids
        hv = next(v for v in violations if v.rule_id == "heritage_register")
        assert hv.severity == Severity.INFORMATIONAL

    def test_heritage_district_flag(self) -> None:
        """Given: Site is within a Heritage Conservation District.
        When: Checking compliance.
        Then: heritage_district informational violation returned."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(extracted, self._site(in_heritage_district=1))
        rule_ids = [v.rule_id for v in violations]
        assert "heritage_district" in rule_ids

    def test_mtsa_relaxation_flag(self) -> None:
        """Given: Site is within a Major Transit Station Area.
        When: Checking compliance.
        Then: mtsa_relaxation informational violation returned."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(extracted, self._site(in_mtsa=1))
        rule_ids = [v.rule_id for v in violations]
        assert "mtsa_relaxation" in rule_ids

    def test_trca_regulated_area_informational(self) -> None:
        """Given: Site is within a TRCA regulated area.
        When: Checking compliance.
        Then: trca_regulated informational violation returned."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(extracted, self._site(in_trca_regulated_area=1))
        rule_ids = [v.rule_id for v in violations]
        assert "trca_regulated" in rule_ids
        tv = next(v for v in violations if v.rule_id == "trca_regulated")
        assert tv.severity == Severity.INFORMATIONAL

    def test_no_trca_violation_when_flag_zero(self) -> None:
        """Given: Site is not in a TRCA regulated area.
        When: Checking compliance.
        Then: No trca_regulated violation."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(extracted, self._site(in_trca_regulated_area=0))
        rule_ids = [v.rule_id for v in violations]
        assert "trca_regulated" not in rule_ids

    def test_no_trca_violation_when_flag_absent(self) -> None:
        """Given: Site context has no in_trca_regulated_area key.
        When: Checking compliance.
        Then: No trca_regulated violation (graceful default)."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(extracted, self._site())
        rule_ids = [v.rule_id for v in violations]
        assert "trca_regulated" not in rule_ids

    def test_greenbelt_violation_informational(self) -> None:
        """Given: Site is within the Ontario Greenbelt.
        When: Checking compliance.
        Then: greenbelt violation with INFORMATIONAL severity is returned."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(extracted, self._site(in_greenbelt=1))
        rule_ids = [v.rule_id for v in violations]
        assert "greenbelt" in rule_ids
        gv = next(v for v in violations if v.rule_id == "greenbelt")
        assert gv.severity == Severity.INFORMATIONAL

    def test_no_greenbelt_violation_when_flag_zero(self) -> None:
        """Given: Site is not in the Greenbelt.
        When: Checking compliance.
        Then: No greenbelt violation."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(extracted, self._site(in_greenbelt=0))
        rule_ids = [v.rule_id for v in violations]
        assert "greenbelt" not in rule_ids

    def test_no_greenbelt_violation_when_flag_absent(self) -> None:
        """Given: Site context has no in_greenbelt key.
        When: Checking compliance.
        Then: No greenbelt violation (graceful default)."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(extracted, self._site())
        rule_ids = [v.rule_id for v in violations]
        assert "greenbelt" not in rule_ids

    def test_holding_provision_flag(self) -> None:
        """Given: Site zoning has a Holding (H) symbol.
        When: Checking compliance.
        Then: holding_provision violation with NEEDS_REZONING."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(extracted, self._site(zoning_holding=1))
        rule_ids = [v.rule_id for v in violations]
        assert "holding_provision" in rule_ids
        hv = next(v for v in violations if v.rule_id == "holding_provision")
        assert hv.severity == Severity.NEEDS_REZONING

    def test_exception_flag_informational(self) -> None:
        """Given: Site zoning has a site-specific exception.
        When: Checking compliance.
        Then: zoning_exception informational violation returned."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(extracted, self._site(zoning_exception=1))
        rule_ids = [v.rule_id for v in violations]
        assert "zoning_exception" in rule_ids
        ev = next(v for v in violations if v.rule_id == "zoning_exception")
        assert ev.severity == Severity.INFORMATIONAL

    def test_exception_includes_exception_number(self) -> None:
        """Given: Site has zoning_exception=1 and zoning_exception_no='252'.
        When: Checking compliance.
        Then: section_ref and suggested_remedy reference '252'."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(
            extracted,
            self._site(zoning_exception=1, zoning_exception_no="252"),
        )
        ev = next(v for v in violations if v.rule_id == "zoning_exception")
        assert "252" in ev.section_ref
        assert "252" in ev.suggested_remedy

    def test_no_exception_when_flag_zero(self) -> None:
        """Given: Site has zoning_exception=0.
        When: Checking compliance.
        Then: No zoning_exception violation."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(extracted, self._site(zoning_exception=0))
        rule_ids = [v.rule_id for v in violations]
        assert "zoning_exception" not in rule_ids

    def test_no_flags_for_clean_site(self) -> None:
        """Given: Site with no contextual flags.
        When: Checking compliance with no proposed features.
        Then: No violations returned."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(extracted, self._site())
        assert violations == []

    def test_description_preserved_in_features(self) -> None:
        """Given: A description with meaningful content.
        When: Extracting features.
        Then: description field is stored on the result."""
        result = extract_project_features("A 6-storey apartment with 40 units.")
        assert result.description == "A 6-storey apartment with 40 units."

    def test_violation_dataclass_fields(self) -> None:
        """Given: A compliance check that produces a violation.
        When: Inspecting the Violation object.
        Then: All required fields are present and correctly typed."""
        extracted = ProjectFeatures(
            proposed_storeys=20,
            proposed_units=None,
            proposed_use=None,
            has_ground_floor_retail=False,
        )
        site = SiteContext(
            zoning_max_storeys=8,
            zoning_max_units=None,
            zoning_max_density=None,
            permitted_use_category=None,
            in_heritage_register=0,
            in_heritage_district=0,
            in_mtsa=0,
            zoning_holding=0,
        )
        violations = check_compliance(extracted, site)
        assert len(violations) == 1
        v = violations[0]
        assert isinstance(v, Violation)
        assert isinstance(v.rule_id, str)
        assert isinstance(v.section_ref, str)
        assert isinstance(v.observed, str)
        assert isinstance(v.allowed, str)
        assert isinstance(v.severity, Severity)
        assert isinstance(v.suggested_remedy, str)


# ---------------------------------------------------------------------------
# use_classifier — heavy industrial / extractive keywords
# ---------------------------------------------------------------------------


class TestUseClassifierHeavyIndustrial:
    def test_mine_classified_as_employment(self) -> None:
        """Given: Description mentioning a coal mine.
        When: Classifying use.
        Then: Classified as employment."""
        assert classify_use("A coal mine with underground extraction.") == "employment"

    def test_refinery_classified_as_employment(self) -> None:
        """Given: Description mentioning a refinery.
        When: Classifying use.
        Then: Classified as employment."""
        assert classify_use("An oil refinery with processing units.") == "employment"

    def test_abattoir_classified_as_employment(self) -> None:
        """Given: Description mentioning an abattoir.
        When: Classifying use.
        Then: Classified as employment."""
        assert classify_use("A meat processing abattoir.") == "employment"

    def test_smelter_classified_as_employment(self) -> None:
        """Given: Description mentioning a smelter.
        When: Classifying use.
        Then: Classified as employment."""
        assert classify_use("A copper smelter and foundry.") == "employment"

    def test_quarry_classified_as_employment(self) -> None:
        """Given: Description mentioning a quarry.
        When: Classifying use.
        Then: Classified as employment."""
        assert classify_use("A stone quarry and aggregate facility.") == "employment"


# ---------------------------------------------------------------------------
# check_compliance — absolutely prohibited uses (By-law §60.20.20.10(1))
# ---------------------------------------------------------------------------


class TestComplianceProhibitedUses:
    def _employment_site(self, **kwargs: object) -> SiteContext:
        base: dict = {
            "zoning_max_storeys": None,
            "zoning_max_units": None,
            "zoning_max_density": None,
            "permitted_use_category": "Employment Industrial",
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_mtsa": 0,
            "zoning_holding": 0,
        }
        base.update(kwargs)
        return SiteContext.model_validate(base)

    def test_coal_mine_fires_prohibited_use(self) -> None:
        """Given: A coal mine proposal on an employment-zoned site.
        When: Checking compliance.
        Then: prohibited_use violation returned even in an employment zone."""
        features = extract_project_features(
            "A coal mine with underground extraction shafts and processing facilities."
        )
        violations = check_compliance(features, self._employment_site())
        rule_ids = [v.rule_id for v in violations]
        assert "prohibited_use" in rule_ids
        pv = next(v for v in violations if v.rule_id == "prohibited_use")
        assert pv.severity == Severity.NEEDS_REZONING

    def test_oil_refinery_fires_prohibited_use(self) -> None:
        """Given: A crude petroleum oil refinery proposal.
        When: Checking compliance.
        Then: prohibited_use violation returned."""
        features = extract_project_features(
            "A crude petroleum oil refinery processing facility."
        )
        violations = check_compliance(features, self._employment_site())
        rule_ids = [v.rule_id for v in violations]
        assert "prohibited_use" in rule_ids

    def test_smelter_fires_prohibited_use(self) -> None:
        """Given: A metallic ore smelting facility.
        When: Checking compliance.
        Then: prohibited_use violation returned."""
        features = extract_project_features(
            "A copper smelter for metallic ore processing."
        )
        violations = check_compliance(features, self._employment_site())
        rule_ids = [v.rule_id for v in violations]
        assert "prohibited_use" in rule_ids

    def test_abattoir_fires_prohibited_use(self) -> None:
        """Given: An abattoir proposal on an employment site.
        When: Checking compliance.
        Then: prohibited_use violation returned."""
        features = extract_project_features("A meat processing abattoir facility.")
        violations = check_compliance(features, self._employment_site())
        rule_ids = [v.rule_id for v in violations]
        assert "prohibited_use" in rule_ids

    def test_residential_not_prohibited(self) -> None:
        """Given: Standard residential proposal in an employment zone.
        When: Checking compliance.
        Then: No prohibited_use violation (use mismatch is a separate rule)."""
        features = extract_project_features(
            "A 6-storey residential building with 40 units."
        )
        violations = check_compliance(features, self._employment_site())
        rule_ids = [v.rule_id for v in violations]
        assert "prohibited_use" not in rule_ids

    def test_warehouse_not_prohibited(self) -> None:
        """Given: Standard warehouse proposal in an employment zone.
        When: Checking compliance.
        Then: No prohibited_use violation (warehouse is a permitted employment use)."""
        features = extract_project_features(
            "A logistics warehouse distribution centre."
        )
        violations = check_compliance(features, self._employment_site())
        rule_ids = [v.rule_id for v in violations]
        assert "prohibited_use" not in rule_ids


# ---------------------------------------------------------------------------
# building_type extraction
# ---------------------------------------------------------------------------


class TestExtractBuildingType:
    def test_extracts_apartment(self) -> None:
        """Given: Description containing 'apartment'.
        When: Extracting features.
        Then: building_type is 'apartment'."""
        result = extract_project_features(
            "A 6-storey apartment building with 40 units."
        )
        assert result.building_type == "apartment"

    def test_extracts_duplex(self) -> None:
        """Given: Description containing 'duplex'.
        When: Extracting features.
        Then: building_type is 'duplex'."""
        result = extract_project_features("A duplex at the rear of the lot.")
        assert result.building_type == "duplex"

    def test_extracts_triplex(self) -> None:
        """Given: Description containing 'triplex'.
        When: Extracting features.
        Then: building_type is 'triplex'."""
        result = extract_project_features("Conversion of a house to a triplex.")
        assert result.building_type == "triplex"

    def test_extracts_townhouse(self) -> None:
        """Given: Description containing 'townhouse'.
        When: Extracting features.
        Then: building_type is 'townhouse'."""
        result = extract_project_features("A 3-storey townhouse with 1 unit.")
        assert result.building_type == "townhouse"

    def test_extracts_semi_detached(self) -> None:
        """Given: Description containing 'semi-detached'.
        When: Extracting features.
        Then: building_type is 'semi_detached'."""
        result = extract_project_features("A semi-detached house renovation.")
        assert result.building_type == "semi_detached"

    def test_extracts_fourplex(self) -> None:
        """Given: Description containing 'fourplex'.
        When: Extracting features.
        Then: building_type is 'fourplex'."""
        result = extract_project_features("A new fourplex on the rear lot.")
        assert result.building_type == "fourplex"

    def test_infers_apartment_from_high_storey_count(self) -> None:
        """Given: Description with 17 storeys and no building type keyword.
        When: Extracting features.
        Then: building_type inferred as 'apartment'."""
        result = extract_project_features(
            "A 17-storey mixed-use building with 258 units."
        )
        assert result.building_type == "apartment"

    def test_infers_apartment_from_large_unit_count(self) -> None:
        """Given: Description with 20+ units and no building type keyword.
        When: Extracting features.
        Then: building_type inferred as 'apartment'."""
        result = extract_project_features("A residential building with 200 units.")
        assert result.building_type == "apartment"

    def test_infers_multiplex_from_mid_unit_count(self) -> None:
        """Given: Description with 5-19 units, <=4 storeys, no keyword.
        When: Extracting features.
        Then: building_type inferred as 'multiplex'."""
        result = extract_project_features(
            "A 3-storey residential building with 8 units."
        )
        assert result.building_type == "multiplex"

    def test_infers_fourplex_from_4_units(self) -> None:
        """Given: Description with exactly 4 units and no building type keyword.
        When: Extracting features.
        Then: building_type inferred as 'fourplex'."""
        result = extract_project_features(
            "A 3-storey residential building with 4 units."
        )
        assert result.building_type == "fourplex"

    def test_infers_triplex_from_3_units(self) -> None:
        """Given: Description with exactly 3 units and no building type keyword.
        When: Extracting features.
        Then: building_type inferred as 'triplex'."""
        result = extract_project_features("A residential building with 3 units.")
        assert result.building_type == "triplex"

    def test_infers_duplex_from_2_units(self) -> None:
        """Given: Description with exactly 2 units and no building type keyword.
        When: Extracting features.
        Then: building_type inferred as 'duplex'."""
        result = extract_project_features("A residential building with 2 units.")
        assert result.building_type == "duplex"

    def test_keyword_takes_precedence_over_inference(self) -> None:
        """Given: Description with 'townhouse' keyword and 10 units.
        When: Extracting features.
        Then: keyword match wins; building_type is 'townhouse' not 'multiplex'."""
        result = extract_project_features("A stacked townhouse with 10 units.")
        assert result.building_type == "townhouse"

    def test_infers_apartment_from_mixed_use_building(self) -> None:
        """Given: A mid-rise mixed-use commercial-residential building with no
        explicit residential type keyword or unit count.
        When: Extracting features.
        Then: building_type is inferred as 'apartment' — a mixed-use building is
        an apartment-form building by definition, never detached/semi/townhouse,
        so the residential building type is NOT a data gap."""
        result = extract_project_features(
            "A 4-storey mixed use commercial residential building with a medical "
            "clinic on the ground floor"
        )
        assert result.proposed_use == "mixed_use"
        assert result.building_type == "apartment"

    def test_explicit_type_still_wins_over_mixed_use(self) -> None:
        """Given: A mixed-use proposal that names an explicit low-rise type.
        When: Extracting features.
        Then: the explicit keyword wins over the mixed-use apartment inference."""
        result = extract_project_features(
            "A mixed-use stacked townhouse development with retail at grade."
        )
        assert result.building_type == "townhouse"

    def test_building_type_none_when_unspecified(self) -> None:
        """Given: Description with no keyword and only 1 unit (ambiguous).
        When: Extracting features.
        Then: building_type is None."""
        result = extract_project_features("A residential building with 1 unit.")
        assert result.building_type is None

    def test_building_type_none_for_empty(self) -> None:
        """Given: Empty description.
        When: Extracting features.
        Then: building_type is None."""
        result = extract_project_features("")
        assert result.building_type is None


# ---------------------------------------------------------------------------
# proposed_height_m extraction
# ---------------------------------------------------------------------------


class TestExtractHeightM:
    def test_extracts_metres_spelling(self) -> None:
        """Given: Description with 'metres'.
        When: Extracting features.
        Then: proposed_height_m is populated."""
        result = extract_project_features("A building 15 metres tall.")
        assert result.proposed_height_m == pytest.approx(15.0)

    def test_extracts_meter_spelling(self) -> None:
        """Given: Description with 'meter' (American spelling).
        When: Extracting features.
        Then: proposed_height_m is populated."""
        result = extract_project_features("A 12.5 meter structure.")
        assert result.proposed_height_m == pytest.approx(12.5)

    def test_extracts_abbreviated_m(self) -> None:
        """Given: Description with 'm' abbreviation.
        When: Extracting features.
        Then: proposed_height_m is populated."""
        result = extract_project_features("A 20m residential tower.")
        assert result.proposed_height_m == pytest.approx(20.0)

    def test_returns_none_when_absent(self) -> None:
        """Given: Description with no height in metres.
        When: Extracting features.
        Then: proposed_height_m is None."""
        result = extract_project_features("A 12-storey residential building.")
        assert result.proposed_height_m is None

    def test_returns_none_for_empty_description(self) -> None:
        """Given: Empty description.
        When: Extracting features.
        Then: proposed_height_m is None."""
        result = extract_project_features("")
        assert result.proposed_height_m is None


# ---------------------------------------------------------------------------
# _check_height_m compliance rule
# ---------------------------------------------------------------------------


class TestComplianceHeightM:
    def _site(self, zoning_max_height_m: float | None = None) -> SiteContext:
        return SiteContext.model_validate(
            {
                "zoning_class": "RM",
                "zoning_max_units": None,
                "zoning_max_storeys": None,
                "zoning_max_height_m": zoning_max_height_m,
                "zoning_max_density": None,
                "permitted_use_category": "Residential",
                "in_heritage_register": 0,
                "in_heritage_district": 0,
                "in_mtsa": 0,
                "in_secondary_plan": 0,
                "zoning_holding": 0,
            }
        )

    def _features(self, height_m: float | None) -> ProjectFeatures:
        return ProjectFeatures(
            proposed_storeys=None,
            proposed_units=None,
            proposed_use="residential",
            has_ground_floor_retail=False,
            proposed_height_m=height_m,
        )

    def test_no_violation_when_within_limit(self) -> None:
        """Given: Proposed height within zone maximum.
        When: Checking compliance.
        Then: No height_exceeds_max violation."""
        violations = check_compliance(self._features(10.0), self._site(12.0))
        rule_ids = [v.rule_id for v in violations]
        assert "height_exceeds_max" not in rule_ids

    def test_needs_variance_for_small_excess(self) -> None:
        """Given: Proposed height slightly over limit (≤10% excess).
        When: Checking compliance.
        Then: height_exceeds_max violation with NEEDS_VARIANCE severity."""
        violations = check_compliance(self._features(13.0), self._site(12.0))
        height_vs = [v for v in violations if v.rule_id == "height_exceeds_max"]
        assert len(height_vs) == 1
        assert height_vs[0].severity == Severity.NEEDS_VARIANCE

    def test_needs_rezoning_for_large_excess(self) -> None:
        """Given: Proposed height far over limit (>10% excess).
        When: Checking compliance.
        Then: height_exceeds_max violation with NEEDS_REZONING severity."""
        violations = check_compliance(self._features(20.0), self._site(12.0))
        height_vs = [v for v in violations if v.rule_id == "height_exceeds_max"]
        assert len(height_vs) == 1
        assert height_vs[0].severity == Severity.NEEDS_REZONING

    def test_no_violation_when_height_unknown(self) -> None:
        """Given: No proposed height extracted.
        When: Checking compliance.
        Then: No height violation."""
        violations = check_compliance(self._features(None), self._site(12.0))
        rule_ids = [v.rule_id for v in violations]
        assert "height_exceeds_max" not in rule_ids

    def test_no_violation_when_max_unknown(self) -> None:
        """Given: No zone height limit available.
        When: Checking compliance.
        Then: No height violation (cannot determine exceedance)."""
        violations = check_compliance(self._features(20.0), self._site(None))
        rule_ids = [v.rule_id for v in violations]
        assert "height_exceeds_max" not in rule_ids


class TestExtractFSI:
    def test_extracts_fsi_of_phrasing(self) -> None:
        """Given: 'an FSI of 5.0 times' (320 McCowan phrasing).
        When: Extracting.
        Then: proposed_fsi == 5.0."""
        features = extract_project_features(
            "522 vehicular parking spaces and an FSI of 5.0 times, in response "
            "to City staff comments."
        )
        assert features.proposed_fsi == 5.0

    def test_extracts_floor_space_index_phrasing(self) -> None:
        """Given: 'a Floor Space Index of 5.5 times the lot'.
        When: Extracting.
        Then: proposed_fsi == 5.5."""
        features = extract_project_features(
            "would result in a Floor Space Index of 5.5 times the lot."
        )
        assert features.proposed_fsi == 5.5

    def test_extracts_fsi_parenthetical_phrasing(self) -> None:
        """Given: 'a total Floor Space Index (FSI) of 5.4' (408 Livingston phrasing).
        When: Extracting.
        Then: proposed_fsi == 5.4."""
        features = extract_project_features(
            "30,888 sq. m of gross floor area (GFA), which results in a total "
            "Floor Space Index (FSI) of 5.4"
        )
        assert features.proposed_fsi == 5.4

    def test_extracts_density_times_the_lot_phrasing(self) -> None:
        """Given: 'a density of 32.27 times the lot area' (36 Eglinton phrasing).
        When: Extracting.
        Then: proposed_fsi == 32.27."""
        features = extract_project_features(
            "The total gross floor area would be 45,112 square metres resulting "
            "in a density of 32.27 times the lot area."
        )
        assert features.proposed_fsi == 32.27

    def test_no_fsi_when_absent(self) -> None:
        """Given: A description with no density language.
        When: Extracting.
        Then: proposed_fsi is None."""
        features = extract_project_features("A 4-storey building with 10 units.")
        assert features.proposed_fsi is None


class TestComplianceInferredHeight:
    def _site(
        self,
        max_height: float | None,
        max_storeys: int | None = None,
    ) -> SiteContext:
        return SiteContext.model_validate(
            {
                "zoning_class": "CR",
                "zoning_max_storeys": max_storeys,
                "zoning_max_units": None,
                "zoning_max_height_m": max_height,
                "zoning_max_density": None,
                "permitted_use_category": "Commercial Residential (mixed)",
            }
        )

    def test_violation_when_storeys_imply_height_far_over_limit(self) -> None:
        """Given: 28 storeys stated, no metres, 18m limit, no storey limit
        (the 68 Wellesley shape — 28 x 3m = 84m, 4.7x the limit).
        When: Checking compliance.
        Then: height_exceeds_max_inferred NEEDS_REZONING violation."""
        features = extract_project_features("a 28 storey mixed-use building")
        violations = check_compliance(features, self._site(18.0))
        inferred = [v for v in violations if v.rule_id == "height_exceeds_max_inferred"]
        assert len(inferred) == 1
        assert inferred[0].severity == Severity.NEEDS_REZONING

    def test_no_violation_within_inference_slack(self) -> None:
        """Given: 4 storeys (inferred 12m) against an 11m limit — over, but
        within the 25% slack reserved for the 3m/storey assumption.
        When: Checking compliance.
        Then: No inferred-height violation."""
        features = extract_project_features("a 4-storey residential building")
        violations = check_compliance(features, self._site(11.0))
        rule_ids = [v.rule_id for v in violations]
        assert "height_exceeds_max_inferred" not in rule_ids

    def test_no_inference_when_height_stated(self) -> None:
        """Given: Height stated explicitly in metres (the direct check owns it).
        When: Checking compliance.
        Then: No inferred-height violation (height_exceeds_max fires instead)."""
        features = extract_project_features("a 28-storey, 90 metre tower")
        violations = check_compliance(features, self._site(18.0))
        rule_ids = [v.rule_id for v in violations]
        assert "height_exceeds_max_inferred" not in rule_ids
        assert "height_exceeds_max" in rule_ids

    def test_no_inference_when_storey_limit_encoded(self) -> None:
        """Given: The zone has a storey limit — the storeys check owns the
        dimension; inferring height too would double-flag it.
        When: Checking compliance.
        Then: No inferred-height violation (storeys_exceed_max fires instead)."""
        features = extract_project_features("a 28 storey mixed-use building")
        violations = check_compliance(features, self._site(18.0, max_storeys=6))
        rule_ids = [v.rule_id for v in violations]
        assert "height_exceeds_max_inferred" not in rule_ids
        assert "storeys_exceed_max" in rule_ids


class TestComplianceFSI:
    def _site(self, max_density: float | None) -> SiteContext:
        return SiteContext.model_validate(
            {
                "zoning_class": "RM",
                "zoning_max_storeys": None,
                "zoning_max_units": None,
                "zoning_max_height_m": None,
                "zoning_max_density": max_density,
                "permitted_use_category": "Residential",
            }
        )

    def test_violation_when_fsi_exceeds_limit(self) -> None:
        """Given: Stated FSI 5.4 against a 0.85 density limit.
        When: Checking compliance.
        Then: fsi_exceeds_max NEEDS_REZONING violation."""
        features = extract_project_features("a total Floor Space Index (FSI) of 5.4")
        violations = check_compliance(features, self._site(0.85))
        fsi_vs = [v for v in violations if v.rule_id == "fsi_exceeds_max"]
        assert len(fsi_vs) == 1
        assert fsi_vs[0].severity == Severity.NEEDS_REZONING

    def test_needs_variance_for_small_fsi_excess(self) -> None:
        """Given: Stated FSI 0.9 against a 0.85 limit (≈6% excess).
        When: Checking compliance.
        Then: fsi_exceeds_max with NEEDS_VARIANCE severity."""
        features = extract_project_features("an FSI of 0.9 times")
        violations = check_compliance(features, self._site(0.85))
        fsi_vs = [v for v in violations if v.rule_id == "fsi_exceeds_max"]
        assert len(fsi_vs) == 1
        assert fsi_vs[0].severity == Severity.NEEDS_VARIANCE

    def test_no_violation_when_within_limit(self) -> None:
        """Given: Stated FSI 0.6 against a 0.85 limit.
        When: Checking compliance.
        Then: No FSI violation."""
        features = extract_project_features("an FSI of 0.6 times")
        violations = check_compliance(features, self._site(0.85))
        rule_ids = [v.rule_id for v in violations]
        assert "fsi_exceeds_max" not in rule_ids

    def test_no_violation_when_limit_unknown(self) -> None:
        """Given: Stated FSI 5.4 but no encoded density limit.
        When: Checking compliance.
        Then: No FSI violation (cannot determine exceedance)."""
        features = extract_project_features("an FSI of 5.4 times")
        violations = check_compliance(features, self._site(None))
        rule_ids = [v.rule_id for v in violations]
        assert "fsi_exceeds_max" not in rule_ids


class TestComplianceZeroLimits:
    """A zoning limit of 0 means 'no encoded limit', not 'maximum of zero'.

    Regression for a ZeroDivisionError surfaced by scripts/planning_act_eval.py
    on real enriched rows where zoning_max_density == 0, and for the false
    positives a 0 storey/unit limit would otherwise produce.
    """

    def _site(self, **overrides) -> SiteContext:
        site = {
            "zoning_class": "RM",
            "zoning_max_storeys": None,
            "zoning_max_units": None,
            "zoning_max_height_m": None,
            "zoning_max_density": None,
            "permitted_use_category": "Residential",
        }
        site.update(overrides)
        return SiteContext.model_validate(site)

    def test_zero_density_does_not_crash_or_flag(self) -> None:
        features = extract_project_features("a Floor Space Index (FSI) of 5.4")
        violations = check_compliance(features, self._site(zoning_max_density=0))
        assert "fsi_exceeds_max" not in {v.rule_id for v in violations}

    def test_zero_height_does_not_crash_or_flag(self) -> None:
        features = extract_project_features("a 60 metre tower")
        violations = check_compliance(features, self._site(zoning_max_height_m=0))
        assert "height_exceeds_max" not in {v.rule_id for v in violations}

    def test_zero_storey_limit_does_not_flag(self) -> None:
        features = extract_project_features("a 12-storey building")
        violations = check_compliance(features, self._site(zoning_max_storeys=0))
        assert "storeys_exceed_max" not in {v.rule_id for v in violations}

    def test_zero_unit_limit_does_not_flag(self) -> None:
        features = extract_project_features("200 dwelling units")
        violations = check_compliance(features, self._site(zoning_max_units=0))
        ids = {v.rule_id for v in violations}
        assert "units_exceed_max" not in ids
        assert "unit_limit_advisory" not in ids


# ---------------------------------------------------------------------------
# check_compliance — site-specific zoning exception reconciliation
# ---------------------------------------------------------------------------


# Verbatim Exception RM 252 schedule text (By-law 569-2013) as stored in the
# bylaw index — one readable site-specific provision plus address-scoped
# prevailing by-laws not in our corpus.
_RM_252_TEXT = """(252) Exception RM 252

The lands are subject to the following Site Specific Provisions, Prevailing
      By-laws and Prevailing Sections.

      Site Specific Provisions:
        (A) The minimum lot frontage is 8.0 metres for a detached house.
      Prevailing By-laws and Prevailing Sections:
        (A) On the lands known as 1500 Weston Road, City of Toronto By-law 1268-
            2009(OMB)
        (B) On the lands known as 605 Oakwood Avenue, City of Toronto By-law 593-
            2008. [ By-law: 1675-2013; 802-2020 ]
"""

# A synthetic exception whose readable provision DOES modify height — used to
# prove the reconciliation surfaces a relaxation, not just a non-modification.
_HEIGHT_EXC_TEXT = """(99) Exception RM 99

      Site Specific Provisions:
        (A) The maximum permitted height of a building is 18.0 metres.
      Prevailing By-laws and Prevailing Sections: (None Apply)
"""


class TestComplianceException:
    def _site(self, **kwargs: object) -> SiteContext:
        base: dict = {
            "zoning_class": "RM",
            "zoning_max_storeys": None,
            "zoning_max_units": None,
            "zoning_max_density": None,
            "permitted_use_category": None,
            "zoning_exception": 1,
            "zoning_exception_no": "252",
        }
        base.update(kwargs)
        return SiteContext.model_validate(base)

    def test_no_exception_no_violation(self) -> None:
        """Given: Site has no zoning exception.
        When: Checking compliance.
        Then: No zoning_exception violation is emitted."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(
            extracted, self._site(zoning_exception=0, zoning_exception_no=None)
        )
        assert "zoning_exception" not in {v.rule_id for v in violations}

    def test_exception_is_informational(self) -> None:
        """Given: Site has Exception 252 with schedule text.
        When: Checking compliance.
        Then: The zoning_exception finding is INFORMATIONAL (never a failure)."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(
            extracted, self._site(), exception_text=_RM_252_TEXT
        )
        exc = next(v for v in violations if v.rule_id == "zoning_exception")
        assert exc.severity == Severity.INFORMATIONAL

    def test_exception_embeds_verbatim_provisions(self) -> None:
        """Given: Exception RM 252 schedule text is available.
        When: Checking compliance.
        Then: The finding quotes the actual provision verbatim rather than
        telling the user to go review it themselves."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(
            extracted, self._site(), exception_text=_RM_252_TEXT
        )
        exc = next(v for v in violations if v.rule_id == "zoning_exception")
        assert "minimum lot frontage is 8.0 metres" in exc.suggested_remedy
        # The old generic punt phrasing must be gone.
        assert "may be permitted as-of-right under the exception" not in (
            exc.suggested_remedy
        )

    def test_exception_flags_unreadable_prevailing_imports(self) -> None:
        """Given: Exception imports former-municipality by-laws not in our corpus.
        When: Checking compliance.
        Then: The finding caveats those imports as unverifiable (honest gap)."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(
            extracted, self._site(), exception_text=_RM_252_TEXT
        )
        exc = next(v for v in violations if v.rule_id == "zoning_exception")
        assert "1268-2009" in exc.suggested_remedy
        assert "not in our corpus" in exc.suggested_remedy.lower() or (
            "could not" in exc.suggested_remedy.lower()
        )

    def test_exception_does_not_cite_unrelated_violations(self) -> None:
        """Given: A storeys violation plus Exception 252 (which only changes lot
        frontage — a dimension unrelated to the storeys violation).
        When: Checking compliance.
        Then: The exception finding does NOT enumerate the unrelated storeys
        violation. We only speak to dimensions the exception text actually
        touches; manufacturing a "this violation stands" line for every
        unrelated violation is noise, not review."""
        extracted = ProjectFeatures(
            proposed_storeys=4,
            proposed_units=None,
            proposed_use="residential",
            has_ground_floor_retail=False,
        )
        violations = check_compliance(
            extracted,
            self._site(zoning_max_storeys=2),
            exception_text=_RM_252_TEXT,
        )
        # The storeys violation still stands on its own as a separate finding.
        assert "storeys_exceed_max" in {v.rule_id for v in violations}
        exc = next(v for v in violations if v.rule_id == "zoning_exception")
        remedy = exc.suggested_remedy.lower()
        # …but the exception finding must not drag it in, nor manufacture a
        # "not modified / this violation stands" verdict against it.
        assert "storeys_exceed_max" not in remedy
        assert "not modif" not in remedy
        assert "this violation stands" not in remedy
        # The readable provision and the corpus-gap caveat still appear.
        assert "8.0 metres" in exc.suggested_remedy
        assert "not in our corpus" in remedy

    def test_exception_surfaces_relaxed_dimension(self) -> None:
        """Given: A storeys violation plus an exception whose readable provision
        modifies height.
        When: Checking compliance.
        Then: The finding flags that the exception appears to address the
        height dimension and should be confirmed."""
        extracted = ProjectFeatures(
            proposed_storeys=5,
            proposed_units=None,
            proposed_use="residential",
            has_ground_floor_retail=False,
        )
        violations = check_compliance(
            extracted,
            self._site(zoning_exception_no="99", zoning_max_storeys=2),
            exception_text=_HEIGHT_EXC_TEXT,
        )
        exc = next(v for v in violations if v.rule_id == "zoning_exception")
        remedy = exc.suggested_remedy.lower()
        assert "18.0 metres" in exc.suggested_remedy
        assert "appears to" in remedy or "may relax" in remedy or "confirm" in remedy

    def test_exception_without_text_is_honest_about_gap(self) -> None:
        """Given: Site has an exception but its schedule text was not available.
        When: Checking compliance.
        Then: The finding states OUR gap (text unavailable) rather than telling
        the expert to go do the review we should have done."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(extracted, self._site(), exception_text=None)
        exc = next(v for v in violations if v.rule_id == "zoning_exception")
        remedy = exc.suggested_remedy.lower()
        assert "not available" in remedy or "could not" in remedy


# ---------------------------------------------------------------------------
# Prevailing by-law address-scope resolver (pure helpers)
# ---------------------------------------------------------------------------


class TestPrevailingAddressResolver:
    def test_parse_single_address(self) -> None:
        """Given: A scope clause naming one street address.
        When: Parsing addresses.
        Then: Number and normalized street are extracted."""
        addrs = _parse_addresses("On the lands known as 1500 Weston Road, By-law X")
        assert len(addrs) == 1
        assert addrs[0].number == 1500
        assert addrs[0].street == "weston rd"

    def test_parse_multi_number_run(self) -> None:
        """Given: A scope clause with several numbers on one street.
        When: Parsing addresses.
        Then: One address per number is produced, same street."""
        addrs = _parse_addresses("601, 603 and 605 Oakwood Avenue")
        assert sorted(a.number for a in addrs) == [601, 603, 605]
        assert all(a.street == "oakwood ave" for a in addrs)

    def test_bylaw_number_is_not_parsed_as_address(self) -> None:
        """Given: A by-law number with no street suffix.
        When: Parsing addresses.
        Then: Nothing is extracted (guards against by-law numbers)."""
        assert _parse_addresses("City of Toronto By-law 1268-2009(OMB)") == []

    def test_section_reference_is_not_parsed_as_address(self) -> None:
        """Given: A former-by-law section reference.
        When: Parsing addresses.
        Then: Nothing is extracted (it is unscoped, not address-scoped)."""
        assert _parse_addresses("Section 12(2) 269 of former By-law 438-86") == []

    def test_parse_subject_with_trailing_city(self) -> None:
        """Given: A user address with trailing city/province.
        When: Parsing addresses.
        Then: The street address is still extracted cleanly."""
        addrs = _parse_addresses("321 Boon Avenue, Toronto, ON")
        assert len(addrs) == 1
        assert addrs[0].number == 321
        assert addrs[0].street == "boon ave"

    def test_applies_false_on_clear_mismatch(self) -> None:
        """Given: A subject address and a scope address on different streets.
        When: Resolving applicability.
        Then: False (scoped to other lands)."""
        subj = _parse_addresses("321 Boon Avenue")
        scope = _parse_addresses("1500 Weston Road")
        assert _address_applies(subj, scope) is False

    def test_applies_true_on_match(self) -> None:
        """Given: A subject address among the scoped lands.
        When: Resolving applicability.
        Then: True."""
        subj = _parse_addresses("605 Oakwood Avenue")
        scope = _parse_addresses("601, 603 and 605 Oakwood Avenue")
        assert _address_applies(subj, scope) is True

    def test_applies_none_when_subject_unparseable(self) -> None:
        """Given: No parseable subject address.
        When: Resolving applicability.
        Then: None (cannot tell — stay conservative)."""
        scope = _parse_addresses("1500 Weston Road")
        assert _address_applies([], scope) is None


# ---------------------------------------------------------------------------
# Prevailing by-law resolver wired through check_compliance
# ---------------------------------------------------------------------------


# An exception whose prevailing by-laws are address-scoped to OTHER parcels.
_RM_252_RESOLVE = """(252) Exception RM 252

      Site Specific Provisions:
        (A) The minimum lot frontage is 8.0 metres for a detached house.
      Prevailing By-laws and Prevailing Sections:
        (A) On the lands known as 1500 Weston Road, By-law 1268-2009(OMB)
        (B) On the lands known as 601, 603 and 605 Oakwood Avenue, By-law 593-2008.
"""

# An exception whose prevailing entry is an unscoped former-by-law section.
_SECTION_PREVAILING = """(11) Exception RM 11

      Site Specific Provisions: (None Apply)
      Prevailing By-laws and Prevailing Sections:
        (A) Section 12(2) 269 of former City of Toronto By-law 438-86.
"""


class TestPrevailingResolverIntegration:
    def _site(self, **kwargs: object) -> SiteContext:
        base: dict = {
            "zoning_class": "RM",
            "zoning_max_storeys": None,
            "zoning_max_units": None,
            "zoning_max_density": None,
            "permitted_use_category": None,
            "zoning_exception": 1,
            "zoning_exception_no": "252",
        }
        base.update(kwargs)
        return SiteContext.model_validate(base)

    def test_prevailing_scoped_elsewhere_does_not_apply(self) -> None:
        """Given: RM 252's prevailing by-laws scoped to 1500 Weston / 605 Oakwood,
        evaluated for 321 Boon Avenue.
        When: Checking compliance with the site address.
        Then: Each is resolved as 'does not apply' (scoped to other lands), the
        old blanket 'confirm whether any apply' punt is gone, and the honest
        text-gap note remains."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(
            extracted,
            self._site(),
            exception_text=_RM_252_RESOLVE,
            site_address="321 Boon Avenue",
        )
        exc = next(v for v in violations if v.rule_id == "zoning_exception")
        r = exc.suggested_remedy.lower()
        assert "does not apply" in r
        assert "weston road" in r and "oakwood" in r
        assert "confirm whether any apply to this site" not in r
        assert "not in our corpus" in r

    def test_resolver_makes_no_content_clearance_claim(self) -> None:
        """Given: All prevailing by-laws resolve to 'does not apply'.
        When: Checking compliance.
        Then: We never assert a sweeping 'no provisions affect this site' —
        applicability is judged by address scope, not by reading the text."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(
            extracted,
            self._site(),
            exception_text=_RM_252_RESOLVE,
            site_address="321 Boon Avenue",
        )
        exc = next(v for v in violations if v.rule_id == "zoning_exception")
        r = exc.suggested_remedy.lower()
        assert "no unread prevailing provisions" not in r
        assert "no provisions affect this site" not in r

    def test_prevailing_matching_site_applies(self) -> None:
        """Given: The subject site IS one of the scoped parcels.
        When: Checking compliance.
        Then: The by-law is reported as applying (confirm provisions), never as
        'does not apply'."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(
            extracted,
            self._site(),
            exception_text=_RM_252_RESOLVE,
            site_address="605 Oakwood Avenue",
        )
        exc = next(v for v in violations if v.rule_id == "zoning_exception")
        # Isolate the line for the Oakwood-scoped by-law (593-2008): it must be
        # reported as applying, never as "does not apply". (The Weston by-law on
        # the same exception correctly does NOT apply to an Oakwood site.)
        oakwood_line = next(
            ln for ln in exc.suggested_remedy.splitlines() if "593-2008" in ln
        ).lower()
        assert "applies" in oakwood_line
        assert "confirm" in oakwood_line
        assert "does not apply" not in oakwood_line

    def test_prevailing_unscoped_section_applies_to_parcel(self) -> None:
        """Given: An unscoped former-by-law section reference.
        When: Checking compliance for any site.
        Then: It is reported as applying to this parcel, with the honest
        text-gap note (no false 'does not apply')."""
        extracted = ProjectFeatures(None, None, None, False)
        violations = check_compliance(
            extracted,
            self._site(zoning_exception_no="11"),
            exception_text=_SECTION_PREVAILING,
            site_address="321 Boon Avenue",
        )
        exc = next(v for v in violations if v.rule_id == "zoning_exception")
        r = exc.suggested_remedy.lower()
        assert "applies to this parcel" in r
        assert "not in our corpus" in r
        assert "438-86" in exc.suggested_remedy
        assert "does not apply" not in r
