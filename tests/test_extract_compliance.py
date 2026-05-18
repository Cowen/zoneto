"""Tests for project feature extraction and compliance rule engine."""

from __future__ import annotations

from zoneto.analytics.compliance import (
    Severity,
    Violation,
    check_compliance,
)
from zoneto.analytics.extract import ProjectFeatures, extract_project_features
from zoneto.analytics.use_classifier import classify_use

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
    def _site(self, **kwargs: object) -> dict:
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
        return base

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
    def _site(self, **kwargs: object) -> dict:
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
        return base

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
    def _site(self, **kwargs: object) -> dict:
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
        return base

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
# check_compliance — unit limit advisory (low max_units, no explicit count)
# ---------------------------------------------------------------------------


class TestComplianceUnitLimitAdvisory:
    def _site(self, max_units: int | None, **kwargs: object) -> dict:
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
        return base

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
    def _site(self, **kwargs: object) -> dict:
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
        return base

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
        site = {
            "zoning_max_storeys": 8,
            "zoning_max_units": None,
            "zoning_max_density": None,
            "permitted_use_category": None,
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_mtsa": 0,
            "zoning_holding": 0,
        }
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
    def _employment_site(self, **kwargs: object) -> dict:
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
        return base

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
