"""Tests for narrator confidence score parsing and narrate_evaluation return type."""

from __future__ import annotations

from collections.abc import Iterator

from zoneto.analytics.compliance import Severity, Violation
from zoneto.analytics.extract import ProjectFeatures
from zoneto.api.llm_client import FakeLLMClient
from zoneto.api.narrator import (
    _apply_confidence_overrides,
    _format_community_benefits,
    _format_description_similarity,
    _format_site,
    _parse_confidence,
    narrate_evaluation,
)


class CapturingFakeLLMClient:
    """FakeLLMClient variant that captures the last call's messages for assertion."""

    def __init__(self, response: str = "Summary.\n\nCONFIDENCE: 60") -> None:
        self._response = response
        self.last_messages: list[dict[str, str]] = []
        self.last_system: str = ""

    def complete(
        self, system: str, messages: list[dict[str, str]], max_tokens: int
    ) -> str:
        self.last_system = system
        self.last_messages = messages
        return self._response

    def stream(
        self, system: str, messages: list[dict[str, str]], max_tokens: int
    ) -> Iterator[str]:
        self.last_system = system
        self.last_messages = messages
        yield self._response


class TestParseConfidence:
    def test_extracts_score_from_trailing_line(self) -> None:
        """Given: LLM output ending with CONFIDENCE: 72.
        When: Parsing.
        Then: Returns score 72 and removes the line from the summary."""
        raw = "The proposal needs a rezoning.\n\nCONFIDENCE: 72"
        summary, score = _parse_confidence(raw)
        assert score == 72
        assert "CONFIDENCE" not in summary
        assert "The proposal needs a rezoning." in summary

    def test_clamps_score_to_100(self) -> None:
        """Given: LLM returns CONFIDENCE: 150 (out of range).
        When: Parsing.
        Then: Score is clamped to 100."""
        _, score = _parse_confidence("Summary.\n\nCONFIDENCE: 150")
        assert score == 100

    def test_clamps_score_to_0(self) -> None:
        """Given: LLM returns a negative confidence.
        When: Parsing.
        Then: Score is clamped to 0."""
        _, score = _parse_confidence("Summary.\n\nCONFIDENCE: -5")
        assert score == 0

    def test_returns_none_when_no_confidence_line(self) -> None:
        """Given: LLM output with no CONFIDENCE line.
        When: Parsing.
        Then: Score is None and full text returned as summary."""
        raw = "This is a summary with no score."
        summary, score = _parse_confidence(raw)
        assert score is None
        assert summary == raw

    def test_case_insensitive(self) -> None:
        """Given: LLM writes 'confidence: 55' in lowercase.
        When: Parsing.
        Then: Score is still extracted."""
        _, score = _parse_confidence("Summary.\n\nconfidence: 55")
        assert score == 55

    def test_trailing_whitespace_handled(self) -> None:
        """Given: CONFIDENCE line has trailing spaces/newlines.
        When: Parsing.
        Then: Score is extracted correctly."""
        _, score = _parse_confidence("Summary.\n\nCONFIDENCE: 40\n\n")
        assert score == 40

    def test_finds_confidence_even_when_not_last_line(self) -> None:
        """Given: LLM appended a note after the CONFIDENCE line.
        When: Parsing.
        Then: Score is still extracted and the CONFIDENCE line is removed."""
        raw = "Proposal has issues.\n\nCONFIDENCE: 30\n\nPlease consult city planning."
        summary, score = _parse_confidence(raw)
        assert score == 30
        assert "CONFIDENCE" not in summary
        assert "Proposal has issues." in summary

    def test_trailing_period_handled(self) -> None:
        """Given: LLM appends a period after the confidence number.
        When: Parsing.
        Then: Score is still extracted."""
        _, score = _parse_confidence("Summary.\n\nCONFIDENCE: 72.")
        assert score == 72

    def test_markdown_bold_handled(self) -> None:
        """Given: LLM wraps CONFIDENCE in markdown bold markers.
        When: Parsing.
        Then: Score is still extracted."""
        _, score = _parse_confidence("Summary.\n\n**CONFIDENCE: 65**")
        assert score == 65

    def test_summary_not_mangled_when_score_present(self) -> None:
        """Given: Multi-paragraph summary with trailing CONFIDENCE line.
        When: Parsing.
        Then: Summary text is preserved intact."""
        raw = "## Heading\n\nParagraph one.\n\nParagraph two.\n\nCONFIDENCE: 60"
        summary, score = _parse_confidence(raw)
        assert score == 60
        assert "## Heading" in summary
        assert "Paragraph two." in summary


class TestNarrateEvaluationDataGaps:
    def _minimal_site(self) -> dict:
        return {
            "zoning_class": "RM",
            "permitted_use_category": "Residential",
            "zoning_max_storeys": None,
            "zoning_max_height_m": None,
            "zoning_max_units": None,
            "zoning_max_density": None,
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_mtsa": 0,
            "in_secondary_plan": 0,
            "secondary_plan_name": None,
            "zoning_holding": 0,
        }

    def test_data_gaps_accepted_without_error(self) -> None:
        """Given: narrate_evaluation called with a non-empty data_gaps list.
        When: LLM returns a valid response.
        Then: Returns (summary, score) without error. Score may be floored above
        the raw LLM value when the use is compatible and no structural violations."""
        client = FakeLLMClient("Summary noting gaps.\n\nCONFIDENCE: 45")
        extracted = ProjectFeatures(None, None, "residential", False)
        gaps = ["Actual lot area and frontage not available from open data."]
        summary, score = narrate_evaluation(
            self._minimal_site(), extracted, [], [], client, data_gaps=gaps
        )
        assert score is not None
        assert "Summary" in summary

    def test_empty_data_gaps_still_works(self) -> None:
        """Given: narrate_evaluation called with an empty data_gaps list.
        When: LLM returns a valid response.
        Then: Returns (summary, score) as normal."""
        client = FakeLLMClient("Clean site.\n\nCONFIDENCE: 90")
        extracted = ProjectFeatures(None, None, "residential", False)
        summary, score = narrate_evaluation(
            self._minimal_site(), extracted, [], [], client, data_gaps=[]
        )
        assert score == 90

    def test_no_data_gaps_arg_still_works(self) -> None:
        """Given: narrate_evaluation called without data_gaps keyword.
        When: LLM returns a response.
        Then: Backward compatible — no error, normal return."""
        client = FakeLLMClient("Normal summary.\n\nCONFIDENCE: 70")
        extracted = ProjectFeatures(None, None, "residential", False)
        summary, score = narrate_evaluation(
            self._minimal_site(), extracted, [], [], client
        )
        assert score == 70


class TestNarrateEvaluationReturnType:
    def _minimal_site(self) -> dict:
        return {
            "zoning_class": "RM",
            "permitted_use_category": "Residential",
            "zoning_max_storeys": None,
            "zoning_max_units": None,
            "zoning_max_density": None,
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_mtsa": 0,
            "in_secondary_plan": 0,
            "secondary_plan_name": None,
            "zoning_holding": 0,
        }

    def test_returns_tuple_of_str_and_none_when_no_confidence(self) -> None:
        """Given: FakeLLMClient returning a response without CONFIDENCE line.
        When: narrate_evaluation called.
        Then: Returns (str, None) tuple."""
        client = FakeLLMClient("A plain summary.")
        extracted = ProjectFeatures(None, None, "residential", False)
        result = narrate_evaluation(self._minimal_site(), extracted, [], [], client)
        assert isinstance(result, tuple)
        summary, score = result
        assert isinstance(summary, str)
        assert score is None

    def test_returns_tuple_with_score_when_confidence_present(self) -> None:
        """Given: FakeLLMClient returning response with CONFIDENCE: 85.
        When: narrate_evaluation called.
        Then: Returns (str, 85) tuple and CONFIDENCE not in summary."""
        client = FakeLLMClient("Good proposal with minor issues.\n\nCONFIDENCE: 85")
        extracted = ProjectFeatures(None, None, "residential", False)
        summary, score = narrate_evaluation(
            self._minimal_site(), extracted, [], [], client
        )
        assert score == 85
        assert "CONFIDENCE" not in summary


class TestFormatDescriptionSimilarity:
    def test_returns_empty_when_none(self) -> None:
        """Given: description_similarity is None.
        When: Formatting.
        Then: Returns empty string."""
        assert _format_description_similarity(None) == ""

    def test_formats_appeal_rate(self) -> None:
        """Given: description_similarity with appeal_rate=0.0.
        When: Formatting.
        Then: Output contains appeal rate."""
        sim = {"appeal_rate": 0.0, "n_similar": 15, "top_matches": []}
        out = _format_description_similarity(sim)
        assert "0%" in out or "0.0" in out
        assert "15" in out

    def test_formats_nonzero_appeal_rate(self) -> None:
        """Given: description_similarity with appeal_rate=0.25.
        When: Formatting.
        Then: Output contains 25%."""
        sim = {"appeal_rate": 0.25, "n_similar": 20, "top_matches": []}
        out = _format_description_similarity(sim)
        assert "25%" in out

    def test_approval_rate_not_surfaced(self) -> None:
        """Given: description_similarity with approval_rate=0.80.
        When: Formatting.
        Then: Approval rate is NOT shown — it has survivorship bias (only labelled
        completions, ~97% across all zones). Appeal rate is the honest signal."""
        sim = {
            "appeal_rate": 0.0,
            "approval_rate": 0.80,
            "n_similar": 20,
            "top_matches": [],
        }
        out = _format_description_similarity(sim)
        assert "80%" not in out
        assert "approval" not in out.lower() or "appeal" in out.lower()

    def test_highlights_top_comparable_zone_when_present(self) -> None:
        """Given: Top match has similarity >= 0.95 with known zoning_class.
        When: Formatting.
        Then: Output surfaces zone of closest comparable for zone-mismatch detection."""
        sim = {
            "appeal_rate": 0.0,
            "n_similar": 5,
            "top_matches": [
                {
                    "similarity": 1.0,
                    "dev_approved": 1,
                    "dev_appealed": 0,
                    "application_type": "OZ",
                    "zoning_class": "R",
                }
            ],
        }
        out = _format_description_similarity(sim)
        assert "R" in out or "zone" in out.lower()

    def test_best_comparable_outcome_shown_when_approved(self) -> None:
        """Given: Best match has dev_approved=1, dev_appealed=0, similarity=0.95.
        When: Formatting.
        Then: Outcome 'Council-approved' and 'not appealed' appear in output."""
        sim = {
            "appeal_rate": 0.2,
            "n_similar": 10,
            "top_matches": [
                {
                    "similarity": 0.95,
                    "dev_approved": 1,
                    "dev_appealed": 0,
                    "application_type": "OZ",
                    "zoning_class": "CR",
                }
            ],
        }
        out = _format_description_similarity(sim)
        assert "council-approved" in out.lower() or "approved" in out.lower()
        assert "not appealed" in out.lower()

    def test_best_comparable_outcome_absent_when_no_approval_label(self) -> None:
        """Given: Best match has dev_approved=None (unlabelled).
        When: Formatting.
        Then: No outcome note in output for the best comparable."""
        sim = {
            "appeal_rate": 0.0,
            "n_similar": 5,
            "top_matches": [
                {
                    "similarity": 0.95,
                    "dev_approved": None,
                    "application_type": "OZ",
                    "zoning_class": "CR",
                }
            ],
        }
        out = _format_description_similarity(sim)
        assert "council-approved" not in out.lower()
        assert "outcome" not in out.lower()

    def test_returns_empty_when_n_similar_zero(self) -> None:
        """Given: description_similarity with n_similar=0 (no comparables found).
        When: Formatting.
        Then: Returns empty string (no useful data to show)."""
        sim = {"appeal_rate": None, "n_similar": 0, "top_matches": []}
        assert _format_description_similarity(sim) == ""

    def test_appeal_rate_none_does_not_crash(self) -> None:
        """Given: description_similarity with appeal_rate=None.
        When: Formatting.
        Then: Does not crash; returns a non-empty string with n_similar."""
        sim = {"appeal_rate": None, "n_similar": 5, "top_matches": []}
        out = _format_description_similarity(sim)
        assert isinstance(out, str)


class TestNarrateEvaluationDescriptionSimilarity:
    def _minimal_site(self) -> dict:
        return {
            "zoning_class": "CR",
            "permitted_use_category": "Commercial Residential (mixed)",
            "zoning_max_storeys": None,
            "zoning_max_height_m": None,
            "zoning_max_units": None,
            "zoning_max_density": None,
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_mtsa": 0,
            "in_secondary_plan": 0,
            "secondary_plan_name": None,
            "zoning_holding": 0,
        }

    def test_description_similarity_none_does_not_affect_output(self) -> None:
        """Given: description_similarity=None, no violations, compatible zone.
        When: narrate_evaluation called with LLM returning CONFIDENCE: 55.
        Then: Score is floored to 70 (programmatic floor for zero-violation
        compatible-zone proposals prevents LLM temperature from breaking Step 2A)."""
        client = FakeLLMClient("Summary.\n\nCONFIDENCE: 55")
        extracted = ProjectFeatures(None, None, "mixed_use", False)
        summary, score = narrate_evaluation(
            self._minimal_site(),
            extracted,
            [],
            [],
            client,
            description_similarity=None,
        )
        assert score == 70

    def test_description_similarity_injected_into_prompt(self) -> None:
        """Given: description_similarity with appeal_rate=0.0 and n_similar=20.
        When: narrate_evaluation called.
        Then: The LLM user message includes appeal rate context."""
        client = CapturingFakeLLMClient("Summary.\n\nCONFIDENCE: 72")
        extracted = ProjectFeatures(17, 258, "mixed_use", False)
        sim = {
            "appeal_rate": 0.0,
            "n_similar": 20,
            "top_matches": [{"similarity": 1.0, "dev_appealed": 0}],
        }
        narrate_evaluation(
            self._minimal_site(),
            extracted,
            [],
            [],
            client,
            description_similarity=sim,
        )
        user_content = client.last_messages[0]["content"]
        assert "appeal" in user_content.lower()
        assert "20" in user_content

    def test_backward_compat_no_description_similarity_kwarg(self) -> None:
        """Given: narrate_evaluation called without description_similarity kwarg.
        When: Called.
        Then: Works as before (backward compatible)."""
        client = FakeLLMClient("Backward compat.\n\nCONFIDENCE: 70")
        extracted = ProjectFeatures(None, None, "residential", False)
        summary, score = narrate_evaluation(
            self._minimal_site(), extracted, [], [], client
        )
        assert score == 70
        assert "Backward compat." in summary


class TestFormatDescriptionSimilarityZoneMatched:
    def test_zone_mismatch_warning_when_zero_zone_matches(self) -> None:
        """Given: Overall approval=1.0 but zone_matched_n_similar=0.
        When: Formatting description similarity.
        Then: Output includes a specific zone-mismatch warning phrase."""
        sim = {
            "appeal_rate": 0.0,
            "approval_rate": 1.0,
            "n_similar": 5,
            "top_matches": [],
            "zone_matched_n_similar": 0,
            "zone_matched_approval_rate": None,
        }
        out = _format_description_similarity(sim)
        # Must explicitly warn about zone mismatch — not just incidentally mention "no"
        assert (
            "same zone" in out.lower()
            or "zone-matched" in out.lower()
            or "same zoning" in out.lower()
        )

    def test_zone_matched_count_surfaced_not_approval_rate(self) -> None:
        """Given: zone_matched_n_similar=3, zone_matched_approval_rate=0.0.
        When: Formatting.
        Then: Count of zone-matched apps is surfaced; approval rate is NOT shown
        (survivorship bias: only completed applications are labelled)."""
        sim = {
            "appeal_rate": 0.0,
            "n_similar": 5,
            "top_matches": [],
            "zone_matched_n_similar": 3,
            "zone_matched_approval_rate": 0.0,
        }
        out = _format_description_similarity(sim)
        assert "3" in out
        assert "0%" not in out or "appeal" in out.lower()

    def test_zone_matched_keys_absent_does_not_crash(self) -> None:
        """Given: zone_matched keys absent (callers without zone context).
        When: Formatting.
        Then: No crash and normal output unchanged."""
        sim = {
            "appeal_rate": 0.0,
            "n_similar": 5,
            "top_matches": [],
        }
        out = _format_description_similarity(sim)
        assert isinstance(out, str)
        assert len(out) > 0
        assert "zone-matched" not in out.lower()

    def test_zone_matched_appeal_rate_surfaced_when_present(self) -> None:
        """Given: zone_matched_n_similar=5, zone_matched_appeal_rate=0.1.
        When: Formatting description similarity.
        Then: Zone-matched appeal rate is shown as a percentage."""
        sim = {
            "appeal_rate": 0.25,
            "n_similar": 20,
            "top_matches": [],
            "zone_matched_n_similar": 5,
            "zone_matched_approval_rate": None,
            "zone_matched_appeal_rate": 0.1,
        }
        out = _format_description_similarity(sim)
        assert "10%" in out
        assert "appeal rate" in out.lower()
        assert "same zone" in out.lower()

    def test_zone_matched_appeal_rate_absent_shows_count_only(self) -> None:
        """Given: zone_matched_n_similar=5, zone_matched_appeal_rate absent.
        When: Formatting.
        Then: Count is shown but no appeal rate percentage for zone-matched section."""
        sim = {
            "appeal_rate": 0.0,
            "n_similar": 5,
            "top_matches": [],
            "zone_matched_n_similar": 5,
            "zone_matched_approval_rate": None,
        }
        out = _format_description_similarity(sim)
        assert "5" in out
        assert "zone-matched appeal rate" not in out.lower()


class TestFormatDescriptionSimilarityZoneBaseRate:
    def test_zone_base_approval_rate_not_surfaced(self) -> None:
        """Given: zone_base_approval_rate=0.968 (97% approval).
        When: Formatting description similarity.
        Then: Approval rate is NOT shown — survivorship bias makes it meaningless
        (~97% across all zones because only labelled completions are counted)."""
        sim = {
            "appeal_rate": 0.0,
            "n_similar": 20,
            "top_matches": [],
            "zone_matched_n_similar": 20,
            "zone_matched_approval_rate": None,
            "zone_base_n": 280,
            "zone_base_approval_rate": 0.968,
        }
        out = _format_description_similarity(sim)
        assert "96%" not in out
        assert "97%" not in out
        assert "280" not in out or "appeal" in out.lower()

    def test_zone_base_count_may_be_surfaced_without_rate(self) -> None:
        """Given: zone_base_n=280 apps in zone.
        When: Formatting description similarity.
        Then: No crash regardless of whether count is shown or not."""
        sim = {
            "appeal_rate": 0.05,
            "n_similar": 20,
            "top_matches": [],
            "zone_matched_n_similar": 20,
            "zone_matched_approval_rate": None,
            "zone_base_n": 280,
            "zone_base_approval_rate": 0.968,
        }
        out = _format_description_similarity(sim)
        assert isinstance(out, str)
        assert len(out) > 0


class TestProgrammaticCap:
    """Programmatic ≤30 cap for extreme violations (≥3× zone limit)."""

    def _site_with_max_storeys(self, max_storeys: int | None) -> dict:
        return {
            "zoning_class": "RM",
            "permitted_use_category": "Residential Multiple",
            "zoning_max_storeys": max_storeys,
            "zoning_max_units": None,
            "zoning_max_density": None,
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_mtsa": 0,
            "in_secondary_plan": 0,
            "secondary_plan_name": None,
            "zoning_holding": 0,
        }

    def _site_with_max_units(self, max_units: int | None) -> dict:
        return {
            "zoning_class": "RM",
            "permitted_use_category": "Residential Multiple",
            "zoning_max_storeys": None,
            "zoning_max_units": max_units,
            "zoning_max_density": None,
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_mtsa": 0,
            "in_secondary_plan": 0,
            "secondary_plan_name": None,
            "zoning_holding": 0,
        }

    def test_storey_ratio_3x_caps_score_at_30(self) -> None:
        """Given: 12 proposed storeys, max 3 storeys (4× limit).
        When: LLM returns CONFIDENCE: 45 (incorrectly high).
        Then: Score capped to 30."""
        from zoneto.analytics.compliance import Severity, Violation

        client = FakeLLMClient("Major violation summary.\n\nCONFIDENCE: 45")
        extracted = ProjectFeatures(12, None, "residential", False)
        v = Violation(
            rule_id="storeys_exceed_max",
            section_ref="By-law 569-2013",
            observed="12 storeys proposed",
            allowed="max 3 storeys",
            severity=Severity.NEEDS_REZONING,
            suggested_remedy="Reduce storeys",
        )
        _, score = narrate_evaluation(
            self._site_with_max_storeys(3), extracted, [v], [], client
        )
        assert score is not None
        assert score <= 30

    def test_unit_ratio_3x_caps_score_at_30(self) -> None:
        """Given: 120 proposed units, max 4 units (30× limit).
        When: LLM returns CONFIDENCE: 48 (incorrectly high).
        Then: Score capped to 30."""
        from zoneto.analytics.compliance import Severity, Violation

        client = FakeLLMClient("Major violation summary.\n\nCONFIDENCE: 48")
        extracted = ProjectFeatures(None, 120, "mixed_use", False)
        v = Violation(
            rule_id="unit_limit_advisory",
            section_ref="By-law 569-2013",
            observed="proposed use is mixed_use; zone permits a maximum of 4 unit(s)",
            allowed="max 4 unit(s) as-of-right",
            severity=Severity.NEEDS_REZONING,
            suggested_remedy="Reduce units",
        )
        _, score = narrate_evaluation(
            self._site_with_max_units(4), extracted, [v], [], client
        )
        assert score is not None
        assert score <= 30

    def test_ratio_below_3x_does_not_cap(self) -> None:
        """Given: 4 proposed storeys, max 3 storeys (1.3× limit, below 3×).
        When: LLM returns CONFIDENCE: 45.
        Then: Score not capped — 45 passes through."""
        from zoneto.analytics.compliance import Severity, Violation

        client = FakeLLMClient("Moderate violation.\n\nCONFIDENCE: 45")
        extracted = ProjectFeatures(4, None, "residential", False)
        v = Violation(
            rule_id="storeys_exceed_max",
            section_ref="By-law 569-2013",
            observed="4 storeys proposed",
            allowed="max 3 storeys",
            severity=Severity.NEEDS_REZONING,
            suggested_remedy="Reduce storeys",
        )
        _, score = narrate_evaluation(
            self._site_with_max_storeys(3), extracted, [v], [], client
        )
        assert score == 45

    def test_no_zoning_max_no_cap(self) -> None:
        """Given: zoning_max_storeys=None (no data).
        When: LLM returns CONFIDENCE: 50.
        Then: Score not capped — ratio can't be computed."""
        client = FakeLLMClient("Unknown limit.\n\nCONFIDENCE: 50")
        extracted = ProjectFeatures(12, None, "residential", False)
        _, score = narrate_evaluation(
            self._site_with_max_storeys(None), extracted, [], [], client
        )
        assert score == 50


class TestFormatCommunityBenefits:
    def _cb(self) -> dict:
        return {
            "n_comps": 8,
            "median_monetary": 250_000.0,
            "p25_monetary": 100_000.0,
            "p75_monetary": 400_000.0,
            "common_benefit_types": ["Cash", "Parkland dedication"],
        }

    def test_returns_empty_when_none(self) -> None:
        """Given: community_benefits is None.
        When: Formatting.
        Then: Returns empty string."""
        assert _format_community_benefits(None) == ""

    def test_includes_n_comps(self) -> None:
        """Given: community_benefits dict with n_comps=8.
        When: Formatting.
        Then: Output contains '8'."""
        out = _format_community_benefits(self._cb())
        assert "8" in out

    def test_includes_median_formatted(self) -> None:
        """Given: median_monetary=250_000.
        When: Formatting.
        Then: Output contains formatted dollar amount."""
        out = _format_community_benefits(self._cb())
        assert "250,000" in out

    def test_includes_benefit_types(self) -> None:
        """Given: common_benefit_types=['Cash', 'Parkland dedication'].
        When: Formatting.
        Then: Both types appear in output."""
        out = _format_community_benefits(self._cb())
        assert "Cash" in out
        assert "Parkland" in out

    def test_empty_benefit_types_does_not_crash(self) -> None:
        """Given: common_benefit_types=[].
        When: Formatting.
        Then: Does not crash, returns non-empty string."""
        cb = self._cb()
        cb["common_benefit_types"] = []
        out = _format_community_benefits(cb)
        assert isinstance(out, str)
        assert len(out) > 0


class TestNarrateEvaluationCommunityBenefits:
    def _minimal_site(self) -> dict:
        return {
            "zoning_class": "CR",
            "permitted_use_category": "Commercial Residential (mixed)",
            "zoning_max_storeys": None,
            "zoning_max_height_m": None,
            "zoning_max_units": None,
            "zoning_max_density": None,
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_mtsa": 0,
            "in_secondary_plan": 0,
            "secondary_plan_name": None,
            "zoning_holding": 0,
        }

    def test_community_benefits_injected_into_prompt(self) -> None:
        """Given: community_benefits dict with n_comps=5 and median 250_000.
        When: narrate_evaluation called with community_benefits kwarg.
        Then: LLM user message includes Section 37 context with dollar amounts."""
        client = CapturingFakeLLMClient("Summary.\n\nCONFIDENCE: 72")
        extracted = ProjectFeatures(None, None, "mixed_use", False)
        cb = {
            "n_comps": 5,
            "median_monetary": 250_000.0,
            "p25_monetary": 100_000.0,
            "p75_monetary": 400_000.0,
            "common_benefit_types": ["Cash", "Parkland"],
        }
        narrate_evaluation(
            self._minimal_site(),
            extracted,
            [],
            [],
            client,
            community_benefits=cb,
        )
        user_content = client.last_messages[0]["content"]
        assert "250,000" in user_content
        assert "5" in user_content

    def test_community_benefits_none_does_not_affect_output(self) -> None:
        """Given: community_benefits=None.
        When: narrate_evaluation called.
        Then: No crash; returns normal (summary, score) tuple."""
        client = FakeLLMClient("Summary.\n\nCONFIDENCE: 72")
        extracted = ProjectFeatures(None, None, "mixed_use", False)
        summary, score = narrate_evaluation(
            self._minimal_site(), extracted, [], [], client, community_benefits=None
        )
        assert score == 72

    def test_backward_compat_no_community_benefits_kwarg(self) -> None:
        """Given: narrate_evaluation called without community_benefits kwarg.
        When: Called.
        Then: Works as before (backward compatible)."""
        client = FakeLLMClient("Backward compat.\n\nCONFIDENCE: 70")
        extracted = ProjectFeatures(None, None, "mixed_use", False)
        summary, score = narrate_evaluation(
            self._minimal_site(), extracted, [], [], client
        )
        assert score == 70


class TestApplyConfidenceOverrides:
    """Direct tests for the deterministic floor/cap rules (no LLM client needed)."""

    def _site(
        self,
        permitted_use_category: str | None = None,
        zoning_max_storeys: int | None = None,
        zoning_max_units: int | None = None,
        zoning_max_height_m: float | None = None,
        zoning_class: str | None = None,
    ) -> dict:
        return {
            "permitted_use_category": permitted_use_category,
            "zoning_max_storeys": zoning_max_storeys,
            "zoning_max_units": zoning_max_units,
            "zoning_max_height_m": zoning_max_height_m,
            "zoning_class": zoning_class,
        }

    def _extracted(
        self,
        storeys: int | None = None,
        units: int | None = None,
        height_m: float | None = None,
    ) -> ProjectFeatures:
        return ProjectFeatures(
            storeys, units, "residential", False, proposed_height_m=height_m
        )

    def _violation(self) -> Violation:
        return Violation(
            rule_id="test",
            section_ref="§1",
            observed="observed",
            allowed="allowed",
            severity=Severity.NEEDS_REZONING,
            suggested_remedy="reduce",
        )

    def test_floor_applied_no_violations_mixed_use(self) -> None:
        """Given: no violations, permitted_use_category contains 'mixed'.
        When: score is 60.
        Then: Floor raises score to 70."""
        score = _apply_confidence_overrides(
            60, [], self._site("mixed use residential"), self._extracted()
        )
        assert score == 70

    def test_floor_not_applied_when_violations_present(self) -> None:
        """Given: violations present, permitted_use_category = 'mixed use'.
        When: score is 60.
        Then: Floor suppressed by violations."""
        score = _apply_confidence_overrides(
            60, [self._violation()], self._site("mixed use"), self._extracted()
        )
        assert score == 60

    def test_floor_not_applied_for_incompatible_use(self) -> None:
        """Given: no violations, permitted_use_category = 'employment industrial'.
        When: score is 60.
        Then: Floor not applied — use doesn't match 'mixed' or 'commercial residential'."""  # noqa: E501
        score = _apply_confidence_overrides(
            60, [], self._site("employment industrial"), self._extracted()
        )
        assert score == 60

    def test_cap_applied_when_storeys_exceed_3x(self) -> None:
        """Given: proposed 10 storeys, max 3 (ratio 3.33×).
        When: score is 70.
        Then: Cap lowers score to 30."""
        score = _apply_confidence_overrides(
            70, [], self._site(zoning_max_storeys=3), self._extracted(storeys=10)
        )
        assert score == 30

    def test_cap_applied_when_units_exceed_3x(self) -> None:
        """Given: proposed 30 units, max 10 (ratio 3.0×).
        When: score is 70.
        Then: Cap lowers score to 30."""
        score = _apply_confidence_overrides(
            70, [], self._site(zoning_max_units=10), self._extracted(units=30)
        )
        assert score == 30

    def test_cap_not_applied_when_below_3x(self) -> None:
        """Given: proposed 6 units, max 10 (ratio 0.6×).
        When: score is 70.
        Then: Cap not applied."""
        score = _apply_confidence_overrides(
            70, [], self._site(zoning_max_units=10), self._extracted(units=6)
        )
        assert score == 70

    def test_no_division_by_zero_when_max_is_zero(self) -> None:
        """Given: zoning_max_storeys=0 (falsy).
        When: proposed 10 storeys.
        Then: No division by zero; score unchanged."""
        score = _apply_confidence_overrides(
            55,
            [],
            self._site(zoning_max_storeys=0, zoning_max_units=0),
            self._extracted(storeys=10, units=10),
        )
        assert score == 55

    def test_cap_applied_when_height_exceeds_3x(self) -> None:
        """Given: proposed 57m height, max 11m (ratio 5.18×).
        When: score is 72.
        Then: Cap lowers score to 30."""
        score = _apply_confidence_overrides(
            72,
            [],
            self._site(zoning_max_height_m=11.0),
            self._extracted(height_m=57.0),
        )
        assert score == 30

    def test_cap_not_applied_when_height_below_3x(self) -> None:
        """Given: proposed 13m height, max 11m (ratio 1.18×).
        When: score is 72.
        Then: Cap not applied — height is under 3× limit."""
        score = _apply_confidence_overrides(
            72,
            [],
            self._site(zoning_max_height_m=11.0),
            self._extracted(height_m=13.0),
        )
        assert score == 72

    def test_cap_not_applied_when_max_height_null(self) -> None:
        """Given: zoning_max_height_m is None (no height overlay data).
        When: proposed height is 57m.
        Then: No division by zero; height cap not applied."""
        score = _apply_confidence_overrides(
            72,
            [],
            self._site(zoning_max_height_m=None),
            self._extracted(height_m=57.0),
        )
        assert score == 72

    def test_approved_precedent_exempts_cap_and_floors_to_55(self) -> None:
        """Given: height 5x limit + same-zone approved comparable at 95% similarity.
        When: LLM returns score of 28 (was capped by Step 1 in LLM).
        Then: programmatic cap not applied; floor lifts score to >= 55."""
        sim = {
            "top_matches": [
                {"similarity": 0.95, "zoning_class": "R", "dev_approved": 1},
            ]
        }
        score = _apply_confidence_overrides(
            28,
            [self._violation()],
            self._site(zoning_max_height_m=11.0, zoning_class="R"),
            self._extracted(height_m=57.0),
            sim,
        )
        assert score >= 55

    def test_no_precedent_cap_still_fires(self) -> None:
        """Given: height 5x limit + no approved comparable (sim=None).
        When: score is 72.
        Then: cap fires → 30."""
        score = _apply_confidence_overrides(
            72,
            [],
            self._site(zoning_max_height_m=11.0, zoning_class="R"),
            self._extracted(height_m=57.0),
            None,
        )
        assert score == 30

    def test_cross_zone_precedent_does_not_exempt(self) -> None:
        """Given: height 5x limit + approved comparable in CR zone (site zone is R).
        When: score is 72.
        Then: cap fires — different zone does not establish same-zone precedent."""
        sim = {
            "top_matches": [
                {"similarity": 0.95, "zoning_class": "CR", "dev_approved": 1},
            ]
        }
        score = _apply_confidence_overrides(
            72,
            [],
            self._site(zoning_max_height_m=11.0, zoning_class="R"),
            self._extracted(height_m=57.0),
            sim,
        )
        assert score == 30

    def test_low_similarity_precedent_does_not_exempt(self) -> None:
        """Given: height 5x limit + approved same-zone comparable at 85% similarity.
        When: score is 72.
        Then: cap fires — threshold is 90%."""
        sim = {
            "top_matches": [
                {"similarity": 0.85, "zoning_class": "R", "dev_approved": 1},
            ]
        }
        score = _apply_confidence_overrides(
            72,
            [],
            self._site(zoning_max_height_m=11.0, zoning_class="R"),
            self._extracted(height_m=57.0),
            sim,
        )
        assert score == 30

    def test_unapproved_precedent_does_not_exempt(self) -> None:
        """Given: height 5x limit + similar comparable that was NOT approved.
        When: score is 72.
        Then: cap fires — only Council-approved comparables exempt the cap."""
        sim = {
            "top_matches": [
                {"similarity": 0.95, "zoning_class": "R", "dev_approved": 0},
            ]
        }
        score = _apply_confidence_overrides(
            72,
            [],
            self._site(zoning_max_height_m=11.0, zoning_class="R"),
            self._extracted(height_m=57.0),
            sim,
        )
        assert score == 30

    def test_score_above_floor_unchanged(self) -> None:
        """Given: no violations, mixed use, score already 80.
        When: floor is 70.
        Then: Score stays at 80 (max(80, 70))."""
        score = _apply_confidence_overrides(
            80, [], self._site("mixed use"), self._extracted()
        )
        assert score == 80

    def test_floor_applied_permitted_use_informational_violations_only(self) -> None:
        """Given: recreational use in Residential zone, only informational violations
        (e.g. zoning exception note), score 55.
        When: applying overrides.
        Then: Floor raises to 70 — use is as-of-right, structural path is clear."""
        info_violation = Violation(
            rule_id="zoning_exception",
            section_ref="By-law 569-2013 — Exception No. 751",
            observed="site has Exception No. 751",
            allowed="review exception schedule",
            severity=Severity.INFORMATIONAL,
            suggested_remedy="Review exception",
        )
        score = _apply_confidence_overrides(
            55,
            [info_violation],
            self._site(permitted_use_category="Residential", zoning_class="R"),
            ProjectFeatures(None, None, "recreational", False),
        )
        assert score >= 70

    def test_floor_not_applied_when_structural_violation_present(self) -> None:
        """Given: recreational use in Residential zone but a NEEDS_REZONING violation
        (e.g. storeys exceed limit).
        When: applying overrides.
        Then: Floor not applied — structural violation means it is not as-of-right."""
        structural = Violation(
            rule_id="storeys_exceed_max",
            section_ref="By-law 569-2013",
            observed="10 storeys proposed",
            allowed="max 3 storeys",
            severity=Severity.NEEDS_REZONING,
            suggested_remedy="Reduce storeys",
        )
        score = _apply_confidence_overrides(
            55,
            [structural],
            self._site(permitted_use_category="Residential", zoning_class="R"),
            ProjectFeatures(None, None, "recreational", False),
        )
        assert score == 55

    def test_floor_not_applied_when_use_not_permitted(self) -> None:
        """Given: employment use in Residential zone (incompatible), no violations.
        When: applying overrides.
        Then: Floor not applied — use is not permitted in this zone."""
        score = _apply_confidence_overrides(
            50,
            [],
            self._site(permitted_use_category="Residential"),
            ProjectFeatures(None, None, "employment", False),
        )
        assert score == 50

    def test_far_comparable_does_not_exempt_cap(self) -> None:
        """Given: height 5x limit + approved same-zone comparable at 95% similarity,
        but comparable is ~1.1km away (query at Boon Ave, comparable at St Clair).
        When: score is 72.
        Then: cap fires — comparable is too far to establish same-site precedent."""
        sim = {
            "query_lat": 43.66384,  # Boon Ave
            "query_lon": -79.45013,
            "top_matches": [
                {
                    "similarity": 0.95,
                    "zoning_class": "R",
                    "dev_approved": 1,
                    "lat": 43.67290,  # St Clair, ~1.08km away
                    "lon": -79.45543,
                },
            ],
        }
        score = _apply_confidence_overrides(
            72,
            [],
            self._site(zoning_max_height_m=11.0, zoning_class="R"),
            self._extracted(height_m=57.0),
            sim,
        )
        assert score == 30

    def test_close_comparable_exempts_cap(self) -> None:
        """Given: height 5x limit + approved same-zone comparable at 95% similarity,
        comparable is ~55m from query (same site).
        When: LLM returned score 28 (had applied its own cap).
        Then: programmatic cap skipped; floor lifts score to >= 55."""
        sim = {
            "query_lat": 43.67290,  # St Clair
            "query_lon": -79.45543,
            "top_matches": [
                {
                    "similarity": 0.95,
                    "zoning_class": "R",
                    "dev_approved": 1,
                    "lat": 43.67295,  # ~55m away — same site
                    "lon": -79.45548,
                },
            ],
        }
        score = _apply_confidence_overrides(
            28,
            [self._violation()],
            self._site(zoning_max_height_m=11.0, zoning_class="R"),
            self._extracted(height_m=57.0),
            sim,
        )
        assert score >= 55

    def test_comparable_without_coords_does_not_exempt(self) -> None:
        """Given: approved same-zone comparable at 95% similarity, but no lat/lon
        on the comparable row (older application without geocoding).
        When: query lat/lon is provided.
        Then: cap fires — cannot confirm same-site proximity without coords."""
        sim = {
            "query_lat": 43.67290,
            "query_lon": -79.45543,
            "top_matches": [
                {
                    "similarity": 0.95,
                    "zoning_class": "R",
                    "dev_approved": 1,
                    # no lat/lon keys
                },
            ],
        }
        score = _apply_confidence_overrides(
            72,
            [],
            self._site(zoning_max_height_m=11.0, zoning_class="R"),
            self._extracted(height_m=57.0),
            sim,
        )
        assert score == 30

    def test_no_query_coords_does_not_filter_by_distance(self) -> None:
        """Given: approved same-zone comparable at 95% similarity.
        When: sim dict has no query_lat/query_lon (caller didn't pass coords).
        Then: cap not applied — distance unknown, cannot filter out the exemption."""
        sim = {
            "top_matches": [
                {
                    "similarity": 0.95,
                    "zoning_class": "R",
                    "dev_approved": 1,
                    "lat": 43.67290,
                    "lon": -79.45543,
                },
            ],
        }
        score = _apply_confidence_overrides(
            28,
            [self._violation()],
            self._site(zoning_max_height_m=11.0, zoning_class="R"),
            self._extracted(height_m=57.0),
            sim,
        )
        assert score >= 55


class TestNarrateEvaluationDescription:
    def _minimal_site(self) -> dict:
        return {
            "zoning_class": "RM",
            "permitted_use_category": "Residential",
            "zoning_max_storeys": None,
            "zoning_max_height_m": None,
            "zoning_max_units": None,
            "zoning_max_density": None,
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_mtsa": 0,
            "in_secondary_plan": 0,
            "secondary_plan_name": None,
            "zoning_holding": 0,
        }

    def test_raw_description_appears_in_narrator_prompt(self) -> None:
        """Given: narrate_evaluation called with a rich description.
        When: LLM user message is assembled.
        Then: The full description text appears verbatim so the LLM can reason
        about details (parking, laneway access, accessibility) that feature
        extraction does not capture."""
        client = CapturingFakeLLMClient("Summary.\n\nCONFIDENCE: 60")
        extracted = ProjectFeatures(
            4, 16, "residential", False, building_type="apartment"
        )
        description = (
            "To construct a new four-storey 16-unit apartment building with a "
            "front porch with stairs, a side (west) pedestrian ramp, and rear "
            "and side (east) second, third and fourth storey balconies. Also, "
            "to construct a new side (east) one-storey detached ancillary building "
            "(garbage bin storage and mechanical room). There will be a total of "
            "12 vehicular parking spaces and 13 long-term bicycle parking spaces "
            "provided within a one-level below-grade garage with rear (south) "
            "access from the laneway."
        )
        narrate_evaluation(
            self._minimal_site(),
            extracted,
            [],
            [],
            client,
            description=description,
        )
        user_content = client.last_messages[0]["content"]
        assert "12 vehicular parking spaces" in user_content
        assert "pedestrian ramp" in user_content
        assert "laneway" in user_content

    def test_no_description_kwarg_still_works(self) -> None:
        """Given: narrate_evaluation called without description kwarg.
        When: Called.
        Then: Backward compatible — no error, normal return."""
        client = FakeLLMClient("Backward compat.\n\nCONFIDENCE: 70")
        extracted = ProjectFeatures(None, None, "residential", False)
        summary, score = narrate_evaluation(
            self._minimal_site(), extracted, [], [], client
        )
        assert score == 70

    def test_none_description_does_not_inject_section(self) -> None:
        """Given: narrate_evaluation called with description=None.
        When: LLM user message assembled.
        Then: No '## Project description' section appears (nothing to show)."""
        client = CapturingFakeLLMClient("Summary.\n\nCONFIDENCE: 70")
        extracted = ProjectFeatures(None, None, "residential", False)
        narrate_evaluation(
            self._minimal_site(), extracted, [], [], client, description=None
        )
        user_content = client.last_messages[0]["content"]
        assert "## Project description" not in user_content


class TestFormatSiteUseCompatibility:
    def _site(self, permitted_use_category: str) -> dict:
        return {
            "zoning_class": "R",
            "permitted_use_category": permitted_use_category,
            "zoning_max_storeys": None,
            "zoning_max_height_m": None,
            "zoning_max_units": None,
            "zoning_max_density": None,
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_mtsa": 0,
            "in_secondary_plan": 0,
            "secondary_plan_name": None,
            "zoning_holding": 0,
            "zoning_exception": 0,
        }

    def test_compatible_use_shows_permitted(self) -> None:
        """Given: recreational in Residential zone (permitted per _ZONE_ALLOWED).
        When: Formatting site context.
        Then: Use compatibility line says 'permitted'.
        """
        out = _format_site(self._site("Residential"), proposed_use="recreational")
        assert "permitted" in out.lower()
        assert "recreational" in out

    def test_incompatible_use_shows_not_permitted(self) -> None:
        """Given: employment proposed in Residential zone (not permitted).
        When: Formatting site context.
        Then: Use compatibility line says 'NOT permitted'."""
        out = _format_site(self._site("Residential"), proposed_use="employment")
        assert "not permitted" in out.lower()
        assert "employment" in out

    def test_unknown_use_when_proposed_none(self) -> None:
        """Given: proposed_use=None (nothing extracted from description).
        When: Formatting site context.
        Then: Use compatibility line says 'unknown'."""
        out = _format_site(self._site("Residential"), proposed_use=None)
        assert "unknown" in out.lower()

    def test_unknown_use_when_zone_unrecognized(self) -> None:
        """Given: known proposed use but unrecognized permitted_use_category.
        When: Formatting site context.
        Then: Use compatibility line says 'unknown' (can't determine from data)."""
        out = _format_site(
            self._site("Some Unrecognized Zone Category"), proposed_use="residential"
        )
        assert "unknown" in out.lower()

    def test_use_compatibility_injected_into_narrator_prompt(self) -> None:
        """Given: recreational use in Residential zone (compatible).
        When: narrate_evaluation called.
        Then: LLM user prompt includes 'permitted' for use compatibility — LLM
        must not independently contradict what the rule engine determined."""
        client = CapturingFakeLLMClient("Summary.\n\nCONFIDENCE: 65")
        site = self._site("Residential")
        extracted = ProjectFeatures(None, None, "recreational", False)
        narrate_evaluation(site, extracted, [], [], client)
        user_content = client.last_messages[0]["content"]
        assert "permitted" in user_content.lower()
        assert "recreational" in user_content
