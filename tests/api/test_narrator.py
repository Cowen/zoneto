"""Tests for narrator confidence score parsing and narrate_evaluation return type."""

from __future__ import annotations

from zoneto.analytics.extract import ProjectFeatures
from zoneto.api.llm_client import FakeLLMClient
from zoneto.api.narrator import _parse_confidence, narrate_evaluation


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
        Then: Returns (summary, score) without error."""
        client = FakeLLMClient("Summary noting gaps.\n\nCONFIDENCE: 45")
        extracted = ProjectFeatures(None, None, "residential", False)
        gaps = ["Actual lot area and frontage not available from open data."]
        summary, score = narrate_evaluation(
            self._minimal_site(), extracted, [], [], client, data_gaps=gaps
        )
        assert score == 45
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
        assert "Good proposal" in summary
