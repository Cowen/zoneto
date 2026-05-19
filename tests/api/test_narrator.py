"""Tests for narrator confidence score parsing and narrate_evaluation return type."""

from __future__ import annotations

from collections.abc import Iterator

from zoneto.analytics.extract import ProjectFeatures
from zoneto.api.llm_client import FakeLLMClient
from zoneto.api.narrator import (
    _format_description_similarity,
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

    def test_formats_approval_rate(self) -> None:
        """Given: description_similarity with approval_rate=0.80.
        When: Formatting.
        Then: Output contains 80% approval signal."""
        sim = {
            "appeal_rate": 0.0,
            "approval_rate": 0.80,
            "n_similar": 20,
            "top_matches": [],
        }
        out = _format_description_similarity(sim)
        assert "80%" in out

    def test_highlights_top_comparable_when_approved(self) -> None:
        """Given: Top match has similarity >= 0.95 and dev_approved=1, dev_appealed=0.
        When: Formatting.
        Then: Output highlights this as a strong precedent signal."""
        sim = {
            "appeal_rate": 0.0,
            "approval_rate": 1.0,
            "n_similar": 5,
            "top_matches": [
                {"similarity": 1.0, "dev_approved": 1, "dev_appealed": 0, "application_type": "OZ"}
            ],
        }
        out = _format_description_similarity(sim)
        assert "Council-approved" in out
        assert "no OLT" in out.lower() or "no olt" in out.lower() or "no appeal" in out.lower() or "no OLT" in out

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
        """Given: description_similarity=None passed.
        When: narrate_evaluation called.
        Then: Works normally, no error."""
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
        assert score == 55

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
