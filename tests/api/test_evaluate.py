"""Integration tests for POST /evaluate and POST /ask endpoints."""

from __future__ import annotations

import datetime
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
from fastapi.testclient import TestClient

from tests.stubs import stub_narrator_agents
from zoneto.api.app import create_app
from zoneto.api.routes import _community_benefits_context


@pytest.fixture
def app(tmp_path: Path):
    """Create app with stub narrator agents, no bylaw index, no real data."""
    application = create_app(
        data_dir=tmp_path / "data",
        model_dir=tmp_path / "models",
        static_dir=tmp_path / "static",
    )
    # Inject stub narrator agents and no bylaw index
    application.state.narrator = stub_narrator_agents(
        summary="This is a fake compliance summary.", confidence=60
    )
    application.state.bylaw_index = None
    return application


@pytest.fixture
def client(app):
    return TestClient(app, raise_server_exceptions=True)


class TestEvaluateEndpoint:
    def test_evaluate_returns_structured_response(self, client: TestClient) -> None:
        """Given: A valid address and description.
        When: POST /evaluate with mocked geocode and site context.
        Then: Response contains all required fields."""
        with (
            patch(
                "zoneto.api.routes._geocode_address",
                return_value=(43.644, -79.402),
            ),
            patch(
                "zoneto.api.routes.lookup_site_context",
                return_value={
                    "zoning_class": "RM7",
                    "zoning_max_storeys": 8,
                    "zoning_max_units": 50,
                    "zoning_max_density": 2.0,
                    "permitted_use_category": "Residential",
                    "in_heritage_register": 0,
                    "in_heritage_district": 0,
                    "in_secondary_plan": 0,
                    "secondary_plan_name": None,
                    "in_mtsa": 0,
                    "zoning_holding": 0,
                    "zoning_exception": 0,
                    "zoning_exception_no": None,
                    "zoning_min_frontage_m": None,
                    "zoning_min_lot_area_sqm": None,
                    "zoning_max_coverage_pct": None,
                    "zoning_min_sqm_per_unit": None,
                    "zoning_pct_res": None,
                    "zoning_pct_comm": None,
                    "zoning_pct_emp": None,
                },
            ),
        ):
            resp = client.post(
                "/evaluate",
                json={
                    "address": "441 King St W, Toronto",
                    "description": "A 12-storey residential building with 80 units.",
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert "site_context" in data
        assert "extracted" in data
        assert "violations" in data
        assert "relevant_sections" in data
        assert "summary_md" in data
        assert "suggestions" in data

    def test_evaluate_detects_storey_violation(self, client: TestClient) -> None:
        """Given: Proposal exceeding zoning max storeys.
        When: POST /evaluate.
        Then: violations contains a storeys_exceed_max entry."""
        with (
            patch(
                "zoneto.api.routes._geocode_address",
                return_value=(43.644, -79.402),
            ),
            patch(
                "zoneto.api.routes.lookup_site_context",
                return_value={
                    "zoning_class": "RD",
                    "zoning_max_storeys": 4,
                    "zoning_max_units": None,
                    "zoning_max_density": None,
                    "permitted_use_category": "Residential",
                    "in_heritage_register": 0,
                    "in_heritage_district": 0,
                    "in_secondary_plan": 0,
                    "secondary_plan_name": None,
                    "in_mtsa": 0,
                    "zoning_holding": 0,
                    "zoning_exception": 0,
                    "zoning_exception_no": None,
                    "zoning_min_frontage_m": None,
                    "zoning_min_lot_area_sqm": None,
                    "zoning_max_coverage_pct": None,
                    "zoning_min_sqm_per_unit": None,
                    "zoning_pct_res": None,
                    "zoning_pct_comm": None,
                    "zoning_pct_emp": None,
                },
            ),
        ):
            resp = client.post(
                "/evaluate",
                json={
                    "address": "500 Spadina Ave, Toronto",
                    "description": "A 12-storey apartment building with 100 units.",
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        rule_ids = [v["rule_id"] for v in data["violations"]]
        assert "storeys_exceed_max" in rule_ids

    def test_evaluate_without_llm_returns_503(self, app) -> None:
        """Given: App with no LLM client configured.
        When: POST /evaluate.
        Then: Returns 503."""
        app.state.narrator = None
        test_client = TestClient(app, raise_server_exceptions=False)
        with (
            patch(
                "zoneto.api.routes._geocode_address",
                return_value=(43.644, -79.402),
            ),
            patch(
                "zoneto.api.routes.lookup_site_context",
                return_value={
                    "zoning_class": None,
                    "zoning_max_storeys": None,
                    "zoning_max_units": None,
                    "zoning_max_density": None,
                    "permitted_use_category": None,
                    "in_heritage_register": 0,
                    "in_heritage_district": 0,
                    "in_secondary_plan": 0,
                    "secondary_plan_name": None,
                    "in_mtsa": 0,
                    "zoning_holding": 0,
                    "zoning_exception": 0,
                    "zoning_exception_no": None,
                    "zoning_min_frontage_m": None,
                    "zoning_min_lot_area_sqm": None,
                    "zoning_max_coverage_pct": None,
                    "zoning_min_sqm_per_unit": None,
                    "zoning_pct_res": None,
                    "zoning_pct_comm": None,
                    "zoning_pct_emp": None,
                },
            ),
        ):
            resp = test_client.post(
                "/evaluate",
                json={
                    "address": "test",
                    "description": "A building.",
                },
            )
        assert resp.status_code == 503

    def test_evaluate_returns_data_gaps(self, client: TestClient) -> None:
        """Given: A valid proposal in a zone without height overlay.
        When: POST /evaluate.
        Then: Response includes a non-empty 'data_gaps' list."""
        with (
            patch(
                "zoneto.api.routes._geocode_address",
                return_value=(43.644, -79.402),
            ),
            patch(
                "zoneto.api.routes.lookup_site_context",
                return_value={
                    "zoning_class": "RM",
                    "zoning_max_storeys": None,
                    "zoning_max_height_m": None,
                    "zoning_max_units": None,
                    "zoning_max_density": None,
                    "permitted_use_category": "Residential",
                    "in_heritage_register": 0,
                    "in_heritage_district": 0,
                    "in_secondary_plan": 0,
                    "secondary_plan_name": None,
                    "in_mtsa": 0,
                    "zoning_holding": 0,
                    "zoning_exception": 0,
                    "zoning_exception_no": None,
                    "zoning_min_frontage_m": None,
                    "zoning_min_lot_area_sqm": None,
                    "zoning_max_coverage_pct": None,
                    "zoning_min_sqm_per_unit": None,
                    "zoning_pct_res": None,
                    "zoning_pct_comm": None,
                    "zoning_pct_emp": None,
                },
            ),
        ):
            resp = client.post(
                "/evaluate",
                json={
                    "address": "321 Boon Ave, Toronto",
                    "description": "A 3-storey residential building with 4 units.",
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "data_gaps" in data
        assert isinstance(data["data_gaps"], list)
        assert len(data["data_gaps"]) > 0

    def test_evaluate_data_gaps_includes_building_type_when_unknown(
        self, client: TestClient
    ) -> None:
        """Given: A proposal with no recognisable building type in the description.
        When: POST /evaluate.
        Then: data_gaps mentions building type unavailability."""
        with (
            patch(
                "zoneto.api.routes._geocode_address",
                return_value=(43.644, -79.402),
            ),
            patch(
                "zoneto.api.routes.lookup_site_context",
                return_value={
                    "zoning_class": "RM7",
                    "zoning_max_storeys": 8,
                    "zoning_max_height_m": 25.0,
                    "zoning_max_units": 50,
                    "zoning_max_density": 2.0,
                    "permitted_use_category": "Residential",
                    "in_heritage_register": 0,
                    "in_heritage_district": 0,
                    "in_secondary_plan": 0,
                    "secondary_plan_name": None,
                    "in_mtsa": 0,
                    "zoning_holding": 0,
                    "zoning_exception": 0,
                    "zoning_exception_no": None,
                    "zoning_min_frontage_m": None,
                    "zoning_min_lot_area_sqm": None,
                    "zoning_max_coverage_pct": None,
                    "zoning_min_sqm_per_unit": None,
                    "zoning_pct_res": None,
                    "zoning_pct_comm": None,
                    "zoning_pct_emp": None,
                },
            ),
        ):
            resp = client.post(
                "/evaluate",
                json={
                    "address": "441 King St W, Toronto",
                    "description": "A new residential development on the lands.",
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        combined = " ".join(data["data_gaps"]).lower()
        assert "building type" in combined

    def test_evaluate_response_has_description_similarity_field(
        self, client: TestClient
    ) -> None:
        """Given: Valid evaluate request, no enriched data available.
        When: POST /evaluate.
        Then: Response includes description_similarity field (None when no data)."""
        with (
            patch(
                "zoneto.api.routes._geocode_address",
                return_value=(43.644, -79.402),
            ),
            patch(
                "zoneto.api.routes.lookup_site_context",
                return_value={
                    "zoning_class": "RM",
                    "zoning_max_storeys": 3,
                    "zoning_max_height_m": 11.0,
                    "zoning_max_units": 4,
                    "zoning_max_density": 0.8,
                    "permitted_use_category": "Residential",
                    "in_heritage_register": 0,
                    "in_heritage_district": 0,
                    "in_secondary_plan": 0,
                    "secondary_plan_name": None,
                    "in_mtsa": 0,
                    "zoning_holding": 0,
                    "zoning_exception": 1,
                    "zoning_exception_no": "252",
                    "zoning_min_frontage_m": 12.0,
                    "zoning_min_lot_area_sqm": None,
                    "zoning_max_coverage_pct": None,
                    "zoning_min_sqm_per_unit": None,
                    "zoning_pct_res": None,
                    "zoning_pct_comm": None,
                    "zoning_pct_emp": None,
                },
            ),
        ):
            resp = client.post(
                "/evaluate",
                json={
                    "address": "321 Boon Ave, Toronto",
                    "description": "A 3-storey residential building with 4 units.",
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "description_similarity" in data

    def test_evaluate_suggestions_match_violations(self, client: TestClient) -> None:
        """Given: A proposal with violations that have suggested remedies.
        When: POST /evaluate.
        Then: suggestions list contains the violation remedies."""
        with (
            patch(
                "zoneto.api.routes._geocode_address",
                return_value=(43.644, -79.402),
            ),
            patch(
                "zoneto.api.routes.lookup_site_context",
                return_value={
                    "zoning_class": "R",
                    "zoning_max_storeys": 4,
                    "zoning_max_units": None,
                    "zoning_max_density": None,
                    "permitted_use_category": "Residential",
                    "in_heritage_register": 0,
                    "in_heritage_district": 0,
                    "in_secondary_plan": 0,
                    "secondary_plan_name": None,
                    "in_mtsa": 0,
                    "zoning_holding": 0,
                    "zoning_exception": 0,
                    "zoning_exception_no": None,
                    "zoning_min_frontage_m": None,
                    "zoning_min_lot_area_sqm": None,
                    "zoning_max_coverage_pct": None,
                    "zoning_min_sqm_per_unit": None,
                    "zoning_pct_res": None,
                    "zoning_pct_comm": None,
                    "zoning_pct_emp": None,
                },
            ),
        ):
            resp = client.post(
                "/evaluate",
                json={
                    "address": "100 Test St",
                    "description": "A 10-storey apartment building.",
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        # Suggestions should be non-empty when violations exist
        if data["violations"]:
            assert len(data["suggestions"]) > 0


class TestCommunityBenefitsContext:
    def _s37_parquet(self, ref_dir: Path, n: int = 4, ward: str = "10") -> None:
        s37 = pl.DataFrame(
            {
                "ward": [ward] * n,
                "council_date": [datetime.date(2024, 1 + i, 1) for i in range(n)],
                "monetary_value": [100_000.0 * (i + 1) for i in range(n)],
                "community_benefits": (
                    ["Cash", "Parkland", "Cash", "Affordable Housing"] * 4
                )[:n],
                "location": [f"{i * 100} Main St" for i in range(n)],
            }
        )
        ref_dir.mkdir(parents=True, exist_ok=True)
        s37.write_parquet(ref_dir / "section37.parquet")

    def test_returns_none_when_no_s37_parquet(self, tmp_path: Path) -> None:
        """Given: No section37.parquet in data/reference.
        When: _community_benefits_context is called.
        Then: Returns None."""
        result = _community_benefits_context(data_dir=tmp_path)
        assert result is None

    def test_returns_none_when_fewer_than_min_comps(self, tmp_path: Path) -> None:
        """Given: section37.parquet with 2 records, min_comps=3.
        When: _community_benefits_context is called.
        Then: Returns None."""
        self._s37_parquet(tmp_path / "reference", n=2)
        result = _community_benefits_context(data_dir=tmp_path, min_comps=3)
        assert result is None

    def test_returns_dict_with_required_keys(self, tmp_path: Path) -> None:
        """Given: section37.parquet with 4 records.
        When: _community_benefits_context is called with min_comps=3.
        Then: Returns dict with n_comps, monetary stats, and common_benefit_types."""
        self._s37_parquet(tmp_path / "reference", n=4)
        result = _community_benefits_context(data_dir=tmp_path, min_comps=3)
        assert result is not None
        assert result["n_comps"] == 4
        assert "median_monetary" in result
        assert "p25_monetary" in result
        assert "p75_monetary" in result
        assert isinstance(result["common_benefit_types"], list)

    def test_filters_by_ward_number(self, tmp_path: Path) -> None:
        """Given: section37.parquet with ward 10 (3 records) and ward 11 (3 records).
        When: _community_benefits_context called for ward '10'.
        Then: Only ward 10 records are counted."""
        ref_dir = tmp_path / "reference"
        ref_dir.mkdir(parents=True)
        s37 = pl.DataFrame(
            {
                "ward": ["10", "10", "10", "11", "11", "11"],
                "council_date": [datetime.date(2024, 1, 1)] * 6,
                "monetary_value": [
                    100_000.0,
                    200_000.0,
                    300_000.0,
                    9_000_000.0,
                    9_000_000.0,
                    9_000_000.0,
                ],
                "community_benefits": ["Cash"] * 6,
                "location": [f"{i} St" for i in range(6)],
            }
        )
        s37.write_parquet(ref_dir / "section37.parquet")
        result = _community_benefits_context(
            data_dir=tmp_path, ward_number="10", min_comps=3
        )
        assert result is not None
        assert result["n_comps"] == 3
        assert result["median_monetary"] < 500_000

    def test_common_benefit_types_sorted_by_frequency(self, tmp_path: Path) -> None:
        """Given: section37.parquet with Cash 3 times and Parkland 2 times.
        When: _community_benefits_context is called.
        Then: Cash appears first in common_benefit_types."""
        ref_dir = tmp_path / "reference"
        ref_dir.mkdir(parents=True)
        s37 = pl.DataFrame(
            {
                "ward": ["10"] * 5,
                "council_date": [datetime.date(2024, 1, 1)] * 5,
                "monetary_value": [100_000.0] * 5,
                "community_benefits": ["Cash", "Cash", "Cash", "Parkland", "Parkland"],
                "location": [f"{i} St" for i in range(5)],
            }
        )
        s37.write_parquet(ref_dir / "section37.parquet")
        result = _community_benefits_context(data_dir=tmp_path, min_comps=3)
        assert result is not None
        assert result["common_benefit_types"][0] == "Cash"


class TestAskEndpoint:
    def _site_ctx(self) -> dict:
        return {
            "zoning_class": "RM",
            "zoning_max_storeys": 6,
            "zoning_max_units": None,
            "zoning_max_density": None,
            "permitted_use_category": "Residential",
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_secondary_plan": 0,
            "secondary_plan_name": None,
            "in_mtsa": 0,
            "zoning_holding": 0,
            "zoning_exception": 0,
            "zoning_exception_no": None,
            "zoning_min_frontage_m": None,
            "zoning_min_lot_area_sqm": None,
            "zoning_max_coverage_pct": None,
            "zoning_min_sqm_per_unit": None,
            "zoning_pct_res": None,
            "zoning_pct_comm": None,
            "zoning_pct_emp": None,
        }

    def test_ask_returns_answer(self, client: TestClient) -> None:
        """Given: A question about the project.
        When: POST /ask.
        Then: Returns a JSON object with an 'answer' key."""
        with (
            patch(
                "zoneto.api.routes._geocode_address",
                return_value=(43.644, -79.402),
            ),
            patch(
                "zoneto.api.routes.lookup_site_context",
                return_value=self._site_ctx(),
            ),
        ):
            resp = client.post(
                "/ask",
                json={
                    "address": "441 King St W",
                    "description": "A 6-storey residential building.",
                    "question": "What is the maximum permitted height?",
                    "history": [],
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert "answer" in data
        assert isinstance(data["answer"], str)
        assert len(data["answer"]) > 0

    def test_ask_preserves_history(self, client: TestClient) -> None:
        """Given: A question with prior conversation history.
        When: POST /ask.
        Then: Returns 200 without error (history is passed through)."""
        history = [
            {"role": "user", "content": "What zones are there?"},
            {"role": "assistant", "content": "There are R, RM, RD zones."},
        ]
        with (
            patch(
                "zoneto.api.routes._geocode_address",
                return_value=(43.644, -79.402),
            ),
            patch(
                "zoneto.api.routes.lookup_site_context",
                return_value=self._site_ctx(),
            ),
        ):
            resp = client.post(
                "/ask",
                json={
                    "address": "441 King St W",
                    "description": "A 4-storey condo.",
                    "question": "Can I add a rooftop terrace?",
                    "history": history,
                },
            )
        assert resp.status_code == 200

    def test_ask_without_llm_returns_503(self, app) -> None:
        """Given: App with no LLM client.
        When: POST /ask.
        Then: Returns 503."""
        app.state.narrator = None
        test_client = TestClient(app, raise_server_exceptions=False)
        with (
            patch(
                "zoneto.api.routes._geocode_address",
                return_value=(43.644, -79.402),
            ),
            patch(
                "zoneto.api.routes.lookup_site_context",
                return_value=self._site_ctx(),
            ),
        ):
            resp = test_client.post(
                "/ask",
                json={
                    "address": "test",
                    "description": "test",
                    "question": "test?",
                },
            )
        assert resp.status_code == 503


class TestRetrieveChunksExceptionQuery:
    """Tests that _retrieve_chunks uses direct lookup for sites with exceptions."""

    def _make_index(self, queries: list[str], lookups: list[tuple[str, str]]):
        class _MockIndex:
            def search(self, query: str, zones=None, k: int = 6):
                queries.append(query)
                return []

            def lookup_exception(self, zone: str, exception_no: str):
                lookups.append((zone, exception_no))
                return []

        return _MockIndex()

    def test_exception_direct_lookup_called(self) -> None:
        """Given: Site with zoning_exception_no='252'.
        When: _retrieve_chunks is called.
        Then: lookup_exception('RM', '252') is called for direct retrieval."""
        from zoneto.api.routes import _retrieve_chunks

        queries: list[str] = []
        lookups: list[tuple[str, str]] = []
        _retrieve_chunks(
            self._make_index(queries, lookups),
            {
                "zoning_class": "RM",
                "permitted_use_category": "Residential",
                "zoning_exception": 1,
                "zoning_exception_no": "252",
            },
            "a tall building",
            None,
            k=4,
        )
        assert lookups, "lookup_exception was not called"
        assert ("RM", "252") in lookups, (
            f"Expected lookup_exception('RM', '252'), got: {lookups}"
        )

    def test_no_exception_uses_standard_query(self) -> None:
        """Given: Site with no zoning exception.
        When: _retrieve_chunks is called.
        Then: The zone-context query uses the standard 'permitted uses' format
              and lookup_exception is not called."""
        from zoneto.api.routes import _retrieve_chunks

        queries: list[str] = []
        lookups: list[tuple[str, str]] = []
        _retrieve_chunks(
            self._make_index(queries, lookups),
            {
                "zoning_class": "CR",
                "permitted_use_category": "Commercial Residential (mixed)",
                "zoning_exception": 0,
                "zoning_exception_no": None,
            },
            "a mixed-use building",
            None,
            k=4,
        )
        assert queries
        assert "permitted uses" in queries[0]
        assert not lookups, "lookup_exception should not be called without exception_no"


class TestFormatChunks:
    """Tests for _format_chunks: chunk text must not be truncated."""

    def test_full_chunk_text_is_preserved(self) -> None:
        """Given: A chunk whose text is longer than 600 characters.
        When: _format_chunks renders it.
        Then: The full text appears in the output without truncation."""
        from zoneto.analytics.bylaw_index import Chunk
        from zoneto.api.narrator import _format_chunks

        long_text = "x" * 700
        chunk = Chunk(
            chunk_id=1,
            chapter="10",
            section_number="10.80",
            section_title="Exception RM 252",
            source_file="part10.txt",
            zones=["RM"],
            text=long_text,
            score=1.0,
        )
        result = _format_chunks([chunk])
        assert long_text in result, (
            "Chunk text was truncated — expected full 700-char text in output"
        )
