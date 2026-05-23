"""Narrator confidence calibration regression tests.

Integration tests require:
  - ANTHROPIC_API_KEY environment variable set
  - GIS reference data at data/reference/
  - Enriched dev_applications.parquet at data/enriched/ (for similarity)

These tests verify that the narrator assigns calibrated confidence scores
to real Toronto planning applications with known outcomes.

321 Boon Ave: residential R-zone with a 57m/13m height violation (4.4×).
  The programmatic cap in _apply_confidence_overrides() must clamp the score
  to <=30. This is a deterministic guard — no LLM non-determinism, because
  the cap fires in Python after the LLM score is parsed.

1613 St Clair Ave W (folderrsn 5124543, Council-approved OZ 2022) is the
  optimization target documented in .ralph/prompts/OPTIMIZE_1613_ST_CLAIR.md.
  A passing test for that site (confidence >= 55) belongs here once the system
  can reliably produce it — not before.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from zoneto.analytics.compliance import check_compliance
from zoneto.analytics.extract import extract_project_features
from zoneto.api.desc_similarity import score_description_similarity
from zoneto.api.llm_client import AnthropicClient
from zoneto.api.narrator import narrate_evaluation
from zoneto.api.site_context import lookup_site_context

_DESCRIPTION = (
    "The developer intends to develop the subject lands with a 17-storey, 57 meter "
    "mixed-use building consisting of 258 units in 16,732 square meters of residential "
    "floor area and 1,404 square meters of non-residential floor area. Non-residential "
    "floor area would be comprised of 57 square metres of community space, 271 square "
    "metres of retail, and 1,076 square metres of medical office. A total of 575.4 "
    "square meters of indoor amenity area and 721 square meters of outdoor "
    "amenity area is proposed. A Site Plan Control application has also been "
    "submitted in support of the proposal and has been circulated concurrently. "
    "The proposal has been revised to provide medical office space and community "
    "space noted above, simplified floor plates, relocation of parking to provide "
    "nine vehicle parking spaces above grade and 92 parking spaces below grade."
)

_REF_DIR = Path("data/reference")
_DATA_DIR = Path("data")
_MODEL_DIR = Path("models")

# Known coordinates from enriched dev_applications (EPSG:4326, reprojected)
_ST_CLAIR_LAT = 43.67290477633479
_ST_CLAIR_LON = -79.45543595876516

_BOON_LAT = 43.66384
_BOON_LON = -79.45013


def _require_api_key() -> str:
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        pytest.skip("ANTHROPIC_API_KEY not set")
    return key


def _require_ref_dir() -> None:
    if not _REF_DIR.exists():
        pytest.skip(f"GIS reference data not found: {_REF_DIR}")


def _narrate_for_address(
    lat: float,
    lon: float,
    *,
    api_key: str,
) -> tuple[str, int | None]:
    """Given/When/Then helper: run the full evaluate pipeline for a coordinate pair."""
    site = lookup_site_context(lat, lon, _REF_DIR)
    extracted = extract_project_features(_DESCRIPTION)
    violations = check_compliance(extracted, site)

    sim = score_description_similarity(
        _DESCRIPTION,
        data_dir=_DATA_DIR,
        model_dir=_MODEL_DIR,
        zoning_class=site.get("zoning_class"),
        lat=lat,
        lon=lon,
        radius_m=2000.0,
    )

    llm = AnthropicClient(api_key=api_key)
    return narrate_evaluation(
        site,
        extracted,
        violations,
        chunks=[],
        llm_client=llm,
        description_similarity=sim,
    )


@pytest.mark.integration
def test_boon_ave_confidence_cap() -> None:
    """Given a residential R-zone at 321 Boon Ave, when we narrate the same
    extreme mixed-use proposal, then confidence should be <= 35 (height cap fires).
    """
    _require_ref_dir()
    api_key = _require_api_key()
    _, score = _narrate_for_address(_BOON_LAT, _BOON_LON, api_key=api_key)
    assert score is not None, "narrator did not return a confidence score"
    assert score <= 35, (
        f"321 Boon Ave confidence {score} > 35 (extreme violations, expected <=35)"
    )
