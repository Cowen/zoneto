"""Tests for keyword-based proposed-use classifier."""

from __future__ import annotations

import pytest

from zoneto.analytics.use_classifier import classify_use, use_matches_zone

# ---------------------------------------------------------------------------
# classify_use
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "description,expected",
    [
        ("Proposed 12 storey residential apartment building", "residential"),
        ("28-storey condominium tower with 186 dwelling units", "residential"),
        ("Construct stacked townhouse development", "residential"),
        ("New rental apartment building", "residential"),
    ],
)
def test_classify_use_residential(description: str, expected: str) -> None:
    assert classify_use(description) == expected


@pytest.mark.parametrize(
    "description",
    [
        "Proposed mixed-use building with retail at grade and 200 dwelling units",
        "Mixed use development",
        "Residential and commercial tower",
        "Live/work lofts above retail",
    ],
)
def test_classify_use_mixed_use(description: str) -> None:
    assert classify_use(description) == "mixed_use"


@pytest.mark.parametrize(
    "description",
    [
        "New retail plaza",
        "12-storey office building",
        "Hotel with conference centre",
        "Commercial use only — no residential proposed",
    ],
)
def test_classify_use_commercial(description: str) -> None:
    assert classify_use(description) == "commercial"


@pytest.mark.parametrize(
    "description",
    [
        "New industrial warehouse facility",
        "Logistics and distribution centre",
        "Self-storage building",
        "Manufacturing plant expansion",
    ],
)
def test_classify_use_employment(description: str) -> None:
    assert classify_use(description) == "employment"


@pytest.mark.parametrize(
    "description",
    [
        "New elementary school addition",
        "Hospital expansion project",
        "Place of worship with parking",
        "Long-term care facility",
    ],
)
def test_classify_use_institutional(description: str) -> None:
    assert classify_use(description) == "institutional"


def test_classify_use_mixed_beats_residential() -> None:
    """When mixed-use keywords are present alongside residential, mixed_use wins."""
    desc = "Mixed-use building with 200 dwelling units and ground-floor retail"
    assert classify_use(desc) == "mixed_use"


def test_classify_use_case_insensitive() -> None:
    assert classify_use("RESIDENTIAL APARTMENT BUILDING") == "residential"
    assert classify_use("Industrial Warehouse") == "employment"


def test_classify_use_none_for_empty() -> None:
    assert classify_use(None) is None
    assert classify_use("") is None


def test_classify_use_none_when_no_keywords() -> None:
    """Pure procedural text with no use signals returns None."""
    assert classify_use("Rezoning to permit increased height") is None


# ---------------------------------------------------------------------------
# use_matches_zone
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "proposed,permitted,expected",
    [
        # residential category
        ("residential", "Residential", 1),
        ("residential", "Residential Apartment", 1),
        ("mixed_use", "Residential", 1),
        ("commercial", "Residential", 0),
        ("employment", "Residential", 0),
        ("institutional", "Residential", 0),
        # commercial residential (mixed) category
        ("residential", "Commercial Residential (mixed)", 1),
        ("commercial", "Commercial Residential (mixed)", 1),
        ("mixed_use", "Commercial Residential (mixed)", 1),
        ("employment", "Commercial Residential (mixed)", 0),
        # commercial residential employment (mixed) — most permissive
        ("residential", "Commercial Residential Employment (mixed)", 1),
        ("commercial", "Commercial Residential Employment (mixed)", 1),
        ("employment", "Commercial Residential Employment (mixed)", 1),
        ("mixed_use", "Commercial Residential Employment (mixed)", 1),
        ("institutional", "Commercial Residential Employment (mixed)", 0),
        # commercial-only category
        ("commercial", "Commercial", 1),
        ("mixed_use", "Commercial", 1),
        ("residential", "Commercial", 0),
        # employment industrial
        ("employment", "Employment Industrial", 1),
        ("residential", "Employment Industrial", 0),
        ("commercial", "Employment Industrial", 0),
        ("mixed_use", "Employment Industrial", 0),
        # institutional
        ("institutional", "Institutional", 1),
        ("residential", "Institutional", 0),
        # open space / utility — no development uses permitted
        ("residential", "Open Space", 0),
        ("commercial", "Open Space", 0),
        ("employment", "Utility / Transportation", 0),
    ],
)
def test_use_matches_zone_matrix(proposed: str, permitted: str, expected: int) -> None:
    assert use_matches_zone(proposed, permitted) == expected


def test_use_matches_zone_none_when_either_unknown() -> None:
    assert use_matches_zone(None, "Residential") is None
    assert use_matches_zone("residential", None) is None
    assert use_matches_zone(None, None) is None
