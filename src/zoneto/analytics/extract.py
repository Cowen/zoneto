"""Extract structured fields from free-text project descriptions."""

from __future__ import annotations

import re
from dataclasses import dataclass

from zoneto.analytics.use_classifier import classify_use


@dataclass
class ProjectFeatures:
    """Structured fields extracted from a project description."""

    proposed_storeys: int | None
    proposed_units: int | None
    # one of: residential/commercial/mixed_use/employment/institutional/None
    proposed_use: str | None
    has_ground_floor_retail: bool
    description: str | None = None
    proposed_height_m: float | None = None
    # apartment/duplex/triplex/fourplex/multiplex/semi_detached/townhouse/detached/None
    building_type: str | None = None


_STOREY_RE = re.compile(r"(?i)(\d+)\s*-?\s*store?ys?")
_UNIT_RE = re.compile(r"(?i)(\d+)\s+(?:dwelling\s+)?units?")
_HEIGHT_M_RE = re.compile(r"(?i)(\d+(?:\.\d+)?)\s*-?\s*(?:metres?|meters?|m)\b")
_BUILDING_TYPE_MAP: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(?i)\bapartment\b"), "apartment"),
    (re.compile(r"(?i)\bduplex\b"), "duplex"),
    (re.compile(r"(?i)\btriplex\b"), "triplex"),
    (re.compile(r"(?i)\b(fourplex|four-plex|four\s+plex)\b"), "fourplex"),
    (re.compile(r"(?i)\bmultiplex\b"), "multiplex"),
    (re.compile(r"(?i)\bsemi-detached\b|\bsemi\s+detached\b"), "semi_detached"),
    (
        re.compile(
            r"(?i)\b(townhouse|town-house|town\s+house|row\s+house|stacked\s+townhouse)\b"
        ),
        "townhouse",
    ),
    (
        re.compile(
            r"(?i)\b(bungalow|single-?family|detached\s+house|detached\s+dwelling)\b"
        ),
        "detached",
    ),
]

_RETAIL_PHRASES = (
    "ground floor retail",
    "ground-floor retail",
    "retail at grade",
    "at-grade retail",
    "commercial at grade",
)


def extract_project_features(description: str | None) -> ProjectFeatures:
    """Extract structured fields from a project description string.

    Uses regex for numeric values (storeys, units) and keyword matching
    for use type and retail ground floor. Does not call any ML model;
    all extraction is deterministic.

    Args:
        description: Free-text project description, may be None.

    Returns:
        ProjectFeatures with all available fields populated (None when absent).
    """
    if not description:
        return ProjectFeatures(
            proposed_storeys=None,
            proposed_units=None,
            proposed_use=None,
            has_ground_floor_retail=False,
        )

    storeys: int | None = None
    storey_match = _STOREY_RE.search(description)
    if storey_match:
        storeys = int(storey_match.group(1))

    units: int | None = None
    unit_match = _UNIT_RE.search(description)
    if unit_match:
        units = int(unit_match.group(1))

    height_m: float | None = None
    height_match = _HEIGHT_M_RE.search(description)
    if height_match:
        height_m = float(height_match.group(1))

    proposed_use = classify_use(description)

    lower = description.lower()
    has_retail = any(phrase in lower for phrase in _RETAIL_PHRASES)

    building_type: str | None = None
    for pattern, label in _BUILDING_TYPE_MAP:
        if pattern.search(description):
            building_type = label
            break

    return ProjectFeatures(
        proposed_storeys=storeys,
        proposed_units=units,
        proposed_use=proposed_use,
        has_ground_floor_retail=has_retail,
        description=description,
        proposed_height_m=height_m,
        building_type=building_type,
    )
