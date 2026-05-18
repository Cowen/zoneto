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


_STOREY_RE = re.compile(r"(?i)(\d+)\s*-?\s*store?ys?")
_UNIT_RE = re.compile(r"(?i)(\d+)\s+(?:dwelling\s+)?units?")
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

    proposed_use = classify_use(description)

    lower = description.lower()
    has_retail = any(phrase in lower for phrase in _RETAIL_PHRASES)

    return ProjectFeatures(
        proposed_storeys=storeys,
        proposed_units=units,
        proposed_use=proposed_use,
        has_ground_floor_retail=has_retail,
        description=description,
    )
