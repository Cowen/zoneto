"""Keyword-based classifier for proposed use type in application descriptions.

Cheap, debuggable alternative to NLP — flags `residential`, `commercial`,
`mixed_use`, `employment`, or `institutional` so the frontend can compare
against the zone's permitted-use category and surface mismatches.
"""

from __future__ import annotations

import re

# Explicit mixed-use phrases always win — even when only one single-category
# keyword is present, "live/work lofts above retail" is unambiguously mixed.
# "park" must be matched as a whole word — "parking" is a substring false positive.
_PARK_RE = re.compile(r"(?i)\bpark\b")

_MIXED_USE_TRIGGERS: tuple[str, ...] = (
    "mixed use",
    "mixed-use",
    "residential and commercial",
    "live/work",
    "live-work",
    "retail at grade",
    "ground-floor retail",
    "ground floor retail",
)

_KEYWORDS: dict[str, tuple[str, ...]] = {
    "residential": (
        "dwelling unit",
        "residential",
        "condominium",
        "apartment",
        "townhouse",
        "rental",
        "stacked",
    ),
    "commercial": (
        "retail",
        "office building",
        "office tower",
        "commercial",
        "restaurant",
        "hotel",
    ),
    "employment": (
        "industrial",
        "warehouse",
        "logistics",
        "manufacturing",
        "self-storage",
        "self storage",
        "refinery",
        "smelter",
        "smelting",
        "foundry",
        "quarry",
        "mining",
        "mine",
        "petroleum",
        "abattoir",
        "slaughterhouse",
        "asphalt plant",
        "cement plant",
        "rendering plant",
    ),
    "institutional": (
        "school",
        "hospital",
        "place of worship",
        "long-term care",
        "long term care",
        "community centre",
        "community center",
        "library",
        "child care",
        "childcare",
        "day care",
        "daycare",
        "museum",
    ),
    "recreational": (
        "parkland",
        "green space",
        "greenspace",
        "playground",
        "recreation centre",
        "recreation center",
        "recreational facility",
        "recreational use",
        "community garden",
        "sports field",
        "athletic field",
    ),
}

# Strip "no <category>" so "Commercial use only — no residential proposed"
# classifies as commercial, not as a residential-touching mixed proposal.
_NEGATION_RE = re.compile(
    r"\bno\s+(residential|commercial|employment|industrial|institutional)\b",
    re.IGNORECASE,
)

# Permitted GEN_ZONE categories (strings produced by site_context._map_gen_zone)
# mapped to the set of proposed-use buckets that are allowed as-of-right.
_ZONE_ALLOWED: dict[str, frozenset[str]] = {
    "Residential": frozenset(
        {"residential", "mixed_use", "recreational", "institutional"}
    ),
    "Residential Apartment": frozenset(
        {"residential", "mixed_use", "recreational", "institutional"}
    ),
    "Commercial Residential (mixed)": frozenset(
        {"residential", "commercial", "mixed_use", "recreational", "institutional"}
    ),
    "Commercial Residential Employment (mixed)": frozenset(
        {
            "residential",
            "commercial",
            "employment",
            "mixed_use",
            "recreational",
            "institutional",
        }
    ),
    "Commercial": frozenset(
        {"commercial", "mixed_use", "recreational", "institutional"}
    ),
    "Employment Industrial": frozenset({"employment", "recreational"}),
    "Institutional": frozenset({"institutional", "recreational"}),
    "Open Space": frozenset({"recreational"}),
    "Utility / Transportation": frozenset({"recreational", "institutional"}),
}


# Official Plan land-use designations (canonical Toronto OP names, produced by
# reference._OP_CLASS_NORMALIZE) → the proposed-use buckets each designation
# contemplates as-of-conformity. This is the *provincial* Official-Plan layer that
# pairs with the *municipal* zoning layer in _ZONE_ALLOWED: a use outside the
# designation's set signals that an Official Plan Amendment (Planning Act s.22), not
# just a rezoning, is required (s.24 conformity). Employment Areas notably exclude
# residential — the most common real OPA trigger.
_OP_DESIGNATION_ALLOWED: dict[str, frozenset[str]] = {
    "Neighbourhoods": frozenset({"residential", "institutional", "recreational"}),
    "Apartment Neighbourhoods": frozenset(
        {"residential", "institutional", "recreational"}
    ),
    "Mixed Use Areas": frozenset(
        {
            "residential",
            "commercial",
            "mixed_use",
            "employment",
            "institutional",
            "recreational",
        }
    ),
    "Regeneration Areas": frozenset(
        {
            "residential",
            "commercial",
            "mixed_use",
            "employment",
            "institutional",
            "recreational",
        }
    ),
    "Core Employment Areas": frozenset({"employment"}),
    "General Employment Areas": frozenset({"employment", "commercial"}),
    "Institutional Areas": frozenset({"institutional", "recreational"}),
    "Natural Areas": frozenset({"recreational"}),
    "Parks": frozenset({"recreational"}),
    "Other Open Space Areas": frozenset({"recreational"}),
    "Utility Corridors": frozenset({"recreational"}),
}


def classify_use(description: str | None) -> str | None:
    """Return a single use bucket inferred from a description, or None.

    Returns one of: 'residential', 'commercial', 'mixed_use', 'employment',
    'institutional', or None when no signal is found.
    """
    if not description:
        return None

    text = description.lower()

    for trigger in _MIXED_USE_TRIGGERS:
        if trigger in text:
            return "mixed_use"

    text = _NEGATION_RE.sub(" ", text)

    matches: dict[str, int] = {}
    if _PARK_RE.search(text):
        matches["recreational"] = 1
    for category, keywords in _KEYWORDS.items():
        hits = sum(1 for kw in keywords if kw in text)
        if hits:
            matches[category] = matches.get(category, 0) + hits

    if not matches:
        return None
    if len(matches) == 1:
        return next(iter(matches))
    # Multiple categories matched without an explicit mixed-use phrase →
    # the proposal touches more than one use type, classify as mixed.
    return "mixed_use"


def use_matches_zone(
    proposed: str | None, permitted_category: str | None
) -> int | None:
    """Return 1 if proposed use is permitted in the zone, 0 if not, None if unknown."""
    if proposed is None or permitted_category is None:
        return None
    allowed = _ZONE_ALLOWED.get(permitted_category)
    if allowed is None:
        return None
    return 1 if proposed in allowed else 0


def op_use_matches_designation(
    proposed: str | None, designation: str | None
) -> int | None:
    """Return 1 if the proposed use conforms to the Official Plan designation, 0 if
    not, None when either input is unknown or the designation isn't mapped.

    Counterpart to ``use_matches_zone`` for the provincial Official-Plan layer: a 0
    means an Official Plan Amendment (Planning Act s.22) is implicated beyond a plain
    zoning by-law amendment.
    """
    if proposed is None or designation is None:
        return None
    allowed = _OP_DESIGNATION_ALLOWED.get(designation)
    if allowed is None:
        return None
    return 1 if proposed in allowed else 0
