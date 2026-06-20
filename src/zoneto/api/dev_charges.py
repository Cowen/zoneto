"""Toronto municipal development charges: rate schedule + honest estimator.

Development charges (DCs) in Toronto are a **city-wide flat rate schedule** set
by by-law — NOT spatial and NOT zoning-dependent. A rate applies per residential
dwelling unit (by unit type) and per square metre of non-residential ground-floor
gross floor area. The rates change over time via indexing and by-law amendments,
so the schedule is **version-effective-dated**.

This module loads the curated schedule (``development_charges.json``, a packaged
resource — ``data/`` is gitignored, so curated data that cannot be re-fetched
ships inside the package, mirroring ``zoneto.llm.config``'s handling of
``agents.toml``) and produces a :class:`DevelopmentChargeContext` for the
narrator and the API response.

The estimate is deliberately **honest**, because Zoneto presents to experts and a
false-precise number is worse than none. Feature extraction yields a total unit
count but neither the bedroom-type mix nor non-residential floor area, so:

* residential charges are surfaced as a **per-unit range** across the applicable
  dwelling-unit types, never a single false-precise figure;
* the non-residential charge is surfaced as a **per-m² rate** that we decline to
  total without a gross-floor-area input;
* when no unit count was extracted, only the schedule is shown — no total.

Resolution order for the config file mirrors ``zoneto.llm.config``:

  1. an explicit ``config_path`` argument, else
  2. the ``ZONETO_DC_CONFIG`` env var, else
  3. the packaged ``zoneto/api/development_charges.json``.

UPDATE PROCEDURE: when the by-law or indexing changes, append a new version to
``development_charges.json`` (set the prior version's ``effective_to``) and bump
``verified_date``. See that file's ``_comment`` for details.
"""

from __future__ import annotations

import datetime
import json
import os
from importlib import resources
from pathlib import Path

from pydantic import BaseModel

from zoneto.analytics.extract import ProjectFeatures

# Charges this module deliberately does NOT estimate. Surfaced so an expert knows
# the headline figure is municipal DCs only and the true cost stack is larger.
_EXCLUSIONS = [
    "Community Benefits Charge (s.37 / CBC — capped at 4% of land value for "
    "buildings ≥ 5 storeys and ≥ 10 units)",
    "Parkland dedication / cash-in-lieu (Planning Act s.42)",
    "Education development charges (school boards)",
    "Statutory exemptions and reductions (e.g. affordable/inclusionary units, "
    "developments up to 6 units, Section 27 agreement locking) are NOT applied "
    "to the figures below",
]


class ResidentialRate(BaseModel):
    """One residential per-dwelling-unit rate row from the schedule."""

    tenure: str  # "non_rental" | "rental"
    form: str  # "singles_semis" | "multiples" | "apartment" | "dwelling_room"
    unit_type: str
    rate_cad: float


class NonResidentialRate(BaseModel):
    """One non-residential per-square-metre rate row from the schedule."""

    use_type: str
    rate_per_sqm_cad: float


class DCScheduleVersion(BaseModel):
    """A development-charge rate schedule effective over a date window.

    ``effective_to`` is ``None`` for the current (open-ended) version.
    """

    effective_from: datetime.date
    effective_to: datetime.date | None = None
    source_bylaw: str
    source_url: str
    verified_date: datetime.date
    indexing_note: str
    non_residential_ground_floor_only: bool = True
    residential: list[ResidentialRate]
    non_residential: list[NonResidentialRate]

    def covers(self, as_of: datetime.date) -> bool:
        """True when ``as_of`` falls in ``[effective_from, effective_to)``."""
        if as_of < self.effective_from:
            return False
        return self.effective_to is None or as_of < self.effective_to


class DevelopmentChargeContext(BaseModel):
    """Informational DC context for a proposal — never a compliance violation.

    Carries both a structured estimate (when inputs allow) and the full schedule
    for raw display. Mirrors :class:`CommunityBenefitsContext`: it is surfaced to
    the narrator and echoed in the API response, but never feeds the confidence
    score.
    """

    source_bylaw: str
    effective_from: datetime.date
    verified_date: datetime.date
    source_url: str
    indexing_note: str

    # Residential estimate (a RANGE, never a single false-precise total).
    units: int | None = None
    residential_form: str | None = None
    residential_rate_low: float | None = None
    residential_rate_high: float | None = None
    residential_total_low: float | None = None
    residential_total_high: float | None = None

    # Non-residential: a per-m² rate we will not total without a GFA input.
    non_residential_rate_per_sqm: float | None = None
    non_residential_note: str | None = None

    # Raw schedule for display (non-rental rows) + the rental alternative note.
    residential_schedule: list[ResidentialRate] = []
    rental_note: str | None = None

    exclusions: list[str] = []
    caveats: list[str] = []


# building_type (from extract.py) -> schedule "form". Used to narrow the per-unit
# range to the proposal's built form when it is known.
_BUILDING_TYPE_TO_FORM = {
    "apartment": "apartment",
    "multiplex": "multiples",
    "fourplex": "multiples",
    "triplex": "multiples",
    "duplex": "multiples",
}


def _read_config_bytes(config_path: Path | None) -> bytes:
    if config_path is not None:
        return config_path.read_bytes()
    env = os.environ.get("ZONETO_DC_CONFIG")
    if env:
        return Path(env).read_bytes()
    return (resources.files("zoneto.api") / "development_charges.json").read_bytes()


def load_dc_schedule(
    as_of: datetime.date, *, config_path: Path | None = None
) -> DCScheduleVersion | None:
    """Return the schedule version in effect on ``as_of``.

    Picks the version whose ``[effective_from, effective_to)`` window contains
    ``as_of``. Falls back to the latest version with ``effective_from <= as_of``
    (open-ended current schedule). Returns ``None`` when ``as_of`` predates every
    version (we do not guess rates for dates before the earliest curated bylaw).
    """
    data = json.loads(_read_config_bytes(config_path).decode("utf-8"))
    versions = [DCScheduleVersion.model_validate(v) for v in data["versions"]]

    covering = [v for v in versions if v.covers(as_of)]
    if covering:
        return max(covering, key=lambda v: v.effective_from)

    eligible = [v for v in versions if v.effective_from <= as_of]
    if eligible:
        return max(eligible, key=lambda v: v.effective_from)
    return None


def _is_non_residential_use(extracted: ProjectFeatures) -> bool:
    """True when the proposal plausibly includes non-residential floor area."""
    use = (extracted.proposed_use or "").lower()
    if extracted.has_ground_floor_retail:
        return True
    return any(k in use for k in ("mixed", "commercial", "retail", "office", "employ"))


def estimate_development_charges(
    extracted: ProjectFeatures,
    as_of: datetime.date,
    *,
    config_path: Path | None = None,
) -> DevelopmentChargeContext | None:
    """Build a :class:`DevelopmentChargeContext` for a proposal.

    Honesty rules:
      * units known -> residential per-unit **range** (min/max across the
        applicable unit types) and a total range; never a single figure.
      * non-residential use indicated -> per-m² rate, but **no total** (gross
        floor area is not extracted).
      * units unknown -> schedule only, no totals.

    Returns ``None`` when no schedule covers ``as_of`` (so callers surface
    nothing rather than guessing).
    """
    schedule = load_dc_schedule(as_of, config_path=config_path)
    if schedule is None:
        return None

    non_rental = [r for r in schedule.residential if r.tenure == "non_rental"]
    rental = [r for r in schedule.residential if r.tenure == "rental"]

    caveats = [
        "The per-unit development charge depends on dwelling-unit type (bedroom "
        "mix), which is not present in the application text. Figures are shown as "
        "a range across the applicable unit types, not a single amount.",
        schedule.indexing_note,
    ]
    if schedule.non_residential_ground_floor_only:
        caveats.append(
            "Non-residential development charges apply to non-residential gross "
            "floor area on the ground floor only."
        )

    ctx = DevelopmentChargeContext(
        source_bylaw=schedule.source_bylaw,
        effective_from=schedule.effective_from,
        verified_date=schedule.verified_date,
        source_url=schedule.source_url,
        indexing_note=schedule.indexing_note,
        residential_schedule=non_rental,
        exclusions=list(_EXCLUSIONS),
        caveats=caveats,
    )

    # Rental alternative note (purpose-built rental is charged less; we cannot
    # reliably detect tenure from the text, so we default to non-rental and note
    # the lower rental band rather than silently picking one).
    if rental:
        r_low = min(r.rate_cad for r in rental)
        r_high = max(r.rate_cad for r in rental)
        ctx.rental_note = (
            f"Purpose-built rental units are charged less: roughly "
            f"${r_low:,.0f}–${r_high:,.0f} per unit. Figures below assume "
            f"non-rental (ownership) tenure."
        )

    # Residential range when a unit count was extracted.
    units = extracted.proposed_units
    if units is not None and non_rental:
        form = _BUILDING_TYPE_TO_FORM.get(extracted.building_type or "")
        rows = [r for r in non_rental if r.form == form] if form else non_rental
        if not rows:  # form mapped but absent from schedule -> fall back to all
            rows = non_rental
            form = None
        low = min(r.rate_cad for r in rows)
        high = max(r.rate_cad for r in rows)
        ctx.units = units
        ctx.residential_form = form
        ctx.residential_rate_low = low
        ctx.residential_rate_high = high
        ctx.residential_total_low = low * units
        ctx.residential_total_high = high * units

    # Non-residential rate (never totalled — no GFA).
    if _is_non_residential_use(extracted) and schedule.non_residential:
        ctx.non_residential_rate_per_sqm = schedule.non_residential[0].rate_per_sqm_cad
        ctx.non_residential_note = (
            "Cannot total the non-residential portion without gross floor area "
            "(m²); the rate above applies per m² of ground-floor "
            "non-residential GFA."
        )

    return ctx
