"""Tests for the development-charges loader and honest estimator.

CI-safe: exercises the packaged ``development_charges.json`` (deterministic
curated data) plus a temp-file override. No network, no API key.
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path

from zoneto.analytics.extract import ProjectFeatures
from zoneto.api.dev_charges import (
    DevelopmentChargeContext,
    estimate_development_charges,
    load_dc_schedule,
)

# The packaged schedule's current version is effective 2024-06-06 (Bylaw 1137-2022).
_AS_OF = datetime.date(2026, 6, 19)
_EFFECTIVE_FROM = datetime.date(2024, 6, 6)


def _feat(
    *,
    units: int | None = None,
    use: str | None = None,
    building_type: str | None = None,
    ground_floor_retail: bool = False,
) -> ProjectFeatures:
    return ProjectFeatures(
        proposed_storeys=None,
        proposed_units=units,
        proposed_use=use,
        has_ground_floor_retail=ground_floor_retail,
        building_type=building_type,
    )


class TestLoadDcSchedule:
    def test_returns_current_version_for_today(self) -> None:
        sched = load_dc_schedule(_AS_OF)
        assert sched is not None
        assert sched.source_bylaw == "1137-2022"
        assert sched.effective_from == _EFFECTIVE_FROM

    def test_effective_from_boundary_is_covered(self) -> None:
        """Given: as_of exactly equals effective_from.
        Then: that version is returned (window is inclusive of the start)."""
        sched = load_dc_schedule(_EFFECTIVE_FROM)
        assert sched is not None
        assert sched.effective_from == _EFFECTIVE_FROM

    def test_date_before_earliest_version_returns_none(self) -> None:
        """Given: as_of predates the earliest curated bylaw.
        Then: None — we do not guess historical rates."""
        assert load_dc_schedule(_EFFECTIVE_FROM - datetime.timedelta(days=1)) is None

    def test_config_path_override(self, tmp_path: Path) -> None:
        cfg = tmp_path / "dc.json"
        cfg.write_text(
            json.dumps(
                {
                    "versions": [
                        {
                            "effective_from": "2020-01-01",
                            "effective_to": None,
                            "source_bylaw": "TEST-1",
                            "source_url": "https://example.test",
                            "verified_date": "2026-06-19",
                            "indexing_note": "test note",
                            "residential": [
                                {
                                    "tenure": "non_rental",
                                    "form": "apartment",
                                    "unit_type": "Apartments 1 Bedroom",
                                    "rate_cad": 1000,
                                }
                            ],
                            "non_residential": [
                                {"use_type": "Non-res", "rate_per_sqm_cad": 10}
                            ],
                        }
                    ]
                }
            )
        )
        sched = load_dc_schedule(_AS_OF, config_path=cfg)
        assert sched is not None
        assert sched.source_bylaw == "TEST-1"


class TestEstimateResidentialRange:
    def test_apartment_units_yield_apartment_range(self) -> None:
        """Given: 100 apartment units.
        Then: per-unit range is the apartment band (52,676–80,690), totalled."""
        ctx = estimate_development_charges(
            _feat(units=100, use="residential", building_type="apartment"), _AS_OF
        )
        assert ctx is not None
        assert ctx.residential_form == "apartment"
        assert ctx.residential_rate_low == 52676
        assert ctx.residential_rate_high == 80690
        assert ctx.residential_total_low == 52676 * 100
        assert ctx.residential_total_high == 80690 * 100
        # Honesty: it is a range, not a single figure.
        assert ctx.residential_rate_low < ctx.residential_rate_high

    def test_units_without_building_type_use_full_nonrental_range(self) -> None:
        """Given: a unit count but no building form.
        Then: the range spans all non-rental unit types (37,356–137,846)."""
        ctx = estimate_development_charges(_feat(units=50, use="residential"), _AS_OF)
        assert ctx is not None
        assert ctx.residential_form is None
        assert ctx.residential_rate_low == 37356
        assert ctx.residential_rate_high == 137846

    def test_no_units_means_no_total_but_schedule_present(self) -> None:
        """Given: no extracted unit count.
        Then: no totals, but the raw schedule is still surfaced."""
        ctx = estimate_development_charges(_feat(use="residential"), _AS_OF)
        assert ctx is not None
        assert ctx.units is None
        assert ctx.residential_total_low is None
        assert ctx.residential_total_high is None
        assert len(ctx.residential_schedule) > 0


class TestEstimateNonResidential:
    def test_mixed_use_surfaces_rate_but_no_total(self) -> None:
        """Given: a mixed-use proposal (non-res GFA unknown).
        Then: the per-m² rate is shown but explicitly not totalled."""
        ctx = estimate_development_charges(
            _feat(units=100, use="mixed_use", building_type="apartment"), _AS_OF
        )
        assert ctx is not None
        assert ctx.non_residential_rate_per_sqm == 805.64
        assert ctx.non_residential_note is not None
        assert "without gross floor area" in ctx.non_residential_note.lower()

    def test_ground_floor_retail_triggers_non_residential_rate(self) -> None:
        ctx = estimate_development_charges(
            _feat(units=20, use="residential", ground_floor_retail=True), _AS_OF
        )
        assert ctx is not None
        assert ctx.non_residential_rate_per_sqm == 805.64

    def test_pure_residential_has_no_non_residential_rate(self) -> None:
        ctx = estimate_development_charges(
            _feat(units=20, use="residential", building_type="apartment"), _AS_OF
        )
        assert ctx is not None
        assert ctx.non_residential_rate_per_sqm is None


class TestEstimateAlwaysHonest:
    def test_exclusions_and_caveats_always_present(self) -> None:
        ctx = estimate_development_charges(_feat(units=10), _AS_OF)
        assert ctx is not None
        assert ctx.exclusions  # CBC, parkland, education DCs, exemptions
        assert any("Community Benefits" in e for e in ctx.exclusions)
        # The indexing-freeze story is always carried as a caveat.
        assert any("MM29.16" in c for c in ctx.caveats)

    def test_rental_note_present(self) -> None:
        ctx = estimate_development_charges(_feat(units=10), _AS_OF)
        assert ctx is not None
        assert ctx.rental_note is not None
        assert "rental" in ctx.rental_note.lower()

    def test_carries_provenance(self) -> None:
        ctx = estimate_development_charges(_feat(units=10), _AS_OF)
        assert ctx is not None
        assert ctx.source_bylaw == "1137-2022"
        assert ctx.verified_date == datetime.date(2026, 6, 19)
        assert ctx.source_url.startswith("https://")

    def test_returns_none_before_earliest_version(self) -> None:
        ctx = estimate_development_charges(
            _feat(units=10), _EFFECTIVE_FROM - datetime.timedelta(days=1)
        )
        assert ctx is None

    def test_context_is_development_charge_context(self) -> None:
        ctx = estimate_development_charges(_feat(units=10), _AS_OF)
        assert isinstance(ctx, DevelopmentChargeContext)


class TestFormatDevelopmentCharges:
    def test_returns_empty_when_none(self) -> None:
        from zoneto.api.narrator import _format_development_charges

        assert _format_development_charges(None) == ""

    def test_includes_range_and_bylaw(self) -> None:
        from zoneto.api.narrator import _format_development_charges

        ctx = estimate_development_charges(
            _feat(units=100, use="residential", building_type="apartment"), _AS_OF
        )
        out = _format_development_charges(ctx)
        assert "1137-2022" in out
        assert "5,267,600" in out  # 52,676 * 100 (low end of apartment range)
        assert "Excludes" in out

    def test_schedule_only_when_no_units(self) -> None:
        from zoneto.api.narrator import _format_development_charges

        ctx = estimate_development_charges(_feat(use="residential"), _AS_OF)
        out = _format_development_charges(ctx)
        assert "No unit count" in out


class TestNarrateEvaluationDevelopmentCharges:
    def _site(self):
        from zoneto.api.site_context import SiteContext

        return SiteContext.model_validate(
            {
                "zoning_class": "CR",
                "permitted_use_category": "Commercial Residential (mixed)",
            }
        )

    def test_development_charges_injected_into_prompt(self) -> None:
        from tests.stubs import capturing_eval_agent
        from zoneto.api.narrator import narrate_evaluation

        agent, captured = capturing_eval_agent(summary="Summary.", confidence=72)
        ctx = estimate_development_charges(
            _feat(units=100, use="mixed_use", building_type="apartment"), _AS_OF
        )
        narrate_evaluation(
            self._site(),
            _feat(units=100, use="mixed_use", building_type="apartment"),
            [],
            [],
            agent,
            development_charges=ctx,
        )
        assert "Development charges" in captured["prompt"]
        assert "1137-2022" in captured["prompt"]

    def test_development_charges_do_not_change_confidence(self) -> None:
        """Given: identical inputs with and without DC context.
        Then: the clamped confidence is identical — DCs never move the score."""
        from tests.stubs import stub_eval_agent
        from zoneto.api.narrator import narrate_evaluation

        feat = _feat(units=100, use="mixed_use", building_type="apartment")
        ctx = estimate_development_charges(feat, _AS_OF)

        _, score_without = narrate_evaluation(
            self._site(), feat, [], [], stub_eval_agent(confidence=72)
        )
        _, score_with = narrate_evaluation(
            self._site(),
            feat,
            [],
            [],
            stub_eval_agent(confidence=72),
            development_charges=ctx,
        )
        assert score_with == score_without
