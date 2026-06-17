"""Unit tests for the comparable-retrieval metric core.

These are pure functions over plain dicts — no data, no network, CI-safe.
They quantify how concordant a retrieved comp set is with a query on the
structured axes (zone, application_type, scale-of-ask) that actually drive
development outcomes.
"""

from __future__ import annotations

from zoneto.analytics.retrieval_eval import (
    aggregate,
    concordance_at_k,
    count_measurable,
    excess_band,
)


class TestExcessBand:
    def test_none_ratio_returns_none(self) -> None:
        """Given no excess ratio, When banded, Then the band is unknown (None)."""
        assert excess_band(None) is None

    def test_at_or_under_limit_is_within(self) -> None:
        """Given a ratio <= 1.0, When banded, Then it is 'within' the envelope."""
        assert excess_band(0.8) == "within"
        assert excess_band(1.0) == "within"

    def test_modest_overage_is_minor(self) -> None:
        """Given a ratio just over the limit, When banded, Then it is 'minor'."""
        assert excess_band(1.3) == "minor"
        assert excess_band(1.5) == "minor"

    def test_large_overage_is_major(self) -> None:
        """Given a ratio well over the limit, When banded, Then it is 'major'."""
        assert excess_band(2.0) == "major"
        assert excess_band(3.0) == "major"

    def test_extreme_overage_is_extreme(self) -> None:
        """Given a ratio far past the limit, When banded, Then it is 'extreme'."""
        assert excess_band(3.5) == "extreme"


class TestConcordanceAtK:
    def test_fraction_sharing_each_axis(self) -> None:
        """Given retrieved comps, When scored, Then each axis reports the fraction
        of comps sharing the query's value on that axis."""
        query = {"zone": "RM", "type": "OZ"}
        retrieved = [
            {"zone": "RM", "type": "OZ"},
            {"zone": "RM", "type": "SA"},
            {"zone": "R", "type": "OZ"},
            {"zone": "CR", "type": "OZ"},
        ]
        result = concordance_at_k(query, retrieved, axes=("zone", "type"))
        assert result["zone"] == 0.5  # 2 of 4 share RM
        assert result["type"] == 0.75  # 3 of 4 share OZ

    def test_unknown_query_value_yields_none(self) -> None:
        """Given the query lacks a value on an axis, When scored, Then that axis is
        unmeasurable (None) rather than counted as a miss."""
        query = {"zone": None}
        retrieved = [{"zone": "RM"}, {"zone": "R"}]
        result = concordance_at_k(query, retrieved, axes=("zone",))
        assert result["zone"] is None

    def test_all_retrieved_unknown_yields_none(self) -> None:
        """Given no retrieved comp has a value on an axis, When scored, Then the axis
        is None (empty denominator), not a divide-by-zero."""
        query = {"zone": "RM"}
        retrieved = [{"zone": None}, {"zone": None}]
        result = concordance_at_k(query, retrieved, axes=("zone",))
        assert result["zone"] is None

    def test_unknown_retrieved_excluded_from_denominator(self) -> None:
        """Given some comps lack a value, When scored, Then they are excluded from
        the denominator rather than counted as misses."""
        query = {"zone": "RM"}
        retrieved = [{"zone": "RM"}, {"zone": None}, {"zone": "R"}]
        result = concordance_at_k(query, retrieved, axes=("zone",))
        assert result["zone"] == 0.5  # 1 of the 2 known comps share RM


class TestAggregate:
    def test_mean_ignores_none_per_axis(self) -> None:
        """Given per-query scores with gaps, When aggregated, Then each axis means
        only its measurable queries."""
        per_query = [
            {"zone": 0.5, "type": 1.0},
            {"zone": 1.0, "type": None},
            {"zone": None, "type": 0.0},
        ]
        result = aggregate(per_query)
        assert result["zone"] == 0.75  # mean of 0.5, 1.0
        assert result["type"] == 0.5  # mean of 1.0, 0.0

    def test_axis_with_no_measurable_queries_is_none(self) -> None:
        """Given an axis no query could measure, When aggregated, Then it is None."""
        per_query = [{"zone": None}, {"zone": None}]
        result = aggregate(per_query)
        assert result["zone"] is None

    def test_empty_input_yields_empty(self) -> None:
        """Given no per-query scores, When aggregated, Then the result is empty."""
        assert aggregate([]) == {}


class TestCountMeasurable:
    def test_counts_non_none_per_axis(self) -> None:
        """Given per-query scores with gaps, When counted, Then each axis reports how
        many queries could measure it — so a high mean on a sparse axis is visible."""
        per_query = [
            {"zone": 0.5, "scale": None},
            {"zone": 1.0, "scale": None},
            {"zone": None, "scale": 1.0},
        ]
        result = count_measurable(per_query)
        assert result["zone"] == 2
        assert result["scale"] == 1

    def test_empty_input_yields_empty(self) -> None:
        """Given no per-query scores, When counted, Then the result is empty."""
        assert count_measurable([]) == {}
