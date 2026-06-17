"""Unit tests for spatial enrichment helpers."""

from __future__ import annotations

import polars as pl

from zoneto.analytics.spatial import _normalize_zoning_limits


def test_normalize_zoning_limits_nulls_no_limit_sentinel() -> None:
    """Given the by-law -1 'no limit' sentinel (and any non-positive), When the
    zoning limit columns are normalized, Then those values become null so excess
    ratios and displays never treat -1 as a real cap; positive limits survive.
    """
    df = pl.DataFrame(
        {
            "zoning_max_units": pl.Series([-1, 0, 4, 100], dtype=pl.Int32),
            "zoning_max_density": pl.Series([-1.0, 0.0, 2.5, 3.0], dtype=pl.Float64),
        }
    )
    out = _normalize_zoning_limits(df)
    assert out["zoning_max_units"].to_list() == [None, None, 4, 100]
    assert out["zoning_max_density"].to_list() == [None, None, 2.5, 3.0]


def test_normalize_zoning_limits_tolerates_missing_columns() -> None:
    """Given a frame without the zoning limit columns, When normalized, Then it is
    returned unchanged (no KeyError)."""
    df = pl.DataFrame({"other": [1, 2]})
    out = _normalize_zoning_limits(df)
    assert out.columns == ["other"]
