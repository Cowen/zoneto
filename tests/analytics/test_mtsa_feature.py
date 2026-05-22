"""Tests for in_mtsa spatial feature extraction."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from zoneto.analytics.spatial import _add_mtsa_feature


def _write_mtsa_geojson(path: Path) -> None:
    """Write a minimal GeoJSON with a single MTSA polygon covering downtown Toronto."""
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"STATION_NAME": "Union Station"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-79.40, 43.64],
                            [-79.36, 43.64],
                            [-79.36, 43.66],
                            [-79.40, 43.66],
                            [-79.40, 43.64],
                        ]
                    ],
                },
            }
        ],
    }
    path.write_text(json.dumps(geojson))


def test_in_mtsa_true_for_point_inside(tmp_path: Path) -> None:
    """Point at (43.650, -79.383) falls within the MTSA polygon → in_mtsa=1."""
    mtsa_path = tmp_path / "mtsa.geojson"
    _write_mtsa_geojson(mtsa_path)

    df = pl.DataFrame(
        {
            "_rid": pl.Series([0], dtype=pl.Int64),
            "lat": [43.650],
            "lon": [-79.383],
        }
    )
    result = _add_mtsa_feature(df, mtsa_path)
    assert "in_mtsa" in result.columns
    assert result["in_mtsa"][0] == 1


def test_in_mtsa_false_for_point_outside(tmp_path: Path) -> None:
    """Point at (43.700, -79.450) is outside the MTSA polygon → in_mtsa=0."""
    mtsa_path = tmp_path / "mtsa.geojson"
    _write_mtsa_geojson(mtsa_path)

    df = pl.DataFrame(
        {
            "_rid": pl.Series([0], dtype=pl.Int64),
            "lat": [43.700],
            "lon": [-79.450],
        }
    )
    result = _add_mtsa_feature(df, mtsa_path)
    assert result["in_mtsa"][0] == 0


def test_in_mtsa_null_coords_get_zero(tmp_path: Path) -> None:
    """Rows with null lat/lon get in_mtsa=0."""
    mtsa_path = tmp_path / "mtsa.geojson"
    _write_mtsa_geojson(mtsa_path)

    df = pl.DataFrame(
        {
            "_rid": pl.Series([0], dtype=pl.Int64),
            "lat": pl.Series([None], dtype=pl.Float64),
            "lon": pl.Series([None], dtype=pl.Float64),
        }
    )
    result = _add_mtsa_feature(df, mtsa_path)
    assert result["in_mtsa"][0] == 0


def test_in_mtsa_missing_file_returns_zeros(tmp_path: Path) -> None:
    """When mtsa.geojson does not exist, in_mtsa is 0 for all rows."""
    mtsa_path = tmp_path / "mtsa.geojson"  # does not exist

    df = pl.DataFrame(
        {
            "_rid": pl.Series([0, 1], dtype=pl.Int64),
            "lat": [43.650, 43.700],
            "lon": [-79.383, -79.450],
        }
    )
    result = _add_mtsa_feature(df, mtsa_path)
    assert result["in_mtsa"].to_list() == [0, 0]
