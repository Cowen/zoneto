"""Tests for the op_land_use_designation spatial feature."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from zoneto.analytics.spatial import _add_op_land_use_feature


def _write_op_geojson(
    path: Path, bbox: tuple[float, float, float, float], designation: str
) -> None:
    """Write a minimal OP land-use GeoJSON polygon over bbox with a designation."""
    lon_min, lat_min, lon_max, lat_max = bbox
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"op_designation": designation},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [lon_min, lat_min],
                            [lon_max, lat_min],
                            [lon_max, lat_max],
                            [lon_min, lat_max],
                            [lon_min, lat_min],
                        ]
                    ],
                },
            }
        ],
    }
    path.write_text(json.dumps(geojson))


_BBOX = (-79.40, 43.64, -79.36, 43.66)
_INSIDE = (43.650, -79.383)  # lat, lon — inside bbox
_OUTSIDE = (43.750, -79.500)  # lat, lon — outside bbox


class TestAddOpLandUseFeature:
    def test_point_inside_gets_designation(self, tmp_path: Path) -> None:
        op_path = tmp_path / "op_land_use.geojson"
        _write_op_geojson(op_path, _BBOX, "Mixed Use Areas")
        df = pl.DataFrame(
            {
                "_rid": pl.Series([0], dtype=pl.Int64),
                "lat": [_INSIDE[0]],
                "lon": [_INSIDE[1]],
            }
        )
        result = _add_op_land_use_feature(df, op_path)
        assert "op_land_use_designation" in result.columns
        assert result["op_land_use_designation"][0] == "Mixed Use Areas"

    def test_point_outside_gets_null(self, tmp_path: Path) -> None:
        op_path = tmp_path / "op_land_use.geojson"
        _write_op_geojson(op_path, _BBOX, "Mixed Use Areas")
        df = pl.DataFrame(
            {
                "_rid": pl.Series([0], dtype=pl.Int64),
                "lat": [_OUTSIDE[0]],
                "lon": [_OUTSIDE[1]],
            }
        )
        result = _add_op_land_use_feature(df, op_path)
        assert result["op_land_use_designation"][0] is None

    def test_null_coords_get_null(self, tmp_path: Path) -> None:
        op_path = tmp_path / "op_land_use.geojson"
        _write_op_geojson(op_path, _BBOX, "Neighbourhoods")
        df = pl.DataFrame(
            {
                "_rid": pl.Series([0], dtype=pl.Int64),
                "lat": pl.Series([None], dtype=pl.Float64),
                "lon": pl.Series([None], dtype=pl.Float64),
            }
        )
        result = _add_op_land_use_feature(df, op_path)
        assert result["op_land_use_designation"][0] is None

    def test_missing_file_returns_nulls(self, tmp_path: Path) -> None:
        """Optional layer: absent file → null for all rows, dtype String."""
        op_path = tmp_path / "op_missing.geojson"
        df = pl.DataFrame(
            {
                "_rid": pl.Series([0, 1], dtype=pl.Int64),
                "lat": [43.65, 43.70],
                "lon": [-79.38, -79.45],
            }
        )
        result = _add_op_land_use_feature(df, op_path)
        assert result["op_land_use_designation"].dtype == pl.String
        assert result["op_land_use_designation"].to_list() == [None, None]

    def test_multiple_rows_mixed(self, tmp_path: Path) -> None:
        op_path = tmp_path / "op_land_use.geojson"
        _write_op_geojson(op_path, _BBOX, "Neighbourhoods")
        df = pl.DataFrame(
            {
                "_rid": pl.Series([0, 1], dtype=pl.Int64),
                "lat": [_INSIDE[0], _OUTSIDE[0]],
                "lon": [_INSIDE[1], _OUTSIDE[1]],
            }
        )
        result = _add_op_land_use_feature(df, op_path)
        vals = result["op_land_use_designation"].to_list()
        assert vals[0] == "Neighbourhoods"
        assert vals[1] is None
