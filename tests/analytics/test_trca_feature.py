"""Tests for in_trca_regulated_area and in_greenbelt spatial features."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from zoneto.analytics.spatial import _add_greenbelt_feature, _add_trca_feature


def _write_polygon_geojson(path: Path, bbox: tuple[float, float, float, float]) -> None:
    """Write a minimal GeoJSON polygon covering the given bounding box."""
    lon_min, lat_min, lon_max, lat_max = bbox
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
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


_DOWNTOWN_BBOX = (-79.40, 43.64, -79.36, 43.66)
_INSIDE_POINT = (43.650, -79.383)  # lat, lon — inside bbox
_OUTSIDE_POINT = (43.750, -79.500)  # lat, lon — outside bbox


class TestAddTrcaFeature:
    def test_point_inside_gets_flag_1(self, tmp_path: Path) -> None:
        """Given: Point within TRCA regulated area polygon.
        When: _add_trca_feature called.
        Then: in_trca_regulated_area == 1."""
        trca_path = tmp_path / "trca.geojson"
        _write_polygon_geojson(trca_path, _DOWNTOWN_BBOX)
        df = pl.DataFrame(
            {
                "_rid": pl.Series([0], dtype=pl.Int64),
                "lat": [_INSIDE_POINT[0]],
                "lon": [_INSIDE_POINT[1]],
            }
        )
        result = _add_trca_feature(df, trca_path)
        assert "in_trca_regulated_area" in result.columns
        assert result["in_trca_regulated_area"][0] == 1

    def test_point_outside_gets_flag_0(self, tmp_path: Path) -> None:
        """Given: Point outside TRCA regulated area polygon.
        When: _add_trca_feature called.
        Then: in_trca_regulated_area == 0."""
        trca_path = tmp_path / "trca.geojson"
        _write_polygon_geojson(trca_path, _DOWNTOWN_BBOX)
        df = pl.DataFrame(
            {
                "_rid": pl.Series([0], dtype=pl.Int64),
                "lat": [_OUTSIDE_POINT[0]],
                "lon": [_OUTSIDE_POINT[1]],
            }
        )
        result = _add_trca_feature(df, trca_path)
        assert result["in_trca_regulated_area"][0] == 0

    def test_null_coords_get_zero(self, tmp_path: Path) -> None:
        """Given: Row with null lat/lon.
        When: _add_trca_feature called.
        Then: in_trca_regulated_area == 0 (not null)."""
        trca_path = tmp_path / "trca.geojson"
        _write_polygon_geojson(trca_path, _DOWNTOWN_BBOX)
        df = pl.DataFrame(
            {
                "_rid": pl.Series([0], dtype=pl.Int64),
                "lat": pl.Series([None], dtype=pl.Float64),
                "lon": pl.Series([None], dtype=pl.Float64),
            }
        )
        result = _add_trca_feature(df, trca_path)
        assert result["in_trca_regulated_area"][0] == 0

    def test_missing_file_returns_zeros(self, tmp_path: Path) -> None:
        """Given: TRCA GeoJSON file does not exist.
        When: _add_trca_feature called.
        Then: in_trca_regulated_area is 0 for all rows."""
        trca_path = tmp_path / "trca_missing.geojson"
        df = pl.DataFrame(
            {
                "_rid": pl.Series([0, 1], dtype=pl.Int64),
                "lat": [43.65, 43.70],
                "lon": [-79.38, -79.45],
            }
        )
        result = _add_trca_feature(df, trca_path)
        assert result["in_trca_regulated_area"].to_list() == [0, 0]

    def test_multiple_rows_mixed_flags(self, tmp_path: Path) -> None:
        """Given: One point inside, one point outside TRCA area.
        When: _add_trca_feature called.
        Then: Flags are 1 and 0 respectively."""
        trca_path = tmp_path / "trca.geojson"
        _write_polygon_geojson(trca_path, _DOWNTOWN_BBOX)
        df = pl.DataFrame(
            {
                "_rid": pl.Series([0, 1], dtype=pl.Int64),
                "lat": [_INSIDE_POINT[0], _OUTSIDE_POINT[0]],
                "lon": [_INSIDE_POINT[1], _OUTSIDE_POINT[1]],
            }
        )
        result = _add_trca_feature(df, trca_path)
        flags = result["in_trca_regulated_area"].to_list()
        assert flags[0] == 1
        assert flags[1] == 0


class TestAddGreenbeltFeature:
    def test_point_inside_gets_flag_1(self, tmp_path: Path) -> None:
        """Given: Point within Greenbelt polygon.
        When: _add_greenbelt_feature called.
        Then: in_greenbelt == 1."""
        gb_path = tmp_path / "greenbelt.geojson"
        _write_polygon_geojson(gb_path, _DOWNTOWN_BBOX)
        df = pl.DataFrame(
            {
                "_rid": pl.Series([0], dtype=pl.Int64),
                "lat": [_INSIDE_POINT[0]],
                "lon": [_INSIDE_POINT[1]],
            }
        )
        result = _add_greenbelt_feature(df, gb_path)
        assert "in_greenbelt" in result.columns
        assert result["in_greenbelt"][0] == 1

    def test_point_outside_gets_flag_0(self, tmp_path: Path) -> None:
        """Given: Point outside Greenbelt polygon.
        When: _add_greenbelt_feature called.
        Then: in_greenbelt == 0."""
        gb_path = tmp_path / "greenbelt.geojson"
        _write_polygon_geojson(gb_path, _DOWNTOWN_BBOX)
        df = pl.DataFrame(
            {
                "_rid": pl.Series([0], dtype=pl.Int64),
                "lat": [_OUTSIDE_POINT[0]],
                "lon": [_OUTSIDE_POINT[1]],
            }
        )
        result = _add_greenbelt_feature(df, gb_path)
        assert result["in_greenbelt"][0] == 0

    def test_null_coords_get_zero(self, tmp_path: Path) -> None:
        """Given: Row with null lat/lon.
        When: _add_greenbelt_feature called.
        Then: in_greenbelt == 0."""
        gb_path = tmp_path / "greenbelt.geojson"
        _write_polygon_geojson(gb_path, _DOWNTOWN_BBOX)
        df = pl.DataFrame(
            {
                "_rid": pl.Series([0], dtype=pl.Int64),
                "lat": pl.Series([None], dtype=pl.Float64),
                "lon": pl.Series([None], dtype=pl.Float64),
            }
        )
        result = _add_greenbelt_feature(df, gb_path)
        assert result["in_greenbelt"][0] == 0

    def test_missing_file_returns_zeros(self, tmp_path: Path) -> None:
        """Given: Greenbelt GeoJSON file does not exist.
        When: _add_greenbelt_feature called.
        Then: in_greenbelt is 0 for all rows."""
        gb_path = tmp_path / "greenbelt_missing.geojson"
        df = pl.DataFrame(
            {
                "_rid": pl.Series([0, 1], dtype=pl.Int64),
                "lat": [43.65, 43.70],
                "lon": [-79.38, -79.45],
            }
        )
        result = _add_greenbelt_feature(df, gb_path)
        assert result["in_greenbelt"].to_list() == [0, 0]
