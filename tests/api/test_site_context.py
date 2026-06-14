"""Tests for GET /site-context endpoint and lookup_site_context()."""

from __future__ import annotations

import datetime
import json
from pathlib import Path

import polars as pl
import pytest
from starlette.testclient import TestClient

from zoneto.api.app import create_app
from zoneto.api.site_context import lookup_site_context, nearby_applications

# --- fixtures ---


@pytest.fixture
def ref_dir(tmp_path: Path) -> Path:
    """Create minimal reference geodata for spatial lookups.

    Uses tiny GeoJSON polygons around a known test point (43.65, -79.38).
    """
    ref = tmp_path / "reference"
    ref.mkdir()

    # Zoning: a polygon that contains (43.65, -79.38)
    zoning = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"ZN_ZONE": "CR", "UNITS": 200, "DENSITY": 3.5},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-79.39, 43.64],
                            [-79.37, 43.64],
                            [-79.37, 43.66],
                            [-79.39, 43.66],
                            [-79.39, 43.64],
                        ]
                    ],
                },
            }
        ],
    }
    (ref / "zoning.geojson").write_text(json.dumps(zoning))

    # Secondary plans: a polygon that contains our test point
    sp = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"SECONDARY_PLAN_NAME": "King-Spadina"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-79.39, 43.64],
                            [-79.37, 43.64],
                            [-79.37, 43.66],
                            [-79.39, 43.66],
                            [-79.39, 43.64],
                        ]
                    ],
                },
            }
        ],
    }
    (ref / "secondary_plans.geojson").write_text(json.dumps(sp))

    # Official Plan land-use designation: same polygon, "Mixed Use Areas"
    op = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"op_designation": "Mixed Use Areas"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-79.39, 43.64],
                            [-79.37, 43.64],
                            [-79.37, 43.66],
                            [-79.39, 43.66],
                            [-79.39, 43.64],
                        ]
                    ],
                },
            }
        ],
    }
    (ref / "op_land_use.geojson").write_text(json.dumps(op))

    return ref


@pytest.fixture
def data_dir_with_ref(tmp_path: Path, ref_dir: Path) -> Path:
    """Full data_dir with enriched parquet + reference geodata."""
    enriched_dir = tmp_path / "enriched"
    enriched_dir.mkdir(parents=True)

    import datetime

    current_year = datetime.date.today().year
    df = pl.DataFrame(
        {
            "folderrsn": ["F001"],
            "application_type": ["OZ"],
            "ward_number": ["10"],
            "zoning_class": ["CR"],
            "status": ["Active"],
            "year_submitted": pl.Series([current_year - 1], dtype=pl.Int32),
            "lat": [43.65],
            "lon": [-79.38],
            "dev_approved": pl.Series([None], dtype=pl.Int8),
            "dev_appealed": pl.Series([None], dtype=pl.Int8),
            "dev_days_to_decision": pl.Series([None], dtype=pl.Int32),
            "proposed_storeys": pl.Series([None], dtype=pl.Int32),
            "proposed_units": pl.Series([None], dtype=pl.Int32),
            "description": ["desc"],
            "street_num": ["1"],
            "street_name": ["Main St"],
        }
    )
    df.write_parquet(enriched_dir / "dev_applications.parquet")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "metrics.json").write_text(json.dumps({}))

    return tmp_path


@pytest.fixture
def client_with_ref(data_dir_with_ref: Path) -> TestClient:
    app = create_app(
        data_dir=data_dir_with_ref,
        model_dir=data_dir_with_ref / "models",
    )
    with TestClient(app) as c:
        yield c


# --- unit tests for lookup_site_context ---


def test_lookup_returns_zoning_class(ref_dir: Path) -> None:
    """Point inside the test polygon returns zoning_class=CR."""
    result = lookup_site_context(43.65, -79.38, ref_dir)
    assert result["zoning_class"] == "CR"


def test_lookup_returns_zoning_limits(ref_dir: Path) -> None:
    """Point inside the test polygon returns zoning limits."""
    result = lookup_site_context(43.65, -79.38, ref_dir)
    assert result["zoning_max_units"] == 200
    assert result["zoning_max_density"] == 3.5


def test_lookup_returns_secondary_plan(ref_dir: Path) -> None:
    """Point inside secondary plan polygon returns plan name."""
    result = lookup_site_context(43.65, -79.38, ref_dir)
    assert result["secondary_plan_name"] == "King-Spadina"
    assert result["in_secondary_plan"] == 1


def test_lookup_returns_op_designation(ref_dir: Path) -> None:
    """Point inside the OP polygon returns its land-use designation."""
    result = lookup_site_context(43.65, -79.38, ref_dir)
    assert result["op_land_use_designation"] == "Mixed Use Areas"


def test_lookup_outside_all_polygons(ref_dir: Path) -> None:
    """Point far outside all polygons returns nulls and zeros."""
    result = lookup_site_context(44.0, -80.0, ref_dir)
    assert result["op_land_use_designation"] is None
    assert result["zoning_class"] is None
    assert result["zoning_max_units"] is None
    assert result["zoning_max_density"] is None
    assert result["in_secondary_plan"] == 0
    assert result["in_heritage_register"] == 0
    assert result["in_heritage_district"] == 0
    assert result["in_mtsa"] == 0
    # New zoning fields default to None / 0 outside any polygon
    assert result["permitted_use_category"] is None
    assert result["zoning_min_frontage_m"] is None
    assert result["zoning_min_lot_area_sqm"] is None
    assert result["zoning_max_coverage_pct"] is None
    assert result["zoning_min_sqm_per_unit"] is None
    assert result["zoning_holding"] == 0
    assert result["zoning_exception"] == 0
    assert result["zoning_exception_no"] is None
    assert result["zoning_pct_res"] is None
    assert result["zoning_pct_comm"] is None
    assert result["zoning_pct_emp"] is None


def _write_zoning_geojson(ref: Path, properties: dict) -> None:
    """Overwrite ref/zoning.geojson with a single polygon carrying given properties.

    The polygon covers (-79.39,-79.37) × (43.64,43.66), which contains (43.65, -79.38).
    """
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": properties,
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-79.39, 43.64],
                            [-79.37, 43.64],
                            [-79.37, 43.66],
                            [-79.39, 43.66],
                            [-79.39, 43.64],
                        ]
                    ],
                },
            }
        ],
    }
    (ref / "zoning.geojson").write_text(json.dumps(geojson))


def test_lookup_returns_permitted_use_category_residential(ref_dir: Path) -> None:
    """GEN_ZONE=0 → 'Residential' category."""
    _write_zoning_geojson(
        ref_dir,
        {"ZN_ZONE": "R", "UNITS": -1, "DENSITY": -1, "GEN_ZONE": 0},
    )
    result = lookup_site_context(43.65, -79.38, ref_dir)
    assert result["permitted_use_category"] == "Residential"


@pytest.mark.parametrize(
    "code,expected",
    [
        (0, "Residential"),
        (1, "Open Space"),
        (2, "Utility / Transportation"),
        (4, "Employment Industrial"),
        (5, "Institutional"),
        (6, "Commercial Residential Employment (mixed)"),
        (101, "Residential Apartment"),
        (201, "Commercial"),
        (202, "Commercial Residential (mixed)"),
    ],
)
def test_lookup_maps_all_gen_zone_codes(
    ref_dir: Path, code: int, expected: str
) -> None:
    _write_zoning_geojson(
        ref_dir, {"ZN_ZONE": "Z", "UNITS": -1, "DENSITY": -1, "GEN_ZONE": code}
    )
    result = lookup_site_context(43.65, -79.38, ref_dir)
    assert result["permitted_use_category"] == expected


def test_lookup_returns_physical_constraints(ref_dir: Path) -> None:
    """FRONTAGE / ZN_AREA / COVERAGE / AREA_UNITS surfaced when > 0."""
    _write_zoning_geojson(
        ref_dir,
        {
            "ZN_ZONE": "R",
            "UNITS": -1,
            "DENSITY": -1,
            "GEN_ZONE": 0,
            "FRONTAGE": 7.5,
            "ZN_AREA": 220.0,
            "COVERAGE": 35.0,
            "AREA_UNITS": 200.0,
        },
    )
    result = lookup_site_context(43.65, -79.38, ref_dir)
    assert result["zoning_min_frontage_m"] == 7.5
    assert result["zoning_min_lot_area_sqm"] == 220.0
    assert result["zoning_max_coverage_pct"] == 35.0
    assert result["zoning_min_sqm_per_unit"] == 200.0


@pytest.mark.parametrize("sentinel", [-1, 0])
def test_lookup_physical_constraint_sentinels_become_none(
    ref_dir: Path, sentinel: float
) -> None:
    """-1 ('no limit') and 0 (unset) become None for optional minimums."""
    _write_zoning_geojson(
        ref_dir,
        {
            "ZN_ZONE": "R",
            "UNITS": -1,
            "DENSITY": -1,
            "GEN_ZONE": 0,
            "FRONTAGE": sentinel,
            "ZN_AREA": sentinel,
            "COVERAGE": sentinel,
            "AREA_UNITS": sentinel,
        },
    )
    result = lookup_site_context(43.65, -79.38, ref_dir)
    assert result["zoning_min_frontage_m"] is None
    assert result["zoning_min_lot_area_sqm"] is None
    assert result["zoning_max_coverage_pct"] is None
    assert result["zoning_min_sqm_per_unit"] is None


def test_lookup_holding_and_exception_flags(ref_dir: Path) -> None:
    """ZN_HOLDING='Y' → 1; ZN_EXCPTN='Y' with EXCPTN_NO → returned."""
    _write_zoning_geojson(
        ref_dir,
        {
            "ZN_ZONE": "R",
            "UNITS": -1,
            "DENSITY": -1,
            "GEN_ZONE": 0,
            "ZN_HOLDING": "Y",
            "ZN_EXCPTN": "Y",
            "EXCPTN_NO": "42",
        },
    )
    result = lookup_site_context(43.65, -79.38, ref_dir)
    assert result["zoning_holding"] == 1
    assert result["zoning_exception"] == 1
    assert result["zoning_exception_no"] == "42"


def test_lookup_holding_and_exception_off(ref_dir: Path) -> None:
    """ZN_HOLDING='N' and ZN_EXCPTN='N' → 0; exception_no None."""
    _write_zoning_geojson(
        ref_dir,
        {
            "ZN_ZONE": "R",
            "UNITS": -1,
            "DENSITY": -1,
            "GEN_ZONE": 0,
            "ZN_HOLDING": "N",
            "ZN_EXCPTN": "N",
        },
    )
    result = lookup_site_context(43.65, -79.38, ref_dir)
    assert result["zoning_holding"] == 0
    assert result["zoning_exception"] == 0
    assert result["zoning_exception_no"] is None


def test_lookup_mixed_use_fsi_splits(ref_dir: Path) -> None:
    """Mixed-use FSI splits surfaced when set; non-positive becomes None."""
    _write_zoning_geojson(
        ref_dir,
        {
            "ZN_ZONE": "CR",
            "UNITS": -1,
            "DENSITY": 4.0,
            "GEN_ZONE": 202,
            "PRCNT_RES": 2.5,
            "PRCNT_COMM": 1.5,
            "PRCNT_EMMP": -1,
        },
    )
    result = lookup_site_context(43.65, -79.38, ref_dir)
    assert result["zoning_pct_res"] == 2.5
    assert result["zoning_pct_comm"] == 1.5
    assert result["zoning_pct_emp"] is None  # -1 sentinel


def test_lookup_nearby_polygon_fallback(ref_dir: Path) -> None:
    """Given: A point slightly outside (0.0003 deg) the only zoning polygon.
    When: Lookup.
    Then: Falls back to the nearest polygon within tolerance and returns its zone."""
    # The test polygon covers (-79.39,-79.37) × (43.64,43.66)
    # A point at 43.6601 is just outside the polygon's northern edge (43.66)
    result = lookup_site_context(43.6601, -79.38, ref_dir)
    assert result["zoning_class"] == "CR"


def test_lookup_far_point_not_snapped(ref_dir: Path) -> None:
    """Given: A point 0.05 degrees outside all polygons.
    When: Lookup.
    Then: No snap occurs — returns None (too far for the fallback threshold)."""
    result = lookup_site_context(43.71, -79.38, ref_dir)
    assert result["zoning_class"] is None


def test_lookup_missing_reference_files(tmp_path: Path) -> None:
    """Graceful degradation when reference files don't exist."""
    ref = tmp_path / "reference"
    ref.mkdir()
    result = lookup_site_context(43.65, -79.38, ref)
    # Should return defaults, not crash
    assert result["zoning_class"] is None
    assert result["in_heritage_register"] == 0
    assert result["permitted_use_category"] is None
    assert result["zoning_holding"] == 0
    assert result["zoning_exception"] == 0


# --- endpoint tests ---


def test_site_context_endpoint_returns_zoning(client_with_ref: TestClient) -> None:
    response = client_with_ref.get("/site-context?lat=43.65&lon=-79.38")
    assert response.status_code == 200
    body = response.json()
    assert body["zoning_class"] == "CR"
    assert body["zoning_max_units"] == 200
    assert body["zoning_max_density"] == 3.5
    assert body["secondary_plan_name"] == "King-Spadina"
    assert body["in_secondary_plan"] == 1


def test_site_context_requires_lat_lon(client_with_ref: TestClient) -> None:
    response = client_with_ref.get("/site-context")
    assert response.status_code == 422  # missing required params


def test_site_context_outside_city(client_with_ref: TestClient) -> None:
    response = client_with_ref.get("/site-context?lat=44.0&lon=-80.0")
    assert response.status_code == 200
    body = response.json()
    assert body["zoning_class"] is None


def test_lookup_outside_all_polygons_includes_height_defaults(
    ref_dir: Path,
) -> None:
    """Point outside all polygons returns None for height overlay fields."""
    result = lookup_site_context(44.0, -80.0, ref_dir)
    assert result["zoning_max_storeys"] is None
    assert result["zoning_max_height_m"] is None


def _write_height_geojson(ref: Path, properties: dict) -> None:
    """Write ref/zoning_height.geojson with a single polygon carrying given properties.

    The polygon covers (-79.39,-79.37) × (43.64,43.66), containing (43.65, -79.38).
    """
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": properties,
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-79.39, 43.64],
                            [-79.37, 43.64],
                            [-79.37, 43.66],
                            [-79.39, 43.66],
                            [-79.39, 43.64],
                        ]
                    ],
                },
            }
        ],
    }
    (ref / "zoning_height.geojson").write_text(json.dumps(geojson))


def test_lookup_returns_height_overlay_storeys(ref_dir: Path) -> None:
    """Given: zoning_height.geojson has HT_STORIES=6.
    When: Lookup at a point inside the polygon.
    Then: zoning_max_storeys=6 is returned."""
    _write_height_geojson(ref_dir, {"HT_STORIES": 6, "HT_LABEL": 18.5})
    result = lookup_site_context(43.65, -79.38, ref_dir)
    assert result["zoning_max_storeys"] == 6


def test_lookup_returns_height_overlay_height_m(ref_dir: Path) -> None:
    """Given: zoning_height.geojson has HT_LABEL=18.5 (metres).
    When: Lookup at a point inside the polygon.
    Then: zoning_max_height_m=18.5 is returned."""
    _write_height_geojson(ref_dir, {"HT_STORIES": 6, "HT_LABEL": 18.5})
    result = lookup_site_context(43.65, -79.38, ref_dir)
    assert result["zoning_max_height_m"] == pytest.approx(18.5)


@pytest.mark.parametrize("sentinel", [-1, 0])
def test_lookup_height_overlay_sentinel_becomes_none(
    ref_dir: Path, sentinel: float
) -> None:
    """Given: HT_STORIES and HT_LABEL are -1 or 0 (by-law 'no limit' sentinels).
    When: Lookup.
    Then: Both fields are None."""
    _write_height_geojson(ref_dir, {"HT_STORIES": sentinel, "HT_LABEL": sentinel})
    result = lookup_site_context(43.65, -79.38, ref_dir)
    assert result["zoning_max_storeys"] is None
    assert result["zoning_max_height_m"] is None


def test_lookup_no_height_overlay_returns_none_defaults(tmp_path: Path) -> None:
    """Given: No zoning_height.geojson in ref_dir.
    When: Lookup.
    Then: zoning_max_storeys and zoning_max_height_m default to None without error."""
    ref = tmp_path / "reference"
    ref.mkdir()
    result = lookup_site_context(43.65, -79.38, ref)
    assert result["zoning_max_storeys"] is None
    assert result["zoning_max_height_m"] is None


def test_site_context_endpoint_exposes_new_zoning_keys(
    client_with_ref: TestClient,
) -> None:
    """All new zoning fields are present in the endpoint response (Pydantic)."""
    response = client_with_ref.get("/site-context?lat=43.65&lon=-79.38")
    assert response.status_code == 200
    body = response.json()
    for key in (
        "permitted_use_category",
        "zoning_min_frontage_m",
        "zoning_min_lot_area_sqm",
        "zoning_max_coverage_pct",
        "zoning_min_sqm_per_unit",
        "zoning_holding",
        "zoning_exception",
        "zoning_exception_no",
        "zoning_pct_res",
        "zoning_pct_comm",
        "zoning_pct_emp",
        "zoning_max_storeys",
        "zoning_max_height_m",
    ):
        assert key in body, f"Missing key {key} in /site-context response"


# --- nearby_applications tests ---


@pytest.fixture
def nearby_enriched_parquet(tmp_path: Path) -> Path:
    """Enriched parquet with known spatial distribution for nearby_applications tests.

    Centre point: (43.650, -79.380)
    N1 (active, recent, 50m away): (43.6504, -79.380)   -- ~44m north
    N2 (closed, recent, 100m away): (43.651, -79.380)   -- ~111m north
    N3 (active, recent, 1km away): (43.659, -79.380)    -- ~1000m north
    N4 (active, old, 50m away): (43.6504, -79.380)      -- same location, 10yr old
    """
    current_year = datetime.date.today().year
    path = tmp_path / "enriched" / "dev_applications.parquet"
    path.parent.mkdir(parents=True)
    df = pl.DataFrame(
        {
            "folderrsn": ["N1", "N2", "N3", "N4"],
            "application_type": ["OZ", "SA", "OZ", "OZ"],
            "status": ["Under Review", "Closed", "Under Review", "Under Review"],
            "street_num": ["100", "200", "300", "400"],
            "street_name": ["King St", "Queen St", "Bloor St", "Bay St"],
            "date_submitted": [
                f"{current_year - 1}-06-01T00:00:00",
                f"{current_year - 1}-06-01T00:00:00",
                f"{current_year - 1}-06-01T00:00:00",
                f"{current_year - 10}-06-01T00:00:00",
            ],
            "year_submitted": pl.Series(
                [
                    current_year - 1,
                    current_year - 1,
                    current_year - 1,
                    current_year - 10,
                ],
                dtype=pl.Int32,
            ),
            "lat": [43.6504, 43.651, 43.659, 43.6504],
            "lon": [-79.380, -79.380, -79.380, -79.380],
            "is_active": pl.Series([1, 0, 1, 1], dtype=pl.Int8),
            "description": ["OZ desc", "SA desc", "OZ north", "OZ old"],
        }
    )
    df.write_parquet(path)
    return path


def test_nearby_applications_returns_within_radius(
    nearby_enriched_parquet: Path,
) -> None:
    """Given: enriched parquet with rows at known distances from centre.
    When: nearby_applications called with 500m radius.
    Then: rows within 500m are returned; the 1km-away row is excluded."""
    results = nearby_applications(
        43.650, -79.380, nearby_enriched_parquet, radius_m=500.0, years=10
    )
    rsns = {r["folderrsn"] for r in results}
    assert "N1" in rsns
    assert "N2" in rsns
    assert "N3" not in rsns  # ~1000m away


def test_nearby_applications_excludes_old_rows(nearby_enriched_parquet: Path) -> None:
    """Given: enriched parquet with a 10-year-old row near the centre.
    When: nearby_applications called with years=5.
    Then: the old row is excluded."""
    results = nearby_applications(
        43.650, -79.380, nearby_enriched_parquet, radius_m=500.0, years=5
    )
    rsns = {r["folderrsn"] for r in results}
    assert "N4" not in rsns


def test_nearby_applications_distance_m_is_correct(
    nearby_enriched_parquet: Path,
) -> None:
    """Given: N1 is at (43.6504, -79.380), centre at (43.650, -79.380).
    When: nearby_applications called.
    Then: distance_m for N1 is approximately 44m (0.0004 deg * 111111)."""
    results = nearby_applications(
        43.650, -79.380, nearby_enriched_parquet, radius_m=500.0, years=10
    )
    n1 = next(r for r in results if r["folderrsn"] == "N1")
    expected_m = 0.0004 * 111_111.0
    assert n1["distance_m"] == pytest.approx(expected_m, abs=5.0)


def test_nearby_applications_sorted_by_distance(nearby_enriched_parquet: Path) -> None:
    """Given: N1 (~44m) and N2 (~111m) both within radius.
    When: nearby_applications called.
    Then: N1 comes before N2 in results."""
    results = nearby_applications(
        43.650, -79.380, nearby_enriched_parquet, radius_m=500.0, years=10
    )
    rsns = [r["folderrsn"] for r in results]
    assert rsns.index("N1") < rsns.index("N2")


def test_nearby_applications_is_active_flag(nearby_enriched_parquet: Path) -> None:
    """Given: N1 is active (is_active=1), N2 is closed (is_active=0).
    When: nearby_applications called.
    Then: is_active flag is correctly surfaced."""
    results = nearby_applications(
        43.650, -79.380, nearby_enriched_parquet, radius_m=500.0, years=10
    )
    by_rsn = {r["folderrsn"]: r for r in results}
    assert by_rsn["N1"]["is_active"] is True
    assert by_rsn["N2"]["is_active"] is False


def test_nearby_applications_missing_file_returns_empty(tmp_path: Path) -> None:
    """Given: enriched parquet does not exist.
    When: nearby_applications called.
    Then: returns empty list without error."""
    results = nearby_applications(
        43.650, -79.380, tmp_path / "enriched" / "dev_applications.parquet"
    )
    assert results == []


def test_nearby_applications_limit_respected(nearby_enriched_parquet: Path) -> None:
    """Given: multiple rows within radius.
    When: nearby_applications called with limit=1.
    Then: at most 1 result returned."""
    results = nearby_applications(
        43.650, -79.380, nearby_enriched_parquet, radius_m=500.0, years=10, limit=1
    )
    assert len(results) <= 1


def test_nearby_applications_returns_expected_fields(
    nearby_enriched_parquet: Path,
) -> None:
    """Given: nearby row exists.
    When: nearby_applications called.
    Then: result contains required fields."""
    results = nearby_applications(
        43.650, -79.380, nearby_enriched_parquet, radius_m=500.0, years=10
    )
    assert results
    row = results[0]
    for field in (
        "folderrsn",
        "application_type",
        "status",
        "street_address",
        "date_submitted",
        "description",
        "is_active",
        "distance_m",
    ):
        assert field in row, f"Missing field: {field}"
