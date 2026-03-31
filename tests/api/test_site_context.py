"""Tests for GET /site-context endpoint and lookup_site_context()."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
from starlette.testclient import TestClient

from zoneto.api.app import create_app
from zoneto.api.site_context import lookup_site_context

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


def test_lookup_outside_all_polygons(ref_dir: Path) -> None:
    """Point far outside all polygons returns nulls and zeros."""
    result = lookup_site_context(44.0, -80.0, ref_dir)
    assert result["zoning_class"] is None
    assert result["zoning_max_units"] is None
    assert result["zoning_max_density"] is None
    assert result["in_secondary_plan"] == 0
    assert result["in_heritage_register"] == 0
    assert result["in_heritage_district"] == 0
    assert result["in_mtsa"] == 0


def test_lookup_missing_reference_files(tmp_path: Path) -> None:
    """Graceful degradation when reference files don't exist."""
    ref = tmp_path / "reference"
    ref.mkdir()
    result = lookup_site_context(43.65, -79.38, ref)
    # Should return defaults, not crash
    assert result["zoning_class"] is None
    assert result["in_heritage_register"] == 0


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
