"""Tests for FastAPI endpoint handlers."""

from __future__ import annotations

import datetime
import json
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import polars as pl
import pytest
from pytest_httpx import HTTPXMock
from starlette.testclient import TestClient

from zoneto.api.app import create_app


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    """Minimal test data directory with enriched dev_applications parquet."""
    enriched_dir = tmp_path / "enriched"
    enriched_dir.mkdir(parents=True)

    current_year = datetime.date.today().year
    df = pl.DataFrame(
        {
            "folderrsn": ["F001", "F002"],
            "application_type": ["OZ", "SA"],
            "ward_number": ["10", "11"],
            "zoning_class": ["RA1", "RM"],
            "status": ["Approved", "Active"],
            "year_submitted": pl.Series(
                [current_year - 1, current_year - 2], dtype=pl.Int32
            ),
            "lat": [43.650, 43.651],
            "lon": [-79.380, -79.381],
            "dev_approved": pl.Series([1, None], dtype=pl.Int8),
            "dev_appealed": pl.Series([0, None], dtype=pl.Int8),
            "dev_days_to_decision": pl.Series([365, None], dtype=pl.Int32),
            "proposed_storeys": pl.Series([10, None], dtype=pl.Int32),
            "proposed_units": pl.Series([100, None], dtype=pl.Int32),
            "description": ["OZ application", "SA application"],
            "street_num": ["100", "200"],
            "street_name": ["King St", "Queen St"],
        }
    )
    df.write_parquet(enriched_dir / "dev_applications.parquet")

    # Ensure models_dir and metrics.json exist with at least one production_ready model
    models_dir = tmp_path / "models"
    models_dir.mkdir(exist_ok=True)
    metrics = {"dev_days_to_decision": {"production_ready": True}}
    (models_dir / "metrics.json").write_text(json.dumps(metrics))

    return tmp_path


@pytest.fixture
def client(data_dir: Path) -> Generator[TestClient, None, None]:
    """TestClient with app pointed at test data directory (lifespan active)."""
    app = create_app(data_dir=data_dir, model_dir=data_dir / "models")
    with TestClient(app) as client:
        yield client


# --- /health ---


def test_health_returns_ok(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# --- /ready ---


def test_ready_no_enriched_data_returns_503(tmp_path: Path) -> None:
    """503 when enriched/dev_applications.parquet does not exist."""
    app = create_app(data_dir=tmp_path, model_dir=tmp_path / "models")
    with TestClient(app, raise_server_exceptions=False) as c:
        response = c.get("/ready")
    assert response.status_code == 503


def test_ready_with_data_returns_200(client: TestClient) -> None:
    response = client.get("/ready")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["data_available"] is True
    assert isinstance(body["models_loaded"], list)


def test_ready_lists_production_ready_models(tmp_path: Path) -> None:
    """Lists only models with production_ready=true."""
    enriched_dir = tmp_path / "enriched"
    enriched_dir.mkdir(parents=True)
    # create minimal parquet so /ready passes data check
    current_year = datetime.date.today().year
    pl.DataFrame(
        {
            "folderrsn": ["F001"],
            "application_type": ["OZ"],
            "ward_number": ["10"],
            "zoning_class": ["RA1"],
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
    ).write_parquet(enriched_dir / "dev_applications.parquet")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    metrics = {
        "dev_days_to_decision": {"production_ready": True},
        "dev_days_to_decision_candidate": {"production_ready": False},
    }
    (models_dir / "metrics.json").write_text(json.dumps(metrics))

    app = create_app(data_dir=tmp_path, model_dir=models_dir)
    with TestClient(app) as c:
        response = c.get("/ready")
    assert response.status_code == 200
    body = response.json()
    assert body["models_loaded"] == ["dev_days_to_decision"]


# --- /comps ---


def test_comps_returns_applications(client: TestClient) -> None:
    response = client.get("/comps")
    assert response.status_code == 200
    body = response.json()
    assert "applications" in body
    assert isinstance(body["applications"], list)
    assert body["total"] == len(body["applications"])


def test_comps_filters_by_type(client: TestClient) -> None:
    response = client.get("/comps?type=OZ")
    assert response.status_code == 200
    apps = response.json()["applications"]
    assert all(a["application_type"] == "OZ" for a in apps)


def test_comps_filters_by_ward(client: TestClient) -> None:
    response = client.get("/comps?ward=10")
    assert response.status_code == 200
    apps = response.json()["applications"]
    assert all(a["ward_number"] == "10" for a in apps)


def test_comps_missing_data_returns_503(tmp_path: Path) -> None:
    app = create_app(data_dir=tmp_path, model_dir=tmp_path / "models")
    with TestClient(app, raise_server_exceptions=False) as c:
        response = c.get("/comps")
    assert response.status_code == 503


# --- /score ---


def test_score_delegates_to_score_one(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """POST /score calls score_one and returns its result."""
    monkeypatch.setattr(
        "zoneto.api.routes.score_one",
        lambda source, features, model_dir: {"pred_dev_days_p50": 365.0},
    )
    response = client.post(
        "/score",
        json={
            "source": "dev_applications",
            "features": {"application_type": "OZ", "ward_number": "10"},
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["predictions"]["pred_dev_days_p50"] == pytest.approx(365.0)


def test_score_invalid_source_returns_422(client: TestClient) -> None:
    response = client.post(
        "/score",
        json={"source": "invalid_source", "features": {}},
    )
    assert response.status_code == 422


def test_score_no_production_ready_models_returns_empty(
    tmp_path: Path,
) -> None:
    """Returns empty predictions when no models are production_ready."""
    current_year = datetime.date.today().year
    enriched_dir = tmp_path / "enriched"
    enriched_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "folderrsn": ["F001"],
            "application_type": ["OZ"],
            "ward_number": ["10"],
            "zoning_class": ["RA1"],
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
    ).write_parquet(enriched_dir / "dev_applications.parquet")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "metrics.json").write_text(
        json.dumps({"dev_days_to_decision": {"production_ready": False}})
    )
    app = create_app(data_dir=tmp_path, model_dir=models_dir)
    with TestClient(app) as c:
        response = c.post(
            "/score",
            json={"source": "dev_applications", "features": {}},
        )
    assert response.status_code == 200
    assert response.json()["predictions"] == {}


def test_score_returns_empty_when_no_metrics_file(tmp_path: Path) -> None:
    """Returns empty predictions when metrics.json does not exist."""
    current_year = datetime.date.today().year
    enriched_dir = tmp_path / "enriched"
    enriched_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "folderrsn": ["F001"],
            "application_type": ["OZ"],
            "ward_number": ["10"],
            "zoning_class": ["RA1"],
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
    ).write_parquet(enriched_dir / "dev_applications.parquet")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    # No metrics.json created
    app = create_app(data_dir=tmp_path, model_dir=models_dir)
    with TestClient(app) as c:
        response = c.post(
            "/score",
            json={"source": "dev_applications", "features": {}},
        )
    assert response.status_code == 200
    assert response.json()["predictions"] == {}


# --- /geocode ---


def test_geocode_returns_lat_lon(client: TestClient, httpx_mock: HTTPXMock) -> None:
    """GET /geocode returns lat, lon, display_name from Nominatim."""
    httpx_mock.add_response(
        json=[
            {
                "lat": "43.6426",
                "lon": "-79.3871",
                "display_name": "441 King St W, Toronto, ON",
            }
        ]
    )
    response = client.get("/geocode?address=441+King+St+W")
    assert response.status_code == 200
    body = response.json()
    assert body["lat"] == pytest.approx(43.6426)
    assert body["lon"] == pytest.approx(-79.3871)
    assert body["display_name"] == "441 King St W, Toronto, ON"


def test_geocode_no_results_returns_404(
    client: TestClient, httpx_mock: HTTPXMock
) -> None:
    """GET /geocode returns 404 when Nominatim returns empty list."""
    httpx_mock.add_response(json=[])
    response = client.get("/geocode?address=nonexistent+place+xyz")
    assert response.status_code == 404


def test_geocode_nominatim_timeout_returns_504(
    client: TestClient, httpx_mock: HTTPXMock
) -> None:
    """GET /geocode returns 504 when Nominatim times out."""
    httpx_mock.add_exception(httpx.TimeoutException("timed out"))
    response = client.get("/geocode?address=441+King+St+W")
    assert response.status_code == 504


def test_geocode_nominatim_upstream_error_returns_502(
    client: TestClient, httpx_mock: HTTPXMock
) -> None:
    """GET /geocode returns 502 when Nominatim returns a non-2xx status."""
    httpx_mock.add_response(status_code=503)
    response = client.get("/geocode?address=441+King+St+W")
    assert response.status_code == 502


# ---------------------------------------------------------------------------
# _retrieve_chunks — dual-query retrieval helper
# ---------------------------------------------------------------------------


def _make_chunk(chunk_id: int, text: str = "text") -> object:
    """Minimal stand-in for a Chunk dataclass for testing merge logic."""
    from types import SimpleNamespace

    return SimpleNamespace(
        chunk_id=chunk_id,
        chapter="10",
        section_number="10.20",
        section_title="Test",
        source_file="test.txt",
        zones=[],
        text=text,
        score=0.8,
    )


class TestRetrieveChunks:
    def _index(self, zone_results: list, desc_results: list) -> MagicMock:
        """Mock BylawIndex whose search alternates between two result sets."""
        mock = MagicMock()
        mock.search.side_effect = [zone_results, desc_results]
        return mock

    def test_deduplicates_overlapping_results(self) -> None:
        """Given: Both searches return the same chunk.
        When: _retrieve_chunks merges them.
        Then: That chunk appears only once in the result."""
        from zoneto.api.routes import _retrieve_chunks

        shared = _make_chunk(1)
        index = self._index(
            zone_results=[shared], desc_results=[shared, _make_chunk(2)]
        )
        site = {"zoning_class": "RM", "permitted_use_category": "Residential"}
        result = _retrieve_chunks(index, site, "a rental building", None, k=6)
        ids = [c.chunk_id for c in result]
        assert ids.count(1) == 1

    def test_zone_chunks_appear_first(self) -> None:
        """Given: Zone search returns chunk 10, description search returns chunk 20.
        When: Merging.
        Then: Chunk 10 precedes chunk 20 (zone context comes first)."""
        from zoneto.api.routes import _retrieve_chunks

        index = self._index(
            zone_results=[_make_chunk(10)],
            desc_results=[_make_chunk(20)],
        )
        site = {"zoning_class": "RM", "permitted_use_category": "Residential"}
        result = _retrieve_chunks(index, site, "a rental building", None, k=6)
        assert result[0].chunk_id == 10
        assert result[1].chunk_id == 20

    def test_result_bounded_to_k(self) -> None:
        """Given: Both searches return many chunks.
        When: _retrieve_chunks merges with k=4.
        Then: At most 4 chunks returned."""
        from zoneto.api.routes import _retrieve_chunks

        index = self._index(
            zone_results=[_make_chunk(i) for i in range(5)],
            desc_results=[_make_chunk(i + 10) for i in range(5)],
        )
        site = {"zoning_class": "RM", "permitted_use_category": "Residential"}
        result = _retrieve_chunks(index, site, "rental", None, k=4)
        assert len(result) <= 4

    def test_zone_query_includes_zone_class(self) -> None:
        """Given: Site has zoning_class RM.
        When: _retrieve_chunks calls search.
        Then: The first search call (zone query) includes 'RM' in its query string."""
        from zoneto.api.routes import _retrieve_chunks

        index = self._index(zone_results=[], desc_results=[])
        site = {"zoning_class": "RM", "permitted_use_category": "Residential"}
        _retrieve_chunks(index, site, "rental", None, k=6)
        first_query = index.search.call_args_list[0][0][0]
        assert "RM" in first_query
