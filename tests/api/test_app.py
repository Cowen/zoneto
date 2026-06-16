"""Tests for FastAPI app factory."""

from pathlib import Path

from starlette.testclient import TestClient

from zoneto.api.app import _load_production_ready, create_app


def test_load_production_ready_missing_file(tmp_path: Path) -> None:
    """Returns empty dict when metrics.json does not exist."""
    result = _load_production_ready(tmp_path)
    assert result == {}


def test_load_production_ready_reads_flags(tmp_path: Path) -> None:
    """Reads production_ready flags from metrics.json."""
    import json

    metrics = {
        "dev_days_to_decision": {"production_ready": True, "n": 100},
        "dev_days_to_decision_candidate": {"production_ready": False, "n": 50},
    }
    (tmp_path / "metrics.json").write_text(json.dumps(metrics))

    result = _load_production_ready(tmp_path)
    assert result == {
        "dev_days_to_decision": True,
        "dev_days_to_decision_candidate": False,
    }


def test_create_app_returns_fastapi(tmp_path: Path) -> None:
    """create_app returns a FastAPI application."""
    from fastapi import FastAPI

    app = create_app(data_dir=tmp_path, model_dir=tmp_path)
    assert isinstance(app, FastAPI)


def test_app_state_set_on_startup(tmp_path: Path) -> None:
    """Lifespan sets app.state.data_dir and app.state.model_dir."""
    app = create_app(data_dir=tmp_path, model_dir=tmp_path / "models")
    with TestClient(app) as client:
        assert client.app.state.data_dir == tmp_path
        assert client.app.state.model_dir == tmp_path / "models"


def test_frontend_served_at_root(tmp_path: Path) -> None:
    """GET / returns the HTML frontend."""
    # create minimal parquet so the app is ready
    import datetime

    import polars as pl

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

    # create static/index.html in a temp static dir
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "index.html").write_text("<html><body>test</body></html>")

    app = create_app(
        data_dir=tmp_path,
        model_dir=tmp_path / "models",
        static_dir=static_dir,
    )
    c = TestClient(app)
    response = c.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
