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
        "dev_applications_appealed": {"production_ready": True, "n": 100},
        "coa_approved": {"production_ready": False, "n": 50},
    }
    (tmp_path / "metrics.json").write_text(json.dumps(metrics))

    result = _load_production_ready(tmp_path)
    assert result == {"dev_applications_appealed": True, "coa_approved": False}


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
