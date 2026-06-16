"""Tests for /score?explain=true endpoint."""

from __future__ import annotations

import datetime
from pathlib import Path

import polars as pl
import pytest
from starlette.testclient import TestClient

from zoneto.api.app import create_app


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    """TestClient with app pointed at empty test data directory."""
    enriched_dir = tmp_path / "enriched"
    enriched_dir.mkdir(parents=True)
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
            "description": ["test"],
            "street_num": ["1"],
            "street_name": ["Main St"],
        }
    ).write_parquet(enriched_dir / "dev_applications.parquet")
    app = create_app(data_dir=tmp_path, model_dir=tmp_path / "models")
    return TestClient(app)


def test_score_without_explain_has_no_explanations_key(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without ?explain=true, response does not include 'explanations' key."""
    monkeypatch.setattr(
        "zoneto.api.routes.score_one",
        lambda source, features, model_dir: {"pred_dev_days_p50": 365.0},
    )
    response = client.post(
        "/score",
        json={"source": "dev_applications", "features": {"application_type": "OZ"}},
    )
    assert response.status_code == 200
    assert "explanations" not in response.json()


def test_score_with_explain_true_includes_explanations(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With ?explain=true, response includes 'explanations' key."""
    monkeypatch.setattr(
        "zoneto.api.routes.score_one",
        lambda source, features, model_dir: {"pred_dev_days_p50": 365.0},
    )
    monkeypatch.setattr(
        "zoneto.api.routes.explain_one",
        lambda source, features, model_dir, model_name, top_n: [
            {
                "feature": "ward_number__10",
                "shap_value": 0.05,
                "direction": "increases_risk",
            }
        ],
    )
    response = client.post(
        "/score?explain=true",
        json={"source": "dev_applications", "features": {"application_type": "OZ"}},
    )
    assert response.status_code == 200
    body = response.json()
    assert "explanations" in body
    assert isinstance(body["explanations"], dict)


def test_score_explain_true_no_models_returns_empty_explanations(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With ?explain=true but no production models, explanations is empty dict."""
    response = client.post(
        "/score?explain=true",
        json={"source": "dev_applications", "features": {}},
    )
    assert response.status_code == 200
    body = response.json()
    assert body.get("explanations") == {}
