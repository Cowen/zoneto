"""FastAPI route definitions for Zoneto API."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from zoneto.analytics.explain import explain_one
from zoneto.analytics.score import score_one
from zoneto.api.comps import query_comps

router = APIRouter()


# --- response / request models ---


class CompApplication(BaseModel):
    folderrsn: str | None = None
    application_type: str | None = None
    ward_number: str | None = None
    zoning_class: str | None = None
    status: str | None = None
    year_submitted: int | None = None
    lat: float | None = None
    lon: float | None = None
    dev_approved: int | None = None
    dev_appealed: int | None = None
    dev_days_to_decision: int | None = None
    proposed_storeys: int | None = None
    proposed_units: int | None = None
    description: str | None = None
    street_address: str | None = None
    dist_sq: float | None = None


class CompsResponse(BaseModel):
    applications: list[CompApplication]
    total: int


class ScoreRequest(BaseModel):
    source: Literal["dev_applications", "coa", "permits_cleared"]
    features: dict[str, Any]


class ScoreResponse(BaseModel):
    predictions: dict[str, Any]
    production_ready_models: list[str]
    explanations: dict[str, list[dict[str, Any]]] | None = None


# --- endpoints ---


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
def ready(request: Request) -> dict[str, Any]:
    state = request.app.state
    is_ready: bool = getattr(state, "ready", False)
    production_ready: dict[str, bool] = getattr(state, "production_ready", {})
    data_dir: Path = getattr(state, "data_dir", Path("data"))
    data_available = (data_dir / "enriched" / "dev_applications.parquet").exists()

    if not is_ready or not data_available:
        raise HTTPException(status_code=503, detail="Service not ready")

    return {
        "status": "ready",
        "models_loaded": [k for k, v in production_ready.items() if v],
        "data_available": data_available,
    }


@router.get("/comps", response_model=CompsResponse)
def comps(
    request: Request,
    type: str | None = None,
    ward: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    radius_m: float = 500.0,
    years: int = 5,
    limit: int = 20,
) -> CompsResponse:
    data_dir: Path = getattr(request.app.state, "data_dir", Path("data"))
    enriched_path = data_dir / "enriched" / "dev_applications.parquet"

    if not enriched_path.exists():
        raise HTTPException(status_code=503, detail="Enriched data not available")

    records = query_comps(
        enriched_path,
        application_type=type,
        ward_number=ward,
        lat=lat,
        lon=lon,
        radius_m=radius_m,
        years=years,
        limit=limit,
    )
    applications = [CompApplication(**r) for r in records]
    return CompsResponse(applications=applications, total=len(applications))


@router.post("/score", response_model=ScoreResponse, response_model_exclude_none=True)
def score(request: Request, body: ScoreRequest, explain: bool = False) -> ScoreResponse:
    model_dir: Path = getattr(request.app.state, "model_dir", Path("models"))
    production_ready: dict[str, bool] = getattr(
        request.app.state, "production_ready", {}
    )
    ready_model_names = [k for k, v in production_ready.items() if v]

    if not ready_model_names:
        return ScoreResponse(
            predictions={},
            production_ready_models=[],
            explanations={} if explain else None,
        )

    try:
        predictions = score_one(body.source, body.features, model_dir)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    explanations: dict[str, list[dict[str, Any]]] | None = None
    if explain:
        explanations = {}
        for model_name in ready_model_names:
            # Only explain models for the requested source
            source_prefix = body.source.split("_")[0]  # "dev", "coa", "permits"
            if not model_name.startswith(source_prefix):
                continue
            contribs = explain_one(
                source=body.source,
                features=body.features,
                model_dir=model_dir,
                model_name=model_name,
                top_n=5,
            )
            if contribs:
                explanations[model_name] = contribs

    return ScoreResponse(
        predictions=predictions,
        production_ready_models=ready_model_names,
        explanations=explanations,
    )
