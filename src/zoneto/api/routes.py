"""FastAPI route definitions for Zoneto API."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from zoneto.analytics.explain import explain_one
from zoneto.analytics.score import score_one
from zoneto.api.comps import query_comps
from zoneto.api.site_context import lookup_site_context

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
    application_url: str | None = None
    dist_sq: float | None = None
    # Spatial and demographic context (used for hypothetical scoring)
    in_heritage_register: int | None = None
    in_heritage_district: int | None = None
    in_secondary_plan: int | None = None
    secondary_plan_name: str | None = None
    in_mtsa: int | None = None
    ward_pct_renters: float | None = None
    ward_median_income: float | None = None
    ward_pop_density: float | None = None
    ward_pct_detached: float | None = None
    ward_appeal_rate_3y: float | None = None
    has_community_meeting: int | None = None
    proposed_use_category: str | None = None


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


class GeocodeResult(BaseModel):
    lat: float
    lon: float
    display_name: str


class SiteContextResult(BaseModel):
    zoning_class: str | None = None
    zoning_max_units: int | None = None
    zoning_max_density: float | None = None
    permitted_use_category: str | None = None
    zoning_min_frontage_m: float | None = None
    zoning_min_lot_area_sqm: float | None = None
    zoning_max_coverage_pct: float | None = None
    zoning_min_sqm_per_unit: float | None = None
    zoning_holding: int = 0
    zoning_exception: int = 0
    zoning_exception_no: str | None = None
    zoning_pct_res: float | None = None
    zoning_pct_comm: float | None = None
    zoning_pct_emp: float | None = None
    in_heritage_register: int = 0
    in_heritage_district: int = 0
    secondary_plan_name: str | None = None
    in_secondary_plan: int = 0
    in_mtsa: int = 0


# --- endpoints ---


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/geocode", response_model=GeocodeResult)
def geocode(address: str) -> GeocodeResult:
    try:
        resp = httpx.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": address, "format": "json", "countrycodes": "ca", "limit": 1},
            headers={"User-Agent": "zoneto/1.0"},
            timeout=10,
        )
        resp.raise_for_status()
    except httpx.TimeoutException as exc:
        msg = "Geocoding service timed out"
        raise HTTPException(status_code=504, detail=msg) from exc
    except httpx.HTTPStatusError as exc:
        msg = "Geocoding service unavailable"
        raise HTTPException(status_code=502, detail=msg) from exc
    results = resp.json()
    if not results:
        raise HTTPException(status_code=404, detail="Address not found")
    first = results[0]
    return GeocodeResult(
        lat=float(first["lat"]),
        lon=float(first["lon"]),
        display_name=first["display_name"],
    )


@router.get("/site-context", response_model=SiteContextResult)
def site_context(request: Request, lat: float, lon: float) -> SiteContextResult:
    """Look up zoning, heritage, MTSA, and secondary plan at a point."""
    data_dir: Path = getattr(request.app.state, "data_dir", Path("data"))
    ref_dir = data_dir / "reference"
    result = lookup_site_context(lat, lon, ref_dir)
    return SiteContextResult(**result)


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
