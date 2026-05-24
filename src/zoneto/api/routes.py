"""FastAPI route definitions for Zoneto API."""

from __future__ import annotations

import datetime
import re
from pathlib import Path
from typing import Any, Literal

import httpx
import polars as pl
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from zoneto.analytics.compliance import check_compliance
from zoneto.analytics.explain import explain_one
from zoneto.analytics.extract import extract_project_features
from zoneto.analytics.score import score_one
from zoneto.api.comps import query_comps
from zoneto.api.desc_similarity import (
    score_description_similarity,
    score_description_similarity_bert,
)
from zoneto.api.narrator import narrate_evaluation, narrate_question
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
    zoning_max_storeys: int | None = None
    zoning_max_height_m: float | None = None
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
    in_trca_regulated_area: int = 0
    in_greenbelt: int = 0


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


# --- evaluate / ask ---


class ViolationResult(BaseModel):
    rule_id: str
    section_ref: str
    observed: str
    allowed: str
    severity: str
    suggested_remedy: str


class RelevantSection(BaseModel):
    section_number: str
    section_title: str
    source_file: str
    excerpt: str
    score: float


class EvaluateRequest(BaseModel):
    address: str
    description: str


class EvaluateResponse(BaseModel):
    lat: float | None = None
    lon: float | None = None
    site_context: dict[str, Any]
    extracted: dict[str, Any]
    violations: list[ViolationResult]
    relevant_sections: list[RelevantSection]
    summary_md: str
    suggestions: list[str]
    confidence_score: int | None = None
    data_gaps: list[str] = []
    description_similarity: dict[str, Any] | None = None
    community_benefits_context: dict[str, Any] | None = None


class AskRequest(BaseModel):
    address: str
    description: str
    question: str
    history: list[dict[str, str]] = []


def _community_benefits_context(
    data_dir: Path,
    ward_number: str | None = None,
    years: int = 5,
    min_comps: int = 3,
) -> dict[str, Any] | None:
    """Aggregate Section 37 community benefits over a recent year window.

    Filters by ward when provided. Returns None if section37.parquet is absent
    or fewer than min_comps records match.
    """
    s37_path = data_dir / "reference" / "section37.parquet"
    if not s37_path.exists():
        return None

    s37 = pl.read_parquet(s37_path)
    required = {"council_date", "monetary_value", "community_benefits"}
    if not required.issubset(s37.columns):
        return None

    year_cutoff = datetime.date.today().year - years
    s37 = s37.filter(pl.col("council_date").dt.year() >= year_cutoff)

    if ward_number is not None and "ward" in s37.columns:
        s37 = s37.filter(pl.col("ward") == ward_number)

    if len(s37) < min_comps:
        return None

    monetary = s37["monetary_value"].drop_nulls()
    if len(monetary) == 0:
        return None

    benefit_counts: dict[str, int] = {}
    for text in s37["community_benefits"].drop_nulls().to_list():
        for part in str(text).split(";"):
            part = part.strip()
            if part:
                benefit_counts[part] = benefit_counts.get(part, 0) + 1

    common = sorted(benefit_counts, key=lambda k: benefit_counts[k], reverse=True)[:3]

    return {
        "n_comps": len(s37),
        "median_monetary": float(monetary.median()),  # type: ignore[arg-type]
        "p25_monetary": float(monetary.quantile(0.25)),  # type: ignore[arg-type]
        "p75_monetary": float(monetary.quantile(0.75)),  # type: ignore[arg-type]
        "common_benefit_types": common,
    }


def _geocode_address(address: str) -> tuple[float, float]:
    """Geocode an address string to (lat, lon) via Nominatim."""
    try:
        resp = httpx.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": address, "format": "json", "countrycodes": "ca", "limit": 1},
            headers={"User-Agent": "zoneto/1.0"},
            timeout=10,
        )
        resp.raise_for_status()
    except httpx.TimeoutException as exc:
        raise HTTPException(status_code=504, detail="Geocoding timed out") from exc
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail="Geocoding unavailable") from exc
    results = resp.json()
    if not results:
        raise HTTPException(status_code=404, detail="Address not found")
    return float(results[0]["lat"]), float(results[0]["lon"])


def _zone_prefixes(zoning_class: str | None) -> list[str] | None:
    """Extract alphabetic zone prefixes for bylaw index zone filtering."""
    if not zoning_class:
        return None
    m = re.match(r"[A-Z]+", zoning_class)
    return [m.group(0)] if m else None


def _retrieve_chunks(
    bylaw_index: Any,
    site: dict[str, Any],
    description: str,
    extracted_use: str | None,
    *,
    k: int = 6,
) -> list[Any]:
    """Dual-query retrieval: zone-context first, then proposal semantics.

    Runs two searches and merges the results (zone chunks first, deduped by
    chunk_id, total bounded to k). This ensures the LLM always receives the
    site's actual zone chapter even when the proposal description semantically
    resembles a different zone type.
    """
    from zoneto.analytics.bylaw_index import Chunk  # noqa: PLC0415

    zones = _zone_prefixes(site.get("zoning_class"))
    zone_class = site.get("zoning_class") or ""
    use_cat = site.get("permitted_use_category") or ""
    exception_no = site.get("zoning_exception_no")

    # 0. Direct exception lookup — guarantees the exact exception schedule
    # surfaces first when the site has a known exception number. Score=1.0.
    exception_chunks: list[Chunk] = []
    if exception_no and zones:
        exception_chunks = bylaw_index.lookup_exception(zones[0], exception_no)

    # 1. Zone-specific query — anchors context to the site's zone chapter.
    zone_query = (
        f"{zone_class} zone {use_cat} permitted uses height density requirements"
    ).strip()
    zone_chunks: list[Chunk] = bylaw_index.search(zone_query, zones=zones, k=k // 2 + 1)

    # 2. Description-based query — retrieves proposal-relevant sections
    desc_query = f"{description} {extracted_use or ''}".strip()
    desc_chunks: list[Chunk] = bylaw_index.search(desc_query, zones=zones, k=k)

    # Merge: exception chunks first (exact match), then zone, then description
    seen: set[int] = set()
    merged: list[Chunk] = []
    for chunk in (*exception_chunks, *zone_chunks, *desc_chunks):
        if chunk.chunk_id not in seen:
            seen.add(chunk.chunk_id)
            merged.append(chunk)

    return merged[:k]


def _compute_data_gaps(site: dict[str, Any], extracted: Any) -> list[str]:
    """Identify what information is unavailable and would improve the assessment."""
    gaps: list[str] = []
    # Height overlay: absent when site is outside Schedule B coverage (~85% of city)
    if (
        site.get("zoning_max_storeys") is None
        and site.get("zoning_max_height_m") is None
    ):
        if site.get("zoning_class"):
            gaps.append(
                "Height overlay (By-law 569-2013 Schedule B): this site is outside the "
                "height overlay area (~85% of the city). Base zone regulations govern "
                "maximum height; no HT designation applies here."
            )
    # Building type: needed to determine which zone exception standards apply
    if getattr(extracted, "building_type", None) is None:
        gaps.append(
            "Building type (detached, semi-detached, duplex, triplex, "
            "townhouse, apartment): not specified. Some zone exceptions "
            "and standards vary by building type."
        )
    return gaps


@router.post("/evaluate", response_model=EvaluateResponse)
def evaluate(request: Request, body: EvaluateRequest) -> EvaluateResponse:
    """Evaluate a project description against By-law 569-2013 for a given address."""
    data_dir: Path = getattr(request.app.state, "data_dir", Path("data"))
    ref_dir = data_dir / "reference"
    bylaw_index = getattr(request.app.state, "bylaw_index", None)
    llm_client = getattr(request.app.state, "llm_client", None)

    if llm_client is None:
        raise HTTPException(
            status_code=503,
            detail="LLM client not configured. Set ANTHROPIC_API_KEY.",
        )

    lat, lon = _geocode_address(body.address)
    site = lookup_site_context(lat, lon, ref_dir)
    extracted = extract_project_features(body.description)
    violations = check_compliance(extracted, site)

    chunks = []
    if bylaw_index is not None:
        chunks = _retrieve_chunks(
            bylaw_index, site, body.description, extracted.proposed_use, k=6
        )

    data_gaps = _compute_data_gaps(site, extracted)
    cb_context = _community_benefits_context(data_dir=data_dir)
    model_dir: Path = getattr(request.app.state, "model_dir", Path("models"))
    bert_model = getattr(request.app.state, "bert_model", None)

    # BERT similarity preferred; fall back to TF-IDF+SVD when embeddings absent
    if bert_model is not None:
        desc_sim = score_description_similarity_bert(
            body.description or "",
            data_dir=data_dir,
            model=bert_model,
            zoning_class=site.get("zoning_class"),
            lat=lat,
            lon=lon,
            min_dist_m=300.0,
        )
    else:
        desc_sim = score_description_similarity(
            body.description or "",
            data_dir=data_dir,
            model_dir=model_dir,
            zoning_class=site.get("zoning_class"),
            lat=lat,
            lon=lon,
            min_dist_m=300.0,
        )
    summary_md, confidence_score = narrate_evaluation(
        site,
        extracted,
        violations,
        chunks,
        llm_client,
        data_gaps=data_gaps,
        description_similarity=desc_sim,
        community_benefits=cb_context,
    )

    return EvaluateResponse(
        lat=lat,
        lon=lon,
        site_context=site,
        extracted={
            "proposed_storeys": extracted.proposed_storeys,
            "proposed_units": extracted.proposed_units,
            "proposed_use": extracted.proposed_use,
            "has_ground_floor_retail": extracted.has_ground_floor_retail,
            "building_type": extracted.building_type,
        },
        violations=[
            ViolationResult(
                rule_id=v.rule_id,
                section_ref=v.section_ref,
                observed=v.observed,
                allowed=v.allowed,
                severity=v.severity.value,
                suggested_remedy=v.suggested_remedy,
            )
            for v in violations
        ],
        relevant_sections=[
            RelevantSection(
                section_number=c.section_number,
                section_title=c.section_title,
                source_file=c.source_file,
                excerpt=c.text[:400],
                score=round(c.score, 4),
            )
            for c in chunks
        ],
        summary_md=summary_md,
        confidence_score=confidence_score,
        suggestions=[v.suggested_remedy for v in violations if v.suggested_remedy],
        data_gaps=data_gaps,
        description_similarity=desc_sim,
        community_benefits_context=cb_context,
    )


@router.post("/ask")
def ask(request: Request, body: AskRequest) -> dict[str, str]:
    """Answer a follow-up question about a project at an address.

    Uses the same site context + bylaw retrieval as /evaluate, but
    retrieves sections relevant to the specific question rather than the
    full description.
    """
    data_dir: Path = getattr(request.app.state, "data_dir", Path("data"))
    ref_dir = data_dir / "reference"
    bylaw_index = getattr(request.app.state, "bylaw_index", None)
    llm_client = getattr(request.app.state, "llm_client", None)

    if llm_client is None:
        raise HTTPException(
            status_code=503,
            detail="LLM client not configured. Set ANTHROPIC_API_KEY.",
        )

    lat, lon = _geocode_address(body.address)
    site = lookup_site_context(lat, lon, ref_dir)
    extracted = extract_project_features(body.description)
    violations = check_compliance(extracted, site)

    chunks = []
    if bylaw_index is not None:
        chunks = _retrieve_chunks(bylaw_index, site, body.question, None, k=4)

    answer = narrate_question(
        question=body.question,
        site=site,
        extracted=extracted,
        violations=violations,
        retrieved_chunks=chunks,
        history=body.history,
        llm_client=llm_client,
    )
    return {"answer": answer}
