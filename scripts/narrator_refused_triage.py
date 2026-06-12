"""Triage refused / revised-then-approved applications against the narrator.

Runs every unique refused application in data/enriched/dev_applications.parquet
(dev_approved == 0) through the same pipeline as scripts/narrator_eval.py
(site context -> feature extraction -> compliance -> description similarity),
then probes the deterministic confidence overrides from both extremes (5 and
95) to classify which mechanism would bind — without any LLM call:

    floor-70       as-of-right floor fires (refused app looks fully compliant)
    precedent-55   approved same-site comparable exempts the cap
    cap-30         extreme-violation cap fires
    passthrough    no override — the LLM's score stands

Usage:
    uv run python scripts/narrator_refused_triage.py            # probe only
    uv run python scripts/narrator_refused_triage.py --llm      # + real scores
    uv run python scripts/narrator_refused_triage.py --revised  # revised-then-
                                                                # approved set
    uv run python scripts/narrator_refused_triage.py --emit-case 5559219

--llm requires ANTHROPIC_API_KEY and makes one call per application.
--emit-case prints a ready-to-paste golden-case stanza for
tests/fixtures/narrator_eval_cases.json (site snapshot, sim_stub, and
expected_overrides still need a human pass — outcome/verification especially).

Requires data/reference/ and data/enriched/dev_applications.parquet.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import polars as pl

from zoneto.analytics.compliance import Severity, check_compliance
from zoneto.analytics.extract import extract_project_features
from zoneto.api.desc_similarity import score_description_similarity
from zoneto.api.narrator import _apply_confidence_overrides
from zoneto.api.site_context import lookup_site_context

# Keys snapshotted into a golden case's ci.site (fixture convention).
_SITE_SNAPSHOT_KEYS = (
    "zoning_class",
    "zoning_max_units",
    "zoning_max_density",
    "zoning_max_storeys",
    "zoning_max_height_m",
    "permitted_use_category",
    "in_heritage_register",
    "in_heritage_district",
    "in_secondary_plan",
    "in_mtsa",
    "zoning_holding",
    "zoning_exception",
)


def _load_apps(data_dir: Path, *, revised: bool) -> list[dict]:
    path = data_dir / "enriched" / "dev_applications.parquet"
    if not path.exists():
        print(f"ERROR: enriched data not found: {path}")
        sys.exit(2)
    df = pl.read_parquet(path)
    if revised:
        df = df.filter(
            pl.col("description")
            .str.to_lowercase()
            .str.contains("revis|resubmi")
            & (pl.col("dev_approved") == 1)
        )
    else:
        df = df.filter(pl.col("dev_approved") == 0)
    # Multi-address parcels duplicate rows per folderrsn; keep the most
    # usable row (geocoded + described) per application.
    df = (
        df.sort(
            pl.col("lat").is_null(),
            pl.col("description").is_null(),
        )
        .unique(subset="folderrsn", keep="first")
        .sort("date_submitted")
    )
    return df.to_dicts()


def _address(app: dict) -> str:
    parts = [app.get("street_num"), app.get("street_name"), app.get("street_type")]
    return " ".join(str(p) for p in parts if p) or "(no address)"


def _analyze(
    app: dict, *, ref_dir: Path, data_dir: Path, model_dir: Path
) -> dict | None:
    """Run the eval pipeline (sans narration) and probe the overrides.

    Mirrors scripts/narrator_eval.py::_narrate_case up to the LLM call.
    Returns None when the application cannot be analyzed (no coordinates).
    """
    lat, lon = app.get("lat"), app.get("lon")
    if lat is None or lon is None:
        return None
    description = app.get("description") or ""
    site = lookup_site_context(lat, lon, ref_dir)
    extracted = extract_project_features(description)
    violations = check_compliance(extracted, site)
    sim = score_description_similarity(
        description,
        data_dir=data_dir,
        model_dir=model_dir,
        zoning_class=site.get("zoning_class"),
        lat=lat,
        lon=lon,
        radius_m=2000.0,
    )
    low = _apply_confidence_overrides(5, violations, site, extracted, sim)
    high = _apply_confidence_overrides(95, violations, site, extracted, sim)
    return {
        "site": site,
        "extracted": extracted,
        "violations": violations,
        "sim": sim,
        "low": low,
        "high": high,
        "bucket": _bucket(low, high),
    }


def _bucket(low: int, high: int) -> str:
    if high <= 30:
        return "cap-30"
    if low == 70:
        return "floor-70"
    if low == 55:
        return "precedent-55"
    if (low, high) == (5, 95):
        return "passthrough"
    return f"other ({low},{high})"


def _ratios(extracted, site) -> str:
    parts = []
    for attr, key in (
        ("proposed_storeys", "zoning_max_storeys"),
        ("proposed_units", "zoning_max_units"),
        ("proposed_height_m", "zoning_max_height_m"),
    ):
        prop = getattr(extracted, attr) or 0
        limit = site.get(key) or 0
        if prop and limit:
            parts.append(
                f"{attr.removeprefix('proposed_')} {prop}/{limit}"
                f" ({prop / limit:.1f}x)"
            )
        elif prop:
            parts.append(f"{attr.removeprefix('proposed_')} {prop}/—")
    return "; ".join(parts) or "nothing extracted"


def _top_match(sim) -> str:
    matches = (sim or {}).get("top_matches") or []
    if not matches:
        return "none"
    best = matches[0]
    return (
        f"{best.get('folderrsn')} sim={best.get('similarity', 0):.2f}"
        f" approved={best.get('dev_approved')}"
    )


def _narrate(app: dict, analysis: dict, llm) -> int | None:
    from zoneto.api.narrator import narrate_evaluation

    _, score = narrate_evaluation(
        analysis["site"],
        analysis["extracted"],
        analysis["violations"],
        chunks=[],
        llm_client=llm,
        description=app.get("description"),
        description_similarity=analysis["sim"],
    )
    return score


def _emit_case(app: dict, analysis: dict) -> None:
    """Print a fixture stanza skeleton for tests/fixtures/narrator_eval_cases.json."""
    site_snapshot = {k: analysis["site"].get(k) for k in _SITE_SNAPSHOT_KEYS}
    matches = (analysis["sim"] or {}).get("top_matches") or []
    sim_stub = None
    if analysis["bucket"] == "precedent-55" and matches:
        best = matches[0]
        sim_stub = {
            "n_similar": analysis["sim"].get("n_similar"),
            "appeal_rate": analysis["sim"].get("appeal_rate"),
            "query_lat": app["lat"],
            "query_lon": app["lon"],
            "top_matches": [
                {
                    k: best.get(k)
                    for k in (
                        "folderrsn",
                        "application_type",
                        "similarity",
                        "dev_approved",
                        "dev_appealed",
                        "zoning_class",
                        "lat",
                        "lon",
                    )
                }
            ],
        }
    rsn = app["folderrsn"]
    stanza = {
        "id": "FIXME-case-id",
        "label": f"{_address(app)} — FIXME summary",
        "folderrsn": rsn,
        "lat": app["lat"],
        "lon": app["lon"],
        "description": app.get("description"),
        "outcome": {
            "decision": "FIXME (refused/approved)",
            "body": f"FIXME (status: {app.get('status')})",
            "year": None,
            "appealed": None,
            "verification": {
                "source": "Toronto AIC ArcGIS FeatureServer (live query)",
                "url": (
                    "https://services3.arcgis.com/b9WvedVPoizGfvfD/ArcGIS/rest/"
                    "services/COTGEO_IBMS_AIC_POINT/FeatureServer/0/query"
                    f"?where=FOLDERRSN={rsn}&outFields=*&f=json"
                ),
                "verified_date": "FIXME",
            },
        },
        "mechanism": analysis["bucket"],
        "expected_confidence": {"min": None, "max": None},
        "advisory": False,
        "ci": {
            "site": site_snapshot,
            "sim_stub": sim_stub,
            "expected_overrides": {
                "floor_70": analysis["bucket"] == "floor-70",
                "precedent_floor_55": analysis["bucket"] == "precedent-55",
                "cap_30": analysis["bucket"] == "cap-30",
            },
        },
        "notes": "FIXME — refusal research + calibration runs",
    }
    print(json.dumps(stanza, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ref-dir", default="data/reference")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--model-dir", default="models")
    parser.add_argument(
        "--revised",
        action="store_true",
        help="triage revised-then-approved applications instead of refused",
    )
    parser.add_argument(
        "--llm",
        action="store_true",
        help="also narrate each application with the real LLM (1 call each)",
    )
    parser.add_argument(
        "--emit-case",
        metavar="FOLDERRSN",
        help="print a golden-case fixture stanza for one application",
    )
    args = parser.parse_args()

    ref_dir, data_dir, model_dir = (
        Path(args.ref_dir),
        Path(args.data_dir),
        Path(args.model_dir),
    )
    if not ref_dir.exists():
        print(f"ERROR: GIS reference data not found: {ref_dir}")
        sys.exit(2)

    if args.emit_case:
        df = pl.read_parquet(data_dir / "enriched" / "dev_applications.parquet")
        rows = (
            df.filter(pl.col("folderrsn") == args.emit_case)
            .sort(pl.col("lat").is_null())
            .to_dicts()
        )
        if not rows:
            print(f"ERROR: folderrsn {args.emit_case} not found")
            sys.exit(2)
        app = rows[0]
        analysis = _analyze(
            app, ref_dir=ref_dir, data_dir=data_dir, model_dir=model_dir
        )
        if analysis is None:
            print(f"ERROR: folderrsn {args.emit_case} has no coordinates")
            sys.exit(2)
        _emit_case(app, analysis)
        return

    llm = None
    if args.llm:
        if not os.environ.get("ANTHROPIC_API_KEY"):
            print("ERROR: --llm requires ANTHROPIC_API_KEY")
            sys.exit(2)
        from zoneto.api.llm_client import AnthropicClient

        llm = AnthropicClient(api_key=os.environ["ANTHROPIC_API_KEY"])

    apps = _load_apps(data_dir, revised=args.revised)
    label = "revised-then-approved" if args.revised else "refused"
    print(f"{len(apps)} unique {label} applications\n")

    header = (
        "| folderrsn | address | type | status | zone | ratios "
        "| struct | top match | bucket |"
    )
    if llm:
        header += " score |"
    print(header)
    print("|" + "---|" * header.count("|"))

    buckets: dict[str, int] = {}
    scores: list[int] = []
    for app in apps:
        analysis = _analyze(
            app, ref_dir=ref_dir, data_dir=data_dir, model_dir=model_dir
        )
        if analysis is None:
            print(
                f"| {app['folderrsn']} | {_address(app)} "
                f"| {app.get('application_type')} | {app.get('status')} "
                f"| — | no coordinates | — | — | skipped |"
                + (" — |" if llm else "")
            )
            continue
        n_struct = sum(
            1
            for v in analysis["violations"]
            if v.severity != Severity.INFORMATIONAL
        )
        buckets[analysis["bucket"]] = buckets.get(analysis["bucket"], 0) + 1
        row = (
            f"| {app['folderrsn']} | {_address(app)} "
            f"| {app.get('application_type')} | {app.get('status')} "
            f"| {analysis['site'].get('zoning_class') or '?'} "
            f"| {_ratios(analysis['extracted'], analysis['site'])} "
            f"| {n_struct} | {_top_match(analysis['sim'])} "
            f"| {analysis['bucket']} |"
        )
        if llm:
            score = _narrate(app, analysis, llm)
            if score is not None:
                scores.append(score)
            row += f" {score} |"
        print(row)

    print(f"\nbuckets: {dict(sorted(buckets.items()))}")
    if scores:
        scores.sort()
        n = len(scores)
        ge70 = sum(1 for s in scores if s >= 70)
        ge55 = sum(1 for s in scores if s >= 55)
        print(
            f"LLM scores (n={n}): min={scores[0]} median={scores[n // 2]} "
            f"max={scores[-1]} | >=55: {ge55} | >=70: {ge70}"
        )


if __name__ == "__main__":
    main()
