"""Confidence calibration evaluation for the narrator.

Runs each golden case in tests/fixtures/narrator_eval_cases.json through the
full evaluate pipeline (site context -> feature extraction -> compliance ->
description similarity -> narration) and checks the returned confidence score
against the case's expected band, plus cross-case ordering constraints.

Usage:
    uv run python scripts/narrator_eval.py [--case st-clair-1613] [--quiet]

Requires ANTHROPIC_API_KEY, data/reference/, and (for similarity)
data/enriched/dev_applications.parquet + models/desc_tfidf.joblib.
One LLM call is made per case (claude-haiku by default, ~$0.005/case).

Cases marked "advisory" document known limitations: their band is reported but
a miss does not fail the run. The citation-grounding check is likewise
report-only for now (the system prompt contains an example citation the LLM
may echo).

The pytest twin of this harness is tests/api/test_narrator_regression.py —
its pipeline helper mirrors `_narrate_case` below; keep them in sync.

Exit codes: 0 = all asserted bands and orderings hit, 1 = some misses,
2 = missing prerequisites.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

from zoneto.analytics.compliance import (
    Severity,
    check_compliance,
    effective_height_m,
)
from zoneto.analytics.extract import extract_project_features
from zoneto.analytics.use_classifier import use_matches_zone
from zoneto.api.desc_similarity import score_description_similarity
from zoneto.api.llm_client import AnthropicClient
from zoneto.api.narrator import (
    _has_approved_precedent,
    _limits_verified,
    narrate_evaluation,
)
from zoneto.api.site_context import lookup_site_context

_SECTION_RE = re.compile(r"§[\d][\d.()]*")


def _preflight(ref_dir: str) -> None:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ERROR: ANTHROPIC_API_KEY not set.")
        sys.exit(2)
    if not Path(ref_dir).exists():
        print(f"ERROR: GIS reference data not found: {ref_dir}")
        print("Run `just enrich` (or fetch reference data) first.")
        sys.exit(2)


def _narrate_case(
    case: dict,
    *,
    ref_dir: Path,
    data_dir: Path,
    model_dir: Path,
    llm: AnthropicClient,
) -> dict:
    """Run the full evaluate pipeline for one golden case.

    Mirrors the integration helper in tests/api/test_narrator_regression.py.
    """
    site = lookup_site_context(case["lat"], case["lon"], ref_dir)
    extracted = extract_project_features(case["description"])
    violations = check_compliance(extracted, site)
    sim = score_description_similarity(
        case["description"],
        data_dir=data_dir,
        model_dir=model_dir,
        zoning_class=site.get("zoning_class"),
        lat=case["lat"],
        lon=case["lon"],
        radius_m=2000.0,
    )
    summary, score = narrate_evaluation(
        site,
        extracted,
        violations,
        chunks=[],
        llm_client=llm,
        description=case["description"],
        description_similarity=sim,
    )
    return {
        "site": site,
        "extracted": extracted,
        "violations": violations,
        "sim": sim,
        "summary": summary,
        "score": score,
    }


def _mechanism_trace(result: dict) -> str:
    """Describe which deterministic overrides bind, by re-evaluating their inputs."""
    site = result["site"]
    extracted = result["extracted"]
    violations = result["violations"]
    structural = [v for v in violations if v.severity != Severity.INFORMATIONAL]
    parts = []
    if (
        use_matches_zone(extracted.proposed_use, site.get("permitted_use_category"))
        == 1
        and not structural
    ):
        if _limits_verified(extracted, site):
            parts.append("floor-70 (as-of-right)")
        else:
            parts.append("floor-55 (compatible use, limits unverified)")
    if _has_approved_precedent(result["sim"], site.get("zoning_class")):
        parts.append("precedent floor-55")
        precedent = True
    else:
        precedent = False
    inferred = extracted.proposed_height_m is None
    for name, prop, limit in (
        ("storeys", extracted.proposed_storeys, site.get("zoning_max_storeys")),
        ("units", extracted.proposed_units, site.get("zoning_max_units")),
        (
            "height_m (inferred)" if inferred else "height_m",
            effective_height_m(extracted),
            site.get("zoning_max_height_m"),
        ),
        ("fsi", extracted.proposed_fsi, site.get("zoning_max_density")),
    ):
        if prop and limit and prop / limit >= 3.0:
            label = f"{name} {prop:g}/{limit:g}"
            if precedent:
                parts.append(f"cap-30 exempted by precedent ({label})")
            else:
                parts.append(f"cap-30 ({label})")
            break
    return "; ".join(parts) or "no deterministic override (LLM-dominated)"


def _ungrounded_citations(summary: str, violations: list) -> list[str]:
    cited = set(_SECTION_RE.findall(summary))
    known = " ".join(v.section_ref for v in violations)
    return sorted(s for s in cited if s.rstrip(".") not in known)


def run_eval(
    cases_path: str = "tests/fixtures/narrator_eval_cases.json",
    ref_dir: str = "data/reference",
    data_dir: str = "data",
    model_dir: str = "models",
    case_ids: list[str] | None = None,
    *,
    verbose: bool = True,
) -> dict[str, object]:
    """Run the eval and return a results dict."""
    _preflight(ref_dir)
    fixture = json.loads(Path(cases_path).read_text())
    cases = fixture["cases"]
    if case_ids:
        cases = [c for c in cases if c["id"] in case_ids]
        if not cases:
            print(f"ERROR: no cases match {case_ids}")
            sys.exit(2)

    llm = AnthropicClient(api_key=os.environ["ANTHROPIC_API_KEY"])
    results: dict[str, dict] = {}
    passed = failed = advisory_missed = 0

    for case in cases:
        result = _narrate_case(
            case,
            ref_dir=Path(ref_dir),
            data_dir=Path(data_dir),
            model_dir=Path(model_dir),
            llm=llm,
        )
        results[case["id"]] = result
        score = result["score"]
        lo = case["expected_confidence"]["min"]
        hi = case["expected_confidence"]["max"]
        in_band = score is not None and lo <= score <= hi
        advisory = case.get("advisory", False)
        if in_band:
            passed += 1
        elif advisory:
            advisory_missed += 1
        else:
            failed += 1

        if verbose:
            mark = "✓" if in_band else ("~" if advisory else "✗")
            adv = " [advisory]" if advisory else ""
            print(f"  {mark} [{case['id']}]{adv} score={score} band=[{lo}, {hi}]")
            print(f"      {case['label']}")
            n_struct = sum(
                1
                for v in result["violations"]
                if v.severity != Severity.INFORMATIONAL
            )
            print(
                f"      violations: {len(result['violations'])} "
                f"({n_struct} structural) | {_mechanism_trace(result)}"
            )
            ungrounded = _ungrounded_citations(
                result["summary"], result["violations"]
            )
            if ungrounded:
                print(f"      note: citations not in violation refs: {ungrounded}")
            if not in_band:
                print(f"      expected {lo} <= score <= {hi}, got {score}")
            print()

    orderings_passed = orderings_failed = 0
    for o in fixture.get("orderings", []):
        hi_id, lo_id = o["higher"], o["lower"]
        if hi_id not in results or lo_id not in results:
            continue  # subset run — skip orderings with missing cases
        hi_score = results[hi_id]["score"]
        lo_score = results[lo_id]["score"]
        ok = hi_score is not None and lo_score is not None and hi_score > lo_score
        if ok:
            orderings_passed += 1
        else:
            orderings_failed += 1
        if verbose:
            mark = "✓" if ok else "✗"
            print(
                f"  {mark} ordering: {hi_id} ({hi_score}) > {lo_id} ({lo_score})"
                f" — {o['note']}"
            )

    n = len(cases)
    print()
    print(f"bands    : {passed}/{n} in band ({advisory_missed} advisory misses)")
    print(f"orderings: {orderings_passed}/{orderings_passed + orderings_failed}")

    return {
        "n_cases": n,
        "passed": passed,
        "failed": failed,
        "advisory_missed": advisory_missed,
        "orderings_passed": orderings_passed,
        "orderings_failed": orderings_failed,
        "scores": {cid: r["score"] for cid, r in results.items()},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", default="tests/fixtures/narrator_eval_cases.json")
    parser.add_argument(
        "--case", action="append", dest="case_ids", help="run only this case id"
    )
    parser.add_argument("--ref-dir", default="data/reference")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--model-dir", default="models")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    results = run_eval(
        args.cases,
        args.ref_dir,
        args.data_dir,
        args.model_dir,
        args.case_ids,
        verbose=not args.quiet,
    )

    if results["failed"] or results["orderings_failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
