"""Process-classifier evaluation for the Planning Act layer.

The narrator's confidence number is deliberately unaffected by the Planning Act
layer, so the band-based harnesses (narrator-eval/triage) cannot tell whether the
derived statutory *process* is right. This harness measures that directly and
deterministically against ground truth: every enriched application records the
process the applicant actually used (``application_type``), so we can compare it
to the process our compliance engine derives from the proposal-vs-zone violations.

Headline metric — rezoning recall: of applications that actually filed an OZ
(rezoning), how often does the engine flag that a rezoning is needed? Misses are
the system's blindness rate, and the limit-coverage split separates an engine
miss (limits present, no violation derived) from a data gap (no zoning limit to
check — the null-limits problem from specs/2026-06-13-product-critique...).

Usage:
    uv run python scripts/planning_act_eval.py [--limit N] [--quiet]

Exit codes: 0 always (diagnostic harness; thresholds are reported, not enforced).
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

import polars as pl

from zoneto.analytics.compliance import check_compliance
from zoneto.analytics.extract import extract_project_features
from zoneto.analytics.planning_act import additional_processes, path_for_violations

# Enriched columns that map onto the site dict check_compliance reads. The
# enriched batch set lacks zoning_max_height_m, permitted_use_category, holding
# and exception (those are looked up live in /evaluate), so use-mismatch and
# metre-height violations cannot fire here — rezoning recall is a LOWER BOUND.
_SITE_COLS = {
    "zoning_class": "zoning_class",
    "zoning_max_storeys": "zoning_max_storeys",
    "zoning_max_units": "zoning_max_units",
    "zoning_max_density": "zoning_max_density",
    "in_heritage_register": "in_heritage_register",
    "in_heritage_district": "in_heritage_district",
    "in_mtsa": "in_mtsa",
    "in_trca_regulated_area": "in_trca_regulated_area",
    "in_greenbelt": "in_greenbelt",
    # OP land-use designation (interim Borealis source). Unlike permitted_use_category
    # — which is looked up live in /evaluate and so is absent here — this IS enriched,
    # so the s.24/s.22 conformity signal CAN fire in the batch set. It is INFORMATIONAL
    # (does not flip the primary zoning path), so it adds an ORTHOGONAL OPA-detection
    # axis rather than moving rezoning path-recall.
    "op_land_use_designation": "op_land_use_designation",
}

# Which derived path we'd expect for each application_type, where the
# zoning-envelope axis is comparable. SA/SB/CD/PL/CO/TLAB are site-plan,
# parcel-creation, or appeal processes — not "exceeds the zoning envelope" — so
# they are reported but excluded from recall denominators.
_EXPECTED_PATH = {"OZ": "rezoning", "MV": "minor_variance"}
_LIMIT_COLS = ("zoning_max_storeys", "zoning_max_units", "zoning_max_density")


def _derive(row: dict) -> tuple[str, list[str], bool]:
    """Return (primary zoning path, additional process keys, op_nonconforming)."""
    site = {dst: row.get(src) for src, dst in _SITE_COLS.items()}
    extracted = extract_project_features(row.get("description"))
    violations = check_compliance(extracted, site)
    path = path_for_violations(violations)
    op_nonconforming = any(v.rule_id == "op_use_nonconforming" for v in violations)
    return path, additional_processes(extracted), op_nonconforming


def run_eval(
    enriched_path: str = "data/enriched/dev_applications.parquet",
    *,
    limit: int | None = None,
    verbose: bool = True,
) -> dict[str, object]:
    path = Path(enriched_path)
    if not path.exists():
        print(f"ERROR: {path} not found. Run `just enrich` first.")
        sys.exit(2)

    wanted = ["application_type", "description", "dev_approved", *_SITE_COLS]
    available = pl.scan_parquet(path).collect_schema().names()
    df = pl.read_parquet(path).select([c for c in wanted if c in available])
    if limit:
        df = df.head(limit)

    # confusion[(application_type, derived_path)] = count
    confusion: Counter[tuple[str, str]] = Counter()
    # has_limit[(application_type, derived_path)] = count where a zoning limit existed
    by_coverage: Counter[tuple[str, str, bool]] = Counter()
    # OZ recall split by outcome: approved OZ may have upzoned their own site
    # (today's zoning no longer shows the violation → temporal drift), so they
    # are NOT a clean test. dev_approved != 1 (refused/active/unknown) is clean.
    oz_split: Counter[tuple[str, bool]] = Counter()  # (bucket, detected)
    additional: Counter[str] = Counter()  # orthogonal-process trigger counts
    op_covered = 0  # rows with a non-null OP designation (join coverage)
    # OZ detection with/without the OP-conformity signal: (path_only, with_op)
    oz_op_detect = {"path_only": 0, "with_op": 0, "total": 0}

    for row in df.iter_rows(named=True):
        app_type = row.get("application_type") or "?"
        derived, extra, op_nonconforming = _derive(row)
        has_limit = any(row.get(c) is not None for c in _LIMIT_COLS)
        confusion[(app_type, derived)] += 1
        by_coverage[(app_type, derived, has_limit)] += 1
        if row.get("op_land_use_designation") is not None:
            op_covered += 1
        for key in extra:
            additional[key] += 1
        if app_type == "OZ":
            bucket = "approved (drift)" if row.get("dev_approved") == 1 else "clean"
            oz_split[(bucket, derived == "rezoning")] += 1
            oz_op_detect["total"] += 1
            if derived == "rezoning":
                oz_op_detect["path_only"] += 1
            if derived == "rezoning" or op_nonconforming:
                oz_op_detect["with_op"] += 1

    app_types = sorted({k[0] for k in confusion})
    paths = ["as_of_right", "minor_variance", "rezoning", "prohibited"]

    if verbose:
        print(f"\nDerived process vs actual application_type  (n={len(df):,})")
        cells_hdr = " ".join(f"{p[:9]:>9}" for p in paths)
        header = f"  {'type':>6} | {cells_hdr} |  total"
        print(header)
        print("  " + "-" * (len(header) - 2))
        for at in app_types:
            row_counts = [confusion[(at, p)] for p in paths]
            total = sum(row_counts)
            cells = " ".join(f"{c:>9,}" for c in row_counts)
            print(f"  {at:>6} | {cells} | {total:>6,}")

    results: dict[str, object] = {"n": len(df)}

    # --- Recall on the comparable types ---
    for at, want in _EXPECTED_PATH.items():
        total = sum(confusion[(at, p)] for p in paths)
        if not total:
            continue
        if at == "MV":
            detected = confusion[(at, "minor_variance")] + confusion[(at, "rezoning")]
            metric = "relief detected (variance or rezoning)"
        else:
            detected = confusion[(at, want)]
            metric = f"derived == {want}"
        recall = detected / total
        results[f"{at}_recall"] = recall
        if verbose:
            print(f"\n  {at}: {metric} for {detected:,}/{total:,} = {recall:.1%}")
            miss_with_limit = sum(
                v
                for (t, p, hl), v in by_coverage.items()
                if t == at and p == "as_of_right" and hl
            )
            miss_no_limit = sum(
                v
                for (t, p, hl), v in by_coverage.items()
                if t == at and p == "as_of_right" and not hl
            )
            print(
                f"    as_of_right misses: {miss_no_limit:,} had NO zoning limit to "
                f"check (data gap), {miss_with_limit:,} had a limit but no violation "
                "(engine/extraction gap)"
            )

    # --- OZ recall split by temporal drift ---
    for bucket in ("clean", "approved (drift)"):
        det = oz_split[(bucket, True)]
        tot = det + oz_split[(bucket, False)]
        if tot:
            r = det / tot
            if bucket == "clean":
                results["OZ_recall_clean"] = r
            if verbose:
                print(f"  OZ rezoning recall [{bucket}]: {det:,}/{tot:,} = {r:.1%}")
    if verbose:
        print(
            "    (clean = refused/active/unknown OZ, zoning unchanged; approved OZ "
            "may have upzoned its own site → today's polygon hides the violation)"
        )

    # --- Precision: of rows we call 'rezoning', how many actually filed OZ ---
    called_rezoning = sum(confusion[(at, "rezoning")] for at in app_types)
    if called_rezoning:
        prec = confusion[("OZ", "rezoning")] / called_rezoning
        results["rezoning_precision"] = prec
        if verbose:
            print(
                f"\n  rezoning precision: {confusion[('OZ', 'rezoning')]:,}/"
                f"{called_rezoning:,} = {prec:.1%} of rows we call 'rezoning' "
                "actually filed an OZ"
            )

    # --- Official Plan conformity signal (item 4b) ---
    # The OP designation is enriched (permitted_use_category is not), so the
    # s.24/s.22 conformity check adds an orthogonal OPA-detection axis. Report the
    # join coverage and the OZ detection lift from adding it to the zoning path.
    results["op_coverage"] = op_covered / len(df) if len(df) else 0.0
    tot = oz_op_detect["total"]
    if tot:
        recall_path = oz_op_detect["path_only"] / tot
        recall_with_op = oz_op_detect["with_op"] / tot
        results["OZ_recall_path_only"] = recall_path
        results["OZ_recall_with_op"] = recall_with_op
        if verbose:
            print(
                f"\n  Official Plan conformity (interim Borealis layer): "
                f"{op_covered:,}/{len(df):,} rows have a designation "
                f"({results['op_coverage']:.1%} coverage)."
            )
            print(
                f"    OZ detection — zoning path only: {oz_op_detect['path_only']:,}/"
                f"{tot:,} = {recall_path:.1%}; with OP-conformity signal: "
                f"{oz_op_detect['with_op']:,}/{tot:,} = {recall_with_op:.1%} "
                f"(+{recall_with_op - recall_path:.1%} from the OP layer)."
            )

    # --- Orthogonal process triggers (item 4a) ---
    results["additional_process_counts"] = dict(additional)
    if verbose and additional:
        n = len(df)
        print("\n  Orthogonal processes triggered (advisory, not zoning-envelope):")
        for key, cnt in additional.most_common():
            print(f"    {key:18s} {cnt:>7,}  ({cnt / n:.1%} of applications)")

    if verbose:
        print(
            "\n  NOTE: enriched set lacks zoning_max_height_m / "
            "permitted_use_category, so use-mismatch and metre-height violations "
            "cannot fire here — rezoning recall is a LOWER BOUND vs /evaluate.\n"
        )
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--enriched", default="data/enriched/dev_applications.parquet")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
    run_eval(args.enriched, limit=args.limit, verbose=not args.quiet)


if __name__ == "__main__":
    main()
