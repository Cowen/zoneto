"""Retrieval-quality evaluation for comparable development applications.

The narrator's "what happened to similar applications" signal comes from
description-text similarity (``api/desc_similarity.py``). Text similarity is a
proxy for comparability; what actually drives outcomes is the structured profile
— same zone, same statutory path (``application_type``), same scale-of-ask. This
harness measures, by leave-one-out over the enriched corpus, how concordant the
top-k retrieved comps are with each query on those axes, and contrasts that with
a random-comp baseline so the *lift* over chance is visible per axis.

If zone concordance sits near the random baseline, text retrieval isn't
capturing zone at all — exactly the gap a hybrid structured+semantic ranker
would close. This is the baseline that makes such a change measurable rather than
guessed. Counterpart to ``scripts/bylaw_eval.py`` (which evaluates bylaw-chunk
retrieval); this one evaluates comparable-application retrieval.

Usage:
    uv run python scripts/comps_eval.py [--sample N] [--k K] [--seed S] [--quiet]

Exit codes: 0 always (diagnostic harness; thresholds are reported, not enforced).
"""

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

import polars as pl

from zoneto.analytics.retrieval_eval import (
    aggregate,
    concordance_at_k,
    count_measurable,
    excess_band,
)
from zoneto.api.desc_similarity import score_description_similarity

_AXES = ("zone", "type", "scale")


def _attrs_for(row: dict) -> dict[str, object]:
    """Project an enriched row onto the structured comparability axes."""
    ratios = [row.get("unit_excess_ratio"), row.get("storey_excess_ratio")]
    known = [r for r in ratios if r is not None]
    # Worst (largest) overage governs the scale-of-ask band; None if neither known.
    scale = excess_band(max(known)) if known else None
    return {
        "zone": row.get("zoning_class"),
        "type": row.get("application_type"),
        "scale": scale,
    }


def run_eval(
    enriched_path: str = "data/enriched/dev_applications.parquet",
    model_dir: str = "models",
    *,
    sample: int = 150,
    k: int = 10,
    seed: int = 0,
    verbose: bool = True,
) -> dict[str, object]:
    """Run the leave-one-out concordance eval and return a results dict."""
    path = Path(enriched_path)
    if not path.exists():
        print(f"ERROR: {path} not found. Run `just enrich` first.")
        sys.exit(2)
    if not (Path(model_dir) / "desc_tfidf.joblib").exists():
        print(f"ERROR: {model_dir}/desc_tfidf.joblib not found. Run `just train`.")
        sys.exit(2)

    data_dir = path.parent.parent  # data/enriched/x.parquet -> data
    wanted = [
        "folderrsn",
        "description",
        "zoning_class",
        "application_type",
        "unit_excess_ratio",
        "storey_excess_ratio",
    ]
    available = pl.scan_parquet(path).collect_schema().names()
    df = pl.read_parquet(path).select([c for c in wanted if c in available])

    # One row per application; only those usable as a query (has text + id).
    df = df.unique(subset=["folderrsn"], keep="first")
    df = df.filter(
        pl.col("folderrsn").is_not_null()
        & pl.col("description").is_not_null()
        & (pl.col("description").str.strip_chars() != "")
    )

    # folderrsn -> structured attrs, the single source for both real and random
    # comp lookups (so any lift is retrieval, not a metric asymmetry).
    attr_by_rsn: dict[str, dict[str, object]] = {}
    for row in df.iter_rows(named=True):
        attr_by_rsn[str(row["folderrsn"])] = _attrs_for(row)
    all_rsns = list(attr_by_rsn)

    rng = random.Random(seed)
    queries = df.to_dicts()
    rng.shuffle(queries)
    queries = queries[:sample]

    real_scores: list[dict[str, float | None]] = []
    rand_scores: list[dict[str, float | None]] = []
    n_used = 0

    for i, q in enumerate(queries, 1):
        q_rsn = str(q["folderrsn"])
        q_attrs = attr_by_rsn[q_rsn]

        sim = score_description_similarity(
            q["description"],
            data_dir=data_dir,
            model_dir=Path(model_dir),
            top_n=k + 5,  # headroom to drop self before slicing to k
            min_similarity=0.0,
            lat=None,
            lon=None,
        )
        if not sim or not sim.get("top_matches"):
            continue
        matches = [m for m in sim["top_matches"] if str(m.get("folderrsn")) != q_rsn][
            :k
        ]
        if not matches:
            continue
        retrieved = [attr_by_rsn.get(str(m.get("folderrsn")), {}) for m in matches]
        real_scores.append(concordance_at_k(q_attrs, retrieved, _AXES))

        # Random baseline: k comps drawn uniformly (excluding self), same lookup.
        pool = [r for r in all_rsns if r != q_rsn]
        rand_rsns = rng.sample(pool, min(k, len(pool)))
        rand_retrieved = [attr_by_rsn[r] for r in rand_rsns]
        rand_scores.append(concordance_at_k(q_attrs, rand_retrieved, _AXES))
        n_used += 1

        if verbose and i % 25 == 0:
            print(f"  ...{i}/{len(queries)} queries processed", file=sys.stderr)

    concordance = aggregate(real_scores)
    baseline = aggregate(rand_scores)
    measurable = count_measurable(real_scores)
    lift: dict[str, float | None] = {
        a: (
            concordance[a] - baseline[a]
            if concordance.get(a) is not None and baseline.get(a) is not None
            else None
        )
        for a in _AXES
    }

    results: dict[str, object] = {
        "n_queries": n_used,
        "k": k,
        "concordance": concordance,
        "baseline": baseline,
        "lift": lift,
        "measurable": measurable,
    }

    if verbose:
        print(
            f"\nComparable-retrieval concordance@{k} "
            f"(leave-one-out, n={n_used:,} queries)\n"
        )
        hdr = (
            f"  {'axis':>6} | {'retrieval':>10} | {'random':>8} | "
            f"{'lift':>7} | {'n':>5}"
        )
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))
        for a in _AXES:
            c, b, ell = concordance.get(a), baseline.get(a), lift.get(a)
            cs = f"{c:.1%}" if c is not None else "  n/a"
            bs = f"{b:.1%}" if b is not None else "  n/a"
            ls = f"{ell:+.1%}" if ell is not None else "  n/a"
            print(
                f"  {a:>6} | {cs:>10} | {bs:>8} | {ls:>7} | {measurable.get(a, 0):>5,}"
            )
        print(
            "\n  Lift is retrieval concordance minus a random-comp baseline. "
            "Near-zero\n  lift on an axis means description-text retrieval is not "
            "capturing it — a\n  structured/hybrid ranker would. 'n' is how many "
            "queries could measure the\n  axis: 'scale' has low coverage (excess "
            "ratios ~7% of rows), so read its\n  mean with its n. Text-similarity "
            "core only (no proximity or zone-derank);\n  layer those in as "
            "configurable follow-ups.\n"
        )
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--enriched", default="data/enriched/dev_applications.parquet")
    parser.add_argument("--models", default="models")
    parser.add_argument("--sample", type=int, default=150)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
    run_eval(
        args.enriched,
        args.models,
        sample=args.sample,
        k=args.k,
        seed=args.seed,
        verbose=not args.quiet,
    )


if __name__ == "__main__":
    main()
