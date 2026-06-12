"""Retrieval quality evaluation for the bylaw semantic search index.

Measures recall@3 and recall@5 against a labelled query fixture.

Usage:
    uv run python scripts/bylaw_eval.py [--index data/bylaw_index] [--k 5]

A "hit" counts when any result in the top-k has a section_number that appears
in the query's expected_sections list.  Partial credit is not used — either
the right section is returned or it isn't.

Exit codes: 0 = all queries hit at recall@5, 1 = some misses.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _load_index(index_dir: str):  # type: ignore[return]
    try:
        from zoneto.analytics.bylaw_index import BylawIndex

        return BylawIndex(index_dir)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        print("Run `just bylaw-index` first to build the index.")
        sys.exit(2)


def run_eval(
    index_dir: str = "data/bylaw_index",
    queries_path: str = "tests/fixtures/bylaw_eval_queries.json",
    k: int = 5,
    *,
    verbose: bool = True,
) -> dict[str, object]:
    """Run the eval and return a results dict."""
    idx = _load_index(index_dir)
    queries = json.loads(Path(queries_path).read_text())

    hits3 = hits5 = 0
    misses: list[dict] = []

    for q in queries:
        qid = q["id"]
        query_text = q["query"]
        zones = q.get("zones")
        expected = set(q["expected_sections"])

        results = idx.search(query_text, zones=zones, k=k)
        returned_sections = [r.section_number for r in results]

        in_top3 = any(s in expected for s in returned_sections[:3])
        in_top5 = any(s in expected for s in returned_sections[:k])

        if in_top3:
            hits3 += 1
        if in_top5:
            hits5 += 1

        if verbose:
            mark = "✓" if in_top5 else "✗"
            print(f"  {mark} [{qid}]")
            for i, r in enumerate(results[:k]):
                hit = "→" if r.section_number in expected else " "
                print(
                    f"    {hit} {i+1}. {r.section_number}: "
                    f"{r.section_title} ({r.score:.3f})"
                )
            if not in_top5:
                print(f"    Expected one of: {sorted(expected)}")
            print()

    n = len(queries)
    recall3 = hits3 / n
    recall5 = hits5 / n

    print(f"recall@3 : {hits3}/{n} = {recall3:.2%}")
    print(f"recall@5 : {hits5}/{n} = {recall5:.2%}")

    return {
        "recall_at_3": recall3,
        "recall_at_5": recall5,
        "hits_at_3": hits3,
        "hits_at_5": hits5,
        "n_queries": n,
        "misses": misses,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", default="data/bylaw_index")
    parser.add_argument("--queries", default="tests/fixtures/bylaw_eval_queries.json")
    parser.add_argument("--k", type=int, default=5)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    results = run_eval(args.index, args.queries, args.k, verbose=not args.quiet)

    if results["hits_at_5"] < results["n_queries"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
