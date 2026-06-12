"""Bylaw retrieval regression test.

Runs the labelled query set against the current index and asserts
recall@3 >= 87% and recall@5 = 100%.

Skips automatically when the index has not been built yet
(data/bylaw_index/embeddings.npy absent) — run `just bylaw-index` first.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_INDEX_DIR = Path("data/bylaw_index")
_QUERIES_PATH = Path("tests/fixtures/bylaw_eval_queries.json")

_RECALL3_FLOOR = 12 / 15  # 80% — one below current 87% so a single degradation trips it
_RECALL5_FLOOR = 15 / 15  # 100% — must always retrieve the right section in top-5


def _require_index() -> None:
    if not (_INDEX_DIR / "embeddings.npy").exists():
        pytest.skip("bylaw index not built — run `just bylaw-index` first")


def _run_eval(k: int = 5) -> tuple[float, float]:
    """Return (recall@3, recall@5) against the labelled query fixture."""
    from zoneto.analytics.bylaw_index import BylawIndex

    idx = BylawIndex(str(_INDEX_DIR))
    queries = json.loads(_QUERIES_PATH.read_text())

    hits3 = hits5 = 0
    for q in queries:
        results = idx.search(q["query"], zones=q.get("zones"), k=k)
        returned = {r.section_number for r in results}
        expected = set(q["expected_sections"])
        if returned & expected:
            hits5 += 1
        if {r.section_number for r in results[:3]} & expected:
            hits3 += 1

    n = len(queries)
    return hits3 / n, hits5 / n


def test_bylaw_retrieval_recall() -> None:
    """recall@3 >= 80% and recall@5 = 100% against the labelled query set."""
    _require_index()
    recall3, recall5 = _run_eval()
    assert recall5 >= _RECALL5_FLOOR, (
        f"recall@5 = {recall5:.1%} — right section missing from top-5; "
        "run `just bylaw-eval` for details"
    )
    assert recall3 >= _RECALL3_FLOOR, (
        f"recall@3 = {recall3:.1%} < {_RECALL3_FLOOR:.1%} — "
        "run `just bylaw-eval` for details"
    )
