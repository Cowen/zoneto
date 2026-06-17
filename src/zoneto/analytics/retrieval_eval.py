"""Metric core for comparable-application retrieval quality.

Description-text retrieval (``api/desc_similarity.py``) surfaces comps by prose
similarity, but what makes an application a true *outcome* comparable is the
structured profile — same zone, same statutory path (application_type), same
scale-of-ask. These pure functions measure how concordant a retrieved comp set
is with its query on those axes, so retrieval changes can be evaluated against a
baseline rather than guessed. Driver: ``scripts/comps_eval.py``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

# Excess-ratio band thresholds mirror the compliance engine's severity ladder:
# at/under the limit is as-of-right; a modest overage reads as a minor variance;
# a large one needs rezoning; >3x is the "extreme" tier the narrator caps at 30.
# See analytics/compliance.py (_check_storeys / _check_fsi) and the narrator
# override notes in api/CLAUDE.md.
_MINOR_MAX = 1.5
_MAJOR_MAX = 3.0


def excess_band(ratio: float | None) -> str | None:
    """Bucket a proposed/limit excess ratio into a comparability band.

    Returns None when the ratio is unknown. Bands: 'within' (<=1.0), 'minor'
    (<=1.5), 'major' (<=3.0), 'extreme' (>3.0).
    """
    if ratio is None:
        return None
    if ratio <= 1.0:
        return "within"
    if ratio <= _MINOR_MAX:
        return "minor"
    if ratio <= _MAJOR_MAX:
        return "major"
    return "extreme"


def concordance_at_k(
    query: Mapping[str, object],
    retrieved: Sequence[Mapping[str, object]],
    axes: Sequence[str],
) -> dict[str, float | None]:
    """Per-axis fraction of retrieved comps that share the query's value.

    For each axis: None when the query has no value there (unmeasurable) or no
    retrieved comp has a value (empty denominator). Otherwise the share of
    value-bearing comps equal to the query's value. Comps with a null value on an
    axis are excluded from that axis's denominator, not counted as misses.

    The caller is responsible for slicing ``retrieved`` to the top-k of interest.
    """
    result: dict[str, float | None] = {}
    for axis in axes:
        q_val = query.get(axis)
        if q_val is None:
            result[axis] = None
            continue
        known = [r.get(axis) for r in retrieved if r.get(axis) is not None]
        if not known:
            result[axis] = None
            continue
        matches = sum(1 for v in known if v == q_val)
        result[axis] = matches / len(known)
    return result


def aggregate(
    per_query: Sequence[Mapping[str, float | None]],
) -> dict[str, float | None]:
    """Mean each axis over the queries that could measure it.

    None values (unmeasurable for that query) are excluded from the mean. An
    axis no query could measure aggregates to None. Empty input yields {}.
    """
    axes: list[str] = []
    seen: set[str] = set()
    for row in per_query:
        for axis in row:
            if axis not in seen:
                seen.add(axis)
                axes.append(axis)

    result: dict[str, float | None] = {}
    for axis in axes:
        vals = [row[axis] for row in per_query if row.get(axis) is not None]
        result[axis] = sum(vals) / len(vals) if vals else None
    return result


def count_measurable(
    per_query: Sequence[Mapping[str, float | None]],
) -> dict[str, int]:
    """Count, per axis, how many queries could measure it (non-None score).

    Pairs with ``aggregate`` so a high mean on a sparse axis (e.g. the excess-ratio
    band, which has low coverage) is never read without its sample size.
    """
    counts: dict[str, int] = {}
    for row in per_query:
        for axis, val in row.items():
            counts.setdefault(axis, 0)
            if val is not None:
                counts[axis] += 1
    return counts
