"""Description similarity scorer using the trained TF-IDF + SVD pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np


def score_description_similarity(
    description: str,
    data_dir: Path,
    model_dir: Path,
    *,
    top_n: int = 20,
    min_similarity: float = 0.1,
) -> dict[str, Any] | None:
    """Score a description against the enriched application corpus.

    Uses the trained TF-IDF + SVD pipeline (desc_tfidf.joblib) to vectorize
    the description, then computes cosine similarity against desc_svd_* columns
    in the enriched dev_applications parquet.

    Returns None when the model or enriched data is unavailable.
    Returns a dict with:
      - top_matches: list of dicts (folderrsn, application_type, similarity,
        dev_appealed)
      - appeal_rate: float | None (share of labelled similar apps that were appealed)
      - n_similar: int (number of matches above min_similarity)
    """
    tfidf_path = model_dir / "desc_tfidf.joblib"
    enriched_path = data_dir / "enriched" / "dev_applications.parquet"

    if not tfidf_path.exists() or not enriched_path.exists():
        return None

    try:
        import duckdb
        import joblib

        pipeline = joblib.load(tfidf_path)
        query_vec: np.ndarray = pipeline.transform([description or ""])
        if query_vec.ndim == 1:
            query_vec = query_vec.reshape(1, -1)
        query_arr = np.asarray(query_vec, dtype=np.float64)[0]

        # Identify available SVD columns via schema introspection
        con = duckdb.connect()
        schema = con.execute(f"DESCRIBE SELECT * FROM '{enriched_path}'").fetchall()
        available = {str(r[0]) for r in schema}

        svd_cols = [f"desc_svd_{i}" for i in range(20) if f"desc_svd_{i}" in available]
        if not svd_cols:
            return None

        extra_cols = [
            c
            for c in ["dev_appealed", "folderrsn", "application_type", "street_address"]
            if c in available
        ]
        select = ", ".join(svd_cols + extra_cols)
        not_null_filter = f"{svd_cols[0]} IS NOT NULL"
        rows = con.execute(
            f"SELECT {select} FROM '{enriched_path}' WHERE {not_null_filter}"
        ).fetchall()
        con.close()

        if not rows:
            return None

        n_svd = len(svd_cols)
        corpus = np.array(
            [[float(r[i]) for i in range(n_svd)] for r in rows], dtype=np.float64
        )

        # Cosine similarity: normalise then dot-product
        q_norm = query_arr / (np.linalg.norm(query_arr) + 1e-10)
        c_norms = np.linalg.norm(corpus, axis=1, keepdims=True) + 1e-10
        corpus_normed = corpus / c_norms
        similarities: np.ndarray = corpus_normed @ q_norm

        top_indices = np.argsort(similarities)[::-1][:top_n]
        top_matches = []
        for idx in top_indices:
            sim = float(similarities[idx])
            if sim < min_similarity:
                break
            row = rows[idx]
            match: dict[str, Any] = {"similarity": round(sim, 3)}
            for j, col in enumerate(extra_cols):
                match[col] = row[n_svd + j]
            top_matches.append(match)

        # Appeal rate from labelled top matches
        if "dev_appealed" in extra_cols:
            appealed_col = extra_cols.index("dev_appealed")
            labels = [
                int(rows[idx][n_svd + appealed_col])
                for idx in top_indices[:top_n]
                if rows[idx][n_svd + appealed_col] is not None
            ]
            appeal_rate: float | None = sum(labels) / len(labels) if labels else None
        else:
            appeal_rate = None

        return {
            "top_matches": top_matches,
            "appeal_rate": appeal_rate,
            "n_similar": len(top_matches),
        }

    except Exception:  # noqa: BLE001
        return None
