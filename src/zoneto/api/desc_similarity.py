"""Description similarity scorer using the trained TF-IDF + SVD pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer


def score_description_similarity(
    description: str,
    data_dir: Path,
    model_dir: Path,
    *,
    top_n: int = 20,
    min_similarity: float = 0.1,
    zoning_class: str | None = None,
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
            con.close()
            return None

        extra_cols = [
            c
            for c in [
                "dev_appealed",
                "dev_approved",
                "folderrsn",
                "application_type",
                "street_address",
            ]
            if c in available
        ]
        not_null_filter = f"{svd_cols[0]} IS NOT NULL"

        # Fetch full corpus for overall similarity
        select = ", ".join(svd_cols + extra_cols)
        rows = con.execute(
            f"SELECT {select} FROM '{enriched_path}' WHERE {not_null_filter}"
        ).fetchall()

        # Fetch zone-matched corpus when caller supplies zoning_class
        zm_rows: list[Any] = []
        has_zoning_col = "zoning_class" in available
        if zoning_class is not None and has_zoning_col:
            zm_select = ", ".join(svd_cols + extra_cols)
            zm_rows = con.execute(
                f"SELECT {zm_select} FROM '{enriched_path}' "
                f"WHERE {not_null_filter} AND zoning_class = $1",
                [zoning_class],
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

        # Approval rate from labelled top matches
        approval_rate: float | None = None
        if "dev_approved" in extra_cols:
            approved_col = extra_cols.index("dev_approved")
            approved_labels = [
                int(rows[idx][n_svd + approved_col])
                for idx in top_indices[:top_n]
                if rows[idx][n_svd + approved_col] is not None
            ]
            approval_rate = (
                sum(approved_labels) / len(approved_labels) if approved_labels else None
            )

        result: dict[str, Any] = {
            "top_matches": top_matches,
            "appeal_rate": appeal_rate,
            "approval_rate": approval_rate,
            "n_similar": len(top_matches),
        }

        # Zone-matched stats: only included when zoning_class was supplied
        if zoning_class is not None and has_zoning_col:
            if zm_rows:
                zm_corpus = np.array(
                    [[float(r[i]) for i in range(n_svd)] for r in zm_rows],
                    dtype=np.float64,
                )
                zm_norms = np.linalg.norm(zm_corpus, axis=1, keepdims=True) + 1e-10
                zm_normed = zm_corpus / zm_norms
                zm_sims: np.ndarray = zm_normed @ q_norm
                zm_top_idx = np.argsort(zm_sims)[::-1][:top_n]
                zm_matches = [
                    zm_rows[i]
                    for i in zm_top_idx
                    if float(zm_sims[i]) >= min_similarity
                ]
                result["zone_matched_n_similar"] = len(zm_matches)
                if "dev_approved" in extra_cols:
                    apr_idx = extra_cols.index("dev_approved")
                    zm_apr = [
                        int(r[n_svd + apr_idx])
                        for r in zm_matches
                        if r[n_svd + apr_idx] is not None
                    ]
                    result["zone_matched_approval_rate"] = (
                        sum(zm_apr) / len(zm_apr) if zm_apr else None
                    )
                else:
                    result["zone_matched_approval_rate"] = None
                # Zone-matched appeal rate: appeal rate for same-zone similar apps only
                if "dev_appealed" in extra_cols:
                    appealed_col = extra_cols.index("dev_appealed")
                    zm_appealed = [
                        int(r[n_svd + appealed_col])
                        for r in zm_matches
                        if r[n_svd + appealed_col] is not None
                    ]
                    result["zone_matched_appeal_rate"] = (
                        sum(zm_appealed) / len(zm_appealed) if zm_appealed else None
                    )
                else:
                    result["zone_matched_appeal_rate"] = None

            else:
                result["zone_matched_n_similar"] = 0
                result["zone_matched_approval_rate"] = None
                result["zone_matched_appeal_rate"] = None

        return result

    except Exception:  # noqa: BLE001
        return None


def score_description_similarity_bert(
    description: str,
    data_dir: Path,
    *,
    model: "SentenceTransformer | Any | None" = None,
    top_n: int = 20,
    min_similarity: float = 0.1,
    zoning_class: str | None = None,
) -> dict[str, Any] | None:
    """Score a description against the BERT-encoded application corpus.

    Uses pre-computed BERT embeddings (desc_bert_embeddings.npy) from the
    enrichment pipeline for higher semantic accuracy than TF-IDF+SVD.

    Args:
        description: Free-text project description to score.
        data_dir: Root data directory (looks for enriched/ subdirectory).
        model: SentenceTransformer instance. Required — caller must pass the
            loaded BAAI/bge-small-en-v1.5 model (or a compatible stub).
        top_n: Maximum number of top matches to return.
        min_similarity: Minimum cosine similarity threshold.
        zoning_class: If provided, also compute zone-matched stats.

    Returns:
        None when embeddings or index are missing.
        Dict with top_matches, appeal_rate, approval_rate, n_similar, and
        optionally zone_matched_n_similar / zone_matched_approval_rate.
    """
    embeddings_path = data_dir / "enriched" / "desc_bert_embeddings.npy"
    index_path = data_dir / "enriched" / "desc_bert_index.parquet"

    if model is None or not embeddings_path.exists() or not index_path.exists():
        return None

    try:
        import polars as pl

        embeddings: np.ndarray = np.load(str(embeddings_path))
        index_df = pl.read_parquet(index_path)

        query_vec: np.ndarray = model.encode(
            [description or ""], convert_to_numpy=True, show_progress_bar=False
        )
        if query_vec.ndim == 1:
            query_vec = query_vec.reshape(1, -1)
        q = np.asarray(query_vec, dtype=np.float32)[0]

        # Cosine similarity
        q_norm = q / (np.linalg.norm(q) + 1e-10)
        c_norms = np.linalg.norm(embeddings, axis=1, keepdims=True) + 1e-10
        corpus_normed = embeddings / c_norms
        sims: np.ndarray = corpus_normed @ q_norm

        top_idx = np.argsort(sims)[::-1][:top_n]
        top_matches: list[dict[str, Any]] = []
        cols = index_df.columns
        for i in top_idx:
            sim = float(sims[i])
            if sim < min_similarity:
                break
            row = index_df.row(int(i), named=True)
            m: dict[str, Any] = {"similarity": round(sim, 3)}
            for c in cols:
                m[c] = row.get(c)  # include zoning_class so narrator can flag mismatch
            top_matches.append(m)

        # Rates over top matches
        appeal_rate: float | None = None
        if "dev_appealed" in cols:
            ap_vals = [
                int(index_df["dev_appealed"][int(i)])
                for i in top_idx[:top_n]
                if index_df["dev_appealed"][int(i)] is not None
            ]
            appeal_rate = sum(ap_vals) / len(ap_vals) if ap_vals else None

        approval_rate: float | None = None
        if "dev_approved" in cols:
            apr_vals = [
                int(index_df["dev_approved"][int(i)])
                for i in top_idx[:top_n]
                if index_df["dev_approved"][int(i)] is not None
            ]
            approval_rate = sum(apr_vals) / len(apr_vals) if apr_vals else None

        result: dict[str, Any] = {
            "top_matches": top_matches,
            "appeal_rate": appeal_rate,
            "approval_rate": approval_rate,
            "n_similar": len(top_matches),
        }

        # Zone-matched stats when zoning_class provided
        if zoning_class is not None and "zoning_class" in cols:
            zm_mask = index_df["zoning_class"] == zoning_class
            zm_indices = [i for i, m in enumerate(zm_mask.to_list()) if m]
            if zm_indices:
                zm_emb = embeddings[zm_indices]
                zm_norms = np.linalg.norm(zm_emb, axis=1, keepdims=True) + 1e-10
                zm_normed = zm_emb / zm_norms
                zm_sims = zm_normed @ q_norm
                zm_top = np.argsort(zm_sims)[::-1][:top_n]
                zm_matches = [
                    zm_indices[j] for j in zm_top if float(zm_sims[j]) >= min_similarity
                ]
                result["zone_matched_n_similar"] = len(zm_matches)
                if "dev_approved" in cols:
                    zm_apr = [
                        int(index_df["dev_approved"][idx])
                        for idx in zm_matches
                        if index_df["dev_approved"][idx] is not None
                    ]
                    result["zone_matched_approval_rate"] = (
                        sum(zm_apr) / len(zm_apr) if zm_apr else None
                    )
                else:
                    result["zone_matched_approval_rate"] = None
                # Zone-matched appeal rate: appeal rate for same-zone similar apps only
                if "dev_appealed" in cols:
                    zm_ap = [
                        int(index_df["dev_appealed"][idx])
                        for idx in zm_matches
                        if index_df["dev_appealed"][idx] is not None
                    ]
                    result["zone_matched_appeal_rate"] = (
                        sum(zm_ap) / len(zm_ap) if zm_ap else None
                    )
                else:
                    result["zone_matched_appeal_rate"] = None

            else:
                result["zone_matched_n_similar"] = 0
                result["zone_matched_approval_rate"] = None
                result["zone_matched_appeal_rate"] = None

        return result

    except Exception:  # noqa: BLE001
        return None
