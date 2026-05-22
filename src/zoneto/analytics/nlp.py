"""NLP-based feature extraction: TF-IDF, SVD, BERT embeddings."""

from __future__ import annotations

import logging
from pathlib import Path

import joblib
import numpy as np
import polars as pl
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline as SklearnPipeline

logger = logging.getLogger(__name__)


def _extract_text_features(
    df: pl.DataFrame,
    model_dir: Path,
    *,
    n_components: int = 20,
) -> tuple[pl.DataFrame, SklearnPipeline]:
    """Extract TF-IDF + TruncatedSVD features from the description column.

    Fits TfidfVectorizer (max_features=5000) + TruncatedSVD on the description
    column. Serializes the pipeline to model_dir/desc_tfidf.joblib.
    Adds desc_svd_0..{n_components-1} columns to the DataFrame.

    Rows with null descriptions are treated as empty strings (→ zero SVD vector).
    Returns (enriched_df, fitted_pipeline).
    """
    svd_col_names = [f"desc_svd_{i}" for i in range(n_components)]
    zero_svd = [pl.lit(0.0, dtype=pl.Float64).alias(col) for col in svd_col_names]

    if "description" not in df.columns:
        pipeline = SklearnPipeline(
            [
                (
                    "tfidf",
                    TfidfVectorizer(
                        max_features=5000,
                        ngram_range=(1, 2),
                        min_df=1,
                        sublinear_tf=True,
                    ),
                ),
                ("svd", TruncatedSVD(n_components=n_components, random_state=42)),
            ]
        )
        return df.with_columns(zero_svd), pipeline

    texts = df["description"].fill_null("").cast(pl.String).to_list()

    # Fit TF-IDF first to know vocabulary size
    tfidf = TfidfVectorizer(
        max_features=5000, ngram_range=(1, 2), min_df=1, sublinear_tf=True
    )
    tfidf_matrix = tfidf.fit_transform(texts)
    n_features = tfidf_matrix.shape[1]

    # TruncatedSVD requires n_components < n_features (at least 2 features needed)
    # If corpus is too small to fit SVD, return zero vectors
    if n_features < 2:
        logger.warning(
            "TF-IDF vocabulary too small (%d features) — "
            "desc_svd columns will be zeros",
            n_features,
        )
        vectors = np.zeros((len(texts), n_components))
        # Return zero vectors without serializing an unfitted pipeline
        # score.py will fall back to enriched parquet columns if pipeline is absent
        pipeline = SklearnPipeline(
            [
                (
                    "tfidf",
                    TfidfVectorizer(
                        max_features=5000,
                        ngram_range=(1, 2),
                        min_df=1,
                        sublinear_tf=True,
                    ),
                ),
                ("svd", TruncatedSVD(n_components=n_components, random_state=42)),
            ]
        )
        # Don't serialize — unfitted pipeline
        return df.with_columns(zero_svd), pipeline

    # Fit SVD with safe number of components
    safe_n = min(n_components, n_features - 1)
    svd = TruncatedSVD(n_components=safe_n, random_state=42)
    vectors = svd.fit_transform(tfidf_matrix)
    # Pad with zeros if SVD used fewer components than requested
    if vectors.shape[1] < n_components:
        pad = np.zeros((vectors.shape[0], n_components - vectors.shape[1]))
        vectors = np.hstack([vectors, pad])

    # Only serialize when we have a properly fitted pipeline
    pipeline = SklearnPipeline(
        [
            ("tfidf", tfidf),
            ("svd", svd),
        ]
    )

    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, model_dir / "desc_tfidf.joblib")

    svd_cols = [
        pl.Series(f"desc_svd_{i}", vectors[:, i].tolist(), dtype=pl.Float64)
        for i in range(n_components)
    ]
    return df.with_columns(svd_cols), pipeline


def compute_bert_embeddings(data_dir: Path = Path("data")) -> int:
    """Compute BERT embeddings for all dev_application descriptions.

    Uses BAAI/bge-small-en-v1.5 (384-dim) to encode each description.
    Writes two files to data/enriched/:
      - desc_bert_embeddings.npy: float32 array of shape [n_rows, 384]
      - desc_bert_index.parquet: folderrsn, application_type, dev_approved,
        dev_appealed, zoning_class — metadata rows parallel to embeddings

    Idempotent: re-encodes the full corpus each call (cheap to re-run after
    an enrich because the model is cached by sentence-transformers).

    Returns the number of rows encoded.
    """
    from sentence_transformers import SentenceTransformer  # noqa: PLC0415

    enriched_path = data_dir / "enriched" / "dev_applications.parquet"
    if not enriched_path.exists():
        logger.warning(
            "compute_bert_embeddings: enriched parquet not found at %s", enriched_path
        )
        return 0

    df = pl.read_parquet(enriched_path)
    texts = df["description"].fill_null("").cast(pl.String).to_list()

    logger.info("compute_bert_embeddings: encoding %d descriptions...", len(texts))
    # float16 (half-precision) cuts memory and encoding time in half with
    # negligible quality loss for cosine-similarity tasks.
    model = SentenceTransformer("BAAI/bge-small-en-v1.5")
    model.half()
    embeddings = model.encode(
        texts,
        convert_to_numpy=True,
        show_progress_bar=True,
        batch_size=128,
    ).astype(np.float16)

    out_dir = data_dir / "enriched"
    np.save(out_dir / "desc_bert_embeddings.npy", embeddings)
    logger.info("compute_bert_embeddings: saved embeddings shape %s", embeddings.shape)

    # Build index with metadata columns needed for similarity scoring
    index_cols = ["folderrsn", "application_type"]
    for optional in ["dev_approved", "dev_appealed", "zoning_class", "lat", "lon"]:
        if optional in df.columns:
            index_cols.append(optional)
    index_df = df.select(index_cols)
    index_df.write_parquet(out_dir / "desc_bert_index.parquet")
    logger.info("compute_bert_embeddings: saved index with %d rows", len(index_df))

    return len(index_df)
