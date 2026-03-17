"""Tests for TF-IDF + SVD description NLP feature extraction."""
from __future__ import annotations

from pathlib import Path

import polars as pl

from zoneto.analytics.enrich import _extract_text_features


def test_extract_text_features_produces_svd_columns(tmp_path: Path) -> None:
    """_extract_text_features() adds desc_svd_0..19 columns."""
    descriptions = [
        "47-storey mixed-use residential tower with ground floor retail",
        "3-storey office building with underground parking",
        "12-storey condominium with affordable housing units",
        "Heritage property conversion to residential use",
        "Transit-oriented development adjacent to subway station",
    ]
    df = pl.DataFrame(
        {
            "folderrsn": [f"F{i:03d}" for i in range(5)],
            "description": descriptions,
        }
    )

    result, _ = _extract_text_features(df, model_dir=tmp_path, n_components=5)

    svd_cols = [f"desc_svd_{i}" for i in range(5)]
    for col in svd_cols:
        assert col in result.columns, f"Missing column: {col}"
        assert result[col].dtype in (pl.Float32, pl.Float64)


def test_extract_text_features_serializes_vectorizer(tmp_path: Path) -> None:
    """_extract_text_features() saves desc_tfidf.joblib to model_dir."""
    import joblib

    # Use more diverse descriptions to get a larger vocab
    descriptions = [
        "47-storey mixed-use residential tower with ground floor retail",
        "3-storey office building with underground parking",
        "12-storey condominium with affordable housing units",
        "Heritage property conversion to residential use",
    ]
    df = pl.DataFrame(
        {
            "folderrsn": ["F001", "F002", "F003", "F004"],
            "description": descriptions,
        }
    )

    _extract_text_features(df, model_dir=tmp_path, n_components=3)

    joblib_path = tmp_path / "desc_tfidf.joblib"
    assert joblib_path.exists(), "desc_tfidf.joblib must be written to model_dir"

    pipeline = joblib.load(joblib_path)
    out = pipeline.transform(["new application description"])
    assert out.shape[1] >= 1  # At least 1 component, up to 3


def test_extract_text_features_null_descriptions_get_zeros(tmp_path: Path) -> None:
    """Rows with null descriptions get zero-filled SVD columns."""
    df = pl.DataFrame(
        {
            "folderrsn": ["F001", "F002"],
            "description": pl.Series(["OZ tower application", None], dtype=pl.String),
        }
    )

    result, _ = _extract_text_features(df, model_dir=tmp_path, n_components=3)

    f002 = result.filter(pl.col("folderrsn") == "F002")
    for col in [f"desc_svd_{i}" for i in range(3)]:
        assert f002[col][0] == 0.0 or abs(f002[col][0]) < 1e-6


def test_extract_text_features_no_description_column(tmp_path: Path) -> None:
    """When description column is absent, adds zero-filled SVD columns."""
    df = pl.DataFrame({"folderrsn": ["F001", "F002"]})

    result, _ = _extract_text_features(df, model_dir=tmp_path, n_components=3)

    for col in [f"desc_svd_{i}" for i in range(3)]:
        assert col in result.columns
        assert result[col][0] == 0.0 or abs(result[col][0]) < 1e-6
