"""Tests for SHAP explanation generation (explain.py).

The structured classifier models were deleted, but explain.py still implements a
generic SHAP path for HistGradientBoosting classifiers on dev_applications features.
These tests exercise that path with a synthetic classifier, and confirm the graceful
empty-result paths (missing model, unsupported source, SHAP-unsupported survival
model — TreeExplainer does not support GradientBoostingSurvivalAnalysis).
"""

from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

from zoneto.analytics.explain import explain_one
from zoneto.analytics.features import DEV_CAT_COLS, DEV_NUM_COLS
from zoneto.analytics.train import _build_survival_pipeline


def _build_clf_pipeline() -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "cat",
                Pipeline(
                    [
                        (
                            "impute",
                            SimpleImputer(
                                strategy="constant", fill_value="__missing__"
                            ),
                        ),
                        (
                            "encode",
                            OrdinalEncoder(
                                handle_unknown="use_encoded_value", unknown_value=-1
                            ),
                        ),
                    ]
                ),
                DEV_CAT_COLS,
            ),
            ("num", "passthrough", DEV_NUM_COLS),
        ]
    )
    return Pipeline(
        [
            ("preprocessor", preprocessor),
            ("estimator", HistGradientBoostingClassifier(random_state=0)),
        ]
    )


@pytest.fixture
def clf_model_dir(tmp_path: Path) -> Path:
    """Fit a synthetic dev classifier and save it as a .joblib."""
    rng = np.random.default_rng(42)
    n = 30
    X = pd.DataFrame({c: [str(i % 3) for i in range(n)] for c in DEV_CAT_COLS})
    X[DEV_NUM_COLS] = pd.DataFrame(
        rng.integers(0, 5, size=(n, len(DEV_NUM_COLS))).astype(float),
        columns=DEV_NUM_COLS,
    )
    y = (rng.uniform(size=n) < 0.4).astype(int)

    pipe = _build_clf_pipeline()
    pipe.fit(X, y)

    model_dir = tmp_path / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, model_dir / "dev_test_classifier.joblib")
    return model_dir


def test_explain_one_returns_list(clf_model_dir: Path) -> None:
    result = explain_one(
        source="dev_applications",
        features={"application_type": "OZ", "ward_number": "10"},
        model_dir=clf_model_dir,
        model_name="dev_test_classifier",
        top_n=5,
    )
    assert isinstance(result, list)
    assert len(result) > 0


def test_explain_one_top_n_limit(clf_model_dir: Path) -> None:
    result = explain_one(
        source="dev_applications",
        features={"application_type": "OZ", "ward_number": "10"},
        model_dir=clf_model_dir,
        model_name="dev_test_classifier",
        top_n=3,
    )
    assert len(result) <= 3


def test_explain_one_result_shape(clf_model_dir: Path) -> None:
    result = explain_one(
        source="dev_applications",
        features={"application_type": "OZ", "ward_number": "10"},
        model_dir=clf_model_dir,
        model_name="dev_test_classifier",
        top_n=5,
    )
    assert len(result) > 0
    for item in result:
        assert "feature" in item
        assert "shap_value" in item
        assert item["direction"] in ("increases_risk", "decreases_risk")


def test_explain_one_missing_model_returns_empty(tmp_path: Path) -> None:
    result = explain_one(
        source="dev_applications",
        features={"application_type": "OZ"},
        model_dir=tmp_path,
        model_name="dev_test_classifier",
        top_n=5,
    )
    assert result == []


def test_explain_one_unsupported_source_returns_empty(clf_model_dir: Path) -> None:
    result = explain_one(
        source="coa",
        features={},
        model_dir=clf_model_dir,
        model_name="dev_test_classifier",
        top_n=5,
    )
    assert result == []


def test_explain_one_survival_model_returns_empty(tmp_path: Path) -> None:
    """TreeExplainer does not support survival models — explain_one degrades to []."""
    n = 20
    X = pd.DataFrame({c: [str(i % 3) for i in range(n)] for c in DEV_CAT_COLS})
    X[DEV_NUM_COLS] = pd.DataFrame(
        np.random.default_rng(1)
        .integers(0, 5, size=(n, len(DEV_NUM_COLS)))
        .astype(float),
        columns=DEV_NUM_COLS,
    )
    events = np.array([True, False] * 10)
    times = np.array([365 + i * 20 for i in range(n)], dtype=np.int32)
    y = np.array(list(zip(events, times)), dtype=[("event", bool), ("time", np.int32)])
    pipe = _build_survival_pipeline(DEV_CAT_COLS, DEV_NUM_COLS)
    pipe.fit(X, y)
    model_dir = tmp_path / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, model_dir / "dev_days_to_decision.joblib")

    result = explain_one(
        source="dev_applications",
        features={"application_type": "OZ"},
        model_dir=model_dir,
        model_name="dev_days_to_decision",
        top_n=5,
    )
    assert result == []
