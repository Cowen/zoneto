"""Tests for description similarity scorer."""

from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import polars as pl

from zoneto.api.desc_similarity import score_description_similarity


class _FakePipeline:
    """Minimal sklearn-compatible pipeline stub that can be pickled."""

    def transform(self, texts: list[str]) -> np.ndarray:
        return np.ones((len(texts), 20), dtype=np.float64)


def _write_fixture(
    data_dir: Path,
    model_dir: Path,
    appeal_labels: list[int | None] | None = None,
) -> None:
    (data_dir / "enriched").mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)

    n = len(appeal_labels) if appeal_labels is not None else 4
    svd_cols = {f"desc_svd_{i}": [float(i % 3 + 1) * 0.1] * n for i in range(20)}
    df = pl.DataFrame(
        {
            **svd_cols,
            "dev_appealed": appeal_labels if appeal_labels is not None else [0] * n,
            "folderrsn": [str(i) for i in range(n)],
            "application_type": ["OZ"] * n,
            "street_address": ["1 Test"] * n,
        }
    )
    df.write_parquet(str(data_dir / "enriched" / "dev_applications.parquet"))
    joblib.dump(_FakePipeline(), model_dir / "desc_tfidf.joblib")


class TestScoreDescriptionSimilarity:
    def test_returns_none_when_model_missing(self, tmp_path: Path) -> None:
        """Given: No model or enriched data files.
        When: Scoring description similarity.
        Then: Returns None gracefully."""
        result = score_description_similarity(
            "A residential building.",
            data_dir=tmp_path,
            model_dir=tmp_path,
        )
        assert result is None

    def test_returns_none_when_enriched_data_missing(self, tmp_path: Path) -> None:
        """Given: Model file exists but enriched parquet does not.
        When: Scoring description similarity.
        Then: Returns None gracefully."""
        (tmp_path / "desc_tfidf.joblib").touch()
        result = score_description_similarity(
            "A residential building.",
            data_dir=tmp_path,
            model_dir=tmp_path,
        )
        assert result is None

    def test_returns_dict_with_expected_keys(self, tmp_path: Path) -> None:
        """Given: Model and enriched data available.
        When: Scoring description similarity.
        Then: Returns dict with top_matches, appeal_rate, n_similar."""
        data_dir = tmp_path / "data"
        model_dir = tmp_path / "models"
        _write_fixture(data_dir, model_dir, appeal_labels=[0, 1, 0, None, 0, 1])

        result = score_description_similarity(
            "A residential building.",
            data_dir=data_dir,
            model_dir=model_dir,
        )

        assert result is not None
        assert "top_matches" in result
        assert "appeal_rate" in result
        assert "n_similar" in result
        assert isinstance(result["n_similar"], int)
        assert result["n_similar"] >= 0

    def test_appeal_rate_is_fraction(self, tmp_path: Path) -> None:
        """Given: Applications with known appeal labels.
        When: Scoring description similarity.
        Then: appeal_rate is a float between 0.0 and 1.0."""
        data_dir = tmp_path / "data2"
        model_dir = tmp_path / "models2"
        _write_fixture(data_dir, model_dir, appeal_labels=[0, 1, 0, 1])

        result = score_description_similarity(
            "test",
            data_dir=data_dir,
            model_dir=model_dir,
        )

        assert result is not None
        if result["appeal_rate"] is not None:
            assert 0.0 <= result["appeal_rate"] <= 1.0


def _write_fixture_with_zones(
    data_dir: Path,
    model_dir: Path,
    approved_labels: list[int | None],
    zoning_classes: list[str | None],
) -> None:
    (data_dir / "enriched").mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)
    n = len(approved_labels)
    svd_cols = {f"desc_svd_{i}": [float(i % 3 + 1) * 0.1] * n for i in range(20)}
    df = pl.DataFrame(
        {
            **svd_cols,
            "dev_appealed": [0] * n,
            "dev_approved": approved_labels,
            "folderrsn": [str(i) for i in range(n)],
            "application_type": ["OZ"] * n,
            "zoning_class": zoning_classes,
        }
    )
    df.write_parquet(str(data_dir / "enriched" / "dev_applications.parquet"))
    joblib.dump(_FakePipeline(), model_dir / "desc_tfidf.joblib")


def _write_fixture_with_approved(
    data_dir: Path,
    model_dir: Path,
    appeal_labels: list[int | None],
    approved_labels: list[int | None],
) -> None:
    (data_dir / "enriched").mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)
    n = len(appeal_labels)
    svd_cols = {f"desc_svd_{i}": [float(i % 3 + 1) * 0.1] * n for i in range(20)}
    df = pl.DataFrame(
        {
            **svd_cols,
            "dev_appealed": appeal_labels,
            "dev_approved": approved_labels,
            "folderrsn": [str(i) for i in range(n)],
            "application_type": ["OZ"] * n,
        }
    )
    df.write_parquet(str(data_dir / "enriched" / "dev_applications.parquet"))
    joblib.dump(_FakePipeline(), model_dir / "desc_tfidf.joblib")


class TestApprovalRate:
    def test_approval_rate_returned_when_dev_approved_present(
        self, tmp_path: Path
    ) -> None:
        """Given: Enriched data includes dev_approved labels.
        When: Scoring description similarity.
        Then: Returns approval_rate as fraction of approved similar apps."""
        data_dir = tmp_path / "data_apr"
        model_dir = tmp_path / "models_apr"
        _write_fixture_with_approved(data_dir, model_dir, [0, 0, 0, 0], [1, 1, 0, 1])
        result = score_description_similarity(
            "test", data_dir=data_dir, model_dir=model_dir
        )
        assert result is not None
        assert "approval_rate" in result
        assert result["approval_rate"] is not None
        assert 0.0 <= result["approval_rate"] <= 1.0

    def test_top_matches_include_dev_approved(self, tmp_path: Path) -> None:
        """Given: Enriched data includes dev_approved column.
        When: Scoring description similarity.
        Then: Each top_match dict includes dev_approved field."""
        data_dir = tmp_path / "data_apr2"
        model_dir = tmp_path / "models_apr2"
        _write_fixture_with_approved(data_dir, model_dir, [0, 0, 0, 0], [1, 0, None, 1])
        result = score_description_similarity(
            "test", data_dir=data_dir, model_dir=model_dir
        )
        assert result is not None
        for match in result["top_matches"]:
            assert "dev_approved" in match

    def test_approval_rate_none_when_all_labels_missing(self, tmp_path: Path) -> None:
        """Given: All dev_approved labels are None.
        When: Scoring description similarity.
        Then: approval_rate is None."""
        data_dir = tmp_path / "data_apr3"
        model_dir = tmp_path / "models_apr3"
        _write_fixture_with_approved(
            data_dir, model_dir, [0, 0, 0, 0], [None, None, None, None]
        )
        result = score_description_similarity(
            "test", data_dir=data_dir, model_dir=model_dir
        )
        assert result is not None
        assert result.get("approval_rate") is None

    def test_approval_rate_absent_when_no_dev_approved_col(
        self, tmp_path: Path
    ) -> None:
        """Given: Enriched data has no dev_approved column.
        When: Scoring description similarity.
        Then: approval_rate key is absent (or None) — no crash."""
        data_dir = tmp_path / "data_no_apr"
        model_dir = tmp_path / "models_no_apr"
        _write_fixture(data_dir, model_dir, appeal_labels=[0, 1, 0, 1])
        result = score_description_similarity(
            "test", data_dir=data_dir, model_dir=model_dir
        )
        assert result is not None
        # approval_rate may be absent or None — both are acceptable
        assert result.get("approval_rate") is None


class TestZoneMatchedSimilarity:
    def test_zone_matched_keys_returned_when_zoning_class_provided(
        self, tmp_path: Path
    ) -> None:
        """Given: Corpus with mixed zone classes, zoning_class='R' provided.
        When: Scoring description similarity.
        Then: Result includes zone_matched_n_similar and zone_matched_approval_rate."""
        data_dir = tmp_path / "data_zm1"
        model_dir = tmp_path / "models_zm1"
        _write_fixture_with_zones(
            data_dir,
            model_dir,
            approved_labels=[1, 0, 1, 0],
            zoning_classes=["R", "RM", "R", "RM"],
        )
        result = score_description_similarity(
            "test", data_dir=data_dir, model_dir=model_dir, zoning_class="R"
        )
        assert result is not None
        assert "zone_matched_n_similar" in result
        assert "zone_matched_approval_rate" in result

    def test_zone_matched_filters_to_same_zone(self, tmp_path: Path) -> None:
        """Given: Corpus with R and RM applications. Query zone=RM.
        When: Scoring description similarity.
        Then: zone_matched stats only include RM applications."""
        data_dir = tmp_path / "data_zm2"
        model_dir = tmp_path / "models_zm2"
        # 2 R-zone apps (both approved), 2 RM-zone apps (both not approved)
        _write_fixture_with_zones(
            data_dir,
            model_dir,
            approved_labels=[1, 1, 0, 0],
            zoning_classes=["R", "R", "RM", "RM"],
        )
        result = score_description_similarity(
            "test", data_dir=data_dir, model_dir=model_dir, zoning_class="RM"
        )
        assert result is not None
        # Overall includes R apps; zone-matched should only show RM apps: 0%
        assert result["zone_matched_approval_rate"] == 0.0

    def test_zone_matched_none_when_no_same_zone_apps(self, tmp_path: Path) -> None:
        """Given: Corpus has no applications in the query zone.
        When: Scoring with zoning_class='CR' but corpus is all 'R'.
        Then: zone_matched_n_similar=0 and zone_matched_approval_rate=None."""
        data_dir = tmp_path / "data_zm3"
        model_dir = tmp_path / "models_zm3"
        _write_fixture_with_zones(
            data_dir,
            model_dir,
            approved_labels=[1, 1],
            zoning_classes=["R", "R"],
        )
        result = score_description_similarity(
            "test", data_dir=data_dir, model_dir=model_dir, zoning_class="CR"
        )
        assert result is not None
        assert result["zone_matched_n_similar"] == 0
        assert result["zone_matched_approval_rate"] is None

    def test_zone_matched_absent_when_no_zoning_class_param(
        self, tmp_path: Path
    ) -> None:
        """Given: zoning_class not provided.
        When: Scoring description similarity.
        Then: zone_matched keys are absent (backward compatible)."""
        data_dir = tmp_path / "data_zm4"
        model_dir = tmp_path / "models_zm4"
        _write_fixture_with_zones(
            data_dir,
            model_dir,
            approved_labels=[1, 0],
            zoning_classes=["R", "RM"],
        )
        result = score_description_similarity(
            "test", data_dir=data_dir, model_dir=model_dir
        )
        assert result is not None
        assert "zone_matched_n_similar" not in result
        assert "zone_matched_approval_rate" not in result

    def test_zone_matched_appeal_rate_returned(self, tmp_path: Path) -> None:
        """Given: Corpus with R and RM zones, mixed appeal labels.
        When: Scoring with zoning_class='RM'.
        Then: zone_matched_appeal_rate reflects only RM applications."""
        data_dir = tmp_path / "data_zm5"
        model_dir = tmp_path / "models_zm5"
        (data_dir / "enriched").mkdir(parents=True, exist_ok=True)
        model_dir.mkdir(parents=True, exist_ok=True)
        import joblib
        import polars as pl
        n = 4
        svd_cols = {f"desc_svd_{i}": [float(i % 3 + 1) * 0.1] * n for i in range(20)}
        df = pl.DataFrame({
            **svd_cols,
            "dev_appealed": [1, 1, 0, 0],  # R: [1,1], RM: [0,0] → RM appeal=0%
            "dev_approved": [1, 0, 1, 0],
            "folderrsn": ["0", "1", "2", "3"],
            "application_type": ["OZ"] * n,
            "zoning_class": ["R", "R", "RM", "RM"],
        })
        df.write_parquet(str(data_dir / "enriched" / "dev_applications.parquet"))
        joblib.dump(_FakePipeline(), model_dir / "desc_tfidf.joblib")
        result = score_description_similarity(
            "test", data_dir=data_dir, model_dir=model_dir, zoning_class="RM"
        )
        assert result is not None
        assert "zone_matched_appeal_rate" in result
        # RM apps: both appealed=0 → rate = 0.0
        assert result["zone_matched_appeal_rate"] == 0.0


