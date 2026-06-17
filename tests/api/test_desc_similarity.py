"""Tests for description similarity scorer."""

from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import polars as pl

from zoneto.api.desc_similarity import (
    _deduplicate_matches,
    score_description_similarity,
    score_description_similarity_bert,
)


class _FakeBertModel:
    """Minimal SentenceTransformer stub: encodes anything to a constant vector."""

    def encode(self, texts: list[str], **_: object) -> np.ndarray:
        return np.ones((len(texts), 8), dtype=np.float32)


def _write_bert_fixture(
    data_dir: Path,
    *,
    zoning_classes: list[str | None],
    app_types: list[str],
    appeal_labels: list[int | None],
) -> None:
    (data_dir / "enriched").mkdir(parents=True, exist_ok=True)
    n = len(zoning_classes)
    np.save(
        str(data_dir / "enriched" / "desc_bert_embeddings.npy"),
        np.ones((n, 8), dtype=np.float32),
    )
    pl.DataFrame(
        {
            "folderrsn": [str(i) for i in range(n)],
            "application_type": app_types,
            "zoning_class": zoning_classes,
            "dev_appealed": appeal_labels,
            "dev_approved": [1] * n,
        }
    ).write_parquet(str(data_dir / "enriched" / "desc_bert_index.parquet"))


def _write_fixture_zone_type(
    data_dir: Path,
    model_dir: Path,
    *,
    zoning_classes: list[str | None],
    app_types: list[str],
    appeal_labels: list[int | None],
) -> None:
    (data_dir / "enriched").mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)
    n = len(zoning_classes)
    svd_cols = {f"desc_svd_{i}": [float(i % 3 + 1) * 0.1] * n for i in range(20)}
    pl.DataFrame(
        {
            **svd_cols,
            "dev_appealed": appeal_labels,
            "dev_approved": [1] * n,
            "folderrsn": [str(i) for i in range(n)],
            "application_type": app_types,
            "zoning_class": zoning_classes,
        }
    ).write_parquet(str(data_dir / "enriched" / "dev_applications.parquet"))
    joblib.dump(_FakePipeline(), model_dir / "desc_tfidf.joblib")


class TestZoneAndTypeMatched:
    def test_tfidf_zone_matched_rate_restricted_to_application_type(
        self, tmp_path: Path
    ) -> None:
        """Given same-zone comps of mixed process type (OZ appealed, SA not), When an
        application_type is supplied, Then the zone-matched appeal rate is computed
        over that type only — a rezoning's exposure compared to rezonings, not site
        plans."""
        data_dir = tmp_path / "d"
        model_dir = tmp_path / "m"
        _write_fixture_zone_type(
            data_dir,
            model_dir,
            zoning_classes=["RM"] * 4,
            app_types=["OZ", "OZ", "SA", "SA"],
            appeal_labels=[1, 1, 0, 0],
        )
        result = score_description_similarity(
            "test",
            data_dir=data_dir,
            model_dir=model_dir,
            zoning_class="RM",
            application_type="OZ",
        )
        assert result is not None
        assert result["zone_matched_appeal_rate"] == 1.0  # OZ-only: both appealed
        assert result["zone_matched_application_type"] == "OZ"

    def test_bert_zone_matched_rate_restricted_to_application_type(
        self, tmp_path: Path
    ) -> None:
        """Same as above for the BERT scorer path."""
        data_dir = tmp_path / "db"
        _write_bert_fixture(
            data_dir,
            zoning_classes=["RM"] * 4,
            app_types=["OZ", "OZ", "SA", "SA"],
            appeal_labels=[1, 1, 0, 0],
        )
        result = score_description_similarity_bert(
            "test",
            data_dir=data_dir,
            model=_FakeBertModel(),
            zoning_class="RM",
            application_type="OZ",
        )
        assert result is not None
        assert result["zone_matched_appeal_rate"] == 1.0
        assert result["zone_matched_application_type"] == "OZ"


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


def _write_fixture_with_locations(
    data_dir: Path,
    model_dir: Path,
    lats: list[float | None],
    lons: list[float | None],
) -> None:
    (data_dir / "enriched").mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)
    n = len(lats)
    svd_cols = {f"desc_svd_{i}": [float(i % 3 + 1) * 0.1] * n for i in range(20)}
    df = pl.DataFrame(
        {
            **svd_cols,
            "dev_appealed": [0] * n,
            "folderrsn": [str(i) for i in range(n)],
            "application_type": ["OZ"] * n,
            "lat": lats,
            "lon": lons,
        }
    )
    df.write_parquet(str(data_dir / "enriched" / "dev_applications.parquet"))
    joblib.dump(_FakePipeline(), model_dir / "desc_tfidf.joblib")


def _write_fixture_with_location_zones(
    data_dir: Path,
    model_dir: Path,
    lats: list[float | None],
    lons: list[float | None],
    zoning_classes: list[str | None],
) -> None:
    (data_dir / "enriched").mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)
    n = len(lats)
    svd_cols = {f"desc_svd_{i}": [float(i % 3 + 1) * 0.1] * n for i in range(20)}
    df = pl.DataFrame(
        {
            **svd_cols,
            "dev_appealed": [0] * n,
            "folderrsn": [str(i) for i in range(n)],
            "application_type": ["OZ"] * n,
            "lat": lats,
            "lon": lons,
            "zoning_class": zoning_classes,
        }
    )
    df.write_parquet(str(data_dir / "enriched" / "dev_applications.parquet"))
    joblib.dump(_FakePipeline(), model_dir / "desc_tfidf.joblib")


class TestDefaultRadius:
    def test_app_at_3km_excluded_by_default_radius(self, tmp_path: Path) -> None:
        """Given: One app at 1km and one at 3km from query.
        When: Scoring with default radius (2km).
        Then: Only the 1km app appears — 3km one excluded."""
        data_dir = tmp_path / "data_dr"
        model_dir = tmp_path / "models_dr"
        _write_fixture_with_locations(
            data_dir,
            model_dir,
            lats=[43.65 + 0.009, 43.65 + 0.027],  # ~1000m and ~3000m north
            lons=[-79.38, -79.38],
        )
        result = score_description_similarity(
            "test",
            data_dir=data_dir,
            model_dir=model_dir,
            lat=43.65,
            lon=-79.38,
        )
        assert result is not None
        folder_ids = [m["folderrsn"] for m in result["top_matches"]]
        assert "0" in folder_ids
        assert "1" not in folder_ids


class TestSelfExclusion:
    def test_app_at_query_location_excluded_when_min_dist_set(
        self, tmp_path: Path
    ) -> None:
        """Given: App at exact query lat/lon (0m) and another at 300m.
        When: Scoring with min_dist_m=200, radius_m=2000.
        Then: Exact-location app excluded; 300m app included."""
        data_dir = tmp_path / "data_se"
        model_dir = tmp_path / "models_se"
        _write_fixture_with_locations(
            data_dir,
            model_dir,
            lats=[43.65, 43.6527],  # 0m and ~300m north
            lons=[-79.38, -79.38],
        )
        result = score_description_similarity(
            "test",
            data_dir=data_dir,
            model_dir=model_dir,
            lat=43.65,
            lon=-79.38,
            radius_m=2000.0,
            min_dist_m=200.0,
        )
        assert result is not None
        folder_ids = [m["folderrsn"] for m in result["top_matches"]]
        assert "0" not in folder_ids
        assert "1" in folder_ids


class TestZoneDeranking:
    def test_same_zone_ranks_above_different_zone_with_equal_text_sim(
        self, tmp_path: Path
    ) -> None:
        """Given: App A (same zone, nearby) and App B (different zone, nearby),
        equal text similarity (fake pipeline returns identical vectors).
        When: Scoring with zoning_class matching App A.
        Then: App A ranks first (not deranked); App B ranks lower (0.65× penalty)."""
        data_dir = tmp_path / "data_zd"
        model_dir = tmp_path / "models_zd"
        _write_fixture_with_location_zones(
            data_dir,
            model_dir,
            lats=[43.651, 43.652],
            lons=[-79.38, -79.38],
            zoning_classes=["R", "CR"],
        )
        result = score_description_similarity(
            "test",
            data_dir=data_dir,
            model_dir=model_dir,
            lat=43.65,
            lon=-79.38,
            radius_m=2000.0,
            zoning_class="R",
        )
        assert result is not None
        assert len(result["top_matches"]) >= 1
        assert result["top_matches"][0]["folderrsn"] == "0"


class TestProximityFilter:
    def test_far_apps_excluded_when_lat_lon_provided(self, tmp_path: Path) -> None:
        """Given: One Toronto app (43.65, -79.38) and one NYC app (40.71, -74.01).
        When: Scoring with Toronto lat/lon and radius_m=1000.
        Then: Only the Toronto app appears in top_matches."""
        data_dir = tmp_path / "data_prox1"
        model_dir = tmp_path / "models_prox1"
        _write_fixture_with_locations(
            data_dir,
            model_dir,
            lats=[43.65, 40.71],
            lons=[-79.38, -74.01],
        )
        result = score_description_similarity(
            "test",
            data_dir=data_dir,
            model_dir=model_dir,
            lat=43.65,
            lon=-79.38,
            radius_m=1000.0,
        )
        assert result is not None
        folder_ids = [m["folderrsn"] for m in result["top_matches"]]
        assert "0" in folder_ids
        assert "1" not in folder_ids

    def test_all_apps_included_without_lat_lon(self, tmp_path: Path) -> None:
        """Given: Apps at Toronto and NYC locations.
        When: Scoring without lat/lon params.
        Then: Both apps are included (no proximity filter applied)."""
        data_dir = tmp_path / "data_prox2"
        model_dir = tmp_path / "models_prox2"
        _write_fixture_with_locations(
            data_dir,
            model_dir,
            lats=[43.65, 40.71],
            lons=[-79.38, -74.01],
        )
        result = score_description_similarity(
            "test",
            data_dir=data_dir,
            model_dir=model_dir,
        )
        assert result is not None
        folder_ids = [m["folderrsn"] for m in result["top_matches"]]
        assert "0" in folder_ids
        assert "1" in folder_ids

    def test_no_crash_when_corpus_has_no_lat_lon_columns(self, tmp_path: Path) -> None:
        """Given: Enriched parquet without lat/lon columns, lat/lon params provided.
        When: Scoring with lat/lon.
        Then: Returns results normally (skips filter gracefully)."""
        data_dir = tmp_path / "data_prox3"
        model_dir = tmp_path / "models_prox3"
        _write_fixture(data_dir, model_dir, appeal_labels=[0, 1])
        result = score_description_similarity(
            "test",
            data_dir=data_dir,
            model_dir=model_dir,
            lat=43.65,
            lon=-79.38,
            radius_m=1000.0,
        )
        assert result is not None
        assert result["n_similar"] >= 0


class TestDeduplicateMatches:
    """Unit tests for _deduplicate_matches helper."""

    def test_duplicate_folderrsn_keeps_highest_similarity(self) -> None:
        """Given matches with duplicate folderrsn (sorted desc by similarity),
        when deduplicated, then only the highest-similarity entry per app is kept."""
        matches = [
            {"similarity": 0.95, "folderrsn": "A", "dev_appealed": 0},
            {"similarity": 0.90, "folderrsn": "B", "dev_appealed": None},
            {"similarity": 0.85, "folderrsn": "A", "dev_appealed": None},
            {"similarity": 0.80, "folderrsn": "B", "dev_appealed": 1},
        ]
        result = _deduplicate_matches(matches)
        assert len(result) == 2
        assert result[0]["folderrsn"] == "A"
        assert result[0]["similarity"] == 0.95
        assert result[1]["folderrsn"] == "B"
        assert result[1]["similarity"] == 0.90

    def test_none_folderrsn_rows_pass_through(self) -> None:
        """Given matches with None folderrsn, when deduplicated,
        then all None-folderrsn rows are kept (cannot be grouped)."""
        matches = [
            {"similarity": 0.95, "folderrsn": "A"},
            {"similarity": 0.90, "folderrsn": None},
            {"similarity": 0.85, "folderrsn": None},
        ]
        result = _deduplicate_matches(matches)
        assert len(result) == 3

    def test_empty_list_returns_empty(self) -> None:
        """Given empty matches, when deduplicated, then returns empty list."""
        assert _deduplicate_matches([]) == []

    def test_no_folderrsn_key_keeps_all(self) -> None:
        """Given matches without a folderrsn key at all,
        when deduplicated, then all are kept (treated as ungroupable)."""
        matches = [{"similarity": 0.9}, {"similarity": 0.8}]
        result = _deduplicate_matches(matches)
        assert len(result) == 2

    def test_no_duplicates_unchanged(self) -> None:
        """Given matches with all unique folderrsns,
        when deduplicated, then result is identical."""
        matches = [
            {"similarity": 0.95, "folderrsn": "A"},
            {"similarity": 0.90, "folderrsn": "B"},
            {"similarity": 0.85, "folderrsn": "C"},
        ]
        result = _deduplicate_matches(matches)
        assert len(result) == 3


def _write_fixture_with_duplicates(data_dir: Path, model_dir: Path) -> None:
    """Write a fixture where folderrsn 'A' appears 3 times, 'B' once."""
    (data_dir / "enriched").mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)
    n = 4
    svd_cols = {f"desc_svd_{i}": [float(i % 3 + 1) * 0.1] * n for i in range(20)}
    df = pl.DataFrame(
        {
            **svd_cols,
            "dev_appealed": [0, 0, 0, 0],
            "dev_approved": [1, None, None, 0],
            "folderrsn": ["A", "A", "A", "B"],
            "application_type": ["OZ"] * n,
        }
    )
    df.write_parquet(str(data_dir / "enriched" / "dev_applications.parquet"))
    joblib.dump(_FakePipeline(), model_dir / "desc_tfidf.joblib")


class TestDeduplicationInScorer:
    def test_n_similar_counts_distinct_applications(self, tmp_path: Path) -> None:
        """Given a corpus where folderrsn 'A' appears 3 times and 'B' once,
        when scoring, then n_similar should be 2 (distinct applications),
        not 4 (raw rows)."""
        data_dir = tmp_path / "data_dedup"
        model_dir = tmp_path / "models_dedup"
        _write_fixture_with_duplicates(data_dir, model_dir)
        result = score_description_similarity(
            "test", data_dir=data_dir, model_dir=model_dir
        )
        assert result is not None
        assert result["n_similar"] == 2

    def test_top_matches_has_no_duplicate_folderrsn(self, tmp_path: Path) -> None:
        """Given a corpus with duplicate folderrsns, when scoring,
        then top_matches contains at most one entry per folderrsn."""
        data_dir = tmp_path / "data_dedup2"
        model_dir = tmp_path / "models_dedup2"
        _write_fixture_with_duplicates(data_dir, model_dir)
        result = score_description_similarity(
            "test", data_dir=data_dir, model_dir=model_dir
        )
        assert result is not None
        rsns = [m["folderrsn"] for m in result["top_matches"] if m.get("folderrsn")]
        assert len(rsns) == len(set(rsns))


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
        df = pl.DataFrame(
            {
                **svd_cols,
                "dev_appealed": [1, 1, 0, 0],  # R: [1,1], RM: [0,0] → RM appeal=0%
                "dev_approved": [1, 0, 1, 0],
                "folderrsn": ["0", "1", "2", "3"],
                "application_type": ["OZ"] * n,
                "zoning_class": ["R", "R", "RM", "RM"],
            }
        )
        df.write_parquet(str(data_dir / "enriched" / "dev_applications.parquet"))
        joblib.dump(_FakePipeline(), model_dir / "desc_tfidf.joblib")
        result = score_description_similarity(
            "test", data_dir=data_dir, model_dir=model_dir, zoning_class="RM"
        )
        assert result is not None
        assert "zone_matched_appeal_rate" in result
        # RM apps: both appealed=0 → rate = 0.0
        assert result["zone_matched_appeal_rate"] == 0.0
