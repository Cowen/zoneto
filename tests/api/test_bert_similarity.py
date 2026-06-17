"""Tests for BERT-based description similarity scorer."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl


class _FakeBertModel:
    """Deterministic stub for SentenceTransformer — no network, no GPU."""

    def encode(
        self,
        texts: list[str],
        convert_to_numpy: bool = True,
        show_progress_bar: bool = False,
        batch_size: int = 32,
    ) -> np.ndarray:
        n = len(texts)
        # Return all-ones vectors so every pair has cosine similarity 1.0
        return np.ones((n, 8), dtype=np.float32)


def _write_bert_fixture(data_dir: Path, n: int = 4) -> None:
    """Write minimal desc_bert_embeddings.npy and desc_bert_index.parquet."""
    emb_dir = data_dir / "enriched"
    emb_dir.mkdir(parents=True, exist_ok=True)
    embs = np.ones((n, 8), dtype=np.float32)
    np.save(emb_dir / "desc_bert_embeddings.npy", embs)
    df = pl.DataFrame(
        {
            "folderrsn": [str(i) for i in range(n)],
            "application_type": ["OZ"] * n,
            "dev_approved": [1, 0, 1, None][:n],
            "dev_appealed": [0, 0, 1, None][:n],
            "zoning_class": ["R", "RM", "R", "CR"][:n],
        }
    )
    df.write_parquet(emb_dir / "desc_bert_index.parquet")


def _write_bert_fixture_with_locations(
    data_dir: Path,
    lats: list[float | None],
    lons: list[float | None],
    zoning_classes: list[str | None] | None = None,
) -> None:
    """Write BERT fixture with lat/lon (and optional zoning_class) in the index."""
    emb_dir = data_dir / "enriched"
    emb_dir.mkdir(parents=True, exist_ok=True)
    n = len(lats)
    embs = np.ones((n, 8), dtype=np.float32)
    np.save(emb_dir / "desc_bert_embeddings.npy", embs)
    data: dict[str, list] = {
        "folderrsn": [str(i) for i in range(n)],
        "application_type": ["OZ"] * n,
        "dev_appealed": [0] * n,
        "lat": lats,
        "lon": lons,
    }
    if zoning_classes is not None:
        data["zoning_class"] = zoning_classes
    df = pl.DataFrame(data)
    df.write_parquet(emb_dir / "desc_bert_index.parquet")


class TestDefaultRadiusBert:
    def test_app_at_3km_excluded_by_default_radius(self, tmp_path: Path) -> None:
        """Given: One app at 1km and one at 3km from query.
        When: Scoring BERT with default radius (2km).
        Then: Only the 1km app appears."""
        from zoneto.api.desc_similarity import score_description_similarity_bert

        _write_bert_fixture_with_locations(
            tmp_path,
            lats=[43.65 + 0.009, 43.65 + 0.027],
            lons=[-79.38, -79.38],
        )
        result = score_description_similarity_bert(
            "test",
            data_dir=tmp_path,
            model=_FakeBertModel(),
            lat=43.65,
            lon=-79.38,
        )
        assert result is not None
        folder_ids = [m.folderrsn for m in result.top_matches]
        assert "0" in folder_ids
        assert "1" not in folder_ids


class TestSelfExclusionBert:
    def test_app_at_query_location_excluded_when_min_dist_set(
        self, tmp_path: Path
    ) -> None:
        """Given: App at exact query lat/lon and another at 300m.
        When: Scoring BERT with min_dist_m=200.
        Then: Exact-location app excluded; 300m app included."""
        from zoneto.api.desc_similarity import score_description_similarity_bert

        _write_bert_fixture_with_locations(
            tmp_path,
            lats=[43.65, 43.6527],
            lons=[-79.38, -79.38],
        )
        result = score_description_similarity_bert(
            "test",
            data_dir=tmp_path,
            model=_FakeBertModel(),
            lat=43.65,
            lon=-79.38,
            radius_m=2000.0,
            min_dist_m=200.0,
        )
        assert result is not None
        folder_ids = [m.folderrsn for m in result.top_matches]
        assert "0" not in folder_ids
        assert "1" in folder_ids


class TestZoneDeranking:
    def test_same_zone_ranks_above_different_zone(self, tmp_path: Path) -> None:
        """Given: App A (same zone) and App B (different zone), equal text similarity.
        When: Scoring BERT with zoning_class matching App A.
        Then: App A ranks first; App B deranked to second."""
        from zoneto.api.desc_similarity import score_description_similarity_bert

        _write_bert_fixture_with_locations(
            tmp_path,
            lats=[43.651, 43.652],
            lons=[-79.38, -79.38],
            zoning_classes=["R", "CR"],
        )
        result = score_description_similarity_bert(
            "test",
            data_dir=tmp_path,
            model=_FakeBertModel(),
            lat=43.65,
            lon=-79.38,
            radius_m=2000.0,
            zoning_class="R",
        )
        assert result is not None
        assert len(result.top_matches) >= 1
        assert result.top_matches[0].folderrsn == "0"


class TestScoreDescriptionSimilarityBert:
    def test_returns_none_when_embeddings_missing(self, tmp_path: Path) -> None:
        """Given: No BERT embeddings on disk.
        When: Scoring BERT description similarity.
        Then: Returns None gracefully."""
        from zoneto.api.desc_similarity import score_description_similarity_bert

        result = score_description_similarity_bert(
            "A residential building.",
            data_dir=tmp_path,
            model=_FakeBertModel(),
        )
        assert result is None

    def test_returns_dict_with_expected_keys(self, tmp_path: Path) -> None:
        """Given: BERT embeddings and index exist.
        When: Scoring description similarity.
        Then: Returns dict with top_matches, appeal_rate, approval_rate, n_similar."""
        from zoneto.api.desc_similarity import score_description_similarity_bert

        _write_bert_fixture(tmp_path)
        result = score_description_similarity_bert(
            "A residential building.",
            data_dir=tmp_path,
            model=_FakeBertModel(),
        )
        assert result is not None
        assert result.top_matches is not None
        assert result.appeal_rate is None or isinstance(result.appeal_rate, float)
        assert result.approval_rate is None or isinstance(result.approval_rate, float)
        assert result.n_similar is not None

    def test_approval_rate_computed_from_index(self, tmp_path: Path) -> None:
        """Given: Index with dev_approved=[1, 0, 1, None].
        When: Scoring BERT similarity.
        Then: approval_rate = 2/3 (two approved, one not, one null)."""
        from zoneto.api.desc_similarity import score_description_similarity_bert

        _write_bert_fixture(tmp_path, n=4)
        result = score_description_similarity_bert(
            "test", data_dir=tmp_path, model=_FakeBertModel(), top_n=4
        )
        assert result is not None
        # 2 approved of 3 labelled
        assert result.approval_rate is not None
        assert abs(result.approval_rate - 2 / 3) < 1e-6

    def test_zone_matched_filtering(self, tmp_path: Path) -> None:
        """Given: Corpus with R and RM zones. Query zone='RM'.
        When: Scoring with zoning_class='RM'.
        Then: zone_matched_n_similar reflects only RM applications."""
        from zoneto.api.desc_similarity import score_description_similarity_bert

        _write_bert_fixture(tmp_path, n=4)
        result = score_description_similarity_bert(
            "test",
            data_dir=tmp_path,
            model=_FakeBertModel(),
            zoning_class="RM",
            top_n=4,
        )
        assert result is not None
        assert result.zone_matched_n_similar is not None
        # Only 1 RM application in the fixture
        assert result.zone_matched_n_similar == 1

    def test_zone_matched_absent_without_zoning_class(self, tmp_path: Path) -> None:
        """Given: No zoning_class provided.
        When: Scoring BERT similarity.
        Then: zone_matched keys absent (backward compatible)."""
        from zoneto.api.desc_similarity import score_description_similarity_bert

        _write_bert_fixture(tmp_path, n=4)
        result = score_description_similarity_bert(
            "test", data_dir=tmp_path, model=_FakeBertModel()
        )
        assert result is not None
        assert result.zone_matched_n_similar is None

    def test_far_apps_excluded_when_lat_lon_provided(self, tmp_path: Path) -> None:
        """Given: One Toronto app (43.65, -79.38) and one NYC app (40.71, -74.01).
        When: Scoring BERT with Toronto lat/lon and radius_m=1000.
        Then: Only the Toronto app appears in top_matches."""
        from zoneto.api.desc_similarity import score_description_similarity_bert

        _write_bert_fixture_with_locations(
            tmp_path,
            lats=[43.65, 40.71],
            lons=[-79.38, -74.01],
        )
        result = score_description_similarity_bert(
            "test",
            data_dir=tmp_path,
            model=_FakeBertModel(),
            lat=43.65,
            lon=-79.38,
            radius_m=1000.0,
        )
        assert result is not None
        folder_ids = [m.folderrsn for m in result.top_matches]
        assert "0" in folder_ids
        assert "1" not in folder_ids

    def test_all_apps_included_without_lat_lon(self, tmp_path: Path) -> None:
        """Given: Apps at Toronto and NYC locations in BERT index.
        When: Scoring without lat/lon params.
        Then: Both apps included (no proximity filter applied)."""
        from zoneto.api.desc_similarity import score_description_similarity_bert

        _write_bert_fixture_with_locations(
            tmp_path,
            lats=[43.65, 40.71],
            lons=[-79.38, -74.01],
        )
        result = score_description_similarity_bert(
            "test",
            data_dir=tmp_path,
            model=_FakeBertModel(),
        )
        assert result is not None
        folder_ids = [m.folderrsn for m in result.top_matches]
        assert "0" in folder_ids
        assert "1" in folder_ids

    def test_no_crash_when_index_has_no_lat_lon(self, tmp_path: Path) -> None:
        """Given: BERT index without lat/lon columns, lat/lon params provided.
        When: Scoring with lat/lon.
        Then: Returns results normally (skips filter gracefully)."""
        from zoneto.api.desc_similarity import score_description_similarity_bert

        _write_bert_fixture(tmp_path, n=2)
        result = score_description_similarity_bert(
            "test",
            data_dir=tmp_path,
            model=_FakeBertModel(),
            lat=43.65,
            lon=-79.38,
            radius_m=1000.0,
        )
        assert result is not None
        assert result.n_similar >= 0

    def test_zone_base_keys_absent(self, tmp_path: Path) -> None:
        """Given: zone_base_* was removed (survivorship-biased approval rate).
        When: Scoring BERT similarity with zoning_class provided.
        Then: zone_base_approval_rate and zone_base_n are absent."""
        from zoneto.api.desc_similarity import score_description_similarity_bert

        _write_bert_fixture(tmp_path, n=4)
        result = score_description_similarity_bert(
            "test",
            data_dir=tmp_path,
            model=_FakeBertModel(),
            zoning_class="R",
            top_n=4,
        )
        assert result is not None
        assert not hasattr(result, "zone_base_approval_rate")
        assert not hasattr(result, "zone_base_n")
