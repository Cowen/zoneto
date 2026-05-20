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
        assert "top_matches" in result
        assert "appeal_rate" in result
        assert "approval_rate" in result
        assert "n_similar" in result

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
        assert result["approval_rate"] is not None
        assert abs(result["approval_rate"] - 2 / 3) < 1e-6

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
        assert "zone_matched_n_similar" in result
        # Only 1 RM application in the fixture
        assert result["zone_matched_n_similar"] == 1

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
        assert "zone_matched_n_similar" not in result

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
        assert "zone_base_approval_rate" not in result
        assert "zone_base_n" not in result
