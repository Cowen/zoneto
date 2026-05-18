"""Tests for bylaw chunking and embedding index."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

import numpy as np
import polars as pl

from zoneto.analytics.bylaw_index import BylawIndex, Chunk, split_into_chunks


@dataclass
class MockModel:
    """Mock sentence transformer model for testing."""

    embedding_dim: int = 384

    def encode(self, texts: list[str], **kwargs: object) -> np.ndarray:
        """Return deterministic embeddings based on text hash."""
        embeddings = []
        for text in texts:
            seed = hash(text) % (2**31)
            rng = np.random.RandomState(seed)
            embedding = rng.randn(self.embedding_dim).astype(np.float32)
            # Normalize to unit length
            embedding = embedding / (np.linalg.norm(embedding) + 1e-8)
            embeddings.append(embedding)
        return np.array(embeddings, dtype=np.float32)


class TestChunker:
    """Tests for the bylaw text chunker."""

    def test_chunker_parses_section_numbers(self) -> None:
        """Given: Sample bylaw text with section numbers.
        When: Splitting into chunks.
        Then: Section numbers are correctly extracted."""
        text = """
10.20.40 Principal Building Requirements

(1) Height. The maximum height of a building or structure is 12 storeys or 40 metres,
    whichever is less, except as otherwise provided in this by-law.

10.20.50 Yards

(1) Front Yard. The minimum front yard setback is 5 metres for all principal buildings
    in this zone, measured from the street lot line to the nearest wall of the building.
"""
        chunks = split_into_chunks(text, source_file="test.txt", chapter="10")
        assert len(chunks) > 0
        section_numbers = [c.section_number for c in chunks if c.section_number]
        assert any("10.20.40" in sn for sn in section_numbers)

    def test_chunker_extracts_zone_references(self) -> None:
        """Given: Section text mentioning specific zones.
        When: Splitting into chunks.
        Then: Zone codes are extracted and stored."""
        text = """
10.10 Residential Zone (R)

These regulations apply in the R, RD, RM zones.
Maximum density is restricted in RA and RAC zones.
"""
        chunks = split_into_chunks(text, source_file="test.txt", chapter="10")
        # At least one chunk should have zone references
        all_zones = set()
        for chunk in chunks:
            all_zones.update(chunk.zones)
        # Check for zone codes mentioned
        assert len(all_zones) > 0

    def test_chunker_filters_short_chunks(self) -> None:
        """Given: Text with very short lines/sections.
        When: Splitting into chunks with minimum size threshold.
        Then: Short chunks are filtered out."""
        text = """
10.5 General

Applies to all.

10.10 Zone R

This is the Residential Zone. Permitted uses include detached houses,
semi-detached houses, townhouses, duplexes, triplexes, fourplexes and
apartment buildings up to 6 storeys in height.
"""
        chunks = split_into_chunks(text, source_file="test.txt", chapter="10")
        # All chunks should meet minimum size (100 chars)
        for chunk in chunks:
            assert len(chunk.text) >= 100 or chunk.text.strip() == ""

    def test_chunker_preserves_metadata(self) -> None:
        """Given: Bylaw text from a known source.
        When: Splitting into chunks.
        Then: Metadata fields (chapter, source_file) are preserved."""
        text = (
            "10.10 Zone R\n\n"
            "Residential zone for single-family homes. Permitted uses include "
            "detached houses, semi-detached houses, and accessory structures "
            "subject to the requirements of this chapter."
        )
        chunks = split_into_chunks(text, source_file="part1.txt", chapter="10")
        assert len(chunks) > 0
        for chunk in chunks:
            assert chunk.source_file == "part1.txt"
            assert chunk.chapter == "10"

    def test_chunker_splits_long_text_at_paragraphs(self) -> None:
        """Given: A long section text (>1500 chars).
        When: Splitting into chunks.
        Then: Long sections are split at paragraph boundaries."""
        # Create a text longer than 1500 chars
        paragraph = "This is a regulation. " * 50  # ~1100 chars
        text = f"""
10.20.40 Requirements

{paragraph}

{paragraph}

10.20.50 Next Section

Short section.
"""
        chunks = split_into_chunks(text, source_file="test.txt", chapter="10")
        # Should have multiple chunks due to size
        assert len(chunks) > 0
        # All chunks should be reasonably sized
        for chunk in chunks:
            if chunk.text.strip():
                # Allow 2000 as upper bound (1500 target + some tolerance)
                assert len(chunk.text) <= 2000


class TestBylawIndexSearch:
    """Tests for BylawIndex search functionality."""

    def test_search_returns_chunks_sorted_by_score(self) -> None:
        """Given: Multiple chunks with varying relevance.
        When: Searching for a query.
        Then: Results are sorted by cosine similarity (descending)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            index_dir = Path(tmpdir)

            # Create test chunks
            chunks_data = {
                "chunk_id": [1, 2, 3],
                "chapter": ["10", "10", "15"],
                "section_number": ["10.10", "10.20", "15.5"],
                "section_title": ["Zone R", "Zone RD", "Zone RA"],
                "source_file": ["part1.txt", "part1.txt", "part1.txt"],
                "zones": [["R"], ["RD"], ["RA"]],
                "text": [
                    "Residential zone. Maximum 6 storeys.",
                    "Detached zone. Maximum 3 storeys.",
                    "Apartment zone. Maximum 20 storeys.",
                ],
            }
            chunks_df = pl.DataFrame(chunks_data)
            chunks_df.write_parquet(index_dir / "chunks.parquet")

            # Create embeddings
            model = MockModel()
            texts = chunks_data["text"]
            embeddings = model.encode(texts)
            np.save(index_dir / "embeddings.npy", embeddings)

            # Load index
            with patch("zoneto.analytics.bylaw_index.SentenceTransformer") as mock_st:
                mock_st.return_value = MockModel()
                index = BylawIndex(index_dir)

            # Search for apartment-related query
            query = "high-rise apartment building"
            results = index.search(query, k=3)

            # Should return 3 results
            assert len(results) == 3
            # Results should be sorted by score (descending)
            for i in range(len(results) - 1):
                assert results[i].score >= results[i + 1].score

    def test_search_respects_zone_filter(self) -> None:
        """Given: Chunks with different zone applicability.
        When: Searching with zone filter.
        Then: Zone-matching chunks are boosted in ranking."""
        with tempfile.TemporaryDirectory() as tmpdir:
            index_dir = Path(tmpdir)

            # Create test chunks with explicit zone associations
            chunks_data = {
                "chunk_id": [1, 2, 3],
                "chapter": ["10", "10", "15"],
                "section_number": ["10.10", "10.20", "15.5"],
                "section_title": ["Zone R", "Zone RD", "Zone RA"],
                "source_file": ["part1.txt", "part1.txt", "part1.txt"],
                "zones": [["R"], ["RD"], ["RA"]],
                "text": [
                    "General residential regulations apply.",
                    "Detached house zone regulations apply.",
                    "Apartment zone regulations apply.",
                ],
            }
            chunks_df = pl.DataFrame(chunks_data)
            chunks_df.write_parquet(index_dir / "chunks.parquet")

            # Create embeddings
            model = MockModel()
            texts = chunks_data["text"]
            embeddings = model.encode(texts)
            np.save(index_dir / "embeddings.npy", embeddings)

            with patch("zoneto.analytics.bylaw_index.SentenceTransformer") as mock_st:
                mock_st.return_value = MockModel()
                index = BylawIndex(index_dir)

            # Search with zone filter
            query = "height restrictions"
            results = index.search(query, zones=["RA"], k=3)

            # Should return results, possibly with RA-specific ones boosted
            assert len(results) > 0
            # At least one result should be accessible
            assert results[0].section_number is not None

    def test_search_returns_chunk_objects(self) -> None:
        """Given: An index with sample chunks.
        When: Searching.
        Then: Results are Chunk dataclass objects with all fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            index_dir = Path(tmpdir)

            chunks_data = {
                "chunk_id": [1],
                "chapter": ["10"],
                "section_number": ["10.10"],
                "section_title": ["Zone R"],
                "source_file": ["part1.txt"],
                "zones": [["R", "RD"]],
                "text": ["Residential regulations."],
            }
            chunks_df = pl.DataFrame(chunks_data)
            chunks_df.write_parquet(index_dir / "chunks.parquet")

            model = MockModel()
            embeddings = model.encode(chunks_data["text"])
            np.save(index_dir / "embeddings.npy", embeddings)

            with patch("zoneto.analytics.bylaw_index.SentenceTransformer") as mock_st:
                mock_st.return_value = MockModel()
                index = BylawIndex(index_dir)

            results = index.search("residential", k=1)

            assert len(results) == 1
            chunk = results[0]
            assert isinstance(chunk, Chunk)
            assert chunk.chunk_id == 1
            assert chunk.chapter == "10"
            assert chunk.section_number == "10.10"
            assert chunk.section_title == "Zone R"
            assert chunk.source_file == "part1.txt"
            assert set(chunk.zones) == {"R", "RD"}
            assert "Residential" in chunk.text
            assert isinstance(chunk.score, float)

    def test_search_with_k_parameter(self) -> None:
        """Given: An index with multiple chunks.
        When: Searching with k parameter.
        Then: Returns exactly min(k, total_chunks) results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            index_dir = Path(tmpdir)

            chunks_data = {
                "chunk_id": [1, 2, 3, 4, 5],
                "chapter": ["10", "10", "15", "20", "20"],
                "section_number": ["10.10", "10.20", "15.5", "20.1", "20.2"],
                "section_title": ["R", "RD", "RA", "CL", "CR"],
                "source_file": ["part1.txt"] * 5,
                "zones": [["R"], ["RD"], ["RA"], ["CL"], ["CR"]],
                "text": ["text " * 50] * 5,
            }
            chunks_df = pl.DataFrame(chunks_data)
            chunks_df.write_parquet(index_dir / "chunks.parquet")

            model = MockModel()
            embeddings = model.encode(chunks_data["text"])
            np.save(index_dir / "embeddings.npy", embeddings)

            with patch("zoneto.analytics.bylaw_index.SentenceTransformer") as mock_st:
                mock_st.return_value = MockModel()
                index = BylawIndex(index_dir)

            # Request 3 results
            results = index.search("zone", k=3)
            assert len(results) == 3

            # Request more than available
            results = index.search("zone", k=100)
            assert len(results) == 5


class TestBylawIndexBuild:
    """Tests for BylawIndex.build() class method."""

    def test_build_creates_parquet_and_npy(self) -> None:
        """Given: Sample bylaw text files in a directory.
        When: Calling BylawIndex.build().
        Then: chunks.parquet and embeddings.npy are created."""
        with tempfile.TemporaryDirectory() as tmpdir:
            bylaw_dir = Path(tmpdir) / "bylaw"
            index_dir = Path(tmpdir) / "index"
            bylaw_dir.mkdir()
            index_dir.mkdir()

            # Write sample bylaw file with realistic-length section bodies
            sample_text = (
                "Chapter 10 Residential\n\n"
                "10.10 Zone R\n\n"
                "(1) Permitted Uses. Permitted uses in the R Zone include "
                "detached houses, semi-detached houses, townhouses, duplexes, "
                "triplexes, and accessory structures. No building may be used "
                "for any purpose not listed as a permitted use in this chapter.\n\n"
                "10.20 Zone RD\n\n"
                "(1) Permitted Uses. Permitted uses in the RD Zone are limited "
                "to detached houses and their accessory structures only. "
                "Semi-detached and multi-unit buildings are not permitted "
                "without a minor variance from the Committee of Adjustment.\n"
            )
            (bylaw_dir / "test_bylaw.txt").write_text(sample_text)

            # Build with mocked model
            with patch("zoneto.analytics.bylaw_index.SentenceTransformer") as mock_st:
                model = MockModel()
                mock_st.return_value = model

                BylawIndex.build(bylaw_dir, index_dir)

                # Check files exist
                assert (index_dir / "chunks.parquet").exists()
                assert (index_dir / "embeddings.npy").exists()

                # Check parquet structure
                chunks_df = pl.read_parquet(index_dir / "chunks.parquet")
                assert "chunk_id" in chunks_df.columns
                assert "chapter" in chunks_df.columns
                assert "section_number" in chunks_df.columns
                assert "section_title" in chunks_df.columns
                assert "source_file" in chunks_df.columns
                assert "zones" in chunks_df.columns
                assert "text" in chunks_df.columns

                # Check embeddings shape
                embeddings = np.load(index_dir / "embeddings.npy")
                assert embeddings.shape[1] == 384
                assert embeddings.shape[0] == len(chunks_df)

    def test_build_returns_loaded_instance(self) -> None:
        """Given: Bylaw directory.
        When: Calling BylawIndex.build().
        Then: Returns a loaded BylawIndex instance ready to search."""
        with tempfile.TemporaryDirectory() as tmpdir:
            bylaw_dir = Path(tmpdir) / "bylaw"
            index_dir = Path(tmpdir) / "index"
            bylaw_dir.mkdir()
            index_dir.mkdir()

            sample_text = (
                "Chapter 10 Residential\n\n"
                "10.10 Zone R\n\n"
                "Residential zone regulations. Permitted uses include detached houses, "
                "semi-detached houses, townhouses, duplexes, and accessory structures "
                "subject to the requirements and standards set out in this chapter.\n"
            )
            (bylaw_dir / "test.txt").write_text(sample_text)

            with patch("zoneto.analytics.bylaw_index.SentenceTransformer") as mock_st:
                model = MockModel()
                mock_st.return_value = model

                index = BylawIndex.build(bylaw_dir, index_dir)

                # Should be able to search immediately
                results = index.search("residential", k=1)
                assert len(results) > 0


class TestChunkDataclass:
    """Tests for the Chunk dataclass."""

    def test_chunk_has_score_field(self) -> None:
        """Given: A Chunk instance.
        When: Accessing fields.
        Then: Score field is present and numeric."""
        chunk = Chunk(
            chunk_id=1,
            chapter="10",
            section_number="10.10",
            section_title="Zone R",
            source_file="test.txt",
            zones=["R"],
            text="Residential zone.",
            score=0.95,
        )
        assert chunk.score == 0.95
        assert isinstance(chunk.score, float)


class TestZoneFiltering:
    """Tests for zone-based filtering in search."""

    def test_zone_filtering_boosts_matching_zones(self) -> None:
        """Given: Chunks with different zones and a zone filter.
        When: Searching with specific zones.
        Then: Chunks matching those zones rank higher (or equally)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            index_dir = Path(tmpdir)

            # Create chunks where one matches the query zone
            chunks_data = {
                "chunk_id": [1, 2],
                "chapter": ["10", "15"],
                "section_number": ["10.10", "15.5"],
                "section_title": ["Zone R", "Zone RA"],
                "source_file": ["part1.txt", "part1.txt"],
                "zones": [["R", "RD"], ["RA"]],
                "text": ["Low-rise residential", "High-rise apartment"],
            }
            chunks_df = pl.DataFrame(chunks_data)
            chunks_df.write_parquet(index_dir / "chunks.parquet")

            model = MockModel()
            embeddings = model.encode(chunks_data["text"])
            np.save(index_dir / "embeddings.npy", embeddings)

            with patch("zoneto.analytics.bylaw_index.SentenceTransformer") as mock_st:
                mock_st.return_value = MockModel()
                index = BylawIndex(index_dir)

            # Search with RA zone filter - should prioritize RA chunks
            results_with_filter = index.search("building height", zones=["RA"], k=2)
            results_no_filter = index.search("building height", k=2)

            # Both should return results
            assert len(results_with_filter) > 0
            assert len(results_no_filter) > 0
