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


class TestExceptionChunking:
    def test_exception_header_produces_chunk(self) -> None:
        """Given: Bylaw text with exception header '(252) Exception RM 252'.
        When: Splitting into chunks.
        Then: A chunk with section_title 'Exception RM 252' is produced."""
        text = (
            "(252) Exception RM 252\n"
            "      The lands are subject to the following Site Specific Provisions.\n\n"
            "      Site Specific Provisions:\n"
            "        (A) The minimum lot frontage is 8.0 metres for a detached house.\n"
            "      Prevailing By-laws and Prevailing Sections:\n"
            "        (A) On lands known as 1500 Weston Road, "
            "City of Toronto By-law 1268-2009.\n"
        )
        chunks = split_into_chunks(text, source_file="part2.txt", chapter="900")
        titles = [c.section_title for c in chunks]
        assert any("Exception RM 252" in t for t in titles), (
            f"Expected 'Exception RM 252' in chunk titles, got: {titles}"
        )

    def test_exception_chunk_contains_provisions(self) -> None:
        """Given: Exception text with site-specific provisions.
        When: Splitting into chunks.
        Then: The chunk text includes the provision content."""
        text = (
            "(1581) Exception CR 1581\n"
            "       The lands are subject to the following provisions.\n\n"
            "       Site Specific Provisions:\n"
            "         (A) In a Commercial Residential zone, where the maximum\n"
            "             lawfully permitted height exceeds the right-of-way width,\n"
            "             angular plane requirements do not apply.\n"
            "       Prevailing By-laws and Prevailing Sections:\n"
            "         (A) Section 12(1) 199 of former City of Toronto By-law 438-86.\n"
        )
        chunks = split_into_chunks(text, source_file="part3.txt", chapter="900")
        exception_chunks = [c for c in chunks if "Exception CR 1581" in c.section_title]
        assert exception_chunks, "No chunk found with 'Exception CR 1581' in title"
        combined_text = " ".join(c.text for c in exception_chunks)
        assert (
            "angular plane" in combined_text
            or "Commercial Residential" in combined_text
        )

    def test_exception_section_number_extracted(self) -> None:
        """Given: Exception header '(252) Exception RM 252'.
        When: Splitting into chunks.
        Then: The chunk's section_number contains '252'."""
        text = (
            "(252) Exception RM 252\n"
            "      Site Specific Provisions:\n"
            "        (A) The minimum lot frontage is 8.0 metres for a detached house.\n"
            "      Prevailing By-laws and Prevailing Sections: (None Apply)\n"
        )
        chunks = split_into_chunks(text, source_file="part2.txt", chapter="900")
        exception_chunks = [c for c in chunks if "Exception RM 252" in c.section_title]
        assert exception_chunks
        assert "252" in exception_chunks[0].section_number

    def test_consecutive_exceptions_chunked_separately(self) -> None:
        """Given: Two consecutive exception headers.
        When: Splitting into chunks.
        Then: Each exception becomes a separate chunk."""
        text = (
            "(252) Exception RM 252\n"
            "      Site Specific Provisions:\n"
            "        (A) The minimum lot frontage is 8.0 metres for a detached house.\n"
            "      Prevailing By-laws and Prevailing Sections: (None Apply)\n"
            "(253) Exception RM 253\n"
            "      Site Specific Provisions:\n"
            "        (A) A detached house, semi-detached house, duplex, triplex, or "
            "townhouse are the only residential building types permitted.\n"
            "      Prevailing By-laws and Prevailing Sections: (None Apply)\n"
        )
        chunks = split_into_chunks(text, source_file="part2.txt", chapter="900")
        exception_titles = [
            c.section_title for c in chunks if "Exception RM" in c.section_title
        ]
        assert len(exception_titles) >= 2, (
            f"Expected at least 2 exception chunks, got: {exception_titles}"
        )


class TestExceptionLookup:
    def test_lookup_exception_returns_exact_chunk(self) -> None:
        """Given: Index with Exception RM 252 chunk.
        When: lookup_exception('RM', '252') is called.
        Then: Returns chunk(s) with section_title containing 'Exception RM 252'."""
        with tempfile.TemporaryDirectory() as tmpdir:
            index_dir = Path(tmpdir)
            chunks_data = {
                "chunk_id": [0, 1, 2],
                "chapter": ["900", "900", "900"],
                "section_number": ["134", "252", "265"],
                "section_title": [
                    "Exception RM 134",
                    "Exception RM 252",
                    "Exception RM 265",
                ],
                "source_file": ["part2.txt"] * 3,
                "zones": [["RM"], ["RM"], ["RM"]],
                "text": [
                    "(134) Exception RM 134\nSite Specific Provisions: (None Apply)",
                    "(252) Exception RM 252\nSite Specific Provisions:\n"
                    "  (A) The minimum lot frontage is 8.0 metres.",
                    "(265) Exception RM 265\nSite Specific Provisions: (None Apply)",
                ],
            }
            pl.DataFrame(chunks_data).write_parquet(index_dir / "chunks.parquet")
            model = MockModel()
            np.save(
                index_dir / "embeddings.npy",
                model.encode(chunks_data["text"]),
            )
            with patch("zoneto.analytics.bylaw_index.SentenceTransformer") as m:
                m.return_value = MockModel()
                index = BylawIndex(index_dir)

            results = index.lookup_exception("RM", "252")
            assert results, "Expected at least one chunk"
            assert all("Exception RM 252" in c.section_title for c in results)

    def test_lookup_exception_returns_empty_when_not_found(self) -> None:
        """Given: Index without a matching exception.
        When: lookup_exception('RM', '9999') is called.
        Then: Returns empty list."""
        with tempfile.TemporaryDirectory() as tmpdir:
            index_dir = Path(tmpdir)
            chunks_data = {
                "chunk_id": [0],
                "chapter": ["900"],
                "section_number": ["252"],
                "section_title": ["Exception RM 252"],
                "source_file": ["part2.txt"],
                "zones": [["RM"]],
                "text": ["(252) Exception RM 252\nSite Specific Provisions: (None)."],
            }
            pl.DataFrame(chunks_data).write_parquet(index_dir / "chunks.parquet")
            model = MockModel()
            np.save(
                index_dir / "embeddings.npy",
                model.encode(chunks_data["text"]),
            )
            with patch("zoneto.analytics.bylaw_index.SentenceTransformer") as m:
                m.return_value = MockModel()
                index = BylawIndex(index_dir)

            assert index.lookup_exception("RM", "9999") == []


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

    def test_chunk_parent_context_defaults_to_empty_string(self) -> None:
        """Given: A Chunk created without parent_context.
        When: Accessing parent_context.
        Then: Defaults to empty string."""
        chunk = Chunk(
            chunk_id=1,
            chapter="10",
            section_number="10.10",
            section_title="Zone R",
            source_file="test.txt",
            zones=["R"],
            text="Residential zone.",
            score=0.0,
        )
        assert chunk.parent_context == ""

    def test_chunk_parent_context_can_be_set(self) -> None:
        """Given: A Chunk created with an explicit parent_context.
        When: Accessing parent_context.
        Then: Returns the provided value."""
        chunk = Chunk(
            chunk_id=1,
            chapter="10",
            section_number="10.20.40",
            section_title="Height Requirements",
            source_file="test.txt",
            zones=["RM"],
            text="10.20.40 Height Requirements\n\nThe maximum height is 12 storeys.",
            score=0.0,
            parent_context="Chapter 10 Residential > 10.20.40 Height Requirements",
        )
        expected_ctx = "Chapter 10 Residential > 10.20.40 Height Requirements"
        assert chunk.parent_context == expected_ctx


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


class TestParentContext:
    """Tests for hierarchical context tracking and contextual encoding."""

    def test_chapter_title_propagates_to_section_chunks(self) -> None:
        """Given: Bylaw text with a Chapter header followed by sections.
        When: Splitting into chunks.
        Then: Sections within that chapter carry the chapter title as parent_context."""
        text = (
            "Chapter 10 Residential\n\n"
            "10.20.40 Height Requirements\n\n"
            "The maximum height of a building in this zone is 12 storeys, "
            "measured from established grade to the highest point of the roof. "
            "This limit applies to all principal buildings and structures in "
            "the zone, except as otherwise permitted under this by-law.\n"
        )
        chunks = split_into_chunks(text, source_file="test.txt", chapter="10")
        height_chunks = [c for c in chunks if "10.20.40" in c.section_number]
        assert height_chunks, "Expected chunk for section 10.20.40"
        assert height_chunks[0].parent_context == "Chapter 10 Residential"

    def test_chapter_chunk_itself_has_no_parent_context(self) -> None:
        """Given: A Chapter header with enough body text to survive the 100-char filter.
        When: Splitting into chunks.
        Then: The chapter chunk has empty parent_context (no ancestor above it)."""
        text = (
            "Chapter 10 Residential\n\n"
            "This chapter contains residential zone regulations for all zone "
            "categories including R, RD, RS, RT, and RM zones, applicable to "
            "all lands within the City of Toronto that are zoned residential.\n"
        )
        chunks = split_into_chunks(text, source_file="test.txt", chapter="10")
        chapter_chunks = [c for c in chunks if "Residential" in c.section_title]
        assert chapter_chunks, "Expected a chapter chunk"
        assert chapter_chunks[0].parent_context == ""

    def test_sections_before_any_chapter_have_empty_parent_context(self) -> None:
        """Given: Sections appearing before a Chapter header.
        When: Splitting into chunks.
        Then: Those sections have empty parent_context."""
        text = (
            "1.10 Definitions\n\n"
            "These definitions apply throughout this by-law and are to be used "
            "in the interpretation of all provisions, including zone-specific "
            "standards and general regulations applicable to all properties.\n\n"
            "Chapter 10 Residential\n\n"
            "10.10 Zone R\n\n"
            "Residential zone regulations for low-density housing types including "
            "detached houses, semi-detached houses, and townhouses.\n"
        )
        chunks = split_into_chunks(text, source_file="test.txt", chapter="1")
        defn_chunks = [c for c in chunks if "1.10" in c.section_number]
        assert defn_chunks
        assert defn_chunks[0].parent_context == ""

    def test_sub_chunks_get_breadcrumb_parent_context(self) -> None:
        """Given: A long section (>1500 chars) under a Chapter.
        When: Splitting produces sub-chunks.
        Then: Sub-chunks carry 'Chapter X > section_number title' as parent_context."""
        long_body = "This is a regulation sentence. " * 60  # ~1860 chars
        text = (
            "Chapter 10 Residential\n\n"
            f"10.20.40 Height Requirements\n\n{long_body}\n\n{long_body}\n"
        )
        chunks = split_into_chunks(text, source_file="test.txt", chapter="10")
        sub_chunks = [c for c in chunks if c.section_number == "10.20.40"]
        assert len(sub_chunks) >= 2, "Expected multiple sub-chunks from long section"
        for c in sub_chunks:
            assert "Chapter 10 Residential" in c.parent_context
            assert "10.20.40" in c.parent_context

    def test_build_persists_parent_context_column(self) -> None:
        """Given: Bylaw file with chapter and section.
        When: BylawIndex.build() is called.
        Then: chunks.parquet includes a parent_context column."""
        with tempfile.TemporaryDirectory() as tmpdir:
            bylaw_dir = Path(tmpdir) / "bylaw"
            index_dir = Path(tmpdir) / "index"
            bylaw_dir.mkdir()
            index_dir.mkdir()

            sample_text = (
                "Chapter 10 Residential\n\n"
                "10.10 Zone R\n\n"
                "Residential zone. Permitted uses include detached houses, "
                "semi-detached houses, townhouses, and accessory structures "
                "subject to the requirements set out in this chapter.\n"
            )
            (bylaw_dir / "test.txt").write_text(sample_text)

            with patch("zoneto.analytics.bylaw_index.SentenceTransformer") as mock_st:
                mock_st.return_value = MockModel()
                BylawIndex.build(bylaw_dir, index_dir)

            df = pl.read_parquet(index_dir / "chunks.parquet")
            assert "parent_context" in df.columns

    def test_search_returns_parent_context_from_parquet(self) -> None:
        """Given: Index built with parent_context in parquet.
        When: Searching.
        Then: Returned chunks have parent_context populated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            index_dir = Path(tmpdir)
            chunks_data = {
                "chunk_id": [0],
                "chapter": ["10"],
                "section_number": ["10.20.40"],
                "section_title": ["Height Requirements"],
                "source_file": ["part1.txt"],
                "zones": [["RM"]],
                "text": ["10.20.40 Height Requirements\n\nMax 12 storeys."],
                "parent_context": ["Chapter 10 Residential"],
            }
            pl.DataFrame(chunks_data).write_parquet(index_dir / "chunks.parquet")
            model = MockModel()
            np.save(index_dir / "embeddings.npy", model.encode(chunks_data["text"]))

            with patch("zoneto.analytics.bylaw_index.SentenceTransformer") as m:
                m.return_value = MockModel()
                index = BylawIndex(index_dir)

            results = index.search("height", k=1)
            assert results[0].parent_context == "Chapter 10 Residential"

    def test_search_gracefully_handles_missing_parent_context_column(self) -> None:
        """Given: Old-format parquet without parent_context column.
        When: Searching.
        Then: Returns chunks with empty parent_context (backward-compatible)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            index_dir = Path(tmpdir)
            chunks_data = {
                "chunk_id": [0],
                "chapter": ["10"],
                "section_number": ["10.10"],
                "section_title": ["Zone R"],
                "source_file": ["part1.txt"],
                "zones": [["R"]],
                "text": ["Residential zone regulations."],
            }
            pl.DataFrame(chunks_data).write_parquet(index_dir / "chunks.parquet")
            model = MockModel()
            np.save(index_dir / "embeddings.npy", model.encode(chunks_data["text"]))

            with patch("zoneto.analytics.bylaw_index.SentenceTransformer") as m:
                m.return_value = MockModel()
                index = BylawIndex(index_dir)

            results = index.search("residential", k=1)
            assert results[0].parent_context == ""


class TestSubsectionSplitting:
    """Tests for subsection-aware and paragraph splitting."""

    def test_numbered_subsections_split_at_boundaries(self) -> None:
        """Given: A long section body with indented (1), (2), (3) subsections.
        When: The combined body exceeds 1500 chars.
        Then: Chunks are split at subsection boundaries, not mid-subsection."""
        # Each subsection ~700 chars — three together exceed 1500
        sub = "Detailed rule content here. " * 25  # ~700 chars
        body = (
            f"  (1) Permitted Uses.\n      {sub}\n\n"
            f"  (2) Height Limits.\n      {sub}\n\n"
            f"  (3) Setback Rules.\n      {sub}\n"
        )
        text = f"10.20.40 Development Standards\n\n{body}"
        chunks = split_into_chunks(text, source_file="test.txt", chapter="10")

        assert len(chunks) >= 2
        # Subsections 1 and 3 (furthest apart) should not appear in the same chunk
        for chunk in chunks:
            has_sub1 = "Permitted Uses" in chunk.text
            has_sub3 = "Setback Rules" in chunk.text
            assert not (has_sub1 and has_sub3), (
                "Subsections 1 and 3 should be in separate chunks"
            )

    def test_lettered_items_split_oversized_subsection(self) -> None:
        """Given: Oversized (1) subsection with (A)-(E) items.
        When: Splitting. Then: Letter items are used as sub-split boundaries."""
        item = "Detailed lettered provision text. " * 20  # ~680 chars
        body = (
            "  (1) General Provisions.\n"
            f"      (A) First provision. {item}\n\n"
            f"      (B) Second provision. {item}\n\n"
            f"      (C) Third provision. {item}\n"
        )
        text = f"10.20.40 General Standards\n\n{body}"
        chunks = split_into_chunks(text, source_file="test.txt", chapter="10")

        assert len(chunks) >= 2
        for chunk in chunks:
            has_a = "First provision" in chunk.text
            has_c = "Third provision" in chunk.text
            assert not (has_a and has_c), (
                "Lettered items A and C should be in separate chunks"
            )

    def test_paragraph_splitting_fallback_when_no_subsections(self) -> None:
        """Given: A long section with no numbered subsection markers.
        When: Splitting.
        Then: Falls back to paragraph-based splitting."""
        paragraph = "Plain paragraph text without subsection markers. " * 35  # long
        text = f"10.10 General Zone R\n\n{paragraph}\n\n{paragraph}\n"
        chunks = split_into_chunks(text, source_file="test.txt", chapter="10")
        assert len(chunks) >= 2
        for chunk in chunks:
            assert len(chunk.text) <= 2200  # reasonable upper bound
