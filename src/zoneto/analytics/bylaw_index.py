"""Bylaw chunking, embedding, and semantic search."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import polars as pl
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity


@dataclass
class Chunk:
    """A single by-law section chunk with metadata and embedding score."""

    chunk_id: int
    chapter: str
    section_number: str
    section_title: str
    source_file: str
    zones: list[str]
    text: str
    score: float
    parent_context: str = field(default="")


def split_into_chunks(text: str, source_file: str, chapter: str) -> list[Chunk]:
    """Split bylaw text into section-level chunks with metadata.

    Splits on section headers (e.g., "10.20.40"), then wraps long chunks
    at subsection or paragraph breaks with sentence-aligned overlap.
    Filters out chunks < 100 chars.

    Args:
        text: Full bylaw text from a file.
        source_file: Name of the source file (e.g., "part1.txt").
        chapter: Chapter number (e.g., "10").

    Returns:
        List of Chunk objects with text, metadata, and zone references.
    """
    chunks: list[Chunk] = []
    chunk_id = 0

    # Split on section headers: "10.20.40", "Chapter X", or "(252) Exception RM 252".
    # Lookahead (?=\s+Exception) prevents matching sub-item labels like "(1) Height.".
    section_pattern = (
        r"^(?:Chapter\s+\d+|(?:\d+\.)+\d+|\(\d+\)(?=\s+Exception))"
        r"\s+([^\n]*?)(?:\n|$)"
    )

    # Split text into sections
    section_matches = list(re.finditer(section_pattern, text, re.MULTILINE))

    if not section_matches:
        # No sections found; treat as single chunk if large enough
        if len(text) >= 100:
            zones = _extract_zones(text)
            chunks.append(
                Chunk(
                    chunk_id=chunk_id,
                    chapter=chapter,
                    section_number="",
                    section_title="",
                    source_file=source_file,
                    zones=zones,
                    text=text.strip(),
                    score=0.0,
                )
            )
        return chunks

    # Track the most recent chapter header seen so sub-chunks inherit ancestry.
    current_chapter_title = ""

    # Process each section
    for i, match in enumerate(section_matches):
        section_header = match.group(0)
        section_title = match.group(1).strip()
        section_start = match.end()

        # End of this section is the start of the next section
        if i < len(section_matches) - 1:
            section_end = section_matches[i + 1].start()
        else:
            section_end = len(text)

        section_body = text[section_start:section_end].strip()

        # Extract section number from header
        section_num_match = re.search(r"(\d+\.?)+", section_header)
        section_number = section_num_match.group(0) if section_num_match else ""

        # Detect chapter headers and update ancestry tracker.
        is_chapter = bool(re.match(r"^Chapter\s+\d+", section_header.strip()))
        if is_chapter:
            current_chapter_title = section_header.strip()

        # For non-chapter sections, the parent context is the chapter title.
        # For sub-chunks from long sections, it also includes the section identity.
        section_parent_ctx = current_chapter_title if not is_chapter else ""
        sub_chunk_parent_ctx = _build_parent_ctx(
            current_chapter_title, section_number, section_title
        )

        # Include the header line in the chunk text so retrieval has the full context.
        # Short section bodies are common (e.g., single-line rules); including the
        # header prevents them from falling below the min-length filter.
        section_text = (
            section_header.strip() + "\n\n" + section_body
            if section_body
            else section_header.strip()
        )

        # Extract zone references
        zones = _extract_zones(section_text)

        # If section is longer than ~1500 chars, split at subsection/paragraph breaks
        if len(section_text) > 1500:
            sub_chunks = _split_long_section(
                section_body, section_number, section_title, zones
            )
            for sub_text in sub_chunks:
                full_sub = section_header.strip() + "\n\n" + sub_text
                if len(full_sub) >= 100:
                    chunk_id += 1
                    chunks.append(
                        Chunk(
                            chunk_id=chunk_id,
                            chapter=chapter,
                            section_number=section_number,
                            section_title=section_title,
                            source_file=source_file,
                            zones=zones,
                            text=full_sub.strip(),
                            score=0.0,
                            parent_context=sub_chunk_parent_ctx,
                        )
                    )
        else:
            if len(section_text) >= 100:
                chunk_id += 1
                chunks.append(
                    Chunk(
                        chunk_id=chunk_id,
                        chapter=chapter,
                        section_number=section_number,
                        section_title=section_title,
                        source_file=source_file,
                        zones=zones,
                        text=section_text.strip(),
                        score=0.0,
                        parent_context=section_parent_ctx,
                    )
                )

    return chunks


def _build_parent_ctx(
    chapter_title: str, section_number: str, section_title: str
) -> str:
    """Combine chapter and section identity into a breadcrumb string for encoding."""
    section_part = f"{section_number} {section_title}".strip()
    if chapter_title and section_part:
        return f"{chapter_title} > {section_part}"
    return chapter_title or section_part


def _split_long_section(
    text: str, section_number: str, section_title: str, zones: list[str]
) -> list[str]:
    """Split a long section at subsection or paragraph breaks.

    Strategy (in order):
    1. Split at numbered subsection boundaries: "  (1) ...", "  (2) ..."
       If ≥2 parts found, accumulate into ≤1500-char chunks with no overlap
       (boundaries are semantic, so overlap would duplicate rules).
       Sub-parts still > 1500 chars are further split at lettered items "(A)".
    2. Fall back to paragraph splitting with sentence-boundary overlap.

    Args:
        text: The section body text to split.
        section_number: (unused, for context).
        section_title: (unused, for context).
        zones: (unused, for context).

    Returns:
        List of text strings, each ≤1500 chars where possible.
    """
    # Step 1: numbered subsection boundaries — "  (1) ...", "  (2) ..."
    num_parts = re.split(r"(?=^\s{2,}\(\d+\)\s)", text, flags=re.MULTILINE)
    num_parts = [p for p in num_parts if p.strip()]

    if len(num_parts) >= 2:
        chunks: list[str] = []
        current = ""
        for part in num_parts:
            test = current + part if current else part
            if len(test) > 1500 and current:
                chunks.append(current)
                current = part
            else:
                current = test
        if current:
            chunks.append(current)

        # Further split any oversized chunk at lettered items "(A)", "(B)", ...
        result: list[str] = []
        for chunk in chunks:
            if len(chunk) > 1500:
                result.extend(_split_at_letters_or_paragraphs(chunk))
            else:
                result.append(chunk)
        return result

    # Step 2: paragraph splitting with sentence-boundary overlap
    return _split_at_paragraphs(text)


def _split_at_letters_or_paragraphs(text: str) -> list[str]:
    """Split at lettered sub-items "(A)", "(B)", ... or fall back to paragraphs."""
    letter_parts = re.split(r"(?=^\s{4,}\([A-Z]\)\s)", text, flags=re.MULTILINE)
    letter_parts = [p for p in letter_parts if p.strip()]

    if len(letter_parts) >= 2:
        chunks: list[str] = []
        current = ""
        for part in letter_parts:
            test = current + part if current else part
            if len(test) > 1500 and current:
                chunks.append(current)
                current = part
            else:
                current = test
        if current:
            chunks.append(current)
        return chunks

    return _split_at_paragraphs(text)


def _split_at_paragraphs(text: str) -> list[str]:
    """Split text at paragraph breaks with sentence-boundary overlap."""
    paragraphs = re.split(r"\n\n+", text)
    chunks: list[str] = []
    current_chunk = ""

    for paragraph in paragraphs:
        paragraph = paragraph.strip()
        if not paragraph:
            continue

        test_chunk = current_chunk + "\n\n" + paragraph if current_chunk else paragraph

        if len(test_chunk) > 1500 and current_chunk:
            chunks.append(current_chunk)
            # Roll back to the last sentence boundary rather than cutting mid-sentence.
            tail = current_chunk[-400:] if len(current_chunk) > 400 else current_chunk
            boundaries = [m.end() for m in re.finditer(r"[.!?]\s+", tail)]
            overlap = tail[boundaries[-1] :] if boundaries else tail[-200:]
            current_chunk = overlap + "\n\n" + paragraph if overlap else paragraph
        else:
            current_chunk = test_chunk

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def _extract_zones(text: str) -> list[str]:
    """Extract zone codes from text.

    Looks for patterns like:
    - "in the R, RD, RM zones"
    - "R zone" or "RA zone"
    - "R and RA zones"

    Args:
        text: Section text.

    Returns:
        List of unique zone codes found.
    """
    zones: set[str] = set()

    # Pattern: "in the [ZONE CODES] zones" or "in the [ZONE CODES]"
    # Zone codes are typically 1-3 uppercase letters, possibly with numbers
    zone_patterns = [
        r"in the ([\w\s,]+?)\s*zones?",  # "in the R, RD, RM zones"
        r"\b([A-Z]{1,3})\s+zone\b",  # "R zone", "RA zone"
        r"(?:Zone\s+)?([A-Z]{1,3})(?:\s+|,|\.|$)",  # Just zone code
    ]

    for pattern in zone_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            # Split on commas and spaces, clean up
            if isinstance(match, tuple):
                zone_str = match[0] if match else ""
            else:
                zone_str = match

            # Extract individual codes
            codes = re.findall(r"[A-Z]{1,3}", zone_str)
            for code in codes:
                # Only add if valid zone code (1-3 uppercase letters)
                if len(code) <= 3 and code.isupper():
                    zones.add(code)

    return sorted(zones)


class BylawIndex:
    """Semantic search index over by-law chunks."""

    def __init__(self, index_dir: Path) -> None:
        """Load chunks and embeddings from disk.

        Args:
            index_dir: Directory containing chunks.parquet and embeddings.npy.

        Raises:
            FileNotFoundError: If required files are missing.
        """
        self.index_dir = Path(index_dir)
        self.chunks_path = self.index_dir / "chunks.parquet"
        self.embeddings_path = self.index_dir / "embeddings.npy"

        if not self.chunks_path.exists() or not self.embeddings_path.exists():
            raise FileNotFoundError(
                f"Missing chunks.parquet or embeddings.npy in {index_dir}"
            )

        # Load chunks DataFrame
        self.chunks_df = pl.read_parquet(self.chunks_path)

        # Load embeddings array
        self.embeddings = np.load(self.embeddings_path)

        # Initialize model for query encoding
        # Keep in sync with api/app.py _BERT_MODEL_NAME
        self.model = SentenceTransformer("BAAI/bge-small-en-v1.5")

    @classmethod
    def build(
        cls,
        bylaw_dir: Path,
        index_dir: Path,
        progress: Callable[[str], None] | None = None,
    ) -> "BylawIndex":
        """Build index from bylaw text files.

        Chunks all .txt files in bylaw_dir, encodes with sentence-transformers,
        saves to index_dir.

        Args:
            bylaw_dir: Directory with .txt bylaw files.
            index_dir: Directory to write chunks.parquet and embeddings.npy.
            progress: Optional callable for step-level log messages.

        Returns:
            Loaded BylawIndex instance ready to search.
        """
        bylaw_dir = Path(bylaw_dir)
        index_dir = Path(index_dir)
        index_dir.mkdir(parents=True, exist_ok=True)

        _log = progress or (lambda _: None)

        # Collect all chunks from all files
        all_chunks: list[Chunk] = []
        bylaw_files = sorted(bylaw_dir.glob("*.txt"))

        for bylaw_file in bylaw_files:
            _log(f"  Chunking {bylaw_file.name}...")
            text = bylaw_file.read_text(encoding="utf-8", errors="replace")

            # Try to extract chapter from content (Chapter X pattern)
            chapter_match = re.search(r"Chapter\s+(\d+)", text)
            chapter = chapter_match.group(1) if chapter_match else "0"

            chunks = split_into_chunks(
                text, source_file=bylaw_file.name, chapter=chapter
            )
            all_chunks.extend(chunks)
            _log(f"    {len(chunks):,} chunks")

        # Renumber chunks with global IDs
        for i, chunk in enumerate(all_chunks):
            chunk.chunk_id = i

        # Create DataFrame — parent_context persisted so it survives round-trips
        chunks_data = {
            "chunk_id": [c.chunk_id for c in all_chunks],
            "chapter": [c.chapter for c in all_chunks],
            "section_number": [c.section_number for c in all_chunks],
            "section_title": [c.section_title for c in all_chunks],
            "source_file": [c.source_file for c in all_chunks],
            "zones": [c.zones for c in all_chunks],
            "text": [c.text for c in all_chunks],
            "parent_context": [c.parent_context for c in all_chunks],
        }
        chunks_df = pl.DataFrame(chunks_data)
        chunks_df.write_parquet(index_dir / "chunks.parquet")

        # Encode all chunks — show_progress_bar renders a tqdm bar to stderr.
        # Prepend parent_context to the encoding text so the embedding captures
        # chapter/section ancestry; the stored Chunk.text remains unchanged for display.
        _log(
            f"  Encoding {len(all_chunks):,} chunks with BAAI/bge-small-en-v1.5"
            " (CPU — takes 2–8 min)..."
        )
        model = SentenceTransformer("BAAI/bge-small-en-v1.5")
        texts = [
            f"{c.parent_context}\n\n{c.text}" if c.parent_context else c.text
            for c in all_chunks
        ]
        embeddings = model.encode(
            texts, convert_to_numpy=True, show_progress_bar=True
        ).astype(np.float32)

        np.save(index_dir / "embeddings.npy", embeddings)

        return cls(index_dir)

    def search(
        self, query: str, zones: list[str] | None = None, k: int = 8
    ) -> list[Chunk]:
        """Search for relevant bylaw chunks.

        Encodes query, computes cosine similarity, optionally boosts zone matches.

        Args:
            query: Search query string.
            zones: Optional list of zone codes to boost (e.g., ["RA", "RM"]).
            k: Number of results to return.

        Returns:
            Top-k chunks sorted by score (highest first).
        """
        # Encode query as a single-element list so the model always returns shape [1, D]
        query_embedding = self.model.encode([query], convert_to_numpy=True).astype(
            np.float32
        )
        if query_embedding.ndim == 1:
            query_embedding = query_embedding.reshape(1, -1)

        # Compute similarities
        similarities = cosine_similarity(query_embedding, self.embeddings)[0]

        # Optionally boost by zone overlap
        if zones:
            zones_set = set(zones)
            for i, chunk_zones in enumerate(self.chunks_df["zones"]):
                chunk_zones_set = set(chunk_zones)
                # If there's any overlap, boost the score by 10%
                if chunk_zones_set & zones_set:
                    similarities[i] *= 1.1

        # Sort by similarity (descending) and get top-k
        top_indices = np.argsort(-similarities)[:k]

        # Build result Chunks with scores
        results = []
        for idx in top_indices:
            row = self.chunks_df.row(int(idx), named=True)
            chunk = Chunk(
                chunk_id=row["chunk_id"],
                chapter=row["chapter"],
                section_number=row["section_number"],
                section_title=row["section_title"],
                source_file=row["source_file"],
                zones=list(row["zones"]),
                text=row["text"],
                score=float(similarities[idx]),
                parent_context=row.get("parent_context") or "",
            )
            results.append(chunk)

        return results

    def lookup_exception(self, zone: str, exception_no: str) -> list[Chunk]:
        """Direct lookup for a named exception schedule by zone and number.

        Filters chunks whose section_title contains 'Exception {zone} {exception_no}'.
        Score is set to 1.0 (exact match). Returns empty list when not found.

        Args:
            zone: Zone code prefix (e.g., "RM").
            exception_no: Exception number as string (e.g., "252").

        Returns:
            All matching chunks, score=1.0.
        """
        title_target = f"Exception {zone} {exception_no}"
        matching = self.chunks_df.filter(
            pl.col("section_title").str.contains(title_target, literal=True)
        )
        results = []
        for row in matching.iter_rows(named=True):
            results.append(
                Chunk(
                    chunk_id=row["chunk_id"],
                    chapter=row["chapter"],
                    section_number=row["section_number"],
                    section_title=row["section_title"],
                    source_file=row["source_file"],
                    zones=list(row["zones"]),
                    text=row["text"],
                    score=1.0,
                    parent_context=row.get("parent_context") or "",
                )
            )
        return results
