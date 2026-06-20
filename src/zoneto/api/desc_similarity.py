"""Description similarity scorer using the trained TF-IDF + SVD pipeline."""

from __future__ import annotations

import math
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
from pydantic import BaseModel, ConfigDict

from zoneto.analytics.retrieval_eval import magnitude_band as _compute_band

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer

# Minimum same-magnitude comps before the magnitude filter is applied — below
# this, fall back to the broader zone(+type) set rather than report a rate over
# a noisy handful.
_MIN_MAGNITUDE_COMPS = 3


class SimilarMatch(BaseModel):
    """A single comparable application surfaced by description similarity.

    All columns beyond ``similarity`` are optional — the scorers attach whichever
    are present in the enriched corpus. ``extra="ignore"`` lets the BERT path's
    dynamic index columns that aren't modelled here pass through harmlessly.
    """

    model_config = ConfigDict(extra="ignore")

    similarity: float
    folderrsn: str | None = None
    application_type: str | None = None
    dev_appealed: int | None = None
    dev_approved: int | None = None
    zoning_class: str | None = None
    street_address: str | None = None
    lat: float | None = None
    lon: float | None = None
    proposed_storeys: int | None = None
    proposed_units: int | None = None


class DescriptionSimilarity(BaseModel):
    """Aggregate description-similarity result returned by the scorers.

    The ``zone_matched_*`` fields are populated only when the caller supplies a
    ``zoning_class`` (and, for the magnitude axis, enough same-scale comps).
    """

    top_matches: list[SimilarMatch]
    appeal_rate: float | None = None
    approval_rate: float | None = None
    n_similar: int = 0
    query_lat: float | None = None
    query_lon: float | None = None
    zone_matched_application_type: str | None = None
    zone_matched_magnitude: str | None = None
    zone_matched_n_similar: int | None = None
    zone_matched_approval_rate: float | None = None
    zone_matched_appeal_rate: float | None = None
    # The individual comps behind zone_matched_n_similar, so the cited count is
    # backed by the actual applications (len == zone_matched_n_similar).
    zone_matched_matches: list[SimilarMatch] = []


def _deduplicate_matches(matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate top_matches by folderrsn, keeping the highest-similarity entry.

    Rows without a folderrsn key (or with None) pass through unchanged — they
    cannot be grouped and deduplicating them would silently drop valid results.
    The input is assumed to be sorted by similarity descending, so the first
    occurrence of each folderrsn is already the best match for that application.
    """
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for m in matches:
        rsn = m.get("folderrsn")
        if rsn is None:
            result.append(m)
        else:
            key = str(rsn)
            if key not in seen:
                seen.add(key)
                result.append(m)
    return result


def score_description_similarity(
    description: str,
    data_dir: Path,
    model_dir: Path,
    *,
    top_n: int = 20,
    min_similarity: float = 0.1,
    zoning_class: str | None = None,
    application_type: str | None = None,
    magnitude_band: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    radius_m: float = 2000.0,
    min_dist_m: float = 0.0,
) -> DescriptionSimilarity | None:
    """Score a description against the enriched application corpus.

    Uses the trained TF-IDF + SVD pipeline (desc_tfidf.joblib) to vectorize
    the description, then computes cosine similarity against desc_svd_* columns
    in the enriched dev_applications parquet.

    Returns None when the model or enriched data is unavailable.
    Returns a dict with:
      - top_matches: list of dicts (folderrsn, application_type, similarity,
        dev_appealed)
      - appeal_rate: float | None (share of labelled similar apps that were appealed)
      - n_similar: int (number of matches above min_similarity)
    """
    tfidf_path = model_dir / "desc_tfidf.joblib"
    enriched_path = data_dir / "enriched" / "dev_applications.parquet"

    if not tfidf_path.exists() or not enriched_path.exists():
        return None

    try:
        import duckdb
        import joblib

        pipeline = joblib.load(tfidf_path)
        query_vec: np.ndarray = pipeline.transform([description or ""])
        if query_vec.ndim == 1:
            query_vec = query_vec.reshape(1, -1)
        query_arr = np.asarray(query_vec, dtype=np.float64)[0]

        # Identify available SVD columns via schema introspection
        con = duckdb.connect()
        schema = con.execute(f"DESCRIBE SELECT * FROM '{enriched_path}'").fetchall()
        available = {str(r[0]) for r in schema}

        svd_cols = [f"desc_svd_{i}" for i in range(20) if f"desc_svd_{i}" in available]
        if not svd_cols:
            con.close()
            return None

        # Logical match columns paired with their SQL select expression. The
        # enriched corpus stores the address as components, so street_address is
        # composed the same way nearby_applications does (so the surfaced comps
        # carry a human-readable address, not just a folderrsn).
        extra_cols: list[str] = []
        extra_select: list[str] = []
        for c in [
            "dev_appealed",
            "dev_approved",
            "folderrsn",
            "application_type",
            "zoning_class",
            "lat",
            "lon",
            "proposed_storeys",
            "proposed_units",
        ]:
            if c in available:
                extra_cols.append(c)
                extra_select.append(c)
        if "street_num" in available and "street_name" in available:
            extra_cols.append("street_address")
            extra_select.append(
                "TRIM(COALESCE(CAST(street_num AS VARCHAR), '') || ' ' || "
                "COALESCE(street_name, '')) AS street_address"
            )
        not_null_filter = f"{svd_cols[0]} IS NOT NULL"

        prox_filter = ""
        has_latlon = "lat" in available and "lon" in available
        if lat is not None and lon is not None and has_latlon:
            lat_delta = radius_m / 111_111.0
            lon_delta = radius_m / (111_111.0 * math.cos(math.radians(lat)))
            prox_filter = (
                f" AND lat BETWEEN {lat - lat_delta} AND {lat + lat_delta}"
                f" AND lon BETWEEN {lon - lon_delta} AND {lon + lon_delta}"
            )
            if min_dist_m > 0:
                excl_lat = min_dist_m / 111_111.0
                excl_lon = min_dist_m / (111_111.0 * math.cos(math.radians(lat)))
                prox_filter += (
                    f" AND NOT (lat BETWEEN {lat - excl_lat} AND {lat + excl_lat}"
                    f" AND lon BETWEEN {lon - excl_lon} AND {lon + excl_lon})"
                )

        # Fetch corpus (proximity-filtered when lat/lon provided)
        select = ", ".join(svd_cols + extra_select)
        rows = con.execute(
            f"SELECT {select} FROM '{enriched_path}'"
            f" WHERE {not_null_filter}{prox_filter}"
        ).fetchall()

        # Fetch zone-matched corpus when caller supplies zoning_class. When an
        # application_type is also supplied, restrict to it — a rezoning's appeal
        # exposure should be compared to rezonings, not site-plan applications.
        zm_rows: list[Any] = []
        has_zoning_col = "zoning_class" in available
        zm_type = application_type if "application_type" in available else None
        if zoning_class is not None and has_zoning_col:
            zm_select = ", ".join(svd_cols + extra_select)
            zm_where = f"{not_null_filter} AND zoning_class = $1"
            zm_params: list[Any] = [zoning_class]
            if zm_type is not None:
                zm_where += " AND application_type = $2"
                zm_params.append(zm_type)
            zm_rows = con.execute(
                f"SELECT {zm_select} FROM '{enriched_path}' WHERE {zm_where}",
                zm_params,
            ).fetchall()

        con.close()

        if not rows:
            return None

        n_svd = len(svd_cols)
        corpus = np.array(
            [[float(r[i]) for i in range(n_svd)] for r in rows], dtype=np.float64
        )

        # Cosine similarity: normalise then dot-product
        q_norm = query_arr / (np.linalg.norm(query_arr) + 1e-10)
        c_norms = np.linalg.norm(corpus, axis=1, keepdims=True) + 1e-10
        corpus_normed = corpus / c_norms
        similarities: np.ndarray = np.array(corpus_normed @ q_norm)

        # Zone deranking: penalise apps from a different zone class
        if zoning_class is not None and "zoning_class" in extra_cols:
            zone_col_idx = n_svd + extra_cols.index("zoning_class")
            for i, row in enumerate(rows):
                app_zone = row[zone_col_idx]
                if app_zone is not None and app_zone != zoning_class:
                    similarities[i] *= 0.65

        top_indices = np.argsort(similarities)[::-1][:top_n]
        top_matches = []
        for idx in top_indices:
            sim = float(similarities[idx])
            if sim < min_similarity:
                break
            row = rows[idx]
            match: dict[str, Any] = {"similarity": round(sim, 3)}
            for j, col in enumerate(extra_cols):
                match[col] = row[n_svd + j]
            top_matches.append(match)

        # Deduplicate by folderrsn — enriched parquet may have multiple rows per
        # application (e.g. one per year). Keep highest-similarity match per app.
        top_matches = _deduplicate_matches(top_matches)

        # Rates computed from deduplicated matches (not raw indices) so duplicate
        # applications don't inflate or distort the aggregate statistics.
        appeal_rate: float | None = None
        if "dev_appealed" in extra_cols:
            ap_vals = [
                int(m["dev_appealed"])
                for m in top_matches
                if m.get("dev_appealed") is not None
            ]
            appeal_rate = sum(ap_vals) / len(ap_vals) if ap_vals else None

        approval_rate: float | None = None
        if "dev_approved" in extra_cols:
            apr_vals = [
                int(m["dev_approved"])
                for m in top_matches
                if m.get("dev_approved") is not None
            ]
            approval_rate = sum(apr_vals) / len(apr_vals) if apr_vals else None

        result: dict[str, Any] = {
            "top_matches": top_matches,
            "appeal_rate": appeal_rate,
            "approval_rate": approval_rate,
            "n_similar": len(top_matches),
            "query_lat": lat,
            "query_lon": lon,
        }

        # Zone-matched stats: only included when zoning_class was supplied
        if zoning_class is not None and has_zoning_col:
            if zm_type is not None:
                result["zone_matched_application_type"] = zm_type
            # Further restrict to comps of the same built-form scale, when the
            # caller supplies the query's magnitude band and enough same-scale
            # comps survive (else keep the broader zone(+type) set).
            if (
                magnitude_band is not None
                and "proposed_storeys" in extra_cols
                and zm_rows
            ):
                st_idx = n_svd + extra_cols.index("proposed_storeys")
                un_idx = n_svd + extra_cols.index("proposed_units")
                mag_rows = [
                    r
                    for r in zm_rows
                    if _compute_band(r[st_idx], r[un_idx]) == magnitude_band
                ]
                if len(mag_rows) >= _MIN_MAGNITUDE_COMPS:
                    zm_rows = mag_rows
                    result["zone_matched_magnitude"] = magnitude_band
            if zm_rows:
                zm_corpus = np.array(
                    [[float(r[i]) for i in range(n_svd)] for r in zm_rows],
                    dtype=np.float64,
                )
                zm_norms = np.linalg.norm(zm_corpus, axis=1, keepdims=True) + 1e-10
                zm_normed = zm_corpus / zm_norms
                zm_sims: np.ndarray = zm_normed @ q_norm
                zm_top_idx = np.argsort(zm_sims)[::-1][:top_n]

                # Build zm_matches as dicts (similarity + all columns) so the
                # cited count is backed by the actual comps, deduped by folderrsn.
                zm_match_dicts: list[dict[str, Any]] = []
                for i in zm_top_idx:
                    zm_sim = float(zm_sims[i])
                    if zm_sim < min_similarity:
                        continue
                    row = zm_rows[i]
                    d: dict[str, Any] = {"similarity": round(zm_sim, 3)}
                    for j, col in enumerate(extra_cols):
                        d[col] = row[n_svd + j]
                    zm_match_dicts.append(d)

                zm_match_dicts = _deduplicate_matches(zm_match_dicts)
                result["zone_matched_matches"] = zm_match_dicts
                result["zone_matched_n_similar"] = len(zm_match_dicts)

                if "dev_approved" in extra_cols:
                    zm_apr = [
                        int(d["dev_approved"])
                        for d in zm_match_dicts
                        if d.get("dev_approved") is not None
                    ]
                    result["zone_matched_approval_rate"] = (
                        sum(zm_apr) / len(zm_apr) if zm_apr else None
                    )
                else:
                    result["zone_matched_approval_rate"] = None

                if "dev_appealed" in extra_cols:
                    zm_ap = [
                        int(d["dev_appealed"])
                        for d in zm_match_dicts
                        if d.get("dev_appealed") is not None
                    ]
                    result["zone_matched_appeal_rate"] = (
                        sum(zm_ap) / len(zm_ap) if zm_ap else None
                    )
                else:
                    result["zone_matched_appeal_rate"] = None

            else:
                result["zone_matched_n_similar"] = 0
                result["zone_matched_approval_rate"] = None
                result["zone_matched_appeal_rate"] = None

        return DescriptionSimilarity.model_validate(result)

    except Exception:  # noqa: BLE001
        return None


def score_description_similarity_bert(
    description: str,
    data_dir: Path,
    *,
    model: "SentenceTransformer | Any | None" = None,
    top_n: int = 20,
    min_similarity: float = 0.1,
    zoning_class: str | None = None,
    application_type: str | None = None,
    magnitude_band: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    radius_m: float = 2000.0,
    min_dist_m: float = 0.0,
) -> DescriptionSimilarity | None:
    """Score a description against the BERT-encoded application corpus.

    Uses pre-computed BERT embeddings (desc_bert_embeddings.npy) from the
    enrichment pipeline for higher semantic accuracy than TF-IDF+SVD.

    Args:
        description: Free-text project description to score.
        data_dir: Root data directory (looks for enriched/ subdirectory).
        model: SentenceTransformer instance. Required — caller must pass the
            loaded BAAI/bge-small-en-v1.5 model (or a compatible stub).
        top_n: Maximum number of top matches to return.
        min_similarity: Minimum cosine similarity threshold.
        zoning_class: If provided, also compute zone-matched stats.

    Returns:
        None when embeddings or index are missing.
        Dict with top_matches, appeal_rate, approval_rate, n_similar, and
        optionally zone_matched_n_similar / zone_matched_approval_rate.
    """
    embeddings_path = data_dir / "enriched" / "desc_bert_embeddings.npy"
    index_path = data_dir / "enriched" / "desc_bert_index.parquet"

    if model is None or not embeddings_path.exists() or not index_path.exists():
        return None

    try:
        import polars as pl

        embeddings: np.ndarray = np.load(str(embeddings_path))
        index_df = pl.read_parquet(index_path)

        # Proximity filter: mask corpus to outer ring [min_dist_m, radius_m]
        cols = index_df.columns
        if lat is not None and lon is not None and "lat" in cols and "lon" in cols:
            lat_delta = radius_m / 111_111.0
            lon_delta = radius_m / (111_111.0 * math.cos(math.radians(lat)))
            excl_lat = min_dist_m / 111_111.0
            excl_lon = (
                min_dist_m / (111_111.0 * math.cos(math.radians(lat)))
                if min_dist_m > 0
                else 0.0
            )
            lat_vals = index_df["lat"].to_list()
            lon_vals = index_df["lon"].to_list()
            keep = np.array(
                [
                    lv is not None
                    and lnv is not None
                    and (lat - lat_delta) <= lv <= (lat + lat_delta)
                    and (lon - lon_delta) <= lnv <= (lon + lon_delta)
                    and not (
                        min_dist_m > 0
                        and (lat - excl_lat) <= lv <= (lat + excl_lat)
                        and (lon - excl_lon) <= lnv <= (lon + excl_lon)
                    )
                    for lv, lnv in zip(lat_vals, lon_vals)
                ]
            )
            embeddings = embeddings[keep]
            index_df = index_df.filter(pl.Series(keep))

        query_vec: np.ndarray = model.encode(
            [description or ""], convert_to_numpy=True, show_progress_bar=False
        )
        if query_vec.ndim == 1:
            query_vec = query_vec.reshape(1, -1)
        q = np.asarray(query_vec, dtype=np.float32)[0]

        # Cosine similarity
        q_norm = q / (np.linalg.norm(q) + 1e-10)
        c_norms = np.linalg.norm(embeddings, axis=1, keepdims=True) + 1e-10
        corpus_normed = embeddings / c_norms
        sims: np.ndarray = np.array(corpus_normed @ q_norm)

        # Zone deranking: penalise apps from a different zone class
        cols = index_df.columns
        if zoning_class is not None and "zoning_class" in cols:
            app_zones = index_df["zoning_class"].to_list()
            for i, app_zone in enumerate(app_zones):
                if app_zone is not None and app_zone != zoning_class:
                    sims[i] *= 0.65

        top_idx = np.argsort(sims)[::-1][:top_n]
        top_matches: list[dict[str, Any]] = []
        for i in top_idx:
            sim = float(sims[i])
            if sim < min_similarity:
                break
            row = index_df.row(int(i), named=True)
            m: dict[str, Any] = {"similarity": round(sim, 3)}
            for c in cols:
                m[c] = row.get(c)  # include zoning_class so narrator can flag mismatch
            top_matches.append(m)

        top_matches = _deduplicate_matches(top_matches)

        # Rates computed from deduplicated matches
        appeal_rate: float | None = None
        if "dev_appealed" in cols:
            ap_vals = [
                int(m["dev_appealed"])
                for m in top_matches
                if m.get("dev_appealed") is not None
            ]
            appeal_rate = sum(ap_vals) / len(ap_vals) if ap_vals else None

        approval_rate: float | None = None
        if "dev_approved" in cols:
            apr_vals = [
                int(m["dev_approved"])
                for m in top_matches
                if m.get("dev_approved") is not None
            ]
            approval_rate = sum(apr_vals) / len(apr_vals) if apr_vals else None

        result: dict[str, Any] = {
            "top_matches": top_matches,
            "appeal_rate": appeal_rate,
            "approval_rate": approval_rate,
            "n_similar": len(top_matches),
            "query_lat": lat,
            "query_lon": lon,
        }

        # Zone-matched stats when zoning_class provided. When an application_type
        # is also supplied, restrict to it (rezonings compared to rezonings).
        if zoning_class is not None and "zoning_class" in cols:
            zm_mask = index_df["zoning_class"] == zoning_class
            zm_type = application_type if "application_type" in cols else None
            if zm_type is not None:
                zm_mask = zm_mask & (index_df["application_type"] == zm_type)
                result["zone_matched_application_type"] = zm_type
            zm_indices = [i for i, m in enumerate(zm_mask.to_list()) if m]
            # Further restrict to the same built-form scale when the caller
            # supplies the query band and enough same-scale comps survive.
            if magnitude_band is not None and "proposed_storeys" in cols and zm_indices:
                _storeys = index_df["proposed_storeys"].to_list()
                _units = index_df["proposed_units"].to_list()
                mag_indices = [
                    i
                    for i in zm_indices
                    if _compute_band(_storeys[i], _units[i]) == magnitude_band
                ]
                if len(mag_indices) >= _MIN_MAGNITUDE_COMPS:
                    zm_indices = mag_indices
                    result["zone_matched_magnitude"] = magnitude_band
            if zm_indices:
                zm_emb = embeddings[zm_indices]
                zm_norms = np.linalg.norm(zm_emb, axis=1, keepdims=True) + 1e-10
                zm_normed = zm_emb / zm_norms
                zm_sims = zm_normed @ q_norm
                zm_top = np.argsort(zm_sims)[::-1][:top_n]
                # Build dicts (similarity + all columns) so the cited count is
                # backed by the actual comps, deduped by folderrsn.
                zm_match_dicts: list[dict[str, Any]] = []
                for j in zm_top:
                    zm_sim = float(zm_sims[j])
                    if zm_sim < min_similarity:
                        continue
                    row = index_df.row(int(zm_indices[j]), named=True)
                    d = {"similarity": round(zm_sim, 3)}
                    for c in cols:
                        d[c] = row.get(c)
                    zm_match_dicts.append(d)
                zm_match_dicts = _deduplicate_matches(zm_match_dicts)
                result["zone_matched_matches"] = zm_match_dicts
                result["zone_matched_n_similar"] = len(zm_match_dicts)
                if "dev_approved" in cols:
                    zm_apr = [
                        int(d["dev_approved"])
                        for d in zm_match_dicts
                        if d.get("dev_approved") is not None
                    ]
                    result["zone_matched_approval_rate"] = (
                        sum(zm_apr) / len(zm_apr) if zm_apr else None
                    )
                else:
                    result["zone_matched_approval_rate"] = None
                # Zone-matched appeal rate: appeal rate for same-zone similar apps only
                if "dev_appealed" in cols:
                    zm_ap = [
                        int(d["dev_appealed"])
                        for d in zm_match_dicts
                        if d.get("dev_appealed") is not None
                    ]
                    result["zone_matched_appeal_rate"] = (
                        sum(zm_ap) / len(zm_ap) if zm_ap else None
                    )
                else:
                    result["zone_matched_appeal_rate"] = None

            else:
                result["zone_matched_n_similar"] = 0
                result["zone_matched_approval_rate"] = None
                result["zone_matched_appeal_rate"] = None

        return DescriptionSimilarity.model_validate(result)

    except Exception:  # noqa: BLE001
        return None
