"""Enrichment pipeline: fetch reference data, label outcomes, spatial join."""

from __future__ import annotations

import logging
from datetime import date as _date
from pathlib import Path

import polars as pl

from zoneto.analytics.extract import _FSI_RES
from zoneto.analytics.labels import (
    _DEV_ACTIVE_SET,
    _DEV_APPEALED_SET,
    _DEV_APPROVED_SET,
    _DEV_DAYS_CAP,
    _DEV_REFUSED_SET,
    _DEV_SURVIVAL_TYPES,
    _label_from_sets,
)
from zoneto.analytics.labels import (
    match_olt_to_dev as match_olt_to_dev,  # noqa: F401
)
from zoneto.analytics.nlp import extract_text_features
from zoneto.analytics.reference import fetch_reference as fetch_reference  # noqa: F401
from zoneto.analytics.spatial import enrich_ward_features, spatial_join_dev
from zoneto.analytics.use_classifier import classify_use

logger = logging.getLogger(__name__)


def _compute_ward_appeal_rate_3y(df: pl.DataFrame) -> pl.DataFrame:
    """Add ward_appeal_rate_3y: rolling 3-year appeal rate per ward.

    For each row, computes the appeal rate in the same ward using only
    OZ/SA rows from the 3 years strictly before the row's year_submitted.
    Returns null when no prior data exists for the ward.
    """
    # Build per-ward-per-year appeal stats from labeled OZ/SA rows
    labeled = df.filter(pl.col("dev_appealed").is_not_null()).select(
        ["ward_number", "year_submitted", "dev_appealed"]
    )
    if labeled.is_empty():
        return df.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("ward_appeal_rate_3y")
        )

    ward_year_stats = labeled.group_by(["ward_number", "year_submitted"]).agg(
        pl.col("dev_appealed").sum().alias("appeals"),
        pl.col("dev_appealed").count().alias("total"),
    )

    # For each unique (ward, year) in the data, compute rolling 3-year rate
    unique_ward_years = df.select(["ward_number", "year_submitted"]).unique()
    # Cross-join with ward_year_stats and filter to prior 3 years
    rates = (
        unique_ward_years.join(ward_year_stats, on="ward_number", suffix="_hist")
        .filter(
            (pl.col("year_submitted_hist") < pl.col("year_submitted"))
            & (pl.col("year_submitted_hist") >= pl.col("year_submitted") - 3)
        )
        .group_by(["ward_number", "year_submitted"])
        .agg(
            (pl.col("appeals").sum() / pl.col("total").sum()).alias(
                "ward_appeal_rate_3y"
            ),
        )
    )

    return df.join(rates, on=["ward_number", "year_submitted"], how="left")


def enrich_coa(data_dir: Path = Path("data")) -> int:
    """Enrich COA parquet with ward features; write data/enriched/coa.parquet.

    No outcome labels are computed: the coa_approved and coa_days_to_approval
    models were deleted (AUC 0.535 / R² < 0 — no signal in structured COA fields).

    Returns row count written.
    """
    df = pl.read_parquet(data_dir / "coa", hive_partitioning=True)

    # Deduplicate on application key (consolidated CSV may overlap individual year CSVs)
    if "reference_file" in df.columns:
        df = df.unique(subset=["reference_file"], keep="first", maintain_order=True)

    # Rename ward → ward_number (cast to str for consistency)
    df = df.rename({"ward": "ward_number"}).with_columns(
        pl.col("ward_number").cast(pl.Utf8)
    )

    # year_submitted from in_date
    df = df.with_columns(
        pl.col("in_date").dt.year().cast(pl.Int32).alias("year_submitted")
    )

    # Enrich with ward profiles
    df = enrich_ward_features(df, data_dir)

    out = data_dir / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "coa.parquet")
    return len(df)


def enrich_dev(data_dir: Path = Path("data")) -> int:
    """Enrich dev_applications parquet with spatial features and outcome labels.

    Writes data/enriched/dev_applications.parquet. Returns row count.
    """
    df = pl.read_parquet(data_dir / "dev_applications", hive_partitioning=True)

    # --- AIC application records: prefer over CKAN for matching folderrsn ---
    # If data/aic_applications/ exists (from 'zoneto aic --full'), merge it:
    # - AIC records override CKAN records for the same folderrsn
    # - AIC-only records (not in CKAN) are added to the dataset
    aic_apps_path = data_dir / "aic_applications"
    if aic_apps_path.exists() and any(aic_apps_path.rglob("*.parquet")):
        aic_df = pl.read_parquet(aic_apps_path, hive_partitioning=True)
        # Ensure folderrsn is String for join
        if "folderrsn" in aic_df.columns:
            aic_df = aic_df.with_columns(pl.col("folderrsn").cast(pl.String))
        if "folderrsn" in df.columns:
            df = df.with_columns(pl.col("folderrsn").cast(pl.String))
        aic_rsns = (
            set(aic_df["folderrsn"].to_list())
            if "folderrsn" in aic_df.columns
            else set()
        )

        # AIC fields not in CKAN schema — these are safely added without displacing
        # any existing CKAN data (lat/lon, milestone info, scraped_at).
        # Shared fields (application_type, year, source_name) stay as CKAN values.
        aic_extra_cols = [c for c in aic_df.columns if c not in df.columns]

        # 1. CKAN rows with no AIC match — pad with null AIC columns
        ckan_only = df.filter(~pl.col("folderrsn").is_in(list(aic_rsns)))
        if aic_extra_cols:
            ckan_only = ckan_only.with_columns(
                [pl.lit(None).cast(aic_df[c].dtype).alias(c) for c in aic_extra_cols]
            )

        # 2. CKAN rows that AIC has — enrich in-place with AIC-only fields.
        #    CKAN fields (status, description, address, etc.) are preserved as-is.
        #    Deduplicate AIC by folderrsn first (keep most recent milestone) so the
        #    left join doesn't fan out CKAN rows when AIC has repeated entries.
        ckan_matched = df.filter(pl.col("folderrsn").is_in(list(aic_rsns)))
        if aic_extra_cols:
            aic_deduped = (
                aic_df.sort("latest_milestone_date", descending=True, nulls_last=True)
                if "latest_milestone_date" in aic_df.columns
                else aic_df
            ).unique(subset=["folderrsn"], keep="first")
            aic_for_join = aic_deduped.select(["folderrsn"] + aic_extra_cols)
            ckan_matched = ckan_matched.join(aic_for_join, on="folderrsn", how="left")
        else:
            ckan_matched = ckan_matched.with_columns(
                [pl.lit(None).cast(pl.String).alias(c) for c in []]
            )

        # 3. AIC rows not in CKAN at all — add with null CKAN fields
        ckan_rsns = set(df["folderrsn"].to_list())
        aic_only_rsns = aic_rsns - ckan_rsns
        parts = [ckan_only, ckan_matched]
        if aic_only_rsns:
            aic_only = aic_df.filter(pl.col("folderrsn").is_in(list(aic_only_rsns)))
            for col in df.columns:
                if col not in aic_only.columns:
                    aic_only = aic_only.with_columns(
                        pl.lit(None).cast(df[col].dtype).alias(col)
                    )
            for col in aic_extra_cols:
                if col not in aic_only.columns:
                    aic_only = aic_only.with_columns(
                        pl.lit(None).cast(aic_df[col].dtype).alias(col)
                    )
            parts.append(aic_only)

        df = pl.concat(parts, how="diagonal")
        logger.info(
            "enrich_dev: enriched %d CKAN rows with AIC fields, added %d AIC-only rows"
            " (%d total)",
            len(ckan_matched),
            len(aic_only_rsns),
            len(df),
        )

    # year_submitted and _submitted_date from date_submitted
    # (may be Date/Datetime or String like "2022-08-09T00:00:00")
    _ds_dtype = df["date_submitted"].dtype
    if _ds_dtype.is_temporal():
        _year_expr = pl.col("date_submitted").dt.year().cast(pl.Int32)
        _date_expr = pl.col("date_submitted").cast(pl.Date)
    else:
        # String like "2022-08-09T00:00:00" — slice to date part
        _year_expr = (
            pl.col("date_submitted").str.slice(0, 4).cast(pl.Int32, strict=False)
        )
        _date_expr = pl.col("date_submitted").str.slice(0, 10).str.to_date(strict=False)
    df = df.with_columns(
        _year_expr.alias("year_submitted"),
        _date_expr.alias("_submitted_date"),
    )

    # has_community_meeting
    df = df.with_columns(
        pl.col("community_meeting_date")
        .is_not_null()
        .cast(pl.Int8)
        .alias("has_community_meeting"),
    )

    # Spatial enrichment (monkeypatchable)
    df = spatial_join_dev(df, data_dir)

    df = df.with_columns(
        pl.col("status")
        .map_elements(
            lambda v: _label_from_sets(v, _DEV_APPROVED_SET, _DEV_REFUSED_SET),
            return_dtype=pl.Int8,
        )
        .alias("dev_approved"),
        # dev_appealed: 1=appeal filed, 0=closed without appeal, null=active/non-OZ/SA.
        # Restricted to OZ+SA only — these are the types with AIC decision milestones.
        # Covers ALL closed OZ/SA applications (not just explicitly-approved ones) so
        # the training base rate reflects the true Toronto appeal rate (~15-25%),
        # not a selection-biased 50/50 split.
        pl.when(~pl.col("application_type").is_in(list(_DEV_SURVIVAL_TYPES)))
        .then(None)
        .when(pl.col("status").is_null())
        .then(None)
        .when(
            pl.col("status")
            .str.strip_chars()
            .str.to_lowercase()
            .is_in(list(_DEV_ACTIVE_SET))
        )
        .then(None)
        .when(
            pl.col("status")
            .str.strip_chars()
            .str.to_lowercase()
            .is_in(list(_DEV_APPEALED_SET))
        )
        .then(pl.lit(1, dtype=pl.Int8))
        .otherwise(pl.lit(0, dtype=pl.Int8))
        .alias("dev_appealed"),
    )

    # is_active: 1 for pending applications (not an ML feature — output flag only)
    df = df.with_columns(
        pl.col("status")
        .map_elements(
            lambda v: (
                1 if (v is not None and v.strip().lower() in _DEV_ACTIVE_SET) else 0
            ),
            return_dtype=pl.Int8,
        )
        .alias("is_active")
    )

    # ward_appeal_rate_3y: rolling 3-year appeal rate per ward using only prior years.
    # Avoids temporal leakage — each row only sees data from years before its own.
    # Uses OZ+SA rows with non-null dev_appealed for the base rate calculation.
    df = _compute_ward_appeal_rate_3y(df)

    # has_parent_application: SA/CD linked to a parent OZ implies upstream rezoning
    # decided
    if "parent_folder_number" in df.columns:
        df = df.with_columns(
            pl.col("parent_folder_number")
            .is_not_null()
            .cast(pl.Int8)
            .alias("has_parent_application")
        )
    else:
        df = df.with_columns(pl.lit(0, dtype=pl.Int8).alias("has_parent_application"))

    # postal_fsa: neighbourhood proxy (first 3 chars of postal code e.g. "M5V")
    if "postal" in df.columns:
        df = df.with_columns(pl.col("postal").str.slice(0, 3).alias("postal_fsa"))
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.String).alias("postal_fsa"))

    # Enrich with ward profiles
    df = enrich_ward_features(df, data_dir)

    # --- AIC decision date join and survival labels ---
    aic_path = data_dir / "reference" / "aic_decisions.parquet"
    if aic_path.exists() and "folderrsn" in df.columns:
        aic = pl.read_parquet(aic_path).select(["folderrsn", "decision_date"])
        df = df.join(aic, on="folderrsn", how="left")
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.Date).alias("decision_date"))

    _today = _date.today()

    # dev_days_to_decision: days from date_submitted to decision_date (OZ/SA only)
    # Intermediate column _raw_days used to simplify capping logic.
    df = df.with_columns(
        pl.when(
            pl.col("application_type").is_in(list(_DEV_SURVIVAL_TYPES))
            & pl.col("decision_date").is_not_null()
        )
        .then(
            (pl.col("decision_date") - pl.col("_submitted_date"))
            .dt.total_days()
            .cast(pl.Int32)
        )
        .otherwise(None)
        .alias("_raw_days")
    )
    df = df.with_columns(
        pl.when(
            pl.col("application_type").is_in(list(_DEV_SURVIVAL_TYPES))
            & pl.col("decision_date").is_not_null()
            & (pl.col("_raw_days") <= _DEV_DAYS_CAP)
        )
        .then(pl.col("_raw_days"))
        .otherwise(None)
        .cast(pl.Int32)
        .alias("dev_days_to_decision")
    )

    # dev_decision_event: 1 = has decision, 0 = active, null = not OZ/SA
    df = df.with_columns(
        pl.when(~pl.col("application_type").is_in(list(_DEV_SURVIVAL_TYPES)))
        .then(None)
        .when(pl.col("decision_date").is_not_null())
        .then(pl.lit(1, dtype=pl.Int8))
        .when(pl.col("is_active") == 1)
        .then(pl.lit(0, dtype=pl.Int8))
        .otherwise(None)
        .alias("dev_decision_event")
    )

    # dev_days_observed: actual days to decision for events (uncapped, for survival
    # model time axis); today-submitted for censored; null for non-OZ/SA.
    df = df.with_columns(
        pl.when(pl.col("dev_decision_event") == 1)
        .then(pl.col("_raw_days"))  # use uncapped actual time for survival model
        .when(pl.col("dev_decision_event") == 0)
        .then(
            (pl.lit(_today) - pl.col("_submitted_date")).dt.total_days().cast(pl.Int32)
        )
        .otherwise(None)
        .alias("dev_days_observed")
    )
    df = df.drop("_raw_days", "_submitted_date")

    # proposed_storeys: extract storey count from description (e.g. "12 storey",
    # "28-storey", "3 storeys") — regex captures first match, case-insensitive.
    if "description" in df.columns:
        df = df.with_columns(
            pl.col("description")
            .str.extract(r"(?i)(\d+)\s*-?\s*store?ys?", 1)
            .cast(pl.Int32, strict=False)
            .alias("proposed_storeys")
        )
        # proposed_units: extract unit count (e.g. "551 units", "186 dwelling units")
        df = df.with_columns(
            pl.col("description")
            .str.extract(r"(?i)(\d+)\s+(?:dwelling\s+)?units?", 1)
            .cast(pl.Int32, strict=False)
            .alias("proposed_units")
        )
        # proposed_fsi: extract a stated floor space index ("an FSI of 5.0",
        # "density of 3.2 times the lot"). Reuses the extract.py patterns (DRY);
        # coalesce takes the first pattern that matches.
        df = df.with_columns(
            pl.coalesce(
                [pl.col("description").str.extract(p.pattern, 1) for p in _FSI_RES]
            )
            .cast(pl.Float64, strict=False)
            .alias("proposed_fsi")
        )
    else:
        df = df.with_columns(
            pl.lit(None, dtype=pl.Int32).alias("proposed_storeys"),
            pl.lit(None, dtype=pl.Int32).alias("proposed_units"),
            pl.lit(None, dtype=pl.Float64).alias("proposed_fsi"),
        )

    # unit_excess_ratio: proposed_units / zoning_max_units.
    # Values > 1.0 mean the proposal exceeds zoning — strong appeal signal.
    df = df.with_columns(
        pl.when(
            pl.col("proposed_units").is_not_null()
            & pl.col("zoning_max_units").is_not_null()
            & (pl.col("zoning_max_units") > 0)
        )
        .then(pl.col("proposed_units") / pl.col("zoning_max_units"))
        .otherwise(None)
        .cast(pl.Float64)
        .alias("unit_excess_ratio")
    )

    # storey_excess_ratio: proposed_storeys / zoning_max_storeys.
    # Per by-law readme, HT_STORIES <= 0 means "no limit" (already nulled
    # in _add_height_feature). Better coverage than unit_excess_ratio.
    df = df.with_columns(
        pl.when(
            pl.col("proposed_storeys").is_not_null()
            & pl.col("zoning_max_storeys").is_not_null()
            & (pl.col("zoning_max_storeys") > 0)
        )
        .then(pl.col("proposed_storeys") / pl.col("zoning_max_storeys"))
        .otherwise(None)
        .cast(pl.Float64)
        .alias("storey_excess_ratio")
    )

    # fsi_excess_ratio: proposed_fsi / zoning_max_density (FSI). FSI is the density
    # regulator for the ~half of sites that have no unit cap (zoning_max_units = -1),
    # so this is the higher-coverage excess signal than unit_excess_ratio.
    df = df.with_columns(
        pl.when(
            pl.col("proposed_fsi").is_not_null()
            & pl.col("zoning_max_density").is_not_null()
            & (pl.col("zoning_max_density") > 0)
        )
        .then(pl.col("proposed_fsi") / pl.col("zoning_max_density"))
        .otherwise(None)
        .cast(pl.Float64)
        .alias("fsi_excess_ratio")
    )

    # is_combined_application: OZ with OPA in description field (case-insensitive)
    if "description" in df.columns:
        df = df.with_columns(
            pl.when(
                (pl.col("application_type") == "OZ")
                & pl.col("description").str.to_uppercase().str.contains("OPA")
            )
            .then(pl.lit(1, dtype=pl.Int8))
            .otherwise(pl.lit(0, dtype=pl.Int8))
            .alias("is_combined_application")
        )
    else:
        df = df.with_columns(pl.lit(0, dtype=pl.Int8).alias("is_combined_application"))

    # proposed_use_category: keyword-based bucket inferred from description.
    # Display field for the UI — not consumed by any model. See use_classifier.
    if "description" in df.columns:
        descs = df["description"].to_list()
        df = df.with_columns(
            pl.Series(
                "proposed_use_category",
                [classify_use(d) for d in descs],
                dtype=pl.Utf8,
            )
        )
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("proposed_use_category"))

    # --- NLP features: TF-IDF + SVD on description column ---
    # NOTE: TF-IDF vocabulary is fit on the full corpus (all applications).
    # This introduces minor vocabulary leakage into temporal CV folds — the IDF
    # weights reflect future documents. The impact is small given max_features=5000
    # cap and SVD compression to 20 dimensions. Future work: fit TF-IDF per year
    # to eliminate leakage entirely, but current approach is acceptable for v1.
    _model_dir = data_dir.parent / "models"
    df, _ = extract_text_features(df, _model_dir)

    out = data_dir / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "dev_applications.parquet")
    return len(df)


def enrich_permits(data_dir: Path = Path("data")) -> int:
    """Normalize the permits_cleared parquet (numeric coercion + application_year).

    The permit_issuance_days outcome model was deleted (R² 0.039 — queue depth, the
    real driver, is not in open data), so no outcome label is computed here.

    Writes data/enriched/permits_cleared.parquet. Returns row count written.
    """
    df = pl.read_parquet(data_dir / "permits_cleared", hive_partitioning=True)

    # Coerce string numeric columns to Float64 (remove comma thousands separators)
    _str_num_cols = ["est_const_cost", "dwelling_units_created", "dwelling_units_lost"]
    for _col in _str_num_cols:
        if _col in df.columns and df[_col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(_col).str.replace_all(",", "").cast(pl.Float64, strict=False)
            )

    df = df.with_columns(
        pl.col("application_date").dt.year().cast(pl.Int32).alias("application_year")
    )

    out = data_dir / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "permits_cleared.parquet")
    return len(df)
