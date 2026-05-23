"""Outcome labels and OLT decision matching."""

from __future__ import annotations

import difflib
from pathlib import Path

import polars as pl

# ---------------------------------------------------------------------------
# Status label sets (lowercase after strip)
# ---------------------------------------------------------------------------
_DEV_APPROVED_SET: frozenset[str] = frozenset(
    {
        "noac issued",
        "council approved",
        "draft plan approved",
        "final approval completed",
        "omb approved",
        "approved",
        "omb partially approved",
    }
)
_DEV_REFUSED_SET: frozenset[str] = frozenset({"refused", "omb refused"})
_DEV_APPEALED_SET: frozenset[str] = frozenset(
    {
        "omb appeal",
        "appeal received",
        "omb approved",
        "omb refused",
        "omb partially approved",
    }
)
_DEV_ACTIVE_SET: frozenset[str] = frozenset(
    {
        "under review",
        "on hold",
        "referred",
        "deferred",
        "information requested",
    }
)
_COA_APPROVED_SET: frozenset[str] = frozenset(
    {
        "approved",
        "conditional approval",
        "approved with conditions",
        "approved on condition",
    }
)
_COA_REFUSED_SET: frozenset[str] = frozenset({"refused"})
_DEV_SURVIVAL_TYPES: frozenset[str] = frozenset({"OZ", "SA"})
_DEV_DAYS_CAP = 3650


def _label_from_sets(
    val: str | None,
    positive_set: frozenset[str],
    negative_set: frozenset[str],
) -> int | None:
    """Map a status string to 1/0/null via two frozensets.

    Returns 1 if val (stripped, lowercased) is in positive_set,
    0 if in negative_set, None otherwise.
    """
    if val is None:
        return None
    v = val.strip().lower()
    if v in positive_set:
        return 1
    if v in negative_set:
        return 0
    return None


def match_olt_to_dev(
    dev_df: pl.DataFrame,
    data_dir: Path,
    *,
    confidence_threshold: float = 0.75,
) -> pl.DataFrame:
    """Fuzzy-match OLT decisions to dev_applications via address similarity.

    Reads data_dir/reference/olt_decisions.parquet (if present).
    For each dev_application, finds the OLT case with the highest address
    similarity score above `confidence_threshold`.

    Address similarity uses difflib.SequenceMatcher on normalized address
    strings (street_num + street_name vs OLT address).

    Adds columns: olt_case_number (String|null), olt_outcome (String|null),
    olt_decision_date (Date|null).

    Returns enriched DataFrame. If olt_decisions.parquet is absent,
    adds the three columns as all-null and returns unchanged.
    """
    null_cols = [
        pl.lit(None, dtype=pl.String).alias("olt_case_number"),
        pl.lit(None, dtype=pl.String).alias("olt_outcome"),
        pl.lit(None, dtype=pl.Date).alias("olt_decision_date"),
    ]

    olt_path = data_dir / "reference" / "olt_decisions.parquet"
    if not olt_path.exists():
        return dev_df.with_columns(null_cols)

    olt_df = pl.read_parquet(olt_path)
    olt_addresses = [
        a.lower().split(",")[0].strip()
        for a in olt_df["address"].fill_null("").to_list()
    ]
    olt_cases = olt_df["case_number"].fill_null("").to_list()
    olt_outcomes = olt_df["outcome"].fill_null("").to_list()
    # olt_dates are pl.Date objects; None values handled by fill_null with a sentinel
    olt_dates = olt_df["decision_date"].to_list()

    # Pre-index OLT addresses by street number (first token) for O(1) lookup.
    # Maps street_num -> list of indices into olt_addresses where
    # that street_num appears.
    olt_by_street: dict[str, list[int]] = {}
    for i, addr in enumerate(olt_addresses):
        street_num_token = addr.split()[0] if addr else ""
        if street_num_token:
            if street_num_token not in olt_by_street:
                olt_by_street[street_num_token] = []
            olt_by_street[street_num_token].append(i)

    # Fallback list: all indices when no street number match exists
    all_indices = list(range(len(olt_addresses)))

    matched_cases: list[str | None] = []
    matched_outcomes: list[str | None] = []
    matched_dates: list[object | None] = []  # will be date | None

    for row in dev_df.iter_rows(named=True):
        street_num = str(row.get("street_num") or "").strip()
        street_name = str(row.get("street_name") or "").strip()
        dev_address = f"{street_num} {street_name}".lower().strip()

        if not dev_address:
            matched_cases.append(None)
            matched_outcomes.append(None)
            matched_dates.append(None)
            continue

        # Determine which OLT addresses to compare against.
        # Prefer matching by street number; fall back to all addresses if no match.
        street_num_token = street_num if street_num else ""
        candidate_indices = olt_by_street.get(street_num_token, all_indices)

        best_ratio = 0.0
        best_idx = -1
        for i in candidate_indices:
            ratio = difflib.SequenceMatcher(None, dev_address, olt_addresses[i]).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_idx = i

        if best_ratio >= confidence_threshold and best_idx >= 0:
            matched_cases.append(olt_cases[best_idx])
            matched_outcomes.append(olt_outcomes[best_idx])
            matched_dates.append(olt_dates[best_idx])
        else:
            matched_cases.append(None)
            matched_outcomes.append(None)
            matched_dates.append(None)

    return dev_df.with_columns(
        [
            pl.Series("olt_case_number", matched_cases, dtype=pl.String),
            pl.Series("olt_outcome", matched_outcomes, dtype=pl.String),
            pl.Series("olt_decision_date", matched_dates, dtype=pl.Date),
        ]
    )


def _add_section37_features(df: pl.DataFrame, data_dir: Path) -> pl.DataFrame:
    """Join Section 37 community benefits to dev_applications by address match.

    Reads data/reference/section37.parquet. Normalizes addresses and does an
    exact match on the lowercased, stripped address string. Multiple S.37
    records for the same address are aggregated: monetary values are summed
    and benefit texts are joined with "; ".

    Adds columns:
    - s37_monetary_value (Float64 | null): total CAD value of S.37 agreements
    - s37_benefit_text (String | null): concatenated Community Benefits text

    If section37.parquet is absent or the DataFrame lacks an address column,
    both columns are null for all rows.
    """
    null_cols = [
        pl.lit(None, dtype=pl.Float64).alias("s37_monetary_value"),
        pl.lit(None, dtype=pl.String).alias("s37_benefit_text"),
    ]

    s37_path = data_dir / "reference" / "section37.parquet"
    if not s37_path.exists():
        return df.with_columns(null_cols)

    if "address" not in df.columns:
        return df.with_columns(null_cols)

    s37 = pl.read_parquet(s37_path)
    if "location" not in s37.columns:
        return df.with_columns(null_cols)

    # Normalize addresses: lowercase, strip, collapse internal whitespace
    def _norm(s: str | None) -> str:
        if not s:
            return ""
        return " ".join(s.lower().split())

    s37_locations = [_norm(loc) for loc in s37["location"].fill_null("").to_list()]
    s37_monetary = s37["monetary_value"].to_list() if "monetary_value" in s37.columns else [None] * len(s37)
    s37_benefits = (
        s37["community_benefits"].fill_null("").to_list()
        if "community_benefits" in s37.columns
        else [""] * len(s37)
    )

    # Build index: normalized location → list of (monetary_value, benefit_text)
    loc_index: dict[str, list[tuple[float | None, str]]] = {}
    for loc, mon, ben in zip(s37_locations, s37_monetary, s37_benefits):
        if loc:
            if loc not in loc_index:
                loc_index[loc] = []
            loc_index[loc].append((mon, ben))

    dev_addresses = [_norm(a) for a in df["address"].fill_null("").to_list()]

    s37_monetary_vals: list[float | None] = []
    s37_benefit_texts: list[str | None] = []

    for addr in dev_addresses:
        matches = loc_index.get(addr, [])
        if not matches:
            s37_monetary_vals.append(None)
            s37_benefit_texts.append(None)
        else:
            total_monetary: float | None = None
            for mon, _ in matches:
                if mon is not None:
                    total_monetary = (total_monetary or 0.0) + mon
            benefits = [b for _, b in matches if b]
            benefit_text = "; ".join(benefits) if benefits else None
            s37_monetary_vals.append(total_monetary)
            s37_benefit_texts.append(benefit_text)

    return df.with_columns(
        [
            pl.Series("s37_monetary_value", s37_monetary_vals, dtype=pl.Float64),
            pl.Series("s37_benefit_text", s37_benefit_texts, dtype=pl.String),
        ]
    )
