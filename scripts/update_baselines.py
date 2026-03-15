"""Regenerate tests/fixtures/model_baselines.json from current enriched data."""

from __future__ import annotations

import datetime
import json
from pathlib import Path

from zoneto.analytics.features import (
    COA_CAT_COLS,
    COA_NUM_COLS,
    DEV_CAT_COLS,
    DEV_NUM_COLS,
    PERMIT_CAT_COLS,
    PERMIT_NUM_COLS,
)
from zoneto.analytics.train import evaluate_source

ENRICHED_DIR = Path("data/enriched")
BASELINES_PATH = Path("tests/fixtures/model_baselines.json")

_MEAN_KEYS = {"roc_auc_mean", "brier_score_mean", "avg_precision_mean", "r2_mean", "mae_mean", "rmse_mean"}

_MODELS: list[tuple[str, str, list[str], list[str], bool]] = [
    ("dev_applications_approved", "dev_approved", DEV_CAT_COLS, DEV_NUM_COLS, False),
    ("dev_applications_appealed", "dev_appealed", DEV_CAT_COLS, DEV_NUM_COLS, False),
    ("coa_approved", "coa_approved", COA_CAT_COLS, COA_NUM_COLS, False),
    ("coa_days_to_approval", "coa_days_to_approval", COA_CAT_COLS, COA_NUM_COLS, True),
    ("permit_issuance_days", "permit_issuance_days", PERMIT_CAT_COLS, PERMIT_NUM_COLS, True),
]

_SOURCE_FILE: dict[str, str] = {
    "dev_applications_approved": "dev_applications",
    "dev_applications_appealed": "dev_applications",
    "coa_approved": "coa",
    "coa_days_to_approval": "coa",
    "permit_issuance_days": "permits_cleared",
}

_YEAR_COL: dict[str, str | None] = {
    "dev_applications_approved": "year_submitted",
    "dev_applications_appealed": "year_submitted",
    "coa_approved": "year_submitted",
    "coa_days_to_approval": "year_submitted",
    "permit_issuance_days": None,
}


def main() -> None:
    baselines: dict[str, object] = {
        "_meta": {"updated": datetime.date.today().isoformat()},
    }

    for model_name, label_col, cat_cols, num_cols, regressor in _MODELS:
        source = _SOURCE_FILE[model_name]
        enriched_path = ENRICHED_DIR / f"{source}.parquet"
        if not enriched_path.exists():
            print(f"SKIP {model_name}: {enriched_path} not found")
            continue

        print(f"Evaluating {model_name} ...", end=" ", flush=True)
        metrics = evaluate_source(
            enriched_path,
            label_col,
            cat_cols,
            num_cols,
            regressor=regressor,
            cv=5,
            year_col=_YEAR_COL[model_name],
        )
        snapshot = {k: round(float(v), 4) for k, v in metrics.items() if k in _MEAN_KEYS}
        baselines[model_name] = snapshot
        print("done", snapshot)

    BASELINES_PATH.write_text(json.dumps(baselines, indent=2) + "\n")
    print(f"\nWrote {BASELINES_PATH}")


if __name__ == "__main__":
    main()
