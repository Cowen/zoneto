"""Regenerate tests/fixtures/model_baselines.json from current enriched data.

The survival model (dev_days_to_decision) is the only served predictive model, so
this snapshots its cross-validated Harrell's concordance index. The structured
classifier/regressor models were deleted for failing the quality bar.
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path

from zoneto.analytics.features import DEV_CAT_COLS, DEV_NUM_COLS
from zoneto.analytics.train import evaluate_survival

ENRICHED_DIR = Path("data/enriched")
BASELINES_PATH = Path("tests/fixtures/model_baselines.json")


def main() -> None:
    baselines: dict[str, object] = {
        "_meta": {
            "updated": datetime.date.today().isoformat(),
            "notes": (
                "Survival model (dev_days_to_decision) is the only served "
                "predictive model."
            ),
        },
    }

    enriched_path = ENRICHED_DIR / "dev_applications.parquet"
    if not enriched_path.exists():
        print(f"SKIP dev_days_to_decision: {enriched_path} not found")
    else:
        print("Evaluating dev_days_to_decision ...", end=" ", flush=True)
        metrics = evaluate_survival(
            enriched_path,
            time_col="dev_days_observed",
            event_col="dev_decision_event",
            cat_cols=DEV_CAT_COLS,
            num_cols=DEV_NUM_COLS,
        )
        snapshot = {
            "concordance_index_mean": round(
                float(metrics["concordance_index_mean"]), 4
            )
        }
        baselines["dev_days_to_decision"] = snapshot
        print("done", snapshot)

    BASELINES_PATH.write_text(json.dumps(baselines, indent=2) + "\n")
    print(f"\nWrote {BASELINES_PATH}")


if __name__ == "__main__":
    main()
