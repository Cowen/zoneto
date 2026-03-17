"""AICSource: Source protocol implementation for AIC full application records."""

from __future__ import annotations

from pathlib import Path

import polars as pl


class AICSource:
    """Source that fetches full application records from the AIC ArcGIS FeatureServer.

    Unlike CKANSource, this source reads from the ArcGIS REST API directly —
    providing a live alternative to the retired CKAN dev_applications dataset.
    """

    name: str = "aic_applications"

    def __init__(
        self,
        data_dir: Path = Path("data"),
        *,
        batch_size: int = 200,
    ) -> None:
        self._data_dir = data_dir
        self._batch_size = batch_size

    def fetch(self) -> pl.DataFrame:
        """Fetch all AIC application records from ArcGIS and return as DataFrame.

        Writes Hive-partitioned Parquet to data_dir/aic_applications/ and
        returns the full DataFrame.
        """
        from zoneto.sources.aic import fetch_aic_applications  # noqa: PLC0415

        fetch_aic_applications(self._data_dir, batch_size=self._batch_size)
        return pl.read_parquet(
            self._data_dir / "aic_applications",
            hive_partitioning=True,
        )
