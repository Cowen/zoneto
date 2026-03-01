from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class CKANConfig(BaseModel):
    """Configuration for a CKAN-based data source."""

    dataset_id: str
    access_mode: Literal["datastore", "bulk_csv"]
    year_start: int = 2015
    year_column: str = "application_date"
