from __future__ import annotations

import pytest
from pydantic import ValidationError

from zoneto.models import CKANConfig


def test_datastore_defaults() -> None:
    config = CKANConfig(dataset_id="my-dataset", access_mode="datastore")
    assert config.dataset_id == "my-dataset"
    assert config.access_mode == "datastore"
    assert config.year_start == 2015


def test_bulk_csv_custom_year() -> None:
    config = CKANConfig(dataset_id="my-dataset", access_mode="bulk_csv", year_start=2020)
    assert config.year_start == 2020


def test_invalid_access_mode_raises() -> None:
    with pytest.raises(ValidationError):
        CKANConfig(dataset_id="my-dataset", access_mode="invalid")  # type: ignore[arg-type]


def test_missing_access_mode_raises() -> None:
    with pytest.raises(ValidationError):
        CKANConfig(dataset_id="my-dataset")  # type: ignore[call-arg]
