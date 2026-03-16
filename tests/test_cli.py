from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
from typer.testing import CliRunner

from zoneto.cli import app

runner = CliRunner()


def test_sync_unknown_source_exits_with_code_1() -> None:
    """Providing an unknown --source name exits with code 1."""
    result = runner.invoke(app, ["sync", "--source", "nonexistent"])
    assert result.exit_code == 1
    assert "Unknown source" in result.output


def test_status_shows_no_data_before_any_sync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """status prints a table with 'no data' for all sources before sync is run."""
    monkeypatch.setattr("zoneto.cli.DATA_DIR", tmp_path)
    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0
    assert "no data" in result.output


def test_sync_writes_data_to_disk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """sync calls fetch, writes Parquet data, and exits 0."""
    fake_df = pl.DataFrame(
        {
            "year": pl.Series([2024], dtype=pl.Int32),
            "permit_no": ["A001"],
            "source_name": ["fake"],
        }
    )
    mock_source = MagicMock()
    mock_source.fetch.return_value = fake_df

    monkeypatch.setattr("zoneto.cli.SOURCES", {"fake": mock_source})
    monkeypatch.setattr("zoneto.cli.DATA_DIR", tmp_path)

    result = runner.invoke(app, ["sync"])
    assert result.exit_code == 0
    assert (tmp_path / "fake").exists()


def test_sync_continues_after_source_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An exception from one source is printed but other sources still run."""
    good_df = pl.DataFrame(
        {
            "year": pl.Series([2024], dtype=pl.Int32),
            "permit_no": ["A001"],
            "source_name": ["good"],
        }
    )
    bad_source = MagicMock()
    bad_source.fetch.side_effect = RuntimeError("network failure")
    good_source = MagicMock()
    good_source.fetch.return_value = good_df

    monkeypatch.setattr("zoneto.cli.SOURCES", {"bad": bad_source, "good": good_source})
    monkeypatch.setattr("zoneto.cli.DATA_DIR", tmp_path)

    result = runner.invoke(app, ["sync"])
    assert result.exit_code == 0  # does not abort on error
    assert good_source.fetch.called  # good source was still attempted


def test_aic_command_calls_fetch_aic_decisions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """zoneto aic calls fetch_aic_decisions and exits 0."""
    monkeypatch.setattr("zoneto.cli.DATA_DIR", tmp_path)
    mock_fetch = MagicMock(return_value=42)
    monkeypatch.setattr("zoneto.cli.fetch_aic_decisions_arcgis", mock_fetch)

    result = runner.invoke(app, ["aic"])

    assert result.exit_code == 0
    assert mock_fetch.called
    assert "42" in result.output


def test_enrich_no_fetch_aic_skips_aic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """zoneto enrich --no-fetch-aic skips AIC fetch."""
    monkeypatch.setattr("zoneto.cli.DATA_DIR", tmp_path)
    mock_fetch_aic = MagicMock(return_value=0)
    monkeypatch.setattr("zoneto.cli.fetch_aic_decisions_arcgis", mock_fetch_aic)
    mock_enrich_dev = MagicMock(return_value=0)
    monkeypatch.setattr("zoneto.cli.enrich_dev", mock_enrich_dev)
    mock_enrich_coa = MagicMock(return_value=0)
    monkeypatch.setattr("zoneto.cli.enrich_coa", mock_enrich_coa)
    mock_enrich_permits = MagicMock(return_value=0)
    monkeypatch.setattr("zoneto.cli.enrich_permits", mock_enrich_permits)
    monkeypatch.setattr("zoneto.cli.fetch_reference", MagicMock(return_value=None))

    result = runner.invoke(app, ["enrich", "--no-fetch-aic"])

    assert result.exit_code == 0
    assert not mock_fetch_aic.called


def test_enrich_fetch_aic_default_calls_aic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """zoneto enrich (default) calls fetch_aic_decisions."""
    monkeypatch.setattr("zoneto.cli.DATA_DIR", tmp_path)
    mock_fetch_aic = MagicMock(return_value=5)
    monkeypatch.setattr("zoneto.cli.fetch_aic_decisions_arcgis", mock_fetch_aic)
    mock_enrich_dev = MagicMock(return_value=0)
    monkeypatch.setattr("zoneto.cli.enrich_dev", mock_enrich_dev)
    mock_enrich_coa = MagicMock(return_value=0)
    monkeypatch.setattr("zoneto.cli.enrich_coa", mock_enrich_coa)
    mock_enrich_permits = MagicMock(return_value=0)
    monkeypatch.setattr("zoneto.cli.enrich_permits", mock_enrich_permits)
    monkeypatch.setattr("zoneto.cli.fetch_reference", MagicMock(return_value=None))

    result = runner.invoke(app, ["enrich"])

    assert result.exit_code == 0
    assert mock_fetch_aic.called
