from __future__ import annotations

from typer.testing import CliRunner

from zoneto.cli import app

runner = CliRunner()


def test_sync_stub() -> None:
    result = runner.invoke(app, ["sync"])
    assert result.exit_code == 0
    assert "sync: not yet implemented" in result.output


def test_status_stub() -> None:
    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0
    assert "status: not yet implemented" in result.output
