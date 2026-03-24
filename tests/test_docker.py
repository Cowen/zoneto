"""Smoke tests for Docker configuration files (no Docker required)."""

from pathlib import Path


def test_dockerfile_exists() -> None:
    assert Path("Dockerfile").exists(), "Dockerfile must exist"


def test_dockerignore_exists() -> None:
    assert Path(".dockerignore").exists(), ".dockerignore must exist"


def test_dockerfile_exposes_8000() -> None:
    content = Path("Dockerfile").read_text()
    assert "EXPOSE 8000" in content, "Dockerfile must expose port 8000"


def test_dockerfile_runs_zoneto_serve() -> None:
    content = Path("Dockerfile").read_text()
    assert "zoneto" in content and "serve" in content, (
        "Dockerfile CMD must run 'zoneto serve'"
    )


def test_dockerignore_excludes_tests() -> None:
    content = Path(".dockerignore").read_text()
    assert "tests/" in content, ".dockerignore must exclude tests/"


def test_dockerignore_excludes_raw_data_but_keeps_enriched() -> None:
    content = Path(".dockerignore").read_text()
    assert "data/" in content, ".dockerignore must exclude raw data/"
    assert "!data/enriched/" in content, ".dockerignore must keep data/enriched/"
    assert "!data/scores/" in content, ".dockerignore must keep data/scores/"
