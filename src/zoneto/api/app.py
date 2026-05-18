"""FastAPI application factory for Zoneto serving layer."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI


def _load_production_ready(model_dir: Path) -> dict[str, bool]:
    """Load production_ready flags from metrics.json. Returns empty dict if absent."""
    metrics_path = model_dir / "metrics.json"
    if not metrics_path.exists():
        return {}
    with open(metrics_path) as f:
        metrics: dict[str, Any] = json.load(f)
    return {name: bool(m.get("production_ready", False)) for name, m in metrics.items()}


def create_app(
    data_dir: Path | None = None,
    model_dir: Path | None = None,
    static_dir: Path | None = None,
) -> FastAPI:
    """Create and configure the FastAPI application."""
    resolved_data_dir = data_dir or Path("data")
    resolved_model_dir = model_dir or Path("models")
    resolved_static_dir = static_dir or Path("static")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.data_dir = resolved_data_dir
        app.state.model_dir = resolved_model_dir
        app.state.production_ready = _load_production_ready(resolved_model_dir)
        app.state.ready = True

        # Bylaw index — optional; absent if `zoneto bylaw-index` hasn't been run yet
        bylaw_index_dir = resolved_data_dir / "bylaw_index"
        if (bylaw_index_dir / "chunks.parquet").exists():
            from zoneto.analytics.bylaw_index import BylawIndex  # noqa: PLC0415

            app.state.bylaw_index = BylawIndex(bylaw_index_dir)
        else:
            app.state.bylaw_index = None

        # LLM client — optional; absent when ANTHROPIC_API_KEY is not set
        import os  # noqa: PLC0415

        if os.environ.get("ANTHROPIC_API_KEY"):
            from zoneto.api.llm_client import make_llm_client  # noqa: PLC0415

            app.state.llm_client = make_llm_client()
        else:
            app.state.llm_client = None

        yield

    app = FastAPI(title="Zoneto", version="0.1.0", lifespan=lifespan)

    from zoneto.api.routes import (
        router,  # noqa: PLC0415 (deferred to avoid circular import)
    )

    app.include_router(router)

    if resolved_static_dir.exists():
        from fastapi.staticfiles import StaticFiles  # noqa: PLC0415

        app.mount(
            "/",
            StaticFiles(directory=resolved_static_dir, html=True),
            name="static",
        )

    return app
