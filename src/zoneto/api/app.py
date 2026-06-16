"""FastAPI application factory for Zoneto serving layer."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

_BERT_MODEL_NAME = "BAAI/bge-small-en-v1.5"


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
        from zoneto.analytics.score import _load_production_ready  # noqa: PLC0415

        app.state.data_dir = resolved_data_dir
        app.state.model_dir = resolved_model_dir
        app.state.production_ready = _load_production_ready(
            resolved_model_dir, default=False
        )
        app.state.ready = True

        # Bylaw index — optional; absent if `zoneto bylaw-index` hasn't been run yet
        bylaw_index_dir = resolved_data_dir / "bylaw_index"
        if (bylaw_index_dir / "chunks.parquet").exists():
            from zoneto.analytics.bylaw_index import BylawIndex  # noqa: PLC0415

            app.state.bylaw_index = BylawIndex(bylaw_index_dir)
        else:
            app.state.bylaw_index = None

        # BERT sentence model — reuse from BylawIndex if loaded, otherwise load
        # standalone when BERT embeddings are present and BylawIndex is absent.
        bert_embeddings_path = (
            resolved_data_dir / "enriched" / "desc_bert_embeddings.npy"
        )
        if bert_embeddings_path.exists():
            if app.state.bylaw_index is not None:
                app.state.bert_model = app.state.bylaw_index.model
            else:
                from sentence_transformers import SentenceTransformer  # noqa: PLC0415

                app.state.bert_model = SentenceTransformer(_BERT_MODEL_NAME)
        else:
            app.state.bert_model = None

        # LLM client — optional; absent when ANTHROPIC_API_KEY is not set
        import os  # noqa: PLC0415

        if os.environ.get("ANTHROPIC_API_KEY"):
            from zoneto.llm.agents import make_narrator_agents  # noqa: PLC0415

            app.state.narrator = make_narrator_agents()
        else:
            app.state.narrator = None

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


def create_app_from_env() -> FastAPI:
    """App factory that reads paths from environment variables.

    Used by uvicorn --reload mode, which requires an import string and cannot
    receive CLI-supplied Path arguments directly.

    Environment variables:
        ZONETO_DATA_DIR   (default: "data")
        ZONETO_MODEL_DIR  (default: "models")
        ZONETO_STATIC_DIR (default: "static")
    """
    import os  # noqa: PLC0415

    return create_app(
        data_dir=Path(os.environ.get("ZONETO_DATA_DIR", "data")),
        model_dir=Path(os.environ.get("ZONETO_MODEL_DIR", "models")),
        static_dir=Path(os.environ.get("ZONETO_STATIC_DIR", "static")),
    )
