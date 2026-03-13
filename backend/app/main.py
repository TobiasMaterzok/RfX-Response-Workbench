from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routers.cases import router as cases_router
from app.api.routers.dev import router as dev_router
from app.api.routers.health import router as health_router
from app.api.routers.pipeline import router as pipeline_router
from app.api.routers.product_truth import router as product_truth_router
from app.api.routers.session import router as session_router
from app.config import Settings, get_settings
from app.db import assert_database_schema_current
from app.exceptions import ConfigurationFailure, RfxError, ScopeViolation, ValidationFailure
from app.services.container import build_container


def create_app(*, settings: Settings | None = None, container=None) -> FastAPI:
    effective_settings = settings or get_settings()
    app = FastAPI(title="RfX RAG Expert")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["Content-Disposition"],
    )
    app.state.container = container or build_container(effective_settings)

    @app.on_event("startup")
    async def validate_database_schema() -> None:
        with app.state.container.session_factory() as session:
            assert_database_schema_current(session)

    @app.exception_handler(ValidationFailure)
    async def validation_error_handler(request: Request, exc: ValidationFailure) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(ScopeViolation)
    async def scope_error_handler(request: Request, exc: ScopeViolation) -> JSONResponse:
        return JSONResponse(status_code=403, content={"detail": str(exc)})

    @app.exception_handler(ConfigurationFailure)
    async def configuration_error_handler(
        request: Request, exc: ConfigurationFailure
    ) -> JSONResponse:
        return JSONResponse(status_code=503, content={"detail": str(exc)})

    @app.exception_handler(RfxError)
    async def rfx_error_handler(request: Request, exc: RfxError) -> JSONResponse:
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    app.include_router(health_router)
    app.include_router(session_router)
    app.include_router(pipeline_router)
    app.include_router(cases_router)
    app.include_router(dev_router)
    app.include_router(product_truth_router)
    return app


app = create_app()
