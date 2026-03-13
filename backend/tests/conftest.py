from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app import config as config_module
from app.config import Settings, build_settings, clear_settings_cache
from app.db import expected_alembic_head
from app.main import create_app
from app.models import Base
from app.services.ai import StubAIService
from app.services.container import ServiceContainer
from app.services.identity import ensure_local_identity
from app.services.storage import LocalObjectStorage


@pytest.fixture
def settings(tmp_path: Path):
    return build_settings(
        env_file=None,
        database_url="sqlite+pysqlite:///:memory:",
        storage_root=tmp_path / "storage",
        local_tenant_slug="local-workspace",
        local_tenant_name="Local Workspace",
        local_user_email="local.user@example.test",
        local_user_name="Local Admin",
    )


@pytest.fixture
def session_factory() -> sessionmaker[Session]:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    with engine.begin() as connection:
        connection.execute(text("create table alembic_version (version_num varchar(32) not null)"))
        connection.execute(
            text("insert into alembic_version (version_num) values (:version_num)"),
            {"version_num": expected_alembic_head()},
        )
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


@pytest.fixture
def container(settings: Settings, session_factory: sessionmaker[Session]) -> ServiceContainer:
    container = ServiceContainer(
        settings=settings,
        session_factory=session_factory,
        storage=LocalObjectStorage(settings),
        ai_service=StubAIService(),
    )
    with session_factory() as session:
        ensure_local_identity(session, settings)
        session.commit()
    return container


@pytest.fixture
def session(session_factory: sessionmaker[Session]) -> Session:
    with session_factory() as session:
        yield session


@pytest.fixture
def auth_headers(settings: Settings) -> dict[str, str]:
    return {
        "X-Tenant-Slug": settings.local_tenant_slug,
        "X-User-Email": settings.local_user_email,
    }


@pytest.fixture
def client(container: ServiceContainer, settings: Settings) -> TestClient:
    app = create_app(settings=settings, container=container)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def isolate_settings_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(config_module, "ROOT_ENV_FILE", tmp_path / ".test-env")
    clear_settings_cache()
    yield
    clear_settings_cache()
