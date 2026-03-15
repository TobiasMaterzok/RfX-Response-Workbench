from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.config import Settings, build_settings
from app.db import assert_database_schema_current, expected_alembic_head
from app.exceptions import ConfigurationFailure
from app.main import create_app
from app.models import Base
from app.services.ai import StubAIService
from app.services.container import ServiceContainer
from app.services.storage import LocalObjectStorage


def stale_settings(tmp_path: Path) -> Settings:
    return build_settings(
        env_file=None,
        database_url=f"sqlite+pysqlite:///{tmp_path / 'stale-schema.db'}",
        storage_root=tmp_path / "storage",
        local_tenant_slug="local-workspace",
        local_tenant_name="Local Workspace",
        local_user_email="local.user@example.test",
        local_user_name="Local Admin",
    )


def stale_session_factory(tmp_path: Path) -> tuple[Settings, sessionmaker]:
    settings = stale_settings(tmp_path)
    engine = create_engine(settings.database_url, future=True)
    Base.metadata.create_all(bind=engine)
    with engine.begin() as connection:
        connection.execute(text("create table alembic_version (version_num varchar(32) not null)"))
        connection.execute(
            text("insert into alembic_version (version_num) values (:version_num)"),
            {"version_num": "stale_local_revision"},
        )
    return settings, sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def test_schema_guard_rejects_stale_revision(tmp_path: Path) -> None:
    _settings, factory = stale_session_factory(tmp_path)
    with factory() as session, pytest.raises(
        ConfigurationFailure,
        match="alembic -c backend/alembic.ini upgrade head",
    ):
        assert_database_schema_current(session)


def test_app_startup_fails_closed_on_stale_schema(tmp_path: Path) -> None:
    settings, factory = stale_session_factory(tmp_path)
    container = ServiceContainer(
        settings=settings,
        session_factory=factory,
        storage=LocalObjectStorage(settings),
        ai_service=StubAIService(),
    )
    app = create_app(settings=settings, container=container)
    with pytest.raises(
        ConfigurationFailure,
        match="expected .* Run `python -m alembic -c backend/alembic.ini upgrade head`",
    ), TestClient(app):
        pass


def test_expected_alembic_head_is_available() -> None:
    head = expected_alembic_head()
    assert isinstance(head, str)
    assert head


def test_repo_root_alembic_command_resolves_config_from_any_cwd(repo_root: Path) -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "alembic",
            "-c",
            str(repo_root / "backend" / "alembic.ini"),
            "heads",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
