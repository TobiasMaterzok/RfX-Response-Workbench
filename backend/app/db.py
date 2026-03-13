from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings, get_settings
from app.exceptions import ConfigurationFailure

ROOT_ALEMBIC_UPGRADE_COMMAND = "python -m alembic -c backend/alembic.ini upgrade head"
WINDOWS_LOCAL_SETUP_DOC = "docs/windows-local-setup.md"


def build_engine(settings: Settings | None = None):
    effective_settings = settings or get_settings()
    return create_engine(effective_settings.database_url, future=True)


def build_session_factory(settings: Settings | None = None) -> sessionmaker[Session]:
    return sessionmaker(bind=build_engine(settings), autoflush=False, expire_on_commit=False)

BACKEND_ROOT = Path(__file__).resolve().parents[1]
ALEMBIC_INI = BACKEND_ROOT / "alembic.ini"


def schema_upgrade_guidance() -> str:
    return (
        f"Run `{ROOT_ALEMBIC_UPGRADE_COMMAND}` before starting the app. "
        f"On Win11, see `{WINDOWS_LOCAL_SETUP_DOC}`."
    )


@lru_cache(maxsize=1)
def expected_alembic_head() -> str:
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("script_location", str(BACKEND_ROOT / "alembic"))
    script = ScriptDirectory.from_config(config)
    head = script.get_current_head()
    if head is None:
        raise ConfigurationFailure("Could not determine Alembic head from repository migrations.")
    return head


def assert_database_schema_current(session: Session) -> None:
    try:
        current = session.execute(text("select version_num from alembic_version")).scalar_one_or_none()
    except Exception as exc:
        raise ConfigurationFailure(
            f"Database schema metadata is missing. {schema_upgrade_guidance()}"
        ) from exc
    head = expected_alembic_head()
    if current != head:
        raise ConfigurationFailure(
            f"Database schema is at revision {current or 'none'}, expected {head}. "
            + schema_upgrade_guidance()
        )
