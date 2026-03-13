from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Final, cast

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
ROOT_ENV_FILE: Final[Path] = REPO_ROOT / ".env"
USE_ROOT_ENV_FILE: Final[object] = object()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RFX_", extra="ignore")

    database_url: str = Field(
        default="postgresql+psycopg://postgres:postgres@localhost:5432/rfx_rag_expert"
    )
    storage_root: Path = Field(default=Path("./storage"))
    openai_api_key: str | None = Field(default=None, validation_alias="OPENAI_API_KEY")
    openai_response_model: str = "gpt-5.2"
    openai_embedding_model: str = "text-embedding-3-small"
    local_tenant_slug: str = "local-workspace"
    local_tenant_name: str = "Local Workspace"
    local_user_email: str = "local.user@example.test"
    local_user_name: str = "Local Admin"


def build_settings(
    *,
    env_file: Path | str | None | object = USE_ROOT_ENV_FILE,
    **overrides: object,
) -> Settings:
    resolved_env_file = ROOT_ENV_FILE if env_file is USE_ROOT_ENV_FILE else env_file
    settings_kwargs = {"_env_file": resolved_env_file, **cast(dict[str, Any], overrides)}
    return Settings(**settings_kwargs)  # type: ignore[call-arg]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return build_settings()


def clear_settings_cache() -> None:
    get_settings.cache_clear()
