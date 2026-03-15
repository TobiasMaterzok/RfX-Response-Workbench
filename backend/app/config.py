from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Final, cast

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.models.vector import EMBEDDING_VECTOR_DIMENSIONS

REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
ROOT_ENV_FILE: Final[Path] = REPO_ROOT / ".env"
USE_ROOT_ENV_FILE: Final[object] = object()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RFX_", extra="ignore", populate_by_name=True)

    database_url: str = Field(
        default="postgresql+psycopg://postgres:postgres@localhost:5432/rfx_rag_expert"
    )
    storage_root: Path = Field(default=Path("./storage"))
    llm_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("LLM_API_KEY", "OPENAI_API_KEY", "AZURE_OPENAI_API_KEY"),
    )
    llm_api_base_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "LLM_API_BASE_URL",
            "OPENAI_BASE_URL",
            "AZURE_OPENAI_API_BASE_URL",
        ),
    )
    openai_response_model: str = "gpt-5.2"
    openai_embedding_model: str = "text-embedding-3-small"
    openai_embedding_dimensions: int = EMBEDDING_VECTOR_DIMENSIONS
    local_tenant_slug: str = "local-workspace"
    local_tenant_name: str = "Local Workspace"
    local_user_email: str = "local.user@example.test"
    local_user_name: str = "Local Admin"

    @field_validator("llm_api_key", "llm_api_base_url", mode="before")
    @classmethod
    def _blank_string_to_none(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("openai_embedding_dimensions", mode="before")
    @classmethod
    def _blank_embedding_dimensions_uses_default(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return EMBEDDING_VECTOR_DIMENSIONS
        return value

    @model_validator(mode="after")
    def _restore_legacy_aliases_when_preferred_env_is_blank(self) -> Settings:
        if self.llm_api_key is None:
            self.llm_api_key = _first_nonblank_env(
                "OPENAI_API_KEY",
                "AZURE_OPENAI_API_KEY",
            )
        if self.llm_api_base_url is None:
            self.llm_api_base_url = _first_nonblank_env(
                "OPENAI_BASE_URL",
                "AZURE_OPENAI_API_BASE_URL",
            )
        return self

    @property
    def openai_api_key(self) -> str | None:
        return self.llm_api_key

    @property
    def openai_base_url(self) -> str | None:
        return self.llm_api_base_url


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


def _first_nonblank_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value
    return None
