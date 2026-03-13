from __future__ import annotations

from pathlib import Path

from app import config as config_module
from app.config import build_settings, clear_settings_cache, get_settings


def test_get_settings_reads_root_env_file(monkeypatch, tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "RFX_DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/from_dotenv",
                "RFX_STORAGE_ROOT=./dotenv-storage",
                "OPENAI_API_KEY=dotenv-key",
                "RFX_OPENAI_RESPONSE_MODEL=gpt-5.2",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(config_module, "ROOT_ENV_FILE", env_path)
    clear_settings_cache()
    settings = get_settings()
    assert settings.database_url.endswith("/from_dotenv")
    assert settings.storage_root == Path("./dotenv-storage")
    assert settings.openai_api_key == "dotenv-key"


def test_process_env_overrides_dotenv(monkeypatch, tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "RFX_DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/from_dotenv\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(config_module, "ROOT_ENV_FILE", env_path)
    monkeypatch.setenv(
        "RFX_DATABASE_URL",
        "postgresql+psycopg://postgres:postgres@localhost:5432/from_process_env",
    )
    clear_settings_cache()
    settings = get_settings()
    assert settings.database_url.endswith("/from_process_env")


def test_build_settings_with_env_file_none_is_hermetic(monkeypatch, tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "RFX_DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/from_dotenv\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(config_module, "ROOT_ENV_FILE", env_path)
    clear_settings_cache()
    settings = build_settings(env_file=None)
    assert settings.database_url.endswith("/rfx_rag_expert")
