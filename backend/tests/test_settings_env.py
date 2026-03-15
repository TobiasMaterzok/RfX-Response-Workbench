from __future__ import annotations

from pathlib import Path

from app import config as config_module
from app.config import build_settings, clear_settings_cache, get_settings
from app.services.ai import llm_provider_name_from_settings


def test_get_settings_reads_root_env_file(monkeypatch, tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "RFX_DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/from_dotenv",
                "RFX_STORAGE_ROOT=./dotenv-storage",
                "LLM_API_KEY=dotenv-key",
                "LLM_API_BASE_URL=https://example-resource.openai.azure.com/openai/v1/",
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
    assert settings.llm_api_key == "dotenv-key"
    assert settings.openai_api_key == "dotenv-key"
    assert settings.llm_api_base_url == "https://example-resource.openai.azure.com/openai/v1/"
    assert llm_provider_name_from_settings(settings) == "azure_openai"


def test_legacy_openai_api_key_alias_still_loads(monkeypatch, tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "OPENAI_API_KEY=legacy-key\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(config_module, "ROOT_ENV_FILE", env_path)
    clear_settings_cache()
    settings = get_settings()
    assert settings.llm_api_key == "legacy-key"
    assert settings.openai_api_key == "legacy-key"


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


def test_blank_preferred_env_does_not_mask_legacy_process_env(monkeypatch, tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("", encoding="utf-8")
    monkeypatch.setattr(config_module, "ROOT_ENV_FILE", env_path)
    monkeypatch.setenv("LLM_API_KEY", "")
    monkeypatch.setenv("OPENAI_API_KEY", "legacy-key")
    monkeypatch.setenv("LLM_API_BASE_URL", "")
    monkeypatch.setenv("OPENAI_BASE_URL", "https://api.openai.com/v1/")
    clear_settings_cache()
    settings = get_settings()
    assert settings.llm_api_key == "legacy-key"
    assert settings.llm_api_base_url == "https://api.openai.com/v1/"


def test_build_settings_accepts_direct_llm_overrides() -> None:
    settings = build_settings(
        env_file=None,
        llm_api_key="direct-key",
        llm_api_base_url="https://example-resource.openai.azure.com/openai/v1/",
    )
    assert settings.llm_api_key == "direct-key"
    assert settings.llm_api_base_url == "https://example-resource.openai.azure.com/openai/v1/"


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
