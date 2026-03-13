from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings
from app.db import build_session_factory
from app.services.ai import AIService, OpenAIAIService
from app.services.storage import LocalObjectStorage


@dataclass
class ServiceContainer:
    settings: Settings
    session_factory: sessionmaker[Session]
    storage: LocalObjectStorage
    ai_service: AIService


def build_container(settings: Settings) -> ServiceContainer:
    return ServiceContainer(
        settings=settings,
        session_factory=build_session_factory(settings),
        storage=LocalObjectStorage(settings),
        ai_service=OpenAIAIService(settings),
    )
