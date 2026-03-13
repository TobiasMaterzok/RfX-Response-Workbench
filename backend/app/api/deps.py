from __future__ import annotations

from collections.abc import Generator

from fastapi import Depends, Header, Request
from sqlalchemy.orm import Session

from app.services.container import ServiceContainer
from app.services.identity import UserContext, require_user_context


def get_container(request: Request) -> ServiceContainer:
    return request.app.state.container


def get_session(
    container: ServiceContainer = Depends(get_container),
) -> Generator[Session, None, None]:
    session = container.session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_user_context(
    tenant_slug: str = Header(alias="X-Tenant-Slug"),
    user_email: str = Header(alias="X-User-Email"),
    session: Session = Depends(get_session),
) -> UserContext:
    return require_user_context(session, tenant_slug=tenant_slug, user_email=user_email)
