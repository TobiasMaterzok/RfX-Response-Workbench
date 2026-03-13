from __future__ import annotations

from dataclasses import dataclass

from slugify import slugify
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import Settings
from app.exceptions import ScopeViolation, ValidationFailure
from app.models.entities import Membership, Tenant, User
from app.models.enums import MembershipRole


@dataclass(frozen=True)
class UserContext:
    tenant: Tenant
    user: User


def require_user_context(session: Session, *, tenant_slug: str, user_email: str) -> UserContext:
    tenant = session.scalar(select(Tenant).where(Tenant.slug == tenant_slug))
    if tenant is None:
        raise ScopeViolation(
            f"Tenant slug '{tenant_slug}' is unknown. Run the ensure-local-identity command before using the UI."
        )
    user = session.scalar(select(User).where(User.email == user_email))
    if user is None:
        raise ScopeViolation(
            f"User email '{user_email}' is unknown. Run the ensure-local-identity command before using the UI."
        )
    membership = session.scalar(
        select(Membership).where(Membership.tenant_id == tenant.id, Membership.user_id == user.id)
    )
    if membership is None:
        raise ScopeViolation(f"User '{user_email}' does not belong to tenant '{tenant_slug}'.")
    return UserContext(tenant=tenant, user=user)


def ensure_local_identity(session: Session, settings: Settings) -> UserContext:
    tenant = session.scalar(select(Tenant).where(Tenant.slug == settings.local_tenant_slug))
    if tenant is None:
        tenant = Tenant(name=settings.local_tenant_name, slug=slugify(settings.local_tenant_slug))
        session.add(tenant)
        session.flush()
    user = session.scalar(select(User).where(User.email == settings.local_user_email))
    if user is None:
        user = User(email=settings.local_user_email, display_name=settings.local_user_name)
        session.add(user)
        session.flush()
    membership = session.scalar(
        select(Membership).where(Membership.tenant_id == tenant.id, Membership.user_id == user.id)
    )
    if membership is None:
        membership = Membership(
            tenant_id=tenant.id,
            user_id=user.id,
            role=MembershipRole.ADMIN,
        )
        session.add(membership)
        session.flush()
    if tenant.slug != settings.local_tenant_slug:
        raise ValidationFailure(
            f"Local tenant slug '{tenant.slug}' does not match configured slug '{settings.local_tenant_slug}'."
        )
    return UserContext(tenant=tenant, user=user)
