from fastapi import APIRouter, Depends

from app.api.deps import get_user_context
from app.schemas.api import SessionContextResponse
from app.services.identity import UserContext

router = APIRouter(prefix="/api/session", tags=["session"])


@router.get("/context", response_model=SessionContextResponse)
def session_context(
    user_context: UserContext = Depends(get_user_context),
) -> SessionContextResponse:
    return SessionContextResponse(
        tenant_id=user_context.tenant.id,
        tenant_slug=user_context.tenant.slug,
        tenant_name=user_context.tenant.name,
        user_id=user_context.user.id,
        user_email=user_context.user.email,
        user_name=user_context.user.display_name,
    )
