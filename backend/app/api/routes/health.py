from fastapi import APIRouter, Depends

from ...core.config import Settings
from ...dependencies import get_app_settings


router = APIRouter(prefix="/health", tags=["health"])


@router.get("")
def healthcheck(settings: Settings = Depends(get_app_settings)) -> dict[str, object]:
    return {
        "status": "ok",
        "environment": settings.environment,
        "supabase_configured": settings.supabase_configured,
    }
