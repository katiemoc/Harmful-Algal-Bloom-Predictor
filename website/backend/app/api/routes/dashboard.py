from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from ...dependencies import get_dashboard_service
from ...services.dashboard_service import DashboardDataError, DashboardService


router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("", response_model=dict[str, Any])
def get_dashboard_payload(
    dashboard_service: DashboardService = Depends(get_dashboard_service),
) -> dict[str, Any]:
    try:
        return dashboard_service.get_payload()
    except DashboardDataError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
