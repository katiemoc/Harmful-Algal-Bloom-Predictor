from fastapi import APIRouter

from .routes.health import router as health_router
from .routes.pipeline import router as pipeline_router


api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(pipeline_router)
