from functools import lru_cache

from .core.config import Settings, get_settings
from .services.dashboard_service import DashboardService
from .services.ingestion_service import IngestionService
from .services.supabase_service import SupabaseService
from .services.transformers import transformer_registry


@lru_cache
def get_supabase_service() -> SupabaseService:
    return SupabaseService(get_settings())


@lru_cache
def get_dashboard_service() -> DashboardService:
    return DashboardService()


def get_ingestion_service() -> IngestionService:
    return IngestionService(
        supabase_service=get_supabase_service(),
        transformer_registry=transformer_registry,
    )


def get_app_settings() -> Settings:
    return get_settings()
