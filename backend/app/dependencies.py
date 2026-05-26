from functools import lru_cache

from .core.config import Settings, get_settings
from .services.ingestion_service import IngestionService
from .services.model_feed_service import ModelFeedService
from .services.supabase_service import SupabaseService
from .services.transformers import transformer_registry


@lru_cache
def get_supabase_service() -> SupabaseService:
    return SupabaseService(get_settings())


def get_ingestion_service() -> IngestionService:
    return IngestionService(
        supabase_service=get_supabase_service(),
        transformer_registry=transformer_registry,
    )


def get_model_feed_service() -> ModelFeedService:
    return ModelFeedService()


def get_app_settings() -> Settings:
    return get_settings()
