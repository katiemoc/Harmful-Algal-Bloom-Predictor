from fastapi import APIRouter, Depends, HTTPException, status

from ...dependencies import get_ingestion_service, get_model_feed_service
from ...models.payloads import (
    IngestRequest,
    IngestResponse,
    ModelFeedBuildRequest,
    ModelFeedCommandResponse,
    ModelFeedUploadRequest,
    QueryRequest,
    QueryResponse,
    TransformPreviewRequest,
    TransformPreviewResponse,
)
from ...services.ingestion_service import IngestionService
from ...services.model_feed_service import ModelFeedService, ModelFeedServiceError
from ...services.supabase_service import (
    SupabaseConfigurationError,
    SupabaseOperationError,
)


router = APIRouter(prefix="/pipeline", tags=["pipeline"])


@router.get("/sources")
def list_sources(
    ingestion_service: IngestionService = Depends(get_ingestion_service),
) -> dict[str, list[str]]:
    return {"sources": ingestion_service.list_sources()}


@router.post("/transform", response_model=TransformPreviewResponse)
def preview_transform(
    request: TransformPreviewRequest,
    ingestion_service: IngestionService = Depends(get_ingestion_service),
) -> TransformPreviewResponse:
    try:
        records = ingestion_service.preview_transform(
            source=request.source,
            payload=request.payload,
            metadata=request.metadata,
        )
        return TransformPreviewResponse(
            source=request.source,
            records=records,
            records_received=len(records),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc


@router.post("/ingest", response_model=IngestResponse)
def ingest_payload(
    request: IngestRequest,
    ingestion_service: IngestionService = Depends(get_ingestion_service),
) -> IngestResponse:
    try:
        return ingestion_service.ingest(request)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except SupabaseConfigurationError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
    except SupabaseOperationError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(exc),
        ) from exc


@router.post("/query", response_model=QueryResponse)
def query_table(
    request: QueryRequest,
    ingestion_service: IngestionService = Depends(get_ingestion_service),
) -> QueryResponse:
    try:
        return ingestion_service.query(request)
    except SupabaseConfigurationError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
    except SupabaseOperationError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(exc),
        ) from exc


@router.post("/model-feed/build", response_model=ModelFeedCommandResponse)
def build_model_feed(
    request: ModelFeedBuildRequest,
    model_feed_service: ModelFeedService = Depends(get_model_feed_service),
) -> ModelFeedCommandResponse:
    try:
        result = model_feed_service.build_weekly_feed(
            start_date=request.start_date,
            use_existing=request.use_existing,
            output=request.output,
        )
        if request.rebuild_dashboard:
            dashboard_result = model_feed_service.rebuild_dashboard()
            result["stdout"] = f"{result['stdout']}\n{dashboard_result['stdout']}"
            result["stderr"] = f"{result['stderr']}\n{dashboard_result['stderr']}"
        return ModelFeedCommandResponse(**result)
    except ModelFeedServiceError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc


@router.post("/model-feed/upload", response_model=ModelFeedCommandResponse)
def upload_model_feed(
    request: ModelFeedUploadRequest,
    model_feed_service: ModelFeedService = Depends(get_model_feed_service),
) -> ModelFeedCommandResponse:
    try:
        result = model_feed_service.upload_weekly_feed(
            feed=request.feed,
            table=request.table,
            batch_size=request.batch_size,
            dry_run=request.dry_run,
        )
        return ModelFeedCommandResponse(**result)
    except ModelFeedServiceError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc


@router.post("/model-feed/rebuild-dashboard", response_model=ModelFeedCommandResponse)
def rebuild_dashboard_from_feed(
    model_feed_service: ModelFeedService = Depends(get_model_feed_service),
) -> ModelFeedCommandResponse:
    try:
        return ModelFeedCommandResponse(**model_feed_service.rebuild_dashboard())
    except ModelFeedServiceError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc
