from fastapi import APIRouter, Depends, HTTPException, status

from ...dependencies import get_ingestion_service
from ...models.payloads import (
    IngestRequest,
    IngestResponse,
    QueryRequest,
    QueryResponse,
    TransformPreviewRequest,
    TransformPreviewResponse,
)
from ...services.ingestion_service import IngestionService
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
