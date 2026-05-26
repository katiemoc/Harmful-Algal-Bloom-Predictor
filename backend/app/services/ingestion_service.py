from typing import Any

from ..core.config import get_settings
from ..models.payloads import IngestRequest, IngestResponse, QueryRequest, QueryResponse
from .supabase_service import SupabaseService
from .transformers import TransformerRegistry


class IngestionService:
    def __init__(
        self,
        supabase_service: SupabaseService,
        transformer_registry: TransformerRegistry,
    ) -> None:
        self.supabase_service = supabase_service
        self.transformer_registry = transformer_registry
        self.settings = get_settings()

    def list_sources(self) -> list[str]:
        return self.transformer_registry.list_sources()

    def preview_transform(
        self,
        source: str,
        payload: dict[str, Any] | list[dict[str, Any]],
        metadata: dict[str, Any],
    ) -> list[dict[str, Any]]:
        records = self._normalize_payload(payload)
        return self.transformer_registry.apply(source=source, records=records, metadata=metadata)

    def ingest(self, request: IngestRequest) -> IngestResponse:
        records = self.preview_transform(
            source=request.source,
            payload=request.payload,
            metadata=request.metadata,
        )

        if request.operation == "upsert":
            response_data = self.supabase_service.upsert_records(
                table=request.table,
                records=records,
                conflict_columns=request.upsert_on,
            )
        else:
            response_data = self.supabase_service.insert_records(
                table=request.table,
                records=records,
            )

        return IngestResponse(
            source=request.source,
            table=request.table,
            operation=request.operation,
            records_received=len(records),
            records_written=len(response_data),
            data=response_data,
        )

    def query(self, request: QueryRequest) -> QueryResponse:
        limit = min(request.limit, self.settings.default_select_limit)
        records = self.supabase_service.query_records(
            table=request.table,
            columns=request.columns,
            filters=request.filters,
            order_by=request.order_by,
            ascending=request.ascending,
            limit=limit,
        )
        return QueryResponse(
            table=request.table,
            records_returned=len(records),
            data=records,
        )

    @staticmethod
    def _normalize_payload(payload: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            if not payload:
                raise ValueError("payload list cannot be empty")
            return payload

        if not payload:
            raise ValueError("payload object cannot be empty")
        return [payload]
