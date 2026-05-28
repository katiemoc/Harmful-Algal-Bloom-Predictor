from typing import Any

import httpx

from ..core.config import Settings
from ..models.payloads import QueryFilter


class SupabaseConfigurationError(RuntimeError):
    """Raised when Supabase credentials are missing."""


class SupabaseOperationError(RuntimeError):
    """Raised when a Supabase operation fails."""


class SupabaseService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._client: httpx.Client | None = None

    def insert_records(self, table: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        try:
            response = self._request(
                "POST",
                table=table,
                json=records,
                headers={"Prefer": "return=representation"},
            )
            return self._extract_data(response)
        except (SupabaseConfigurationError, SupabaseOperationError):
            raise
        except Exception as exc:  # pragma: no cover - external API wrapper
            raise SupabaseOperationError(f"Failed to insert records into '{table}'.") from exc

    def upsert_records(
        self,
        table: str,
        records: list[dict[str, Any]],
        conflict_columns: list[str],
    ) -> list[dict[str, Any]]:
        try:
            params: dict[str, Any] = {}
            if conflict_columns:
                params["on_conflict"] = ",".join(conflict_columns)

            response = self._request(
                "POST",
                table=table,
                params=params,
                json=records,
                headers={"Prefer": "resolution=merge-duplicates,return=representation"},
            )
            return self._extract_data(response)
        except (SupabaseConfigurationError, SupabaseOperationError):
            raise
        except Exception as exc:  # pragma: no cover - external API wrapper
            raise SupabaseOperationError(f"Failed to upsert records into '{table}'.") from exc

    def query_records(
        self,
        table: str,
        columns: list[str],
        filters: list[QueryFilter],
        order_by: str | None,
        ascending: bool,
        limit: int,
    ) -> list[dict[str, Any]]:
        try:
            params: dict[str, Any] = {
                "select": "*" if columns == ["*"] else ",".join(columns),
                "limit": limit,
            }
            for query_filter in filters:
                params[query_filter.column] = self._format_filter_value(query_filter)

            if order_by:
                direction = "asc" if ascending else "desc"
                params["order"] = f"{order_by}.{direction}"

            response = self._request("GET", table=table, params=params)
            return self._extract_data(response)
        except (SupabaseConfigurationError, SupabaseOperationError):
            raise
        except Exception as exc:  # pragma: no cover - external API wrapper
            raise SupabaseOperationError(f"Failed to query records from '{table}'.") from exc

    def _get_client(self) -> httpx.Client:
        if self._client is None:
            if not self.settings.supabase_configured:
                raise SupabaseConfigurationError(
                    "Supabase is not configured. Set SUPABASE_URL and SUPABASE_KEY in "
                    "website/backend/.env, backend/.env, or .env."
                )
            self._client = httpx.Client(
                base_url=f"{self.settings.supabase_url.rstrip('/')}/rest/v1",
                headers=self._base_headers(),
                timeout=30.0,
            )

        return self._client

    @staticmethod
    def _extract_data(response: httpx.Response) -> list[dict[str, Any]]:
        if not response.content:
            return []

        data = response.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        return list(data)

    @staticmethod
    def _format_filter_value(query_filter: QueryFilter) -> str:
        if query_filter.operator == "in":
            values = ",".join(SupabaseService._format_in_value(value) for value in query_filter.value)
            return f"in.({values})"

        return f"{query_filter.operator}.{SupabaseService._format_scalar_value(query_filter.value)}"

    def _request(
        self,
        method: str,
        table: str,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        client = self._get_client()
        merged_headers = self._request_headers(headers or {}, method=method)
        response = client.request(
            method=method,
            url=f"/{table}",
            params=params,
            json=json,
            headers=merged_headers,
        )
        self._raise_for_status(response)
        return response

    def _base_headers(self) -> dict[str, str]:
        return {
            "apikey": self.settings.supabase_key or "",
            "Authorization": f"Bearer {self.settings.supabase_key or ''}",
        }

    def _request_headers(self, headers: dict[str, str], method: str) -> dict[str, str]:
        request_headers = dict(headers)

        if method.upper() == "GET":
            request_headers["Accept-Profile"] = self.settings.supabase_schema
        else:
            request_headers["Content-Profile"] = self.settings.supabase_schema

        return request_headers

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        if response.is_error:
            detail = response.text.strip() or response.reason_phrase
            raise SupabaseOperationError(
                f"Supabase request failed with status {response.status_code}: {detail}"
            )

    @staticmethod
    def _format_scalar_value(value: Any) -> str:
        if isinstance(value, bool):
            return str(value).lower()
        if value is None:
            return "null"
        return str(value)

    @staticmethod
    def _format_in_value(value: Any) -> str:
        if isinstance(value, str):
            escaped = value.replace('"', '\\"')
            return f'"{escaped}"'
        return SupabaseService._format_scalar_value(value)
