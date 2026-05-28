from collections.abc import Callable
from typing import Any


TransformerFunction = Callable[[list[dict[str, Any]], dict[str, Any]], list[dict[str, Any]]]


class TransformerRegistry:
    def __init__(self) -> None:
        self._transformers: dict[str, TransformerFunction] = {}

    def register(self, source: str) -> Callable[[TransformerFunction], TransformerFunction]:
        def decorator(transformer: TransformerFunction) -> TransformerFunction:
            self._transformers[source] = transformer
            return transformer

        return decorator

    def apply(
        self,
        source: str,
        records: list[dict[str, Any]],
        metadata: dict[str, Any],
    ) -> list[dict[str, Any]]:
        transformer = self._transformers.get(source, self._transformers["generic"])
        return transformer(records, metadata)

    def list_sources(self) -> list[str]:
        return sorted(self._transformers.keys())


transformer_registry = TransformerRegistry()


@transformer_registry.register("generic")
def generic_transformer(
    records: list[dict[str, Any]],
    metadata: dict[str, Any],
) -> list[dict[str, Any]]:
    del metadata
    return records


@transformer_registry.register("wrapped_records")
def wrapped_records_transformer(
    records: list[dict[str, Any]],
    metadata: dict[str, Any],
) -> list[dict[str, Any]]:
    normalized_records: list[dict[str, Any]] = []

    for record in records:
        payload = record.get("data", record)
        if not isinstance(payload, dict):
            raise ValueError("wrapped_records expects each record to contain a JSON object in 'data'")
        normalized_records.append(payload)

    del metadata
    return normalized_records
