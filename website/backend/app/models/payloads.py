import re
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_valid_identifier(value: str) -> bool:
    return bool(IDENTIFIER_PATTERN.fullmatch(value))


class IngestRequest(BaseModel):
    source: str = Field(default="generic", min_length=1, max_length=100)
    table: str = Field(min_length=1, max_length=120)
    operation: Literal["insert", "upsert"] = "insert"
    payload: dict[str, Any] | list[dict[str, Any]]
    upsert_on: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("table")
    @classmethod
    def validate_table(cls, value: str) -> str:
        if not _is_valid_identifier(value):
            raise ValueError("table must be a valid SQL-style identifier")
        return value

    @field_validator("upsert_on")
    @classmethod
    def validate_upsert_columns(cls, value: list[str]) -> list[str]:
        for column in value:
            if not _is_valid_identifier(column):
                raise ValueError("upsert_on values must be valid SQL-style identifiers")
        return value


class TransformPreviewRequest(BaseModel):
    source: str = Field(default="generic", min_length=1, max_length=100)
    payload: dict[str, Any] | list[dict[str, Any]]
    metadata: dict[str, Any] = Field(default_factory=dict)


class TransformPreviewResponse(BaseModel):
    source: str
    records_received: int
    records: list[dict[str, Any]]


class IngestResponse(BaseModel):
    source: str
    table: str
    operation: str
    records_received: int
    records_written: int
    data: list[dict[str, Any]]


class QueryFilter(BaseModel):
    column: str = Field(min_length=1, max_length=120)
    operator: Literal["eq", "neq", "gt", "gte", "lt", "lte", "ilike", "in"]
    value: Any

    @field_validator("column")
    @classmethod
    def validate_column(cls, value: str) -> str:
        if not _is_valid_identifier(value):
            raise ValueError("column must be a valid SQL-style identifier")
        return value

    @model_validator(mode="after")
    def validate_in_filter_value(self) -> "QueryFilter":
        if self.operator == "in" and not isinstance(self.value, list):
            raise ValueError("value must be a list when operator is 'in'")
        return self


class QueryRequest(BaseModel):
    table: str = Field(min_length=1, max_length=120)
    columns: list[str] = Field(default_factory=lambda: ["*"])
    filters: list[QueryFilter] = Field(default_factory=list)
    order_by: str | None = Field(default=None, max_length=120)
    ascending: bool = False
    limit: int = Field(default=100, ge=1, le=1000)

    @field_validator("table")
    @classmethod
    def validate_table(cls, value: str) -> str:
        if not _is_valid_identifier(value):
            raise ValueError("table must be a valid SQL-style identifier")
        return value

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("columns cannot be empty")
        for column in value:
            if column != "*" and not _is_valid_identifier(column):
                raise ValueError("columns must contain '*' or valid SQL-style identifiers")
        return value

    @field_validator("order_by")
    @classmethod
    def validate_order_by(cls, value: str | None) -> str | None:
        if value is not None and not _is_valid_identifier(value):
            raise ValueError("order_by must be a valid SQL-style identifier")
        return value


class QueryResponse(BaseModel):
    table: str
    records_returned: int
    data: list[dict[str, Any]]
