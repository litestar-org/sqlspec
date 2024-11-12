from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.types.protocols import StatementType

__all__ = ("BasePsycopgAdapter",)


class BasePsycopgAdapter:
    """Base class for Psycopg adapters with common functionality."""

    def __init__(self) -> None:
        self._driver = None

    def process_sql(self, op_type: StatementType, sql: str) -> str:
        """Process SQL query, converting named parameters to psycopg format."""
        return sql

    def _process_row(self, row: Any, column_names: list[str], record_class: Callable | None = None) -> Any:
        """Process a row into the desired format."""
        if record_class is None:
            return row
        return record_class(**{str(k): v for k, v in zip(column_names, row, strict=False)})
