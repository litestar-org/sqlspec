"""Select query builder."""

from typing import Any

from sqlglot import exp

from sqlspec.statement_new.builder.base import BaseBuilder
from sqlspec.statement_new.result import SQLResult
from sqlspec.typing import RowT


class Select(BaseBuilder[RowT]):
    """SELECT query builder."""

    def __init__(
        self,
        *columns: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._expression = exp.Select()
        if columns:
            self.select(*columns)

    @property
    def _expected_result_type(self) -> "type[SQLResult[RowT]]":
        return SQLResult[RowT]

    def as_schema(self, schema: "type[RowT]") -> "Select[RowT]":
        new_builder = Select[RowT]()
        new_builder._expression = self._expression.copy() if self._expression is not None else None
        new_builder._parameters = self._parameters.copy()
        new_builder._parameter_counter = self._parameter_counter
        new_builder.dialect = self.dialect
        # Store schema for later use
        return new_builder
