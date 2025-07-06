"""Insert query builder."""

from typing import Any, Optional

from sqlglot import exp

from sqlspec.statement_new.builder.base import BaseBuilder
from sqlspec.statement_new.builder.mixins import DatabaseSpecificMixin, InsertOperationsMixin
from sqlspec.statement_new.result import SQLResult
from sqlspec.typing import RowT


class Insert(
    BaseBuilder[RowT],
    InsertOperationsMixin,
    DatabaseSpecificMixin,
):
    """INSERT query builder."""

    def __init__(
        self,
        table: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._expression = exp.Insert()
        if table:
            self.into(table)

    @property
    def _expected_result_type(self) -> "type[SQLResult[RowT]]":
        return SQLResult[RowT]
