"""Delete query builder."""

from typing import Any, Optional

from sqlglot import exp

from sqlspec.statement_new.builder.base import BaseBuilder
from sqlspec.statement_new.builder.mixins import CoreQueryMixin, DatabaseSpecificMixin, DeleteOperationsMixin
from sqlspec.statement_new.result import SQLResult
from sqlspec.typing import RowT


class Delete(
    BaseBuilder[RowT],
    DeleteOperationsMixin,
    CoreQueryMixin,
    DatabaseSpecificMixin,
):
    """DELETE query builder."""

    def __init__(
        self,
        table: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._expression = exp.Delete()
        if table:
            self.from_(table)

    @property
    def _expected_result_type(self) -> "type[SQLResult[RowT]]":
        return SQLResult[RowT]
