"""Refactored, safe SQL query builder for MERGE statements."""

from typing import Any

from sqlglot import exp

from sqlspec.statement_new.builder.base import BaseBuilder
from sqlspec.statement_new.builder.mixins import MergeOperationsMixin
from sqlspec.statement_new.result import SQLResult
from sqlspec.typing import RowT

__all__ = ("Merge",)


class Merge(BaseBuilder[RowT], MergeOperationsMixin):
    """Builder for MERGE statements."""

    @property
    def _expected_result_type(self) -> "type[SQLResult[RowT]]":
        return SQLResult[RowT]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))
