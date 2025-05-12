"""Collection filter datastructures."""

from abc import ABC, abstractmethod
from collections import abc
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generic, Literal, Optional, Protocol, Union, cast

from sqlglot import exp
from typing_extensions import TypeAlias, TypeVar

from sqlspec.statement import SQLStatement

__all__ = (
    "BeforeAfter",
    "CollectionFilter",
    "FilterTypes",
    "InAnyFilter",
    "LimitOffset",
    "NotInCollectionFilter",
    "NotInSearchFilter",
    "OnBeforeAfter",
    "OrderBy",
    "PaginationFilter",
    "SearchFilter",
    "StatementFilter",
    "apply_filter",
)

T = TypeVar("T")


class StatementFilter(Protocol):
    """Protocol for filters that can be appended to a statement."""

    @abstractmethod
    def append_to_statement(self, statement: SQLStatement) -> SQLStatement:
        """Append the filter to the statement.

        Args:
            statement: The SQL statement to modify.

        Returns:
            The modified statement.
        """
        raise NotImplementedError


@dataclass
class BeforeAfter(StatementFilter):
    """Data required to filter a query on a ``datetime`` column."""

    field_name: str
    """Name of the model attribute to filter on."""
    before: Optional[datetime] = None
    """Filter results where field earlier than this."""
    after: Optional[datetime] = None
    """Filter results where field later than this."""

    def append_to_statement(self, statement: SQLStatement) -> SQLStatement:
        conditions = []
        params: dict[str, Any] = {}
        col_expr = exp.column(self.field_name)

        if self.before:
            param_name = statement.generate_param_name(f"{self.field_name}_before")
            conditions.append(exp.LT(this=col_expr, expression=exp.Placeholder(this=param_name)))
            params[param_name] = self.before
        if self.after:
            param_name = statement.generate_param_name(f"{self.field_name}_after")
            conditions.append(exp.GT(this=col_expr, expression=exp.Placeholder(this=param_name)))  # type: ignore[arg-type]
            params[param_name] = self.after

        if conditions:
            final_condition = conditions[0]
            for cond in conditions[1:]:
                final_condition = exp.And(this=final_condition, expression=cond)  # type: ignore[assignment]
            statement.add_condition(final_condition, params)
        return statement


@dataclass
class OnBeforeAfter(StatementFilter):
    """Data required to filter a query on a ``datetime`` column."""

    field_name: str
    """Name of the model attribute to filter on."""
    on_or_before: Optional[datetime] = None
    """Filter results where field is on or earlier than this."""
    on_or_after: Optional[datetime] = None
    """Filter results where field on or later than this."""

    def append_to_statement(self, statement: SQLStatement) -> SQLStatement:
        conditions = []
        params: dict[str, Any] = {}
        col_expr = exp.column(self.field_name)

        if self.on_or_before:
            param_name = statement.generate_param_name(f"{self.field_name}_on_or_before")
            conditions.append(exp.LTE(this=col_expr, expression=exp.Placeholder(this=param_name)))
            params[param_name] = self.on_or_before
        if self.on_or_after:
            param_name = statement.generate_param_name(f"{self.field_name}_on_or_after")
            conditions.append(exp.GTE(this=col_expr, expression=exp.Placeholder(this=param_name)))  # type: ignore[arg-type]
            params[param_name] = self.on_or_after

        if conditions:
            final_condition = conditions[0]
            for cond in conditions[1:]:
                final_condition = exp.And(this=final_condition, expression=cond)  # type: ignore[assignment]
            statement.add_condition(final_condition, params)
        return statement


class InAnyFilter(StatementFilter, ABC, Generic[T]):
    """Subclass for methods that have a `prefer_any` attribute."""

    @abstractmethod
    def append_to_statement(self, statement: SQLStatement) -> SQLStatement:
        raise NotImplementedError


@dataclass
class CollectionFilter(InAnyFilter[T]):
    """Data required to construct a ``WHERE ... IN (...)`` clause."""

    field_name: str
    """Name of the model attribute to filter on."""
    values: Optional[abc.Collection[T]]
    """Values for ``IN`` clause.

    An empty list will return an empty result set, however, if ``None``, the filter is not applied to the query, and all rows are returned. """

    def append_to_statement(self, statement: SQLStatement) -> SQLStatement:
        if self.values is None:
            return statement

        if not self.values:  # Empty collection
            # Add a condition that is always false
            statement.add_condition(exp.false())
            return statement

        placeholder_expressions: list[exp.Placeholder] = []
        current_params: dict[str, Any] = {}

        for i, value_item in enumerate(self.values):
            param_key = statement.generate_param_name(f"{self.field_name}_in_{i}")
            placeholder_expressions.append(exp.Placeholder(this=param_key))
            current_params[param_key] = value_item

        in_condition = exp.In(this=exp.column(self.field_name), expressions=placeholder_expressions)
        statement.add_condition(in_condition, current_params)
        return statement


@dataclass
class NotInCollectionFilter(InAnyFilter[T]):
    """Data required to construct a ``WHERE ... NOT IN (...)`` clause."""

    field_name: str
    """Name of the model attribute to filter on."""
    values: Optional[abc.Collection[T]]
    """Values for ``NOT IN`` clause.

    An empty list or ``None`` will return all rows."""

    def append_to_statement(self, statement: SQLStatement) -> SQLStatement:
        if self.values is None or not self.values:  # Empty list or None, no filter applied
            return statement

        placeholder_expressions: list[exp.Placeholder] = []
        current_params: dict[str, Any] = {}

        for i, value_item in enumerate(self.values):
            param_key = statement.generate_param_name(f"{self.field_name}_notin_{i}")
            placeholder_expressions.append(exp.Placeholder(this=param_key))
            current_params[param_key] = value_item

        in_expr = exp.In(this=exp.column(self.field_name), expressions=placeholder_expressions)
        not_in_condition = exp.Not(this=in_expr)
        statement.add_condition(not_in_condition, current_params)
        return statement


class PaginationFilter(StatementFilter, ABC):
    """Subclass for methods that function as a pagination type."""

    @abstractmethod
    def append_to_statement(self, statement: SQLStatement) -> SQLStatement:
        raise NotImplementedError


@dataclass
class LimitOffset(PaginationFilter):
    """Data required to add limit/offset filtering to a query."""

    limit: int
    """Value for ``LIMIT`` clause of query."""
    offset: int
    """Value for ``OFFSET`` clause of query."""

    def append_to_statement(self, statement: SQLStatement) -> SQLStatement:
        # Generate parameter names for limit and offset
        limit_param_name = statement.generate_param_name("limit_val")
        offset_param_name = statement.generate_param_name("offset_val")

        statement.add_limit(self.limit, param_name=limit_param_name)
        statement.add_offset(self.offset, param_name=offset_param_name)

        return statement


@dataclass
class OrderBy(StatementFilter):
    """Data required to construct a ``ORDER BY ...`` clause."""

    field_name: str
    """Name of the model attribute to sort on."""
    sort_order: Literal["asc", "desc"] = "asc"
    """Sort ascending or descending"""

    def append_to_statement(self, statement: SQLStatement) -> SQLStatement:
        # Basic validation for sort_order, though Literal helps at type checking time
        normalized_sort_order = self.sort_order.lower()
        if normalized_sort_order not in {"asc", "desc"}:
            normalized_sort_order = "asc"

        statement.add_order_by(self.field_name, direction=cast("Literal['asc', 'desc']", normalized_sort_order))

        return statement


@dataclass
class SearchFilter(StatementFilter):
    """Data required to construct a ``WHERE field_name LIKE '%' || :value || '%'`` clause."""

    field_name: Union[str, set[str]]
    """Name of the model attribute to search on."""
    value: str
    """Search value."""
    ignore_case: Optional[bool] = False
    """Should the search be case insensitive."""

    def append_to_statement(self, statement: SQLStatement) -> SQLStatement:
        if not self.value:
            return statement

        search_val_param_name = statement.generate_param_name("search_val")

        # The pattern %value% needs to be handled carefully.
        params = {search_val_param_name: f"%{self.value}%"}
        pattern_expr = exp.Placeholder(this=search_val_param_name)

        like_op = exp.ILike if self.ignore_case else exp.Like

        if isinstance(self.field_name, str):
            condition = like_op(this=exp.column(self.field_name), expression=pattern_expr)
            statement.add_condition(condition, params)
        elif isinstance(self.field_name, set) and self.field_name:
            field_conditions = [like_op(this=exp.column(field), expression=pattern_expr) for field in self.field_name]
            if not field_conditions:
                return statement

            final_condition = field_conditions[0]
            for cond in field_conditions[1:]:
                final_condition = exp.Or(this=final_condition, expression=cond)  # type: ignore[assignment]
            statement.add_condition(final_condition, params)

        return statement


@dataclass
class NotInSearchFilter(SearchFilter):  # Inherits field_name, value, ignore_case
    """Data required to construct a ``WHERE field_name NOT LIKE '%' || :value || '%'`` clause."""

    def append_to_statement(self, statement: SQLStatement) -> SQLStatement:
        if not self.value:
            return statement

        search_val_param_name = statement.generate_param_name("not_search_val")

        params = {search_val_param_name: f"%{self.value}%"}
        pattern_expr = exp.Placeholder(this=search_val_param_name)

        like_op = exp.ILike if self.ignore_case else exp.Like

        if isinstance(self.field_name, str):
            condition = exp.Not(this=like_op(this=exp.column(self.field_name), expression=pattern_expr))
            statement.add_condition(condition, params)
        elif isinstance(self.field_name, set) and self.field_name:
            field_conditions = [
                exp.Not(this=like_op(this=exp.column(field), expression=pattern_expr)) for field in self.field_name
            ]
            if not field_conditions:
                return statement

            # Combine with AND: (field1 NOT LIKE pattern) AND (field2 NOT LIKE pattern) ...
            final_condition = field_conditions[0]
            for cond in field_conditions[1:]:
                final_condition = exp.And(this=final_condition, expression=cond)  # type: ignore[assignment]
            statement.add_condition(final_condition, params)

        return statement


# Function to be imported in SQLStatement module
def apply_filter(statement: SQLStatement, filter_obj: StatementFilter) -> SQLStatement:
    """Apply a statement filter to a SQL statement.

    Args:
        statement: The SQL statement to modify.
        filter_obj: The filter to apply.

    Returns:
        The modified statement.
    """
    return filter_obj.append_to_statement(statement)


FilterTypes: TypeAlias = Union[
    BeforeAfter,
    OnBeforeAfter,
    CollectionFilter[Any],
    LimitOffset,
    OrderBy,
    SearchFilter,
    NotInCollectionFilter[Any],
    NotInSearchFilter,
]
"""Aggregate type alias of the types supported for collection filtering."""
