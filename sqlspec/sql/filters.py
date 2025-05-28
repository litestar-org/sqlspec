"""Collection filter datastructures."""

from abc import ABC, abstractmethod
from collections import abc
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Generic, Literal, Optional, Protocol, Union, runtime_checkable

from sqlglot import Expression, exp
from typing_extensions import TypeAlias, TypeVar

if TYPE_CHECKING:
    from sqlglot.expressions import Condition

    from sqlspec.sql.statement import SQLStatement

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
FilterTypeT = TypeVar("FilterTypeT", bound="StatementFilter")
"""Type variable for filter types.

:class:`~advanced_alchemy.filters.StatementFilter`
"""


@runtime_checkable
class StatementFilter(Protocol):
    """Protocol for filters that can be appended to a statement."""

    @abstractmethod
    def append_to_statement(self, query: "SQLStatement") -> "SQLStatement":
        """Append the filter to the statement.

        Args:
            query: The SQL query object to modify.

        Returns:
            The modified query object.
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

    def append_to_statement(self, query: "SQLStatement") -> "SQLStatement":
        conditions = []
        col_expr = exp.column(self.field_name)

        if self.before:
            param_name = query.add_parameter(
                self.before, name=query.get_unique_parameter_name(f"{self.field_name}_before")
            )
            conditions.append(exp.LT(this=col_expr, expression=exp.Placeholder(this=param_name)))
        if self.after:
            param_name = query.add_parameter(
                self.after, name=query.get_unique_parameter_name(f"{self.field_name}_after")
            )
            conditions.append(exp.GT(this=col_expr, expression=exp.Placeholder(this=param_name)))

        if conditions:
            final_condition = conditions[0]
            for cond in conditions[1:]:
                final_condition = exp.And(this=final_condition, expression=cond)
            query.where(final_condition)
        return query


@dataclass
class OnBeforeAfter(StatementFilter):
    """Data required to filter a query on a ``datetime`` column."""

    field_name: str
    """Name of the model attribute to filter on."""
    on_or_before: Optional[datetime] = None
    """Filter results where field is on or earlier than this."""
    on_or_after: Optional[datetime] = None
    """Filter results where field on or later than this."""

    def append_to_statement(self, query: "SQLStatement") -> "SQLStatement":
        conditions: list[exp.Expression] = []
        col_expr = exp.column(self.field_name)

        if self.on_or_before:
            param_name = query.add_parameter(
                self.on_or_before, name=query.get_unique_parameter_name(f"{self.field_name}_on_or_before")
            )
            conditions.append(exp.LTE(this=col_expr, expression=exp.Placeholder(this=param_name)))
        if self.on_or_after:
            param_name = query.add_parameter(
                self.on_or_after, name=query.get_unique_parameter_name(f"{self.field_name}_on_or_after")
            )
            conditions.append(exp.GTE(this=col_expr, expression=exp.Placeholder(this=param_name)))

        if conditions:
            final_condition = conditions[0]
            for cond in conditions[1:]:
                final_condition = exp.And(this=final_condition, expression=cond)
            query.where(final_condition)
        return query


class InAnyFilter(StatementFilter, ABC, Generic[T]):
    """Subclass for methods that have a `prefer_any` attribute."""

    @abstractmethod
    def append_to_statement(self, query: "SQLStatement") -> "SQLStatement":
        raise NotImplementedError


@dataclass
class CollectionFilter(InAnyFilter[T]):
    """Data required to construct a ``WHERE ... IN (...)`` clause."""

    field_name: str
    """Name of the model attribute to filter on."""
    values: Optional[abc.Collection[T]]
    """Values for ``IN`` clause.

    An empty list will return an empty result set, however, if ``None``, the filter is not applied to the query, and all rows are returned. """

    def append_to_statement(self, query: "SQLStatement") -> "SQLStatement":
        if self.values is None:
            return query

        if not self.values:  # Empty collection
            query.where(exp.false())  # Ensures no rows are returned for an empty IN clause
            return query

        placeholder_expressions: list[exp.Placeholder] = []

        for i, value_item in enumerate(self.values):
            param_key = query.add_parameter(
                value_item, name=query.get_unique_parameter_name(f"{self.field_name}_in_{i}")
            )
            placeholder_expressions.append(exp.Placeholder(this=param_key))

        in_condition = exp.In(this=exp.column(self.field_name), expressions=placeholder_expressions)
        query.where(in_condition)
        return query


@dataclass
class NotInCollectionFilter(InAnyFilter[T]):
    """Data required to construct a ``WHERE ... NOT IN (...)`` clause."""

    field_name: str
    """Name of the model attribute to filter on."""
    values: Optional[abc.Collection[T]]
    """Values for ``NOT IN`` clause.

    An empty list or ``None`` will return all rows."""

    def append_to_statement(self, query: "SQLStatement") -> "SQLStatement":
        if self.values is None or not self.values:
            return query  # No filter applied if values is None or empty

        placeholder_expressions: list[exp.Placeholder] = []

        for i, value_item in enumerate(self.values):
            param_key = query.add_parameter(
                value_item, name=query.get_unique_parameter_name(f"{self.field_name}_notin_{i}")
            )
            placeholder_expressions.append(exp.Placeholder(this=param_key))

        in_expr = exp.In(this=exp.column(self.field_name), expressions=placeholder_expressions)
        not_in_condition = exp.Not(this=in_expr)
        query.where(not_in_condition)
        return query


class PaginationFilter(StatementFilter, ABC):
    """Subclass for methods that function as a pagination type."""

    @abstractmethod
    def append_to_statement(self, query: "SQLStatement") -> "SQLStatement":
        raise NotImplementedError


@dataclass
class LimitOffset(PaginationFilter):
    """Data required to add limit/offset filtering to a query."""

    limit: int
    """Value for ``LIMIT`` clause of query."""
    offset: int
    """Value for ``OFFSET`` clause of query."""

    def append_to_statement(self, query: "SQLStatement") -> "SQLStatement":
        query.limit(self.limit, use_parameter=True)
        query.offset(self.offset, use_parameter=True)
        return query


@dataclass
class OrderBy(StatementFilter):
    """Data required to construct a ``ORDER BY ...`` clause."""

    field_name: str
    """Name of the model attribute to sort on."""
    sort_order: Literal["asc", "desc"] = "asc"
    """Sort ascending or descending"""

    def append_to_statement(self, query: "SQLStatement") -> "SQLStatement":
        normalized_sort_order = self.sort_order.lower()
        if normalized_sort_order not in {"asc", "desc"}:
            normalized_sort_order = "asc"

        if normalized_sort_order == "desc":
            query.order_by(exp.column(self.field_name).desc())
        else:
            query.order_by(exp.column(self.field_name).asc())
        return query


@dataclass
class SearchFilter(StatementFilter):
    """Data required to construct a ``WHERE field_name LIKE '%' || :value || '%'`` clause."""

    field_name: Union[str, set[str]]
    """Name of the model attribute to search on."""
    value: str
    """Search value."""
    ignore_case: Optional[bool] = False
    """Should the search be case insensitive."""

    def append_to_statement(self, query: "SQLStatement") -> "SQLStatement":
        if not self.value:
            return query

        search_value_with_wildcards = f"%{self.value}%"
        search_val_param_name = query.add_parameter(
            search_value_with_wildcards, name=query.get_unique_parameter_name("search_val")
        )

        pattern_expr = exp.Placeholder(this=search_val_param_name)
        like_op = exp.ILike if self.ignore_case else exp.Like

        if isinstance(self.field_name, str):
            condition = like_op(this=exp.column(self.field_name), expression=pattern_expr)
            query.where(condition)
        elif isinstance(self.field_name, set) and self.field_name:
            field_conditions = [like_op(this=exp.column(field), expression=pattern_expr) for field in self.field_name]
            if not field_conditions:
                return query

            final_condition: Condition = field_conditions[0]
            for cond in field_conditions[1:]:
                final_condition = exp.Or(this=final_condition, expression=cond)
            query.where(final_condition)

        return query


@dataclass
class NotInSearchFilter(SearchFilter):
    """Data required to construct a ``WHERE field_name NOT LIKE '%' || :value || '%'`` clause."""

    def append_to_statement(self, query: "SQLStatement") -> "SQLStatement":
        if not self.value:
            return query

        search_value_with_wildcards = f"%{self.value}%"
        search_val_param_name = query.add_parameter(
            search_value_with_wildcards, name=query.get_unique_parameter_name("not_search_val")
        )

        pattern_expr = exp.Placeholder(this=search_val_param_name)
        like_op = exp.ILike if self.ignore_case else exp.Like

        if isinstance(self.field_name, str):
            condition = exp.Not(this=like_op(this=exp.column(self.field_name), expression=pattern_expr))
            query.where(condition)
        elif isinstance(self.field_name, set) and self.field_name:
            field_conditions: list[Expression] = [
                exp.Not(this=like_op(this=exp.column(field), expression=pattern_expr)) for field in self.field_name
            ]
            if not field_conditions:
                return query

            final_condition = field_conditions[0]
            for cond in field_conditions[1:]:
                final_condition = exp.And(this=final_condition, expression=cond)
            query.where(final_condition)

        return query


def apply_filter(query: "SQLStatement", filter_obj: StatementFilter) -> "SQLStatement":
    """Apply a statement filter to a SQL query object.

    Args:
        query: The SQL query object to modify.
        filter_obj: The filter to apply.

    Returns:
        The modified query object.
    """
    return filter_obj.append_to_statement(query)


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
