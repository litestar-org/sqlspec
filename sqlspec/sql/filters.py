"""Collection filter datastructures."""

from abc import ABC, abstractmethod
from collections import abc
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Generic, Literal, Optional, Protocol, Union, runtime_checkable

from sqlglot import exp
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
        conditions: list[Condition] = []
        col_expr = exp.column(self.field_name)

        if self.before:
            param_name = query.get_unique_parameter_name(f"{self.field_name}_before")
            query = query.add_named_parameter(param_name, self.before)
            conditions.append(exp.LT(this=col_expr, expression=exp.Placeholder(this=param_name)))
        if self.after:
            param_name = query.get_unique_parameter_name(f"{self.field_name}_after")
            query = query.add_named_parameter(param_name, self.after)
            conditions.append(exp.GT(this=col_expr, expression=exp.Placeholder(this=param_name)))

        if conditions:
            final_condition = conditions[0]
            for cond in conditions[1:]:
                final_condition = exp.And(this=final_condition, expression=cond)
            query = query.where(final_condition)
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
        conditions: list[Condition] = []

        if self.on_or_before:
            param_name = query.get_unique_parameter_name(f"{self.field_name}_on_or_before")
            query = query.add_named_parameter(param_name, self.on_or_before)
            conditions.append(exp.LTE(this=exp.column(self.field_name), expression=exp.Placeholder(this=param_name)))
        if self.on_or_after:
            param_name = query.get_unique_parameter_name(f"{self.field_name}_on_or_after")
            query = query.add_named_parameter(param_name, self.on_or_after)
            conditions.append(exp.GTE(this=exp.column(self.field_name), expression=exp.Placeholder(this=param_name)))

        if conditions:
            final_condition = conditions[0]
            for cond in conditions[1:]:
                final_condition = exp.And(this=final_condition, expression=cond)
            query = query.where(final_condition)
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

        if not self.values:
            return query.where(exp.false())

        placeholder_expressions: list[exp.Placeholder] = []

        for i, value_item in enumerate(self.values):
            param_key = query.get_unique_parameter_name(f"{self.field_name}_in_{i}")
            query = query.add_named_parameter(param_key, value_item)
            placeholder_expressions.append(exp.Placeholder(this=param_key))

        return query.where(exp.In(this=exp.column(self.field_name), expressions=placeholder_expressions))


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
            return query

        placeholder_expressions: list[exp.Placeholder] = []

        for i, value_item in enumerate(self.values):
            param_key = query.get_unique_parameter_name(f"{self.field_name}_notin_{i}")
            query = query.add_named_parameter(param_key, value_item)
            placeholder_expressions.append(exp.Placeholder(this=param_key))

        return query.where(exp.Not(this=exp.In(this=exp.column(self.field_name), expressions=placeholder_expressions)))


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
        return query.limit(self.limit, use_parameter=True).offset(self.offset, use_parameter=True)


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
            return query.order_by(exp.column(self.field_name).desc())
        return query.order_by(exp.column(self.field_name).asc())


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
        search_val_param_name = query.get_unique_parameter_name("search_val")
        query = query.add_named_parameter(search_val_param_name, search_value_with_wildcards)

        pattern_expr = exp.Placeholder(this=search_val_param_name)
        like_op = exp.ILike if self.ignore_case else exp.Like

        if isinstance(self.field_name, str):
            return query.where(like_op(this=exp.column(self.field_name), expression=pattern_expr))
        if isinstance(self.field_name, set) and self.field_name:
            field_conditions: list[Condition] = [
                like_op(this=exp.column(field), expression=pattern_expr) for field in self.field_name
            ]
            if not field_conditions:
                return query

            final_condition: Condition = field_conditions[0]
            if len(field_conditions) > 1:
                for cond in field_conditions[1:]:
                    final_condition = exp.Or(this=final_condition, expression=cond)
            return query.where(final_condition)

        return query


@dataclass
class NotInSearchFilter(SearchFilter):
    """Data required to construct a ``WHERE field_name NOT LIKE '%' || :value || '%'`` clause."""

    def append_to_statement(self, query: "SQLStatement") -> "SQLStatement":
        if not self.value:
            return query

        search_value_with_wildcards = f"%{self.value}%"
        search_val_param_name = query.get_unique_parameter_name("not_search_val")
        query = query.add_named_parameter(search_val_param_name, search_value_with_wildcards)

        pattern_expr = exp.Placeholder(this=search_val_param_name)
        like_op = exp.ILike if self.ignore_case else exp.Like

        if isinstance(self.field_name, str):
            return query.where(exp.Not(this=like_op(this=exp.column(self.field_name), expression=pattern_expr)))
        if isinstance(self.field_name, set) and self.field_name:
            field_conditions: list[Condition] = [
                exp.Not(this=like_op(this=exp.column(field), expression=pattern_expr)) for field in self.field_name
            ]
            if not field_conditions:
                return query

            final_condition: Condition = field_conditions[0]
            if len(field_conditions) > 1:
                for cond in field_conditions[1:]:
                    final_condition = exp.And(this=final_condition, expression=cond)
            return query.where(final_condition)

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
