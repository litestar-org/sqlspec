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

    from sqlspec.statement import SQL

__all__ = (
    "AnyCollectionFilter",
    "BeforeAfterFilter",
    "FilterTypes",
    "InAnyFilter",
    "InCollectionFilter",
    "LimitOffsetFilter",
    "NotAnyCollectionFilter",
    "NotInCollectionFilter",
    "NotInSearchFilter",
    "OnBeforeAfterFilter",
    "OrderByFilter",
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
    def append_to_statement(self, statement: "SQL") -> "SQL":
        """Append the filter to the statement.

        This method should modify the SQL expression only, not the parameters.
        Parameters should be provided via extract_parameters().
        """
        ...

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract parameters that this filter contributes.

        Returns:
            Tuple of (positional_params, named_params) where:
            - positional_params: List of positional parameter values
            - named_params: Dict of parameter name to value
        """
        return [], {}


@dataclass
class BeforeAfterFilter(StatementFilter):
    """Data required to filter a query on a ``datetime`` column.

    Note:
        After applying this filter, only the filter's parameters (e.g., before/after) will be present in the resulting SQL statement's parameters. Original parameters from the statement are not preserved in the result.
    """

    field_name: str
    """Name of the model attribute to filter on."""
    before: Optional[datetime] = None
    """Filter results where field earlier than this."""
    after: Optional[datetime] = None
    """Filter results where field later than this."""

    def __post_init__(self) -> None:
        """Initialize parameter names."""
        self._param_name_before: Optional[str] = None
        self._param_name_after: Optional[str] = None

        if self.before:
            self._param_name_before = f"{self.field_name}_before"
        if self.after:
            self._param_name_after = f"{self.field_name}_after"

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        named_params = {}
        if self.before and self._param_name_before:
            named_params[self._param_name_before] = self.before
        if self.after and self._param_name_after:
            named_params[self._param_name_after] = self.after
        return [], named_params

    def append_to_statement(self, statement: "SQL") -> "SQL":
        """Apply filter to SQL expression only."""
        conditions: list[Condition] = []
        col_expr = exp.column(self.field_name)

        if self.before and self._param_name_before:
            conditions.append(exp.LT(this=col_expr, expression=exp.Placeholder(this=self._param_name_before)))
        if self.after and self._param_name_after:
            conditions.append(exp.GT(this=col_expr, expression=exp.Placeholder(this=self._param_name_after)))

        if conditions:
            final_condition = conditions[0]
            for cond in conditions[1:]:
                final_condition = exp.And(this=final_condition, expression=cond)
            # Use the SQL object's where method which handles all cases
            result = statement.where(final_condition)
            # Add the filter's parameters to the result
            _, named_params = self.extract_parameters()
            for name, value in named_params.items():
                result = result.add_named_parameter(name, value)
            return result
        return statement


@dataclass
class OnBeforeAfterFilter(StatementFilter):
    """Data required to filter a query on a ``datetime`` column."""

    field_name: str
    """Name of the model attribute to filter on."""
    on_or_before: Optional[datetime] = None
    """Filter results where field is on or earlier than this."""
    on_or_after: Optional[datetime] = None
    """Filter results where field on or later than this."""

    def __post_init__(self) -> None:
        """Initialize parameter names."""
        self._param_name_on_or_before: Optional[str] = None
        self._param_name_on_or_after: Optional[str] = None

        if self.on_or_before:
            self._param_name_on_or_before = f"{self.field_name}_on_or_before"
        if self.on_or_after:
            self._param_name_on_or_after = f"{self.field_name}_on_or_after"

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        named_params = {}
        if self.on_or_before and self._param_name_on_or_before:
            named_params[self._param_name_on_or_before] = self.on_or_before
        if self.on_or_after and self._param_name_on_or_after:
            named_params[self._param_name_on_or_after] = self.on_or_after
        return [], named_params

    def append_to_statement(self, statement: "SQL") -> "SQL":
        conditions: list[Condition] = []

        if self.on_or_before and self._param_name_on_or_before:
            conditions.append(
                exp.LTE(
                    this=exp.column(self.field_name), expression=exp.Placeholder(this=self._param_name_on_or_before)
                )
            )
        if self.on_or_after and self._param_name_on_or_after:
            conditions.append(
                exp.GTE(this=exp.column(self.field_name), expression=exp.Placeholder(this=self._param_name_on_or_after))
            )

        if conditions:
            final_condition = conditions[0]
            for cond in conditions[1:]:
                final_condition = exp.And(this=final_condition, expression=cond)
            result = statement.where(final_condition)
            # Add the filter's parameters to the result
            _, named_params = self.extract_parameters()
            for name, value in named_params.items():
                result = result.add_named_parameter(name, value)
            return result
        return statement


class InAnyFilter(StatementFilter, ABC, Generic[T]):
    """Subclass for methods that have a `prefer_any` attribute."""

    @abstractmethod
    def append_to_statement(self, statement: "SQL") -> "SQL":
        raise NotImplementedError


@dataclass
class InCollectionFilter(InAnyFilter[T]):
    """Data required to construct a ``WHERE ... IN (...)`` clause.

    Note:
        After applying this filter, only the filter's parameters (e.g., the generated IN parameters) will be present in the resulting SQL statement's parameters. Original parameters from the statement are not preserved in the result.
    """

    field_name: str
    """Name of the model attribute to filter on."""
    values: Optional[abc.Collection[T]]
    """Values for ``IN`` clause.

    An empty list will return an empty result set, however, if ``None``, the filter is not applied to the query, and all rows are returned. """

    def __post_init__(self) -> None:
        """Initialize parameter names."""
        self._param_names: list[str] = []
        if self.values:
            for i, _ in enumerate(self.values):
                self._param_names.append(f"{self.field_name}_in_{i}")

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        named_params = {}
        if self.values:
            for i, value in enumerate(self.values):
                named_params[self._param_names[i]] = value
        return [], named_params

    def append_to_statement(self, statement: "SQL") -> "SQL":
        if self.values is None:
            return statement

        if not self.values:
            return statement.where(exp.false())

        placeholder_expressions: list[exp.Placeholder] = [
            exp.Placeholder(this=param_name) for param_name in self._param_names
        ]

        result = statement.where(exp.In(this=exp.column(self.field_name), expressions=placeholder_expressions))
        # Add the filter's parameters to the result
        _, named_params = self.extract_parameters()
        for name, value in named_params.items():
            result = result.add_named_parameter(name, value)
        return result


@dataclass
class NotInCollectionFilter(InAnyFilter[T]):
    """Data required to construct a ``WHERE ... NOT IN (...)`` clause."""

    field_name: str
    """Name of the model attribute to filter on."""
    values: Optional[abc.Collection[T]]
    """Values for ``NOT IN`` clause.

    An empty list or ``None`` will return all rows."""

    def __post_init__(self) -> None:
        """Initialize parameter names."""
        self._param_names: list[str] = []
        if self.values:
            for i, _ in enumerate(self.values):
                self._param_names.append(f"{self.field_name}_notin_{i}_{id(self)}")

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        named_params = {}
        if self.values:
            for i, value in enumerate(self.values):
                named_params[self._param_names[i]] = value
        return [], named_params

    def append_to_statement(self, statement: "SQL") -> "SQL":
        if self.values is None or not self.values:
            return statement

        placeholder_expressions: list[exp.Placeholder] = [
            exp.Placeholder(this=param_name) for param_name in self._param_names
        ]

        result = statement.where(
            exp.Not(this=exp.In(this=exp.column(self.field_name), expressions=placeholder_expressions))
        )
        # Add the filter's parameters to the result
        _, named_params = self.extract_parameters()
        for name, value in named_params.items():
            result = result.add_named_parameter(name, value)
        return result


@dataclass
class AnyCollectionFilter(InAnyFilter[T]):
    """Data required to construct a ``WHERE column_name = ANY (array_expression)`` clause."""

    field_name: str
    """Name of the model attribute to filter on."""
    values: Optional[abc.Collection[T]]
    """Values for ``= ANY (...)`` clause.

    An empty list will result in a condition that is always false (no rows returned).
    If ``None``, the filter is not applied to the query, and all rows are returned.
    """

    def __post_init__(self) -> None:
        """Initialize parameter names."""
        self._param_names: list[str] = []
        if self.values:
            for i, _ in enumerate(self.values):
                self._param_names.append(f"{self.field_name}_any_{i}")

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        named_params = {}
        if self.values:
            for i, value in enumerate(self.values):
                named_params[self._param_names[i]] = value
        return [], named_params

    def append_to_statement(self, statement: "SQL") -> "SQL":
        if self.values is None:
            return statement

        if not self.values:
            # column = ANY (empty_array) is generally false
            return statement.where(exp.false())

        placeholder_expressions: list[exp.Expression] = [
            exp.Placeholder(this=param_name) for param_name in self._param_names
        ]

        array_expr = exp.Array(expressions=placeholder_expressions)
        # Generates SQL like: self.field_name = ANY(ARRAY[?, ?, ...])
        result = statement.where(exp.EQ(this=exp.column(self.field_name), expression=exp.Any(this=array_expr)))
        # Add the filter's parameters to the result
        _, named_params = self.extract_parameters()
        for name, value in named_params.items():
            result = result.add_named_parameter(name, value)
        return result


@dataclass
class NotAnyCollectionFilter(InAnyFilter[T]):
    """Data required to construct a ``WHERE NOT (column_name = ANY (array_expression))`` clause."""

    field_name: str
    """Name of the model attribute to filter on."""
    values: Optional[abc.Collection[T]]
    """Values for ``NOT (... = ANY (...))`` clause.

    An empty list will result in a condition that is always true (all rows returned, filter effectively ignored).
    If ``None``, the filter is not applied to the query, and all rows are returned.
    """

    def __post_init__(self) -> None:
        """Initialize parameter names."""
        self._param_names: list[str] = []
        if self.values:
            for i, _ in enumerate(self.values):
                self._param_names.append(f"{self.field_name}_notany_{i}")

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        named_params = {}
        if self.values:
            for i, value in enumerate(self.values):
                named_params[self._param_names[i]] = value
        return [], named_params

    def append_to_statement(self, statement: "SQL") -> "SQL":
        if self.values is None or not self.values:
            # NOT (column = ANY (empty_array)) is generally true
            # So, if values is empty or None, this filter should not restrict results.
            return statement

        placeholder_expressions: list[exp.Expression] = [
            exp.Placeholder(this=param_name) for param_name in self._param_names
        ]

        array_expr = exp.Array(expressions=placeholder_expressions)
        # Generates SQL like: NOT (self.field_name = ANY(ARRAY[?, ?, ...]))
        condition = exp.EQ(this=exp.column(self.field_name), expression=exp.Any(this=array_expr))
        result = statement.where(exp.Not(this=condition))
        # Add the filter's parameters to the result
        _, named_params = self.extract_parameters()
        for name, value in named_params.items():
            result = result.add_named_parameter(name, value)
        return result


class PaginationFilter(StatementFilter, ABC):
    """Subclass for methods that function as a pagination type."""

    @abstractmethod
    def append_to_statement(self, statement: "SQL") -> "SQL":
        raise NotImplementedError


@dataclass
class LimitOffsetFilter(PaginationFilter):
    """Data required to add limit/offset filtering to a query."""

    limit: int
    """Value for ``LIMIT`` clause of query."""
    offset: int
    """Value for ``OFFSET`` clause of query."""

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        # Return the limit and offset values as named parameters
        return [], {"limit": self.limit, "offset": self.offset}

    def append_to_statement(self, statement: "SQL") -> "SQL":
        return statement.limit(self.limit, use_parameter=True).offset(self.offset, use_parameter=True)


@dataclass
class OrderByFilter(StatementFilter):
    """Data required to construct a ``ORDER BY ...`` clause."""

    field_name: str
    """Name of the model attribute to sort on."""
    sort_order: Literal["asc", "desc"] = "asc"
    """Sort ascending or descending"""

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        # ORDER BY doesn't use parameters, only column names and sort direction
        return [], {}

    def append_to_statement(self, statement: "SQL") -> "SQL":
        normalized_sort_order = self.sort_order.lower()
        if normalized_sort_order not in {"asc", "desc"}:
            normalized_sort_order = "asc"
        if normalized_sort_order == "desc":
            return statement.order_by(exp.column(self.field_name).desc())
        return statement.order_by(exp.column(self.field_name).asc())


@dataclass
class SearchFilter(StatementFilter):
    """Data required to construct a ``WHERE field_name LIKE '%' || :value || '%'`` clause.

    Note:
        After applying this filter, only the filter's parameters (e.g., the generated search parameter) will be present in the resulting SQL statement's parameters. Original parameters from the statement are not preserved in the result.
    """

    field_name: Union[str, set[str]]
    """Name of the model attribute to search on."""
    value: str
    """Search value."""
    ignore_case: Optional[bool] = False
    """Should the search be case insensitive."""

    def __post_init__(self) -> None:
        """Initialize parameter names."""
        self._param_name: Optional[str] = None
        if self.value:
            if isinstance(self.field_name, str):
                self._param_name = f"{self.field_name}_search"
            else:
                # For multiple fields, use a generic search parameter name
                self._param_name = "search_value"

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        named_params = {}
        if self.value and self._param_name:
            search_value_with_wildcards = f"%{self.value}%"
            named_params[self._param_name] = search_value_with_wildcards
        return [], named_params

    def append_to_statement(self, statement: "SQL") -> "SQL":
        if not self.value or not self._param_name:
            return statement

        pattern_expr = exp.Placeholder(this=self._param_name)
        like_op = exp.ILike if self.ignore_case else exp.Like

        result = statement
        if isinstance(self.field_name, str):
            result = statement.where(like_op(this=exp.column(self.field_name), expression=pattern_expr))
        elif isinstance(self.field_name, set) and self.field_name:
            field_conditions: list[Condition] = [
                like_op(this=exp.column(field), expression=pattern_expr) for field in self.field_name
            ]
            if not field_conditions:
                return statement

            final_condition: Condition = field_conditions[0]
            if len(field_conditions) > 1:
                for cond in field_conditions[1:]:
                    final_condition = exp.Or(this=final_condition, expression=cond)
            result = statement.where(final_condition)

        # Add the filter's parameters to the result
        _, named_params = self.extract_parameters()
        for name, value in named_params.items():
            result = result.add_named_parameter(name, value)
        return result


@dataclass
class NotInSearchFilter(SearchFilter):
    """Data required to construct a ``WHERE field_name NOT LIKE '%' || :value || '%'`` clause."""

    def __post_init__(self) -> None:
        """Initialize parameter names."""
        self._param_name: Optional[str] = None
        if self.value:
            if isinstance(self.field_name, str):
                self._param_name = f"{self.field_name}_not_search"
            else:
                # For multiple fields, use a generic search parameter name
                self._param_name = "not_search_value"

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        named_params = {}
        if self.value and self._param_name:
            search_value_with_wildcards = f"%{self.value}%"
            named_params[self._param_name] = search_value_with_wildcards
        return [], named_params

    def append_to_statement(self, statement: "SQL") -> "SQL":
        if not self.value or not self._param_name:
            return statement

        pattern_expr = exp.Placeholder(this=self._param_name)
        like_op = exp.ILike if self.ignore_case else exp.Like

        result = statement
        if isinstance(self.field_name, str):
            result = statement.where(exp.Not(this=like_op(this=exp.column(self.field_name), expression=pattern_expr)))
        elif isinstance(self.field_name, set) and self.field_name:
            field_conditions: list[Condition] = [
                exp.Not(this=like_op(this=exp.column(field), expression=pattern_expr)) for field in self.field_name
            ]
            if not field_conditions:
                return statement

            final_condition: Condition = field_conditions[0]
            if len(field_conditions) > 1:
                for cond in field_conditions[1:]:
                    final_condition = exp.And(this=final_condition, expression=cond)
            result = statement.where(final_condition)

        # Add the filter's parameters to the result
        _, named_params = self.extract_parameters()
        for name, value in named_params.items():
            result = result.add_named_parameter(name, value)
        return result


def apply_filter(statement: "SQL", filter_obj: StatementFilter) -> "SQL":
    """Apply a statement filter to a SQL query object.

    Args:
        statement: The SQL query object to modify.
        filter_obj: The filter to apply.

    Returns:
        The modified query object.
    """
    return filter_obj.append_to_statement(statement)


FilterTypes: TypeAlias = Union[
    BeforeAfterFilter,
    OnBeforeAfterFilter,
    InCollectionFilter[Any],
    LimitOffsetFilter,
    OrderByFilter,
    SearchFilter,
    NotInCollectionFilter[Any],
    NotInSearchFilter,
    AnyCollectionFilter[Any],
    NotAnyCollectionFilter[Any],
]
"""Aggregate type alias of the types supported for collection filtering."""
