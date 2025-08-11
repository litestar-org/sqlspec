"""Filter system for SQL statement manipulation.

This module provides filters that can be applied to SQL statements to add
WHERE clauses, ORDER BY clauses, LIMIT/OFFSET, and other modifications.

Components:
- StatementFilter: Abstract base class for all filters
- BeforeAfterFilter: Date range filtering
- InCollectionFilter: IN clause filtering
- LimitOffsetFilter: Pagination support
- OrderByFilter: Sorting support
- SearchFilter: Text search filtering
- Various collection and negation filters

Features:
- Parameter conflict resolution
- Type-safe filter application
- Cacheable filter configurations
"""

import uuid
from abc import ABC, abstractmethod
from collections import abc
from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, Generic, Literal, Optional, Union

from sqlglot import exp
from typing_extensions import TypeAlias, TypeVar

if TYPE_CHECKING:
    from sqlglot.expressions import Condition

    from sqlspec.core.statement import SQL

__all__ = (
    "AnyCollectionFilter",
    "BeforeAfterFilter",
    "FilterTypeT",
    "FilterTypes",
    "InAnyFilter",
    "InCollectionFilter",
    "LimitOffsetFilter",
    "NotAnyCollectionFilter",
    "NotInCollectionFilter",
    "NotInSearchFilter",
    "OffsetPagination",
    "OnBeforeAfterFilter",
    "OrderByFilter",
    "PaginationFilter",
    "SearchFilter",
    "StatementFilter",
    "apply_filter",
)

T = TypeVar("T")
FilterTypeT = TypeVar("FilterTypeT", bound="StatementFilter")


class StatementFilter(ABC):
    """Abstract base class for filters that can be appended to a statement."""

    __slots__ = ()

    @abstractmethod
    def append_to_statement(self, statement: "SQL") -> "SQL":
        """Append the filter to the statement.

        This method should modify the SQL expression only, not the parameters.
        Parameters should be provided via extract_parameters().
        """

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract parameters that this filter contributes.

        Returns:
            Tuple of (positional_parameters, named_parameters) where:
            - positional_parameters: List of positional parameter values
            - named_parameters: Dict of parameter name to value
        """
        return [], {}

    def _resolve_parameter_conflicts(self, statement: "SQL", proposed_names: list[str]) -> list[str]:
        """Resolve parameter name conflicts.

        Args:
            statement: The SQL statement to check for existing parameters
            proposed_names: List of proposed parameter names

        Returns:
            List of resolved parameter names (same length as proposed_names)
        """
        existing_params = set(statement._named_parameters.keys())
        existing_params.update(statement.parameters.keys() if isinstance(statement.parameters, dict) else [])

        resolved_names = []
        for name in proposed_names:
            if name in existing_params:
                unique_suffix = str(uuid.uuid4()).replace("-", "")[:8]
                resolved_name = f"{name}_{unique_suffix}"
            else:
                resolved_name = name
            resolved_names.append(resolved_name)
            existing_params.add(resolved_name)

        return resolved_names

    @abstractmethod
    def get_cache_key(self) -> tuple[Any, ...]:
        """Return a cache key for this filter's configuration.

        Returns:
            Tuple of hashable values representing the filter's configuration
        """


class BeforeAfterFilter(StatementFilter):
    """Filter for datetime range queries.

    Applies WHERE clauses for before/after datetime filtering.
    """

    __slots__ = ("_param_name_after", "_param_name_before", "after", "before", "field_name")

    field_name: str
    before: Optional[datetime]
    after: Optional[datetime]

    def __init__(self, field_name: str, before: Optional[datetime] = None, after: Optional[datetime] = None) -> None:
        """Initialize the BeforeAfterFilter.

        Args:
            field_name: Name of the model attribute to filter on.
            before: Filter results where field earlier than this.
            after: Filter results where field later than this.
        """
        self.field_name = field_name
        self.before = before
        self.after = after

        self._param_name_before: Optional[str] = None
        self._param_name_after: Optional[str] = None

        if self.before:
            self._param_name_before = f"{self.field_name}_before"
        if self.after:
            self._param_name_after = f"{self.field_name}_after"

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        named_parameters = {}
        if self.before and self._param_name_before:
            named_parameters[self._param_name_before] = self.before
        if self.after and self._param_name_after:
            named_parameters[self._param_name_after] = self.after
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        """Apply filter to SQL expression only."""
        conditions: list[Condition] = []
        col_expr = exp.column(self.field_name)

        # Resolve parameter name conflicts
        proposed_names = []
        if self.before and self._param_name_before:
            proposed_names.append(self._param_name_before)
        if self.after and self._param_name_after:
            proposed_names.append(self._param_name_after)

        if not proposed_names:
            return statement

        resolved_names = self._resolve_parameter_conflicts(statement, proposed_names)

        param_idx = 0
        result = statement
        if self.before and self._param_name_before:
            before_param_name = resolved_names[param_idx]
            param_idx += 1
            conditions.append(exp.LT(this=col_expr, expression=exp.Placeholder(this=before_param_name)))
            result = result.add_named_parameter(before_param_name, self.before)

        if self.after and self._param_name_after:
            after_param_name = resolved_names[param_idx]
            conditions.append(exp.GT(this=col_expr, expression=exp.Placeholder(this=after_param_name)))
            result = result.add_named_parameter(after_param_name, self.after)

        final_condition = conditions[0]
        for cond in conditions[1:]:
            final_condition = exp.And(this=final_condition, expression=cond)
        return result.where(final_condition)

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        return ("BeforeAfterFilter", self.field_name, self.before, self.after)


class OnBeforeAfterFilter(StatementFilter):
    """Data required to filter a query on a ``datetime`` column."""

    __slots__ = ("_param_name_on_or_after", "_param_name_on_or_before", "field_name", "on_or_after", "on_or_before")

    field_name: str
    on_or_before: Optional[datetime]
    on_or_after: Optional[datetime]

    def __init__(
        self, field_name: str, on_or_before: Optional[datetime] = None, on_or_after: Optional[datetime] = None
    ) -> None:
        """Initialize the OnBeforeAfterFilter.

        Args:
            field_name: Name of the model attribute to filter on.
            on_or_before: Filter results where field is on or earlier than this.
            on_or_after: Filter results where field on or later than this.
        """
        self.field_name = field_name
        self.on_or_before = on_or_before
        self.on_or_after = on_or_after

        self._param_name_on_or_before: Optional[str] = None
        self._param_name_on_or_after: Optional[str] = None

        if self.on_or_before:
            self._param_name_on_or_before = f"{self.field_name}_on_or_before"
        if self.on_or_after:
            self._param_name_on_or_after = f"{self.field_name}_on_or_after"

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        named_parameters = {}
        if self.on_or_before and self._param_name_on_or_before:
            named_parameters[self._param_name_on_or_before] = self.on_or_before
        if self.on_or_after and self._param_name_on_or_after:
            named_parameters[self._param_name_on_or_after] = self.on_or_after
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        conditions: list[Condition] = []

        # Resolve parameter name conflicts
        proposed_names = []
        if self.on_or_before and self._param_name_on_or_before:
            proposed_names.append(self._param_name_on_or_before)
        if self.on_or_after and self._param_name_on_or_after:
            proposed_names.append(self._param_name_on_or_after)

        if not proposed_names:
            return statement

        resolved_names = self._resolve_parameter_conflicts(statement, proposed_names)

        param_idx = 0
        result = statement
        if self.on_or_before and self._param_name_on_or_before:
            before_param_name = resolved_names[param_idx]
            param_idx += 1
            conditions.append(
                exp.LTE(this=exp.column(self.field_name), expression=exp.Placeholder(this=before_param_name))
            )
            result = result.add_named_parameter(before_param_name, self.on_or_before)

        if self.on_or_after and self._param_name_on_or_after:
            after_param_name = resolved_names[param_idx]
            conditions.append(
                exp.GTE(this=exp.column(self.field_name), expression=exp.Placeholder(this=after_param_name))
            )
            result = result.add_named_parameter(after_param_name, self.on_or_after)

        final_condition = conditions[0]
        for cond in conditions[1:]:
            final_condition = exp.And(this=final_condition, expression=cond)
        return result.where(final_condition)

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        return ("OnBeforeAfterFilter", self.field_name, self.on_or_before, self.on_or_after)


class InAnyFilter(StatementFilter, ABC, Generic[T]):
    """Subclass for methods that have a `prefer_any` attribute."""

    __slots__ = ()

    def append_to_statement(self, statement: "SQL") -> "SQL":
        raise NotImplementedError


class InCollectionFilter(InAnyFilter[T]):
    """Filter for IN clause queries.

    Constructs WHERE ... IN (...) clauses.
    """

    __slots__ = ("_param_names", "field_name", "values")

    field_name: str
    values: Optional[abc.Collection[T]]

    def __init__(self, field_name: str, values: Optional[abc.Collection[T]]) -> None:
        """Initialize the InCollectionFilter.

        Args:
            field_name: Name of the model attribute to filter on.
            values: Values for ``IN`` clause. An empty list will return an empty result set,
                however, if ``None``, the filter is not applied to the query, and all rows are returned.
        """
        self.field_name = field_name
        self.values = values

        self._param_names: list[str] = []
        if self.values:
            for i, _ in enumerate(self.values):
                self._param_names.append(f"{self.field_name}_in_{i}")

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        named_parameters = {}
        if self.values:
            for i, value in enumerate(self.values):
                named_parameters[self._param_names[i]] = value
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        if self.values is None:
            return statement

        if not self.values:
            return statement.where(exp.false())

        # Resolve parameter name conflicts
        resolved_names = self._resolve_parameter_conflicts(statement, self._param_names)

        placeholder_expressions: list[exp.Placeholder] = [
            exp.Placeholder(this=param_name) for param_name in resolved_names
        ]

        result = statement.where(exp.In(this=exp.column(self.field_name), expressions=placeholder_expressions))

        # Add parameters with resolved names
        for resolved_name, value in zip(resolved_names, self.values):
            result = result.add_named_parameter(resolved_name, value)
        return result

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        values_tuple = tuple(self.values) if self.values is not None else None
        return ("InCollectionFilter", self.field_name, values_tuple)


class NotInCollectionFilter(InAnyFilter[T]):
    """Data required to construct a ``WHERE ... NOT IN (...)`` clause."""

    __slots__ = ("_param_names", "field_name", "values")

    field_name: str
    values: Optional[abc.Collection[T]]

    def __init__(self, field_name: str, values: Optional[abc.Collection[T]]) -> None:
        """Initialize the NotInCollectionFilter.

        Args:
            field_name: Name of the model attribute to filter on.
            values: Values for ``NOT IN`` clause. An empty list or ``None`` will return all rows.
        """
        self.field_name = field_name
        self.values = values

        self._param_names: list[str] = []
        if self.values:
            for i, _ in enumerate(self.values):
                self._param_names.append(f"{self.field_name}_notin_{i}_{id(self)}")

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        named_parameters = {}
        if self.values:
            for i, value in enumerate(self.values):
                named_parameters[self._param_names[i]] = value
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        if self.values is None or not self.values:
            return statement

        # Resolve parameter name conflicts
        resolved_names = self._resolve_parameter_conflicts(statement, self._param_names)

        placeholder_expressions: list[exp.Placeholder] = [
            exp.Placeholder(this=param_name) for param_name in resolved_names
        ]

        result = statement.where(
            exp.Not(this=exp.In(this=exp.column(self.field_name), expressions=placeholder_expressions))
        )

        # Add parameters with resolved names
        for resolved_name, value in zip(resolved_names, self.values):
            result = result.add_named_parameter(resolved_name, value)
        return result

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        values_tuple = tuple(self.values) if self.values is not None else None
        return ("NotInCollectionFilter", self.field_name, values_tuple)


class AnyCollectionFilter(InAnyFilter[T]):
    """Data required to construct a ``WHERE column_name = ANY (array_expression)`` clause."""

    __slots__ = ("_param_names", "field_name", "values")

    field_name: str
    values: Optional[abc.Collection[T]]

    def __init__(self, field_name: str, values: Optional[abc.Collection[T]]) -> None:
        """Initialize the AnyCollectionFilter.

        Args:
            field_name: Name of the model attribute to filter on.
            values: Values for ``= ANY (...)`` clause. An empty list will result in a condition
                that is always false (no rows returned). If ``None``, the filter is not applied
                to the query, and all rows are returned.
        """
        self.field_name = field_name
        self.values = values

        self._param_names: list[str] = []
        if self.values:
            for i, _ in enumerate(self.values):
                self._param_names.append(f"{self.field_name}_any_{i}")

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        named_parameters = {}
        if self.values:
            for i, value in enumerate(self.values):
                named_parameters[self._param_names[i]] = value
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        if self.values is None:
            return statement

        if not self.values:
            return statement.where(exp.false())

        # Resolve parameter name conflicts
        resolved_names = self._resolve_parameter_conflicts(statement, self._param_names)

        placeholder_expressions: list[exp.Expression] = [
            exp.Placeholder(this=param_name) for param_name in resolved_names
        ]

        array_expr = exp.Array(expressions=placeholder_expressions)
        result = statement.where(exp.EQ(this=exp.column(self.field_name), expression=exp.Any(this=array_expr)))

        # Add parameters with resolved names
        for resolved_name, value in zip(resolved_names, self.values):
            result = result.add_named_parameter(resolved_name, value)
        return result

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        values_tuple = tuple(self.values) if self.values is not None else None
        return ("AnyCollectionFilter", self.field_name, values_tuple)


class NotAnyCollectionFilter(InAnyFilter[T]):
    """Data required to construct a ``WHERE NOT (column_name = ANY (array_expression))`` clause."""

    __slots__ = ("_param_names", "field_name", "values")

    def __init__(self, field_name: str, values: Optional[abc.Collection[T]]) -> None:
        """Initialize the NotAnyCollectionFilter.

        Args:
            field_name: Name of the model attribute to filter on.
            values: Values for ``NOT (... = ANY (...))`` clause. An empty list will result in a
                condition that is always true (all rows returned, filter effectively ignored).
                If ``None``, the filter is not applied to the query, and all rows are returned.
        """
        self.field_name = field_name
        self.values = values

        self._param_names: list[str] = []
        if self.values:
            for i, _ in enumerate(self.values):
                self._param_names.append(f"{self.field_name}_not_any_{i}")

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        named_parameters = {}
        if self.values:
            for i, value in enumerate(self.values):
                named_parameters[self._param_names[i]] = value
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        if self.values is None or not self.values:
            return statement

        # Resolve parameter name conflicts
        resolved_names = self._resolve_parameter_conflicts(statement, self._param_names)

        placeholder_expressions: list[exp.Expression] = [
            exp.Placeholder(this=param_name) for param_name in resolved_names
        ]

        array_expr = exp.Array(expressions=placeholder_expressions)
        condition = exp.EQ(this=exp.column(self.field_name), expression=exp.Any(this=array_expr))
        result = statement.where(exp.Not(this=condition))

        # Add parameters with resolved names
        for resolved_name, value in zip(resolved_names, self.values):
            result = result.add_named_parameter(resolved_name, value)
        return result

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        values_tuple = tuple(self.values) if self.values is not None else None
        return ("NotAnyCollectionFilter", self.field_name, values_tuple)


class PaginationFilter(StatementFilter, ABC):
    """Subclass for methods that function as a pagination type."""

    __slots__ = ()

    @abstractmethod
    def append_to_statement(self, statement: "SQL") -> "SQL":
        raise NotImplementedError


class LimitOffsetFilter(PaginationFilter):
    """Data required to add limit/offset filtering to a query."""

    __slots__ = ("_limit_param_name", "_offset_param_name", "limit", "offset")

    limit: int
    offset: int

    def __init__(self, limit: int, offset: int) -> None:
        """Initialize the LimitOffsetFilter.

        Args:
            limit: Value for ``LIMIT`` clause of query.
            offset: Value for ``OFFSET`` clause of query.
        """
        self.limit = limit
        self.offset = offset

        self._limit_param_name = "limit"
        self._offset_param_name = "offset"

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        return [], {self._limit_param_name: self.limit, self._offset_param_name: self.offset}

    def append_to_statement(self, statement: "SQL") -> "SQL":
        import sqlglot
        from sqlglot import exp

        # Resolve parameter name conflicts
        resolved_names = self._resolve_parameter_conflicts(statement, [self._limit_param_name, self._offset_param_name])
        limit_param_name, offset_param_name = resolved_names

        limit_placeholder = exp.Placeholder(this=limit_param_name)
        offset_placeholder = exp.Placeholder(this=offset_param_name)

        # Parse the current SQL to get the statement structure
        try:
            current_statement = sqlglot.parse_one(statement._raw_sql, dialect=getattr(statement, "_dialect", None))
        except Exception:
            # Fallback to wrapping in subquery if parsing fails
            current_statement = exp.Select().from_(f"({statement._raw_sql})")

        if isinstance(current_statement, exp.Select):
            new_statement = current_statement.limit(limit_placeholder).offset(offset_placeholder)
        else:
            # Wrap non-SELECT statements in a subquery
            new_statement = exp.Select().from_(current_statement).limit(limit_placeholder).offset(offset_placeholder)

        result = statement.copy(statement=new_statement)

        result = result.add_named_parameter(limit_param_name, self.limit)
        return result.add_named_parameter(offset_param_name, self.offset)

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        return ("LimitOffsetFilter", self.limit, self.offset)


class OrderByFilter(StatementFilter):
    """Data required to construct a ``ORDER BY ...`` clause."""

    __slots__ = ("field_name", "sort_order")

    field_name: str
    sort_order: Literal["asc", "desc"]

    def __init__(self, field_name: str, sort_order: Literal["asc", "desc"] = "asc") -> None:
        """Initialize the OrderByFilter.

        Args:
            field_name: Name of the model attribute to sort on.
            sort_order: Sort ascending or descending.
        """
        self.field_name = field_name
        self.sort_order = sort_order

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        return [], {}

    def append_to_statement(self, statement: "SQL") -> "SQL":
        converted_sort_order = self.sort_order.lower()
        if converted_sort_order not in {"asc", "desc"}:
            converted_sort_order = "asc"

        col_expr = exp.column(self.field_name)
        order_expr = col_expr.desc() if converted_sort_order == "desc" else col_expr.asc()

        if statement._statement is None:
            new_statement = exp.Select().order_by(order_expr)
        elif isinstance(statement._statement, exp.Select):
            new_statement = statement._statement.order_by(order_expr)
        else:
            new_statement = exp.Select().from_(statement._statement).order_by(order_expr)

        return statement.copy(statement=new_statement)

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        return ("OrderByFilter", self.field_name, self.sort_order)


class SearchFilter(StatementFilter):
    """Filter for text search queries.

    Constructs WHERE field_name LIKE '%value%' clauses.
    """

    __slots__ = ("_param_name", "field_name", "ignore_case", "value")

    field_name: Union[str, set[str]]
    value: str
    ignore_case: Optional[bool]

    def __init__(self, field_name: Union[str, set[str]], value: str, ignore_case: Optional[bool] = False) -> None:
        """Initialize the SearchFilter.

        Args:
            field_name: Name of the model attribute to search on.
            value: Search value.
            ignore_case: Should the search be case insensitive.
        """
        self.field_name = field_name
        self.value = value
        self.ignore_case = ignore_case

        self._param_name: Optional[str] = None
        if self.value:
            if isinstance(self.field_name, str):
                self._param_name = f"{self.field_name}_search"
            else:
                self._param_name = "search_value"

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        named_parameters = {}
        if self.value and self._param_name:
            search_value_with_wildcards = f"%{self.value}%"
            named_parameters[self._param_name] = search_value_with_wildcards
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        if not self.value or not self._param_name:
            return statement

        # Resolve parameter name conflicts
        resolved_names = self._resolve_parameter_conflicts(statement, [self._param_name])
        param_name = resolved_names[0]

        pattern_expr = exp.Placeholder(this=param_name)
        like_op = exp.ILike if self.ignore_case else exp.Like

        if isinstance(self.field_name, str):
            result = statement.where(like_op(this=exp.column(self.field_name), expression=pattern_expr))
        elif isinstance(self.field_name, set) and self.field_name:
            field_conditions: list[Condition] = [
                like_op(this=exp.column(field), expression=pattern_expr) for field in self.field_name
            ]
            if not field_conditions:
                return statement

            final_condition: Condition = field_conditions[0]
            for cond in field_conditions[1:]:
                final_condition = exp.Or(this=final_condition, expression=cond)
            result = statement.where(final_condition)
        else:
            result = statement

        # Add parameter with resolved name
        search_value_with_wildcards = f"%{self.value}%"
        return result.add_named_parameter(param_name, search_value_with_wildcards)

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        field_names = tuple(sorted(self.field_name)) if isinstance(self.field_name, set) else self.field_name
        return ("SearchFilter", field_names, self.value, self.ignore_case)


class NotInSearchFilter(SearchFilter):
    """Data required to construct a ``WHERE field_name NOT LIKE '%' || :value || '%'`` clause."""

    __slots__ = ()

    def __init__(self, field_name: Union[str, set[str]], value: str, ignore_case: Optional[bool] = False) -> None:
        """Initialize the NotInSearchFilter.

        Args:
            field_name: Name of the model attribute to search on.
            value: Search value.
            ignore_case: Should the search be case insensitive.
        """
        super().__init__(field_name, value, ignore_case)

        self._param_name: Optional[str] = None
        if self.value:
            if isinstance(self.field_name, str):
                self._param_name = f"{self.field_name}_not_search"
            else:
                self._param_name = "not_search_value"

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        named_parameters = {}
        if self.value and self._param_name:
            search_value_with_wildcards = f"%{self.value}%"
            named_parameters[self._param_name] = search_value_with_wildcards
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        if not self.value or not self._param_name:
            return statement

        # Resolve parameter name conflicts
        resolved_names = self._resolve_parameter_conflicts(statement, [self._param_name])
        param_name = resolved_names[0]

        pattern_expr = exp.Placeholder(this=param_name)
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

        # Add parameter with resolved name
        search_value_with_wildcards = f"%{self.value}%"
        return result.add_named_parameter(param_name, search_value_with_wildcards)

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        field_names = tuple(sorted(self.field_name)) if isinstance(self.field_name, set) else self.field_name
        return ("NotInSearchFilter", field_names, self.value, self.ignore_case)


class OffsetPagination(Generic[T]):
    """Container for data returned using limit/offset pagination."""

    __slots__ = ("items", "limit", "offset", "total")

    items: Sequence[T]
    limit: int
    offset: int
    total: int

    def __init__(self, items: Sequence[T], limit: int, offset: int, total: int) -> None:
        """Initialize OffsetPagination.

        Args:
            items: List of data being sent as part of the response.
            limit: Maximal number of items to send.
            offset: Offset from the beginning of the query. Identical to an index.
            total: Total number of items.
        """
        self.items = items
        self.limit = limit
        self.offset = offset
        self.total = total


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
