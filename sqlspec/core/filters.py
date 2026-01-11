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
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeAlias

import sqlglot
from mypy_extensions import mypyc_attr
from sqlglot import exp
from typing_extensions import TypeVar

from sqlspec.utils.type_guards import has_field_name

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
    "NotNullFilter",
    "NullFilter",
    "OffsetPagination",
    "OnBeforeAfterFilter",
    "OrderByFilter",
    "PaginationFilter",
    "SearchFilter",
    "StatementFilter",
    "apply_filter",
    "canonicalize_filters",
    "create_filters",
)

T = TypeVar("T")
FilterTypeT = TypeVar("FilterTypeT", bound="StatementFilter")


@mypyc_attr(allow_interpreted_subclasses=True)
class StatementFilter(ABC):
    """Abstract base class for filters that can be appended to a statement."""

    __slots__ = ()

    @abstractmethod
    def append_to_statement(self, statement: "SQL") -> "SQL":
        """Append the filter to the statement.

        This method should modify the SQL expression only, not the parameters.
        Parameters should be provided via extract_parameters().
        """

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract parameters that this filter contributes.

        Returns:
            Tuple of (positional_parameters, named_parameters) where:
            - positional_parameters: List of positional parameter values
            - named_parameters: Dict of parameter name to value
        """
        return [], {}

    def _resolve_parameter_conflicts(self, statement: "SQL", proposed_names: "list[str]") -> "list[str]":
        """Resolve parameter name conflicts.

        Args:
            statement: The SQL statement to check for existing parameters
            proposed_names: List of proposed parameter names

        Returns:
            List of resolved parameter names (same length as proposed_names)
        """
        existing_params = set(statement.named_parameters.keys())
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
    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return a cache key for this filter's configuration.

        Returns:
            Tuple of hashable values representing the filter's configuration
        """


class BeforeAfterFilter(StatementFilter):
    """Filter for datetime range queries.

    Applies WHERE clauses for before/after datetime filtering.
    """

    __slots__ = ("_after", "_before", "_field_name")

    def __init__(self, field_name: str, before: datetime | None = None, after: datetime | None = None) -> None:
        self._field_name = field_name
        self._before = before
        self._after = after

    @property
    def field_name(self) -> str:
        return self._field_name

    @property
    def before(self) -> datetime | None:
        return self._before

    @property
    def after(self) -> datetime | None:
        return self._after

    def get_param_names(self) -> "list[str]":
        """Get parameter names without storing them."""
        names = []
        if self.before:
            names.append(f"{self.field_name}_before")
        if self.after:
            names.append(f"{self.field_name}_after")
        return names

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters."""
        named_parameters = {}
        param_names = self.get_param_names()
        param_idx = 0
        if self.before:
            named_parameters[param_names[param_idx]] = self.before
            param_idx += 1
        if self.after:
            named_parameters[param_names[param_idx]] = self.after
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        """Apply filter to SQL expression only."""
        conditions: list[Condition] = []
        col_expr = exp.column(self.field_name)

        proposed_names = self.get_param_names()
        if not proposed_names:
            return statement

        resolved_names = self._resolve_parameter_conflicts(statement, proposed_names)

        param_idx = 0
        result = statement
        if self.before:
            before_param_name = resolved_names[param_idx]
            param_idx += 1
            conditions.append(exp.LT(this=col_expr, expression=exp.Placeholder(this=before_param_name)))
            result = result.add_named_parameter(before_param_name, self.before)

        if self.after:
            after_param_name = resolved_names[param_idx]
            conditions.append(exp.GT(this=col_expr, expression=exp.Placeholder(this=after_param_name)))
            result = result.add_named_parameter(after_param_name, self.after)

        final_condition = conditions[0]
        for cond in conditions[1:]:
            final_condition = exp.And(this=final_condition, expression=cond)
        return result.where(final_condition)

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        return ("BeforeAfterFilter", self.field_name, self.before, self.after)


class OnBeforeAfterFilter(StatementFilter):
    """Filter for inclusive datetime range queries.

    Applies WHERE clauses for on-or-before/on-or-after datetime filtering.
    """

    __slots__ = ("_field_name", "_on_or_after", "_on_or_before")

    def __init__(
        self, field_name: str, on_or_before: datetime | None = None, on_or_after: datetime | None = None
    ) -> None:
        self._field_name = field_name
        self._on_or_before = on_or_before
        self._on_or_after = on_or_after

    @property
    def field_name(self) -> str:
        return self._field_name

    @property
    def on_or_before(self) -> datetime | None:
        return self._on_or_before

    @property
    def on_or_after(self) -> datetime | None:
        return self._on_or_after

    def get_param_names(self) -> "list[str]":
        """Get parameter names without storing them."""
        names = []
        if self.on_or_before:
            names.append(f"{self.field_name}_on_or_before")
        if self.on_or_after:
            names.append(f"{self.field_name}_on_or_after")
        return names

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters."""
        named_parameters = {}
        param_names = self.get_param_names()
        param_idx = 0
        if self.on_or_before:
            named_parameters[param_names[param_idx]] = self.on_or_before
            param_idx += 1
        if self.on_or_after:
            named_parameters[param_names[param_idx]] = self.on_or_after
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        conditions: list[Condition] = []

        proposed_names = self.get_param_names()
        if not proposed_names:
            return statement

        resolved_names = self._resolve_parameter_conflicts(statement, proposed_names)

        param_idx = 0
        result = statement
        if self.on_or_before:
            before_param_name = resolved_names[param_idx]
            param_idx += 1
            conditions.append(
                exp.LTE(this=exp.column(self.field_name), expression=exp.Placeholder(this=before_param_name))
            )
            result = result.add_named_parameter(before_param_name, self.on_or_before)

        if self.on_or_after:
            after_param_name = resolved_names[param_idx]
            conditions.append(
                exp.GTE(this=exp.column(self.field_name), expression=exp.Placeholder(this=after_param_name))
            )
            result = result.add_named_parameter(after_param_name, self.on_or_after)

        final_condition = conditions[0]
        for cond in conditions[1:]:
            final_condition = exp.And(this=final_condition, expression=cond)
        return result.where(final_condition)

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        return ("OnBeforeAfterFilter", self.field_name, self.on_or_before, self.on_or_after)


class InAnyFilter(StatementFilter, ABC, Generic[T]):
    """Base class for collection-based filters that support ANY operations."""

    __slots__ = ()

    def append_to_statement(self, statement: "SQL") -> "SQL":
        raise NotImplementedError


class InCollectionFilter(InAnyFilter[T]):
    """Filter for IN clause queries.

    Constructs WHERE ... IN (...) clauses.
    """

    __slots__ = ("_field_name", "_values")

    def __init__(self, field_name: str, values: abc.Collection[T] | None = None) -> None:
        self._field_name = field_name
        self._values = values

    @property
    def field_name(self) -> str:
        return self._field_name

    @property
    def values(self) -> abc.Collection[T] | None:
        return self._values

    def get_param_names(self) -> "list[str]":
        """Get parameter names without storing them."""
        if not self.values:
            return []
        return [f"{self.field_name}_in_{i}" for i, _ in enumerate(self.values)]

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters."""
        named_parameters = {}
        if self.values:
            param_names = self.get_param_names()
            for i, value in enumerate(self.values):
                named_parameters[param_names[i]] = value
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        if self.values is None:
            return statement

        if not self.values:
            return statement.where(exp.false())

        resolved_names = self._resolve_parameter_conflicts(statement, self.get_param_names())

        placeholder_expressions: list[exp.Placeholder] = [
            exp.Placeholder(this=param_name) for param_name in resolved_names
        ]

        result = statement.where(exp.In(this=exp.column(self.field_name), expressions=placeholder_expressions))

        for resolved_name, value in zip(resolved_names, self.values, strict=False):
            result = result.add_named_parameter(resolved_name, value)
        return result

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        values_tuple = tuple(self.values) if self.values is not None else None
        return ("InCollectionFilter", self.field_name, values_tuple)


class NotInCollectionFilter(InAnyFilter[T]):
    """Filter for NOT IN clause queries.

    Constructs WHERE ... NOT IN (...) clauses.
    """

    __slots__ = ("_field_name", "_values")

    def __init__(self, field_name: str, values: abc.Collection[T] | None = None) -> None:
        self._field_name = field_name
        self._values = values

    @property
    def field_name(self) -> str:
        return self._field_name

    @property
    def values(self) -> abc.Collection[T] | None:
        return self._values

    def get_param_names(self) -> "list[str]":
        """Get parameter names without storing them."""
        if not self.values:
            return []
        # Use object id to ensure uniqueness between instances
        return [f"{self.field_name}_notin_{i}_{id(self)}" for i, _ in enumerate(self.values)]

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters."""
        named_parameters = {}
        if self.values:
            param_names = self.get_param_names()
            for i, value in enumerate(self.values):
                named_parameters[param_names[i]] = value
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        if self.values is None or not self.values:
            return statement

        resolved_names = self._resolve_parameter_conflicts(statement, self.get_param_names())

        placeholder_expressions: list[exp.Placeholder] = [
            exp.Placeholder(this=param_name) for param_name in resolved_names
        ]

        result = statement.where(
            exp.Not(this=exp.In(this=exp.column(self.field_name), expressions=placeholder_expressions))
        )

        for resolved_name, value in zip(resolved_names, self.values, strict=False):
            result = result.add_named_parameter(resolved_name, value)
        return result

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        values_tuple = tuple(self.values) if self.values is not None else None
        return ("NotInCollectionFilter", self.field_name, values_tuple)


class AnyCollectionFilter(InAnyFilter[T]):
    """Filter for PostgreSQL-style ANY clause queries.

    Constructs WHERE column_name = ANY (array_expression) clauses.
    """

    __slots__ = ("_field_name", "_values")

    def __init__(self, field_name: str, values: abc.Collection[T] | None = None) -> None:
        self._field_name = field_name
        self._values = values

    @property
    def field_name(self) -> str:
        return self._field_name

    @property
    def values(self) -> abc.Collection[T] | None:
        return self._values

    def get_param_names(self) -> "list[str]":
        """Get parameter names without storing them."""
        if not self.values:
            return []
        return [f"{self.field_name}_any_{i}" for i, _ in enumerate(self.values)]

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters."""
        named_parameters = {}
        if self.values:
            param_names = self.get_param_names()
            for i, value in enumerate(self.values):
                named_parameters[param_names[i]] = value
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        if self.values is None:
            return statement

        if not self.values:
            return statement.where(exp.false())

        resolved_names = self._resolve_parameter_conflicts(statement, self.get_param_names())

        placeholder_expressions: list[exp.Expression] = [
            exp.Placeholder(this=param_name) for param_name in resolved_names
        ]

        array_expr = exp.Array(expressions=placeholder_expressions)
        result = statement.where(exp.EQ(this=exp.column(self.field_name), expression=exp.Any(this=array_expr)))

        for resolved_name, value in zip(resolved_names, self.values, strict=False):
            result = result.add_named_parameter(resolved_name, value)
        return result

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        values_tuple = tuple(self.values) if self.values is not None else None
        return ("AnyCollectionFilter", self.field_name, values_tuple)


class NotAnyCollectionFilter(InAnyFilter[T]):
    """Filter for PostgreSQL-style NOT ANY clause queries.

    Constructs WHERE NOT (column_name = ANY (array_expression)) clauses.
    """

    __slots__ = ("_field_name", "_values")

    def __init__(self, field_name: str, values: abc.Collection[T] | None = None) -> None:
        self._field_name = field_name
        self._values = values

    @property
    def field_name(self) -> str:
        return self._field_name

    @property
    def values(self) -> abc.Collection[T] | None:
        return self._values

    def get_param_names(self) -> "list[str]":
        """Get parameter names without storing them."""
        if not self.values:
            return []
        return [f"{self.field_name}_not_any_{i}" for i, _ in enumerate(self.values)]

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters."""
        named_parameters = {}
        if self.values:
            param_names = self.get_param_names()
            for i, value in enumerate(self.values):
                named_parameters[param_names[i]] = value
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        if self.values is None or not self.values:
            return statement

        resolved_names = self._resolve_parameter_conflicts(statement, self.get_param_names())

        placeholder_expressions: list[exp.Expression] = [
            exp.Placeholder(this=param_name) for param_name in resolved_names
        ]

        array_expr = exp.Array(expressions=placeholder_expressions)
        condition = exp.EQ(this=exp.column(self.field_name), expression=exp.Any(this=array_expr))
        result = statement.where(exp.Not(this=condition))

        for resolved_name, value in zip(resolved_names, self.values, strict=False):
            result = result.add_named_parameter(resolved_name, value)
        return result

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        values_tuple = tuple(self.values) if self.values is not None else None
        return ("NotAnyCollectionFilter", self.field_name, values_tuple)


class PaginationFilter(StatementFilter, ABC):
    """Base class for pagination-related filters."""

    __slots__ = ()

    @abstractmethod
    def append_to_statement(self, statement: "SQL") -> "SQL":
        raise NotImplementedError


class LimitOffsetFilter(PaginationFilter):
    """Filter for LIMIT and OFFSET clauses.

    Adds pagination support through LIMIT/OFFSET SQL clauses.
    """

    __slots__ = ("_limit", "_offset")

    def __init__(self, limit: int, offset: int) -> None:
        self._limit = limit
        self._offset = offset

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def offset(self) -> int:
        return self._offset

    def get_param_names(self) -> "list[str]":
        """Get parameter names without storing them."""
        return ["limit", "offset"]

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters."""
        param_names = self.get_param_names()
        return [], {param_names[0]: self.limit, param_names[1]: self.offset}

    def append_to_statement(self, statement: "SQL") -> "SQL":
        resolved_names = self._resolve_parameter_conflicts(statement, self.get_param_names())
        limit_param_name, offset_param_name = resolved_names

        limit_placeholder = exp.Placeholder(this=limit_param_name)
        offset_placeholder = exp.Placeholder(this=offset_param_name)

        if statement.statement_expression is not None:
            current_statement = statement.statement_expression.copy()
        elif not statement.statement_config.enable_parsing:
            current_statement = exp.Select().from_(f"({statement.raw_sql})")
        else:
            try:
                current_statement = sqlglot.parse_one(statement.raw_sql, dialect=statement.dialect)
            except Exception:
                current_statement = exp.Select().from_(f"({statement.raw_sql})")

        if isinstance(current_statement, exp.Select):
            new_statement = current_statement.limit(limit_placeholder).offset(offset_placeholder)
        else:
            new_statement = exp.Select().from_(current_statement).limit(limit_placeholder).offset(offset_placeholder)

        result = statement.copy(statement=new_statement)
        result = result.add_named_parameter(limit_param_name, self.limit)
        return result.add_named_parameter(offset_param_name, self.offset)

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        return ("LimitOffsetFilter", self.limit, self.offset)


class OrderByFilter(StatementFilter):
    """Filter for ORDER BY clauses.

    Adds sorting capability to SQL queries.
    """

    __slots__ = ("_field_name", "_sort_order")

    def __init__(self, field_name: str, sort_order: Literal["asc", "desc"] = "asc") -> None:
        self._field_name = field_name
        self._sort_order = sort_order

    @property
    def field_name(self) -> str:
        return self._field_name

    @property
    def sort_order(self) -> Literal["asc", "desc"]:
        return self._sort_order  # pyright: ignore

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters."""
        return [], {}

    def append_to_statement(self, statement: "SQL") -> "SQL":
        converted_sort_order = self.sort_order.lower()
        if converted_sort_order not in {"asc", "desc"}:
            converted_sort_order = "asc"

        col_expr = exp.column(self.field_name)
        order_expr = col_expr.desc() if converted_sort_order == "desc" else col_expr.asc()

        if statement.statement_expression is not None:
            current_statement = statement.statement_expression.copy()
        elif not statement.statement_config.enable_parsing:
            current_statement = exp.Select().from_(f"({statement.raw_sql})")
        else:
            try:
                current_statement = sqlglot.parse_one(statement.raw_sql, dialect=statement.dialect)
            except Exception:
                current_statement = exp.Select().from_(f"({statement.raw_sql})")

        if isinstance(current_statement, exp.Select):
            new_statement = current_statement.order_by(order_expr)
        else:
            new_statement = exp.Select().from_(current_statement).order_by(order_expr)

        return statement.copy(statement=new_statement)

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        return ("OrderByFilter", self.field_name, self.sort_order)


class SearchFilter(StatementFilter):
    """Filter for text search queries.

    Constructs WHERE field_name LIKE '%value%' clauses.
    """

    __slots__ = ("_field_name", "_ignore_case", "_value")

    def __init__(self, field_name: str | set[str], value: str | None, ignore_case: bool | None = False) -> None:
        self._field_name = field_name
        self._value = value
        self._ignore_case = ignore_case

    @property
    def field_name(self) -> "str | set[str]":
        return self._field_name

    @property
    def value(self) -> str | None:
        return self._value

    @property
    def ignore_case(self) -> bool | None:
        return self._ignore_case

    def get_param_name(self) -> str | None:
        """Get parameter name without storing it."""
        if not self.value:
            return None
        if isinstance(self.field_name, str):
            return f"{self.field_name}_search"
        return "search_value"

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters."""
        named_parameters = {}
        param_name = self.get_param_name()
        if self.value and param_name:
            search_value_with_wildcards = f"%{self.value}%"
            named_parameters[param_name] = search_value_with_wildcards
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        param_name = self.get_param_name()
        if not self.value or not param_name:
            return statement

        resolved_names = self._resolve_parameter_conflicts(statement, [param_name])
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

        search_value_with_wildcards = f"%{self.value}%"
        return result.add_named_parameter(param_name, search_value_with_wildcards)

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        field_names = tuple(sorted(self.field_name)) if isinstance(self.field_name, set) else self.field_name
        return ("SearchFilter", field_names, self.value, self.ignore_case)


class NullFilter(StatementFilter):
    """Filter for IS NULL queries.

    Constructs WHERE field_name IS NULL clauses.
    """

    __slots__ = ("_field_name",)

    def __init__(self, field_name: str) -> None:
        self._field_name = field_name

    @property
    def field_name(self) -> str:
        return self._field_name

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters.

        Returns empty parameters since IS NULL requires no values.
        """
        return [], {}

    def append_to_statement(self, statement: "SQL") -> "SQL":
        """Apply IS NULL filter to SQL expression."""
        col_expr = exp.column(self.field_name)
        is_null_condition = exp.Is(this=col_expr, expression=exp.Null())
        return statement.where(is_null_condition)

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        return ("NullFilter", self.field_name)


class NotNullFilter(StatementFilter):
    """Filter for IS NOT NULL queries.

    Constructs WHERE field_name IS NOT NULL clauses.
    """

    __slots__ = ("_field_name",)

    def __init__(self, field_name: str) -> None:
        self._field_name = field_name

    @property
    def field_name(self) -> str:
        return self._field_name

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters.

        Returns empty parameters since IS NOT NULL requires no values.
        """
        return [], {}

    def append_to_statement(self, statement: "SQL") -> "SQL":
        """Apply IS NOT NULL filter to SQL expression."""
        col_expr = exp.column(self.field_name)
        is_null_condition = exp.Is(this=col_expr, expression=exp.Null())
        is_not_null_condition = exp.Not(this=is_null_condition)
        return statement.where(is_not_null_condition)

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        return ("NotNullFilter", self.field_name)


class NotInSearchFilter(SearchFilter):
    """Filter for negated text search queries.

    Constructs WHERE field_name NOT LIKE '%value%' clauses.
    """

    def get_param_name(self) -> str | None:
        """Get parameter name without storing it."""
        if not self.value:
            return None
        if isinstance(self.field_name, str):
            return f"{self.field_name}_not_search"
        return "not_search_value"

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters."""
        named_parameters = {}
        param_name = self.get_param_name()
        if self.value and param_name:
            search_value_with_wildcards = f"%{self.value}%"
            named_parameters[param_name] = search_value_with_wildcards
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        param_name = self.get_param_name()
        if not self.value or not param_name:
            return statement

        resolved_names = self._resolve_parameter_conflicts(statement, [param_name])
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

        search_value_with_wildcards = f"%{self.value}%"
        return result.add_named_parameter(param_name, search_value_with_wildcards)

    def get_cache_key(self) -> "tuple[Any, ...]":
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


FilterTypes: TypeAlias = (
    BeforeAfterFilter
    | OnBeforeAfterFilter
    | InCollectionFilter[Any]
    | LimitOffsetFilter
    | OrderByFilter
    | SearchFilter
    | NotInCollectionFilter[Any]
    | NotInSearchFilter
    | AnyCollectionFilter[Any]
    | NotAnyCollectionFilter[Any]
    | NullFilter
    | NotNullFilter
)


def create_filters(filters: "list[StatementFilter]") -> "tuple[StatementFilter, ...]":
    """Convert mutable filters to immutable tuple.

    Since StatementFilter classes are now immutable (with read-only properties),
    we just need to convert to a tuple for consistent sharing.

    Args:
        filters: List of StatementFilter objects (already immutable)

    Returns:
        Tuple of StatementFilter objects
    """
    return tuple(filters)


def _filter_sort_key(f: "StatementFilter") -> "tuple[str, str]":
    """Sort key for canonicalizing filters by type and field_name."""
    class_name = type(f).__name__
    field_name = str(f.field_name) if has_field_name(f) else ""
    return (class_name, field_name)


def canonicalize_filters(filters: "list[StatementFilter]") -> "tuple[StatementFilter, ...]":
    """Sort filters by type and field_name for consistent hashing.

    Args:
        filters: List of StatementFilter objects

    Returns:
        Canonically sorted tuple of filters
    """
    return tuple(sorted(filters, key=_filter_sort_key))
