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

from abc import ABC, abstractmethod
from collections import abc
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, TypeAlias

from mypy_extensions import mypyc_attr
from sqlglot import exp
from typing_extensions import TypeVar

from sqlspec.core._pagination import OffsetPagination
from sqlspec.core.query_modifiers import parse_column_for_condition
from sqlspec.utils.type_guards import has_field_name
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from sqlglot.expressions import Condition

    from sqlspec.core.statement import SQL

__all__ = (
    "AnyCollectionFilter",
    "BeforeAfterFilter",
    "BooleanFilter",
    "ChoicesFilter",
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
    "find_filter",
)

T = TypeVar("T")
FilterTypeT = TypeVar("FilterTypeT", bound="StatementFilter")


@mypyc_attr(allow_interpreted_subclasses=True)
class StatementFilter(ABC):
    """Abstract base class for filters that can be appended to a statement."""

    __slots__ = ()

    _is_statement_filter: bool = True

    @abstractmethod
    def append_to_statement(self, statement: "SQL") -> "SQL":
        """Append the filter to the statement.

        This method modifies the SQL expression and adds parameters via
        ``add_named_parameter()`` on the returned statement.
        """

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Return the parameters this filter would contribute.

        This is an introspection helper and is not called during the normal
        filter-application path (``append_to_statement`` handles both
        expression modification and parameter addition).

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
        parameters = statement.parameters
        existing_params = (
            set(parameters.keys()) if isinstance(parameters, dict) else set(statement.named_parameters.keys())
        )

        resolved_names = []
        for name in proposed_names:
            if name in existing_params:
                unique_suffix = str(uuid4()).replace("-", "")[:8]
                resolved_name = f"{name}_{unique_suffix}"
            else:
                resolved_name = name
            resolved_names.append(resolved_name)
            existing_params.add(resolved_name)

        return resolved_names

    def _get_column_expression(self, field_name: "str | exp.Expression") -> exp.Expr:
        """Parse field name into a qualified column if dotted, else bare column.

        Args:
            field_name: Field name string or SQLGlot expression

        Returns:
            exp.Column | exp.Expression: SQLGlot column expression or provided expression
        """
        return parse_column_for_condition(field_name)

    def _sanitize_param_name(self, name: "str | exp.Expression") -> str:
        """Sanitize field name for use as a parameter name by replacing dots with underscores.

        Args:
            name: Original parameter name string or expression

        Returns:
            str: Sanitized parameter name
        """
        if isinstance(name, exp.Expression):
            return f"expr_{str(hash(name)).replace('-', '')[:8]}"
        return name.replace(".", "_")

    def _reconstruction_args(self) -> "tuple[Any, ...]":
        """Return the positional ``__init__`` args needed to rebuild this filter.

        Subclasses must override this to support :func:`copy.deepcopy` and :mod:`pickle`.
        The default raises only when reconstruction is actually attempted, so filters
        that never need to be copied or pickled don't pay an implementation cost.
        """
        msg = (
            f"{type(self).__name__} does not implement _reconstruction_args(); "
            "override it to support copy.deepcopy and pickle"
        )
        raise NotImplementedError(msg)

    def __reduce__(self) -> "tuple[Any, ...]":
        """Reconstruct via ``cls(*ctor_args)`` — works on mypyc native classes."""
        return (type(self), self._reconstruction_args())

    def __eq__(self, other: object) -> bool:
        """Compare filters by their cache key for round-trip and deduplication checks."""
        if not isinstance(other, StatementFilter):
            return NotImplemented
        return self.get_cache_key() == other.get_cache_key()

    def __hash__(self) -> int:
        """Hash via the canonical filter key so unhashable members are normalized."""
        return hash(_canonical_filter_key(self))

    @abstractmethod
    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return a cache key for this filter's configuration.

        Returns:
            Tuple of hashable values representing the filter's configuration
        """


class _DatetimeBoundFilter(StatementFilter):
    """Private base for datetime lower/upper-bound filters."""

    __slots__ = ("_field_name", "_lower_value", "_upper_value")

    _cache_key_name: ClassVar[str] = "_DatetimeBoundFilter"
    _lower_condition: ClassVar["type[Condition]"] = exp.GT
    _lower_param_suffix: ClassVar[str] = "after"
    _upper_condition: ClassVar["type[Condition]"] = exp.LT
    _upper_param_suffix: ClassVar[str] = "before"

    def __init__(
        self,
        field_name: "str | exp.Expression",
        upper_value: datetime | None = None,
        lower_value: datetime | None = None,
    ) -> None:
        self._field_name = field_name
        self._upper_value = upper_value
        self._lower_value = lower_value

    @property
    def field_name(self) -> "str | exp.Expression":
        return self._field_name

    def get_param_names(self) -> "list[str]":
        """Get parameter names without storing them."""
        names = []
        sanitized_field = self._sanitize_param_name(self.field_name)
        if self._upper_value:
            names.append(f"{sanitized_field}_{self._upper_param_suffix}")
        if self._lower_value:
            names.append(f"{sanitized_field}_{self._lower_param_suffix}")
        return names

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters."""
        named_parameters = {}
        param_names = self.get_param_names()
        param_idx = 0
        if self._upper_value:
            named_parameters[param_names[param_idx]] = self._upper_value
            param_idx += 1
        if self._lower_value:
            named_parameters[param_names[param_idx]] = self._lower_value
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        """Apply filter to SQL expression only."""
        conditions: list[Condition] = []

        proposed_names = self.get_param_names()
        if not proposed_names:
            return statement

        resolved_names = self._resolve_parameter_conflicts(statement, proposed_names)

        param_idx = 0
        result = statement
        if self._upper_value:
            upper_param_name = resolved_names[param_idx]
            param_idx += 1
            conditions.append(
                self._upper_condition(
                    this=self._get_column_expression(self.field_name), expression=exp.Placeholder(this=upper_param_name)
                )
            )
            result = result.add_named_parameter(upper_param_name, self._upper_value)

        if self._lower_value:
            lower_param_name = resolved_names[param_idx]
            conditions.append(
                self._lower_condition(
                    this=self._get_column_expression(self.field_name), expression=exp.Placeholder(this=lower_param_name)
                )
            )
            result = result.add_named_parameter(lower_param_name, self._lower_value)

        final_condition = conditions[0]
        for cond in conditions[1:]:
            final_condition = exp.And(this=final_condition, expression=cond)
        return result.where(final_condition)

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        return (self._cache_key_name, self.field_name, self._upper_value, self._lower_value)

    def _reconstruction_args(self) -> "tuple[Any, ...]":
        return (self._field_name, self._upper_value, self._lower_value)


class BeforeAfterFilter(_DatetimeBoundFilter):
    """Filter for datetime range queries.

    Applies WHERE clauses for before/after datetime filtering.
    """

    __slots__ = ()

    _cache_key_name: ClassVar[str] = "BeforeAfterFilter"
    _lower_condition: ClassVar["type[Condition]"] = exp.GT
    _lower_param_suffix: ClassVar[str] = "after"
    _upper_condition: ClassVar["type[Condition]"] = exp.LT
    _upper_param_suffix: ClassVar[str] = "before"

    def __init__(
        self, field_name: "str | exp.Expression", before: datetime | None = None, after: datetime | None = None
    ) -> None:
        super().__init__(field_name, before, after)

    @property
    def before(self) -> datetime | None:
        return self._upper_value

    @property
    def after(self) -> datetime | None:
        return self._lower_value


class OnBeforeAfterFilter(_DatetimeBoundFilter):
    """Filter for inclusive datetime range queries.

    Applies WHERE clauses for on-or-before/on-or-after datetime filtering.
    """

    __slots__ = ()

    _cache_key_name: ClassVar[str] = "OnBeforeAfterFilter"
    _lower_condition: ClassVar["type[Condition]"] = exp.GTE
    _lower_param_suffix: ClassVar[str] = "on_or_after"
    _upper_condition: ClassVar["type[Condition]"] = exp.LTE
    _upper_param_suffix: ClassVar[str] = "on_or_before"

    def __init__(
        self,
        field_name: "str | exp.Expression",
        on_or_before: datetime | None = None,
        on_or_after: datetime | None = None,
    ) -> None:
        super().__init__(field_name, on_or_before, on_or_after)

    @property
    def on_or_before(self) -> datetime | None:
        return self._upper_value

    @property
    def on_or_after(self) -> datetime | None:
        return self._lower_value


class InAnyFilter(StatementFilter, ABC, Generic[T]):
    """Base class for collection-based filters that support ANY operations."""

    __slots__ = ("_field_name", "_values")

    _cache_key_name: ClassVar[str] = "InAnyFilter"
    _condition_type: ClassVar[Literal["in", "not_in", "any", "not_any"]] = "in"
    _empty_values_return_false: ClassVar[bool] = False
    _parameter_suffix: ClassVar[str] = "value"

    def __init__(self, field_name: "str | exp.Expression", values: abc.Collection[T] | None = None) -> None:
        self._field_name = field_name
        self._values = values

    @property
    def field_name(self) -> "str | exp.Expression":
        return self._field_name

    @property
    def values(self) -> abc.Collection[T] | None:
        return self._values

    def get_param_names(self) -> "list[str]":
        """Get parameter names without storing them."""
        if not self.values:
            return []
        sanitized_field = self._sanitize_param_name(self.field_name)
        suffix = self._parameter_suffix
        return [f"{sanitized_field}_{suffix}_{i}" for i, _ in enumerate(self.values)]

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters."""
        named_parameters = {}
        if self.values:
            param_names = self.get_param_names()
            for i, value in enumerate(self.values):
                named_parameters[param_names[i]] = value
        return [], named_parameters

    def _build_collection_condition(self, col_expr: exp.Expr, placeholders: "list[exp.Expr]") -> exp.Expr:
        condition_type = self._condition_type
        if condition_type == "in":
            return exp.In(this=col_expr, expressions=placeholders)
        if condition_type == "not_in":
            return exp.Not(this=exp.In(this=col_expr, expressions=placeholders))

        array_expr = exp.Array(expressions=placeholders)
        condition = exp.EQ(this=col_expr, expression=exp.Any(this=array_expr))
        if condition_type == "not_any":
            return exp.Not(this=condition)
        return condition

    def append_to_statement(self, statement: "SQL") -> "SQL":
        values = self.values
        if values is None:
            return statement

        if not values:
            return statement.where(exp.false()) if self._empty_values_return_false else statement

        col_expr = self._get_column_expression(self.field_name)
        resolved_names = self._resolve_parameter_conflicts(statement, self.get_param_names())
        placeholders: list[exp.Expr] = [exp.Placeholder(this=param_name) for param_name in resolved_names]
        result = statement.where(self._build_collection_condition(col_expr, placeholders))

        for resolved_name, value in zip(resolved_names, values, strict=False):
            result = result.add_named_parameter(resolved_name, value)
        return result

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        values_tuple = tuple(self.values) if self.values is not None else None
        return (self._cache_key_name, self.field_name, values_tuple)

    def _reconstruction_args(self) -> "tuple[Any, ...]":
        return (self._field_name, self._values)


class InCollectionFilter(InAnyFilter[T]):
    """Filter for IN clause queries.

    Constructs WHERE ... IN (...) clauses.
    """

    __slots__ = ()
    _cache_key_name: ClassVar[str] = "InCollectionFilter"
    _condition_type: ClassVar[Literal["in", "not_in", "any", "not_any"]] = "in"
    _empty_values_return_false: ClassVar[bool] = True
    _parameter_suffix: ClassVar[str] = "in"


class ChoicesFilter(InCollectionFilter[T]):
    """Filter for field matching a defined set of choice values.

    Constructs WHERE ... IN (...) clauses.
    """

    __slots__ = ()
    _cache_key_name: ClassVar[str] = "ChoicesFilter"
    _parameter_suffix: ClassVar[str] = "choices"


class NotInCollectionFilter(InAnyFilter[T]):
    """Filter for NOT IN clause queries.

    Constructs WHERE ... NOT IN (...) clauses.
    """

    __slots__ = ()
    _cache_key_name: ClassVar[str] = "NotInCollectionFilter"
    _condition_type: ClassVar[Literal["in", "not_in", "any", "not_any"]] = "not_in"
    _parameter_suffix: ClassVar[str] = "notin"


class AnyCollectionFilter(InAnyFilter[T]):
    """Filter for PostgreSQL-style ANY clause queries.

    Constructs WHERE column_name = ANY (array_expression) clauses.
    """

    __slots__ = ()
    _cache_key_name: ClassVar[str] = "AnyCollectionFilter"
    _condition_type: ClassVar[Literal["in", "not_in", "any", "not_any"]] = "any"
    _empty_values_return_false: ClassVar[bool] = True
    _parameter_suffix: ClassVar[str] = "any"


class NotAnyCollectionFilter(InAnyFilter[T]):
    """Filter for PostgreSQL-style NOT ANY clause queries.

    Constructs WHERE NOT (column_name = ANY (array_expression)) clauses.
    """

    __slots__ = ()
    _cache_key_name: ClassVar[str] = "NotAnyCollectionFilter"
    _condition_type: ClassVar[Literal["in", "not_in", "any", "not_any"]] = "not_any"
    _parameter_suffix: ClassVar[str] = "not_any"


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

        current_statement = statement._filter_expression()

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

    def _reconstruction_args(self) -> "tuple[Any, ...]":
        return (self._limit, self._offset)


class OrderByFilter(StatementFilter):
    """Filter for ORDER BY clauses.

    Adds sorting capability to SQL queries.
    """

    __slots__ = ("_field_name", "_sort_order")
    _field_name: str | exp.Expression
    _sort_order: Literal["asc", "desc"]

    def __init__(self, field_name: "str | exp.Expression", sort_order: Literal["asc", "desc"] = "asc") -> None:
        if sort_order not in ("asc", "desc"):
            msg = "sort_order must be 'asc' or 'desc'"
            raise ValueError(msg)
        self._field_name = field_name
        self._sort_order = sort_order

    @property
    def field_name(self) -> "str | exp.Expression":
        return self._field_name

    @property
    def sort_order(self) -> Literal["asc", "desc"]:
        return self._sort_order

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters."""
        return [], {}

    def append_to_statement(self, statement: "SQL") -> "SQL":
        col_expr = self._get_column_expression(self.field_name)
        order_expr = col_expr.desc() if self._sort_order == "desc" else col_expr.asc()

        current_statement = statement._filter_expression()

        if isinstance(current_statement, exp.Select):
            new_statement = current_statement.order_by(order_expr)
        else:
            new_statement = exp.Select().from_(current_statement).order_by(order_expr)

        return statement.copy(statement=new_statement)

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        return ("OrderByFilter", self.field_name, self.sort_order)

    def _reconstruction_args(self) -> "tuple[Any, ...]":
        return (self._field_name, self._sort_order)


class _TextSearchFilter(StatementFilter):
    """Private base for LIKE and NOT LIKE filters."""

    __slots__ = ("_field_name", "_ignore_case", "_value")

    _cache_key_name: ClassVar[str] = "SearchFilter"
    _combine_operator: ClassVar[Literal["and", "or"]] = "or"
    _negate: ClassVar[bool] = False
    _param_suffix: ClassVar[str] = "search"
    _set_param_name: ClassVar[str] = "search_value"

    def __init__(
        self,
        field_name: "str | exp.Expression | set[str | exp.Expression]",
        value: str | None,
        ignore_case: bool | None = False,
    ) -> None:
        self._field_name = field_name
        self._value = value
        self._ignore_case = ignore_case if ignore_case is not None else False

    @property
    def field_name(self) -> "str | exp.Expression | set[str | exp.Expression]":
        return self._field_name

    @property
    def value(self) -> str | None:
        return self._value

    @property
    def ignore_case(self) -> bool:
        return self._ignore_case

    @property
    def like_pattern(self) -> str | None:
        """Return the search value wrapped in LIKE wildcards.

        Returns:
            The pattern for a LIKE operation, or None if no value.
        """
        return f"%{self.value}%" if self.value else None

    @staticmethod
    def escape_like_value(value: str) -> str:
        r"""Escape ``%`` and ``_`` metacharacters for safe LIKE/ILIKE pattern matching.

        Backslash is the default LIKE escape character in PostgreSQL (with
        ``standard_conforming_strings`` on), SQLite, and MySQL, so the produced
        pattern is safe without an explicit ``ESCAPE`` clause on those engines.

        Args:
            value: Raw user input to escape before wrapping in wildcards.

        Returns:
            The input with ``\``, ``%``, and ``_`` escaped using ``\``.
        """
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def get_param_name(self) -> str | None:
        """Get parameter name without storing it."""
        if not self.value:
            return None
        if isinstance(self.field_name, str):
            sanitized_field = self._sanitize_param_name(self.field_name)
            return f"{sanitized_field}_{self._param_suffix}"
        if isinstance(self.field_name, exp.Expression):
            return f"{self._sanitize_param_name(self.field_name)}_{self._param_suffix}"
        return self._set_param_name

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters."""
        named_parameters = {}
        param_name = self.get_param_name()
        if self.value and param_name:
            named_parameters[param_name] = self.like_pattern
        return [], named_parameters

    def append_to_statement(self, statement: "SQL") -> "SQL":
        param_name = self.get_param_name()
        if not self.value or not param_name:
            return statement

        resolved_names = self._resolve_parameter_conflicts(statement, [param_name])
        param_name = resolved_names[0]

        if isinstance(self.field_name, (str, exp.Expression)):
            condition = self._build_search_condition(self.field_name, param_name)
            result = statement.where(condition)
        elif isinstance(self.field_name, set) and self.field_name:
            field_conditions = [self._build_search_condition(field, param_name) for field in self.field_name]
            final_condition: Condition = field_conditions[0]
            for cond in field_conditions[1:]:
                if self._combine_operator == "and":
                    final_condition = exp.And(this=final_condition, expression=cond)
                else:
                    final_condition = exp.Or(this=final_condition, expression=cond)
            result = statement.where(final_condition)
        elif isinstance(self.field_name, set):
            return statement
        else:
            msg = (
                f"{type(self).__name__}.field_name must be str, exp.Expression, or set thereof; "
                f"got {type(self.field_name).__name__}"
            )
            raise TypeError(msg)

        return result.add_named_parameter(param_name, self.like_pattern)

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        if isinstance(self.field_name, set):
            field_names: Any = tuple(
                sorted(item.sql() if isinstance(item, exp.Expression) else item for item in self.field_name)
            )
        elif isinstance(self.field_name, exp.Expression):
            field_names = self.field_name.sql()
        else:
            field_names = self.field_name
        return (self._cache_key_name, field_names, self.value, self.ignore_case)

    def _build_search_condition(self, field_name: "str | exp.Expression", param_name: str) -> "Condition":
        like_op = exp.ILike if self.ignore_case else exp.Like
        search_target = (
            field_name if isinstance(field_name, exp.Expression) else self._get_column_expression(field_name)
        )
        condition = like_op(this=search_target, expression=exp.Placeholder(this=param_name))
        return exp.Not(this=condition) if self._negate else condition

    def _reconstruction_args(self) -> "tuple[Any, ...]":
        return (self._field_name, self._value, self._ignore_case)


class SearchFilter(_TextSearchFilter):
    """Filter for text search queries.

    Constructs WHERE field_name LIKE '%value%' clauses.
    """

    __slots__ = ()

    _cache_key_name: ClassVar[str] = "SearchFilter"
    _combine_operator: ClassVar[Literal["and", "or"]] = "or"
    _negate: ClassVar[bool] = False
    _param_suffix: ClassVar[str] = "search"
    _set_param_name: ClassVar[str] = "search_value"


class NotInSearchFilter(SearchFilter):
    """Filter for negated text search queries.

    Constructs WHERE field_name NOT LIKE '%value%' clauses.
    """

    __slots__ = ()

    _cache_key_name: ClassVar[str] = "NotInSearchFilter"
    _combine_operator: ClassVar[Literal["and", "or"]] = "and"
    _negate: ClassVar[bool] = True
    _param_suffix: ClassVar[str] = "not_search"
    _set_param_name: ClassVar[str] = "not_search_value"


class NullFilter(StatementFilter):
    """Filter for IS NULL queries.

    Constructs WHERE field_name IS NULL clauses.
    """

    __slots__ = ("_field_name",)

    def __init__(self, field_name: "str | exp.Expression") -> None:
        self._field_name = field_name

    @property
    def field_name(self) -> "str | exp.Expression":
        return self._field_name

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters.

        Returns empty parameters since IS NULL requires no values.
        """
        return [], {}

    def append_to_statement(self, statement: "SQL") -> "SQL":
        """Apply IS NULL filter to SQL expression."""
        col_expr = self._get_column_expression(self.field_name)
        is_null_condition = exp.Is(this=col_expr, expression=exp.null())
        return statement.where(is_null_condition)

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        return ("NullFilter", self.field_name)

    def _reconstruction_args(self) -> "tuple[Any, ...]":
        return (self._field_name,)


class NotNullFilter(StatementFilter):
    """Filter for IS NOT NULL queries.

    Constructs WHERE field_name IS NOT NULL clauses.
    """

    __slots__ = ("_field_name",)

    def __init__(self, field_name: "str | exp.Expression") -> None:
        self._field_name = field_name

    @property
    def field_name(self) -> "str | exp.Expression":
        return self._field_name

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters.

        Returns empty parameters since IS NOT NULL requires no values.
        """
        return [], {}

    def append_to_statement(self, statement: "SQL") -> "SQL":
        """Apply IS NOT NULL filter to SQL expression."""
        col_expr = self._get_column_expression(self.field_name)
        is_null_condition = exp.Is(this=col_expr, expression=exp.null())
        is_not_null_condition = exp.Not(this=is_null_condition)
        return statement.where(is_not_null_condition)

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        return ("NotNullFilter", self.field_name)

    def _reconstruction_args(self) -> "tuple[Any, ...]":
        return (self._field_name,)


class BooleanFilter(StatementFilter):
    """Filter for boolean comparison queries.

    Constructs WHERE field_name = :param clauses.
    """

    __slots__ = ("_field_name", "_value")

    def __init__(self, field_name: "str | exp.Expression", value: bool) -> None:
        self._field_name = field_name
        self._value = value

    @property
    def field_name(self) -> "str | exp.Expression":
        return self._field_name

    @property
    def value(self) -> bool:
        return self._value

    def get_param_name(self) -> str:
        """Get parameter name without storing it."""
        sanitized_field = self._sanitize_param_name(self.field_name)
        return f"{sanitized_field}_boolean"

    def extract_parameters(self) -> "tuple[list[Any], dict[str, Any]]":
        """Extract filter parameters."""
        return [], {self.get_param_name(): self.value}

    def append_to_statement(self, statement: "SQL") -> "SQL":
        """Apply boolean filter to SQL expression."""
        param_name = self.get_param_name()
        resolved_names = self._resolve_parameter_conflicts(statement, [param_name])
        param_name = resolved_names[0]

        col_expr = self._get_column_expression(self.field_name)
        condition = exp.EQ(this=col_expr, expression=exp.Placeholder(this=param_name))
        result = statement.where(condition)
        return result.add_named_parameter(param_name, self.value)

    def get_cache_key(self) -> "tuple[Any, ...]":
        """Return cache key for this filter configuration."""
        return ("BooleanFilter", self.field_name, self.value)

    def _reconstruction_args(self) -> "tuple[Any, ...]":
        return (self._field_name, self._value)


def apply_filter(statement: "SQL", filter_obj: StatementFilter) -> "SQL":
    """Apply a statement filter to a SQL query object.

    Args:
        statement: The SQL query object to modify.
        filter_obj: The filter to apply.

    Returns:
        The modified query object.
    """
    return filter_obj.append_to_statement(statement)


def find_filter(filter_type: type[FilterTypeT], filters: abc.Sequence[StatementFilter | Any]) -> FilterTypeT | None:
    """Get the filter specified by filter type from the filters.

    Args:
        filter_type: The type of filter to find.
        filters: Filters to search through.

    Returns:
        The match filter instance or None.
    """
    for filter_ in filters:
        if isinstance(filter_, filter_type):
            return filter_
    return None


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
    | BooleanFilter
    | ChoicesFilter[Any]
)


def _filter_sort_key(f: "StatementFilter") -> "tuple[str, str, str]":
    """Sort key for canonicalizing filters by type, field name, and value."""
    class_name = type(f).__name__
    canonical_key = _canonical_filter_key(f)
    field_name = _stable_filter_key_part(f.field_name) if has_field_name(f) else ""
    return (class_name, repr(field_name), repr(canonical_key))


def _stable_filter_key_part(value: Any) -> Any:
    if isinstance(value, exp.Expression):
        return value.sql()
    if isinstance(value, set):
        return tuple(sorted((_stable_filter_key_part(item) for item in value), key=repr))
    if isinstance(value, dict):
        return tuple(
            sorted(
                ((_stable_filter_key_part(key), _stable_filter_key_part(item)) for key, item in value.items()), key=repr
            )
        )
    if isinstance(value, (list, tuple)):
        return tuple(_stable_filter_key_part(item) for item in value)

    try:
        hash(value)
    except TypeError:
        return repr(value)
    return value


def _canonical_filter_key(f: "StatementFilter") -> "tuple[Any, ...]":
    return tuple(_stable_filter_key_part(item) for item in f.get_cache_key())


def canonicalize_filters(filters: "abc.Sequence[StatementFilter]") -> "tuple[StatementFilter, ...]":
    """Deduplicate and sort filters by type and field name for consistent hashing.

    Args:
        filters: Sequence of StatementFilter objects

    Returns:
        Canonically sorted tuple of unique filters
    """
    unique_filters: dict[tuple[Any, ...], StatementFilter] = {}
    for filter_ in filters:
        unique_filters.setdefault(_canonical_filter_key(filter_), filter_)
    return tuple(sorted(unique_filters.values(), key=_filter_sort_key))
