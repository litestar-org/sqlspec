"""Collection filter datastructures."""

from abc import ABC, abstractmethod
from collections import abc
from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, Generic, Literal, Optional, Union

from sqlglot import exp
from typing_extensions import TypeAlias, TypeVar

if TYPE_CHECKING:
    from sqlglot.expressions import Condition

    from sqlspec.statement import SQL

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
"""Type variable for filter types.

:class:`~advanced_alchemy.filters.StatementFilter`
"""


class StatementFilter(ABC):
    """Abstract base class for filters that can be appended to a statement."""

    __slots__ = ()

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

    @abstractmethod
    def get_cache_key(self) -> tuple[Any, ...]:
        """Return a tuple of stable, hashable components that uniquely represent the filter's configuration.

        The cache key should include all parameters that affect the filter's behavior.
        For example, a LimitOffsetFilter would return (limit, offset).

        Returns:
            Tuple of hashable values representing the filter's configuration
        """
        ...


class BeforeAfterFilter(StatementFilter):
    """Data required to filter a query on a ``datetime`` column.

    Note:
        After applying this filter, only the filter's parameters (e.g., before/after) will be present in the resulting SQL statement's parameters. Original parameters from the statement are not preserved in the result.
    """

    __slots__ = ("_param_name_after", "_param_name_before", "after", "before", "field_name")

    # Explicit property declarations for better IDE/linter support
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

        # Initialize parameter names
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
            result = statement.where(final_condition)
            _, named_params = self.extract_parameters()
            for name, value in named_params.items():
                result = result.add_named_parameter(name, value)
            return result
        return statement

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        # Include field name and datetime values (or None) to uniquely identify this filter
        return ("BeforeAfterFilter", self.field_name, self.before, self.after)


class OnBeforeAfterFilter(StatementFilter):
    """Data required to filter a query on a ``datetime`` column."""

    __slots__ = ("_param_name_on_or_after", "_param_name_on_or_before", "field_name", "on_or_after", "on_or_before")

    # Explicit property declarations for better IDE/linter support
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

        # Initialize parameter names
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
            _, named_params = self.extract_parameters()
            for name, value in named_params.items():
                result = result.add_named_parameter(name, value)
            return result
        return statement

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        # Include field name and datetime values (or None) to uniquely identify this filter
        return ("OnBeforeAfterFilter", self.field_name, self.on_or_before, self.on_or_after)


class InAnyFilter(StatementFilter, ABC, Generic[T]):
    """Subclass for methods that have a `prefer_any` attribute."""

    __slots__ = ()

    def append_to_statement(self, statement: "SQL") -> "SQL":
        raise NotImplementedError


class InCollectionFilter(InAnyFilter[T]):
    """Data required to construct a ``WHERE ... IN (...)`` clause.

    Note:
        After applying this filter, only the filter's parameters (e.g., the generated IN parameters) will be present in the resulting SQL statement's parameters. Original parameters from the statement are not preserved in the result.
    """

    __slots__ = ("_param_names", "field_name", "values")

    # Explicit property declarations for better IDE/linter support
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

        # Initialize parameter names
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
        _, named_params = self.extract_parameters()
        for name, value in named_params.items():
            result = result.add_named_parameter(name, value)
        return result

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        # Include field name and values (converted to tuple for hashability)
        values_tuple = tuple(self.values) if self.values is not None else None
        return ("InCollectionFilter", self.field_name, values_tuple)


class NotInCollectionFilter(InAnyFilter[T]):
    """Data required to construct a ``WHERE ... NOT IN (...)`` clause."""

    __slots__ = ("_param_names", "field_name", "values")

    # Explicit property declarations for better IDE/linter support
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

        # Initialize parameter names
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
        _, named_params = self.extract_parameters()
        for name, value in named_params.items():
            result = result.add_named_parameter(name, value)
        return result

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        # Include field name and values (converted to tuple for hashability)
        values_tuple = tuple(self.values) if self.values is not None else None
        return ("NotInCollectionFilter", self.field_name, values_tuple)


class AnyCollectionFilter(InAnyFilter[T]):
    """Data required to construct a ``WHERE column_name = ANY (array_expression)`` clause."""

    __slots__ = ("_param_names", "field_name", "values")

    # Explicit property declarations for better IDE/linter support
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

        # Initialize parameter names
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
        _, named_params = self.extract_parameters()
        for name, value in named_params.items():
            result = result.add_named_parameter(name, value)
        return result

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        # Include field name and values (converted to tuple for hashability)
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

        # Initialize parameter names
        self._param_names: list[str] = []
        if self.values:
            for i, _ in enumerate(self.values):
                self._param_names.append(f"{self.field_name}_not_any_{i}")

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
        _, named_params = self.extract_parameters()
        for name, value in named_params.items():
            result = result.add_named_parameter(name, value)
        return result

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        # Include field name and values (converted to tuple for hashability)
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

    # Explicit property declarations for better IDE/linter support
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

        # Initialize parameter names
        # Generate unique parameter names to avoid conflicts
        import uuid

        unique_suffix = str(uuid.uuid4()).replace("-", "")[:8]
        self._limit_param_name = f"limit_{unique_suffix}"
        self._offset_param_name = f"offset_{unique_suffix}"

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract filter parameters."""
        return [], {self._limit_param_name: self.limit, self._offset_param_name: self.offset}

    def append_to_statement(self, statement: "SQL") -> "SQL":
        # Create limit and offset expressions using our pre-generated parameter names
        from sqlglot import exp

        limit_placeholder = exp.Placeholder(this=self._limit_param_name)
        offset_placeholder = exp.Placeholder(this=self._offset_param_name)

        # Apply LIMIT and OFFSET to the statement
        result = statement

        # Check if the statement supports LIMIT directly
        if isinstance(result._statement, exp.Select):
            new_statement = result._statement.limit(limit_placeholder)
        else:
            # Wrap in a SELECT if the statement doesn't support LIMIT directly
            new_statement = exp.Select().from_(result._statement).limit(limit_placeholder)

        # Add OFFSET
        if isinstance(new_statement, exp.Select):
            new_statement = new_statement.offset(offset_placeholder)

        result = result.copy(statement=new_statement)

        # Add the parameters to the result
        _, named_params = self.extract_parameters()
        for name, value in named_params.items():
            result = result.add_named_parameter(name, value)
        return result.filter(self)

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        # Include limit and offset values
        return ("LimitOffsetFilter", self.limit, self.offset)


class OrderByFilter(StatementFilter):
    """Data required to construct a ``ORDER BY ...`` clause."""

    __slots__ = ("field_name", "sort_order")

    # Explicit property declarations for better IDE/linter support
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
        # ORDER BY doesn't use parameters, only column names and sort direction
        return [], {}

    def append_to_statement(self, statement: "SQL") -> "SQL":
        converted_sort_order = self.sort_order.lower()
        if converted_sort_order not in {"asc", "desc"}:
            converted_sort_order = "asc"

        col_expr = exp.column(self.field_name)
        order_expr = col_expr.desc() if converted_sort_order == "desc" else col_expr.asc()

        # Check if the statement supports ORDER BY directly
        if isinstance(statement._statement, exp.Select):
            new_statement = statement._statement.order_by(order_expr)
        else:
            # Wrap in a SELECT if the statement doesn't support ORDER BY directly
            new_statement = exp.Select().from_(statement._statement).order_by(order_expr)

        return statement.copy(statement=new_statement)

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        # Include field name and sort order
        return ("OrderByFilter", self.field_name, self.sort_order)


class SearchFilter(StatementFilter):
    """Data required to construct a ``WHERE field_name LIKE '%' || :value || '%'`` clause.

    Note:
        After applying this filter, only the filter's parameters (e.g., the generated search parameter) will be present in the resulting SQL statement's parameters. Original parameters from the statement are not preserved in the result.
    """

    __slots__ = ("_param_name", "field_name", "ignore_case", "value")

    # Explicit property declarations for better IDE/linter support
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

        # Initialize parameter names
        self._param_name: Optional[str] = None
        if self.value:
            if isinstance(self.field_name, str):
                self._param_name = f"{self.field_name}_search"
            else:
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

        _, named_params = self.extract_parameters()
        for name, value in named_params.items():
            result = result.add_named_parameter(name, value)
        return result

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        # Include field name(s), value, and ignore_case flag
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
        # Call parent __init__ first
        super().__init__(field_name, value, ignore_case)

        # Override parameter names for NOT search
        self._param_name: Optional[str] = None
        if self.value:
            if isinstance(self.field_name, str):
                self._param_name = f"{self.field_name}_not_search"
            else:
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

        _, named_params = self.extract_parameters()
        for name, value in named_params.items():
            result = result.add_named_parameter(name, value)
        return result

    def get_cache_key(self) -> tuple[Any, ...]:
        """Return cache key for this filter configuration."""
        # Include field name(s), value, and ignore_case flag
        field_names = tuple(sorted(self.field_name)) if isinstance(self.field_name, set) else self.field_name
        return ("NotInSearchFilter", field_names, self.value, self.ignore_case)


class OffsetPagination(Generic[T]):
    """Container for data returned using limit/offset pagination."""

    __slots__ = ("items", "limit", "offset", "total")

    # Explicit property declarations for better IDE/linter support
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
"""Aggregate type alias of the types supported for collection filtering."""
