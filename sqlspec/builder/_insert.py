"""INSERT statement builder.

Provides a fluent interface for building SQL INSERT queries with
parameter binding and validation.
"""

from typing import TYPE_CHECKING, Any, Final

from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._base import QueryBuilder
from sqlspec.builder._dml import InsertFromSelectMixin, InsertIntoClauseMixin, InsertValuesMixin
from sqlspec.builder._explain import ExplainMixin
from sqlspec.builder._parsing_utils import extract_sql_object_expression
from sqlspec.builder._select import ReturningClauseMixin
from sqlspec.core import SQLResult
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.deprecation import warn_deprecation
from sqlspec.utils.serializers import schema_dump, serialize_collection
from sqlspec.utils.type_guards import has_expression_and_sql

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


__all__ = ("Insert",)

ERR_MSG_TABLE_NOT_SET: Final[str] = "The target table must be set using .into() before adding values."
ERR_MSG_INTERNAL_EXPRESSION_TYPE: Final[str] = "Internal error: expression is not an Insert instance as expected."
ERR_MSG_EXPRESSION_NOT_INITIALIZED: Final[str] = "Internal error: base expression not initialized."


class Insert(
    QueryBuilder, ReturningClauseMixin, InsertValuesMixin, InsertFromSelectMixin, InsertIntoClauseMixin, ExplainMixin
):
    """Builder for INSERT statements.

    Constructs SQL INSERT queries with parameter binding and validation.
    """

    __slots__ = ("_columns", "_values_added_count")

    def __init__(self, table: str | None = None, **kwargs: Any) -> None:
        """Initialize INSERT with optional table.

        Args:
            table: Target table name
            **kwargs: Additional QueryBuilder arguments
        """
        (dialect, schema, enable_optimization, optimize_joins, optimize_predicates, simplify_expressions) = (
            self._parse_query_builder_kwargs(kwargs)
        )
        super().__init__(
            dialect=dialect,
            schema=schema,
            enable_optimization=enable_optimization,
            optimize_joins=optimize_joins,
            optimize_predicates=optimize_predicates,
            simplify_expressions=simplify_expressions,
        )

        self._columns: list[str] = []
        self._values_added_count: int = 0

        self._initialize_expression()

        if table:
            self.into(table)

    def _create_base_expression(self) -> exp.Insert:
        """Create a base INSERT expression.

        This method is called by the base QueryBuilder during initialization.

        Returns:
            A new sqlglot Insert expression.
        """
        return exp.Insert()

    @property
    def _expected_result_type(self) -> "type[SQLResult]":
        """Specifies the expected result type for an INSERT query.

        Returns:
            The type of result expected for INSERT operations.
        """
        return SQLResult

    def _get_insert_expression(self) -> exp.Insert:
        """Safely gets and casts the internal expression to exp.Insert.

        Returns:
            The internal expression as exp.Insert.

        Raises:
            SQLBuilderError: If the expression is not initialized or is not an Insert.
        """
        if self._expression is None:
            raise SQLBuilderError(ERR_MSG_EXPRESSION_NOT_INITIALIZED)
        if not isinstance(self._expression, exp.Insert):
            raise SQLBuilderError(ERR_MSG_INTERNAL_EXPRESSION_TYPE)
        return self._expression

    def get_insert_expression(self) -> exp.Insert:
        """Get the insert expression (public API)."""
        return self._get_insert_expression()

    def _bind_values_from_dict(self, data: "Mapping[str, Any]") -> "Self":
        """Bind a single dict row without deprecation warnings (internal)."""
        insert_expr = self._get_insert_expression()
        if insert_expr.args.get("this") is None:
            raise SQLBuilderError(ERR_MSG_TABLE_NOT_SET)

        data_keys = list(data.keys())
        if not self._columns:
            self.columns(*data_keys)
        elif set(self._columns) != set(data_keys):
            msg = f"Dictionary keys {set(data_keys)} do not match existing columns {set(self._columns)}."
            raise SQLBuilderError(msg)

        return self.values(*[data[col] for col in self._columns])

    def _bind_values_from_dicts(self, data: "Sequence[Mapping[str, Any]]") -> "Self":
        """Bind many dict rows without deprecation warnings (internal)."""
        if not data:
            return self

        first_dict = data[0]
        if not self._columns:
            self.columns(*first_dict.keys())

        expected_keys = set(self._columns)
        for i, row_dict in enumerate(data):
            if set(row_dict.keys()) != expected_keys:
                msg = (
                    f"Dictionary at index {i} has keys {set(row_dict.keys())} "
                    f"which do not match expected keys {expected_keys}."
                )
                raise SQLBuilderError(msg)

        for row_dict in data:
            self.values(*[row_dict[col] for col in self._columns])

        return self

    def values_from_dict(self, data: "Mapping[str, Any]") -> "Self":
        """Adds a row of values from a dictionary.

        .. deprecated:: 0.46.2
            Use :meth:`values_from` instead — it accepts dicts, msgspec Structs,
            Pydantic models, dataclasses, and attrs classes.

        Args:
            data: A mapping of column names to values.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If `into()` has not been called to set the table.
        """
        warn_deprecation(
            version="0.46.2",
            deprecated_name="Insert.values_from_dict",
            kind="method",
            alternative="Insert.values_from() — accepts dicts, msgspec Structs, Pydantic models, dataclasses, and attrs classes",
            removal_in="0.48.0",
        )
        return self._bind_values_from_dict(data)

    def values_from_dicts(self, data: "Sequence[Mapping[str, Any]]") -> "Self":
        """Adds multiple rows of values from a sequence of dictionaries.

        .. deprecated:: 0.46.2
            Use :meth:`values_from_many` instead — it accepts dicts, msgspec Structs,
            Pydantic models, dataclasses, and attrs classes.

        Args:
            data: A sequence of mappings, each representing a row of data.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If `into()` has not been called to set the table,
                           or if dictionaries have inconsistent keys.
        """
        warn_deprecation(
            version="0.46.2",
            deprecated_name="Insert.values_from_dicts",
            kind="method",
            alternative="Insert.values_from_many() — accepts dicts, msgspec Structs, Pydantic models, dataclasses, and attrs classes",
            removal_in="0.48.0",
        )
        return self._bind_values_from_dicts(data)

    def values_from(self, data: Any, *, exclude_unset: bool = True) -> "Self":
        """Add a row of values from a dict, dataclass, msgspec.Struct, Pydantic model, or attrs class.

        Schema instances are normalised via :func:`sqlspec.utils.serializers.schema_dump`
        with ``wire_format=False`` then bound as a single row. The dict shape uses Python
        attribute names regardless of msgspec ``rename=`` or Pydantic ``Field(alias=...)``;
        wire-aligned names never make sense for SQL column binding.

        Args:
            data: A dict, dataclass instance, ``msgspec.Struct``, ``pydantic.BaseModel``,
                or ``attrs``-decorated class instance.
            exclude_unset: If True, exclude fields that were never set. Honoured by
                msgspec (UNSET fields), Pydantic (``model_fields_set``), and dataclass
                (via empty default-field semantics). No-op for attrs.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If ``into()`` has not been called or the dict shape is
                incompatible with previously set columns.
        """
        payload = schema_dump(data, exclude_unset=exclude_unset, wire_format=False)
        return self._bind_values_from_dict(payload)

    def values_from_many(self, items: "Sequence[Any]", *, exclude_unset: bool = True) -> "Self":
        """Add multiple rows from a sequence of dicts, dataclasses, msgspec Structs, Pydantic models, or attrs classes.

        Each item is normalised via :func:`sqlspec.utils.serializers.schema_dump` with
        ``wire_format=False`` then bound as a multi-row INSERT. Mixed schema kinds in the
        same sequence are supported (each item normalises independently); however, the
        resulting dict shapes must agree on key sets.

        Args:
            items: A sequence of schema instances or dicts.
            exclude_unset: See :meth:`values_from` for per-library semantics.

        Returns:
            The current builder instance for method chaining. Empty input returns the
            builder unchanged.

        Raises:
            SQLBuilderError: If ``into()`` has not been called or item dict shapes
                disagree on key sets.
        """
        if not items:
            return self
        payload = serialize_collection(items, exclude_unset=exclude_unset, wire_format=False)
        return self._bind_values_from_dicts(payload)

    def on_conflict(self, *columns: str) -> "ConflictBuilder":
        """Adds an ON CONFLICT clause with specified columns.

        Args:
            *columns: Column names that define the conflict. If no columns provided,
                     creates an ON CONFLICT without specific columns (catches all conflicts).

        Returns:
            A ConflictBuilder instance for chaining conflict resolution methods.

        Example:
            ```python
            sql.insert("users").values(id=1, name="John").on_conflict(
                "id"
            ).do_nothing()

            sql.insert("users").values(...).on_conflict(
                "email", "username"
            ).do_update(updated_at=sql.raw("NOW()"))

            sql.insert("users").values(...).on_conflict().do_nothing()
            ```
        """
        return ConflictBuilder(self, columns)

    def on_conflict_do_nothing(self, *columns: str) -> "Insert":
        """Adds an ON CONFLICT DO NOTHING clause (convenience method).

        Args:
            *columns: Column names that define the conflict. If no columns provided,
                     creates an ON CONFLICT without specific columns.

        Returns:
            The current builder instance for method chaining.

        Note:
            This is a convenience method. For more control, use on_conflict().do_nothing().
        """
        return self.on_conflict(*columns).do_nothing()

    def on_duplicate_key_update(self, **kwargs: Any) -> "Insert":
        """Adds MySQL-style ON DUPLICATE KEY UPDATE clause.

        Args:
            **kwargs: Column-value pairs to update on duplicate key.

        Returns:
            The current builder instance for method chaining.

        Note:
            This method creates MySQL-specific ON DUPLICATE KEY UPDATE syntax.
            For PostgreSQL, use on_conflict() instead.
        """
        if not kwargs:
            return self

        insert_expr = self._get_insert_expression()

        set_expressions = []
        for col, val in kwargs.items():
            if has_expression_and_sql(val):
                value_expr = extract_sql_object_expression(val, builder=self)
            elif isinstance(val, exp.Expr):
                value_expr = val
            else:
                param_name = self.generate_unique_parameter_name(col)
                _, param_name = self.add_parameter(val, name=param_name)
                value_expr = exp.Placeholder(this=param_name)

            set_expressions.append(exp.EQ(this=exp.column(col), expression=value_expr))

        on_conflict = exp.OnConflict(duplicate=True, action=exp.var("UPDATE"), expressions=set_expressions or None)

        insert_expr.set("conflict", on_conflict)

        return self


class ConflictBuilder:
    """Builder for ON CONFLICT clauses in INSERT statements.

    Constructs conflict resolution clauses using PostgreSQL-style syntax,
    which SQLGlot can transpile to other dialects.
    """

    __slots__ = ("_columns", "_insert_builder")

    def __init__(self, insert_builder: "Insert", columns: tuple[str, ...]) -> None:
        """Initialize ConflictBuilder.

        Args:
            insert_builder: The parent Insert builder
            columns: Column names that define the conflict
        """
        self._insert_builder = insert_builder
        self._columns = columns

    def do_nothing(self) -> "Insert":
        """Add DO NOTHING conflict resolution.

        Returns:
            The parent Insert builder for method chaining.

        Example:
            ```python
            sql.insert("users").values(id=1, name="John").on_conflict(
                "id"
            ).do_nothing()
            ```
        """
        insert_expr = self._insert_builder.get_insert_expression()

        conflict_keys = [exp.to_identifier(col) for col in self._columns] if self._columns else None
        on_conflict = exp.OnConflict(conflict_keys=conflict_keys, action=exp.var("DO NOTHING"))

        insert_expr.set("conflict", on_conflict)
        return self._insert_builder

    def do_update(self, **kwargs: Any) -> "Insert":
        """Add DO UPDATE conflict resolution with SET clauses.

        Args:
            **kwargs: Column-value pairs to update on conflict.

        Returns:
            The parent Insert builder for method chaining.

        Example:
            ```python
            sql.insert("users").values(id=1, name="John").on_conflict(
                "id"
            ).do_update(
                name="Updated Name", updated_at=sql.raw("NOW()")
            )
            ```
        """
        insert_expr = self._insert_builder.get_insert_expression()

        set_expressions = []
        for col, val in kwargs.items():
            if has_expression_and_sql(val):
                value_expr = extract_sql_object_expression(val, builder=self._insert_builder)
            elif isinstance(val, exp.Expr):
                value_expr = val
            else:
                param_name = self._insert_builder.generate_unique_parameter_name(col)
                _, param_name = self._insert_builder.add_parameter(val, name=param_name)
                value_expr = exp.Placeholder(this=param_name)

            set_expressions.append(exp.EQ(this=exp.column(col), expression=value_expr))

        conflict_keys = [exp.to_identifier(col) for col in self._columns] if self._columns else None
        on_conflict = exp.OnConflict(
            conflict_keys=conflict_keys, action=exp.var("DO UPDATE"), expressions=set_expressions or None
        )

        insert_expr.set("conflict", on_conflict)
        return self._insert_builder
