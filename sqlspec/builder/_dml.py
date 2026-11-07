"""Reusable mixins for INSERT/UPDATE/DELETE builders."""

import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, Self, cast

import sqlglot
from mypy_extensions import trait
from sqlglot import exp
from sqlglot.errors import ParseError, TokenError

from sqlspec.builder._base import QueryBuilder
from sqlspec.builder._parsing_utils import (
    _SIMPLE_IDENTIFIER_RE,
    _normalize_dialect,
    _parse_dml_target,
    _simple_column_expression,
    resolve_value_expression,
)
from sqlspec.exceptions import SQLBuilderError
from sqlspec.protocols import SQLBuilderProtocol
from sqlspec.utils.serializers import schema_dump
from sqlspec.utils.type_guards import is_dict

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.builder._column import Column
    from sqlspec.builder._expression_wrappers import ExpressionWrapper
    from sqlspec.builder._select import Case

__all__ = (
    "DeleteFromClauseMixin",
    "InsertFromSelectMixin",
    "InsertIntoClauseMixin",
    "InsertValuesMixin",
    "ReturningClauseMixin",
    "UpdateFromClauseMixin",
    "UpdateSetClauseMixin",
    "UpdateTableClauseMixin",
)

ARG_PAIR_COUNT = 2
SINGLE_VALUE_COUNT = 1
_RETURNING_WORD_PART = r"[A-Za-z0-9_$]*[A-Za-z_$][A-Za-z0-9_$]*"
_RETURNING_WORD_RE: Final["re.Pattern[str]"] = re.compile(rf"{_RETURNING_WORD_PART}(?:\.{_RETURNING_WORD_PART}){{0,2}}")


@trait
class DeleteFromClauseMixin:
    """Mixin providing FROM clause support for DELETE builders."""

    __slots__ = ()

    dialect: "DialectType | None"

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...

    def from_(self, table: str) -> Self:
        current_expr = self.get_expression()
        if current_expr is None:
            self.set_expression(exp.Delete())
            current_expr = self.get_expression()

        if not isinstance(current_expr, exp.Delete):
            msg = f"Base expression for Delete is {type(current_expr).__name__}, expected Delete."
            raise SQLBuilderError(msg)

        assert current_expr is not None
        current_expr.set("this", _parse_dml_target(table, dialect=self.dialect))
        return self


@trait
class InsertIntoClauseMixin:
    __slots__ = ()

    dialect: "DialectType | None"

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...

    def into(self, table: str) -> Self:
        current_expr = self.get_expression()
        if current_expr is None:
            self.set_expression(exp.Insert())
            current_expr = self.get_expression()

        if not isinstance(current_expr, exp.Insert):
            msg = "Cannot set target table on a non-INSERT expression."
            raise SQLBuilderError(msg)

        assert current_expr is not None
        current_expr.set("this", _parse_dml_target(table, dialect=self.dialect))
        return self


@trait
class InsertValuesMixin:
    __slots__ = ()

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...

    _columns: list[str]

    def columns(self, *columns: str | exp.Expr) -> Self:
        current_expr = self.get_expression()
        if current_expr is None:
            self.set_expression(exp.Insert())
            current_expr = self.get_expression()

        if not isinstance(current_expr, exp.Insert):
            msg = "Cannot set columns on a non-INSERT expression."
            raise SQLBuilderError(msg)

        assert current_expr is not None
        current_this = current_expr.args.get("this")
        if current_this is None:
            msg = "Table must be set using .into() before setting columns."
            raise SQLBuilderError(msg)

        if columns:
            identifiers = [exp.to_identifier(col) if isinstance(col, str) else col for col in columns]
            table_expression = current_this.this if isinstance(current_this, exp.Schema) else current_this
            current_expr.set("this", exp.Schema(this=table_expression.copy(), expressions=identifiers))
        elif isinstance(current_this, exp.Schema):
            table_name = current_this.this
            current_expr.set("this", exp.Table(this=table_name))

        try:
            cols = self._columns
            if not columns:
                cols.clear()
            else:
                cols[:] = [col if isinstance(col, str) else str(col) for col in columns]
        except AttributeError:
            pass
        return self

    def values(self, *values: Any, **kwargs: Any) -> Self:
        current_expr = self.get_expression()
        if current_expr is None:
            self.set_expression(exp.Insert())
            current_expr = self.get_expression()

        if not isinstance(current_expr, exp.Insert):
            msg = "Cannot add values to a non-INSERT expression."
            raise SQLBuilderError(msg)

        assert current_expr is not None
        if current_expr.args.get("this") is None:
            msg = "The target table must be set using .into() before adding values."
            raise SQLBuilderError(msg)

        builder = cast("SQLBuilderProtocol", self)

        positional_values = list(values)
        if len(positional_values) == SINGLE_VALUE_COUNT and is_dict(positional_values[0]) and not kwargs:
            kwargs = positional_values[0]
            positional_values = []

        if kwargs and positional_values:
            msg = "Cannot mix positional values with keyword values."
            raise SQLBuilderError(msg)

        row_expressions: list[exp.Expr] = []
        column_defs: list[str] = list(self._columns or [])

        if kwargs:
            if not column_defs:
                self.columns(*kwargs.keys())
                column_defs = list(self._columns or [])
            for col, val in kwargs.items():
                column_name = str(col).split(".")[-1]
                resolved = resolve_value_expression(val, self, column_name)
                if resolved is not None:
                    row_expressions.append(resolved)
                    continue
                placeholder, _ = builder.create_placeholder(val, column_name)
                row_expressions.append(placeholder)
        else:
            if column_defs and len(positional_values) != len(column_defs):
                msg = (
                    f"Number of values ({len(positional_values)}) does not match the number of specified columns "
                    f"({len(column_defs)})."
                )
                raise SQLBuilderError(msg)

            for index, raw_value in enumerate(positional_values):
                if column_defs and index < len(column_defs):
                    column_token = column_defs[index]
                    column_name = column_token.rsplit(".", maxsplit=1)[-1]
                else:
                    column_name = f"value_{index + 1}"
                resolved = resolve_value_expression(raw_value, self, column_name)
                if resolved is not None:
                    row_expressions.append(resolved)
                    continue
                placeholder, _ = builder.create_placeholder(raw_value, column_name)
                row_expressions.append(placeholder)

        values_node = current_expr.args.get("expression")
        tuple_expression = exp.Tuple(expressions=row_expressions)
        if isinstance(values_node, exp.Values):
            values_node.append("expressions", tuple_expression)
        else:
            current_expr.set("expression", exp.Values(expressions=[tuple_expression]))
        return self

    def add_values(self, values: Sequence[Any]) -> Self:
        return self.values(*values)


@trait
class InsertFromSelectMixin:
    __slots__ = ()

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...

    def from_select(self, select_builder: SQLBuilderProtocol) -> Self:
        current_expr = self.get_expression()
        if current_expr is None:
            self.set_expression(exp.Insert())
            current_expr = self.get_expression()

        if not isinstance(current_expr, exp.Insert):
            msg = "Cannot set INSERT source on a non-INSERT expression."
            raise SQLBuilderError(msg)

        assert current_expr is not None
        if current_expr.args.get("this") is None:
            msg = "The target table must be set using .into() before adding values."
            raise SQLBuilderError(msg)

        if isinstance(self, QueryBuilder):
            select_expr: exp.Expr | None = self._embed_subquery(select_builder, alias_hint="insert_src")
        else:
            subquery_parameters = select_builder.parameters
            if subquery_parameters:
                builder_with_params = cast("SQLBuilderProtocol", self)
                for param_name, param_value in subquery_parameters.items():
                    builder_with_params.add_parameter(param_value, name=param_name)
            raw_expr = select_builder.get_expression()
            select_expr = raw_expr.copy() if isinstance(raw_expr, exp.Expr) else None

        if select_expr is not None and isinstance(select_expr, (exp.Select, exp.SetOperation)):
            select_with = select_expr.args.get("with_")
            if isinstance(select_with, exp.With):
                select_expr.set("with_", None)
                existing_with = current_expr.args.get("with_")
                if isinstance(existing_with, exp.With):
                    for cte_node in select_with.expressions:
                        if cte_node not in existing_with.expressions:
                            existing_with.append("expressions", cte_node.copy())
                    if select_with.args.get("recursive"):
                        existing_with.set("recursive", True)
                else:
                    current_expr.set("with_", select_with)
            current_expr.set("expression", select_expr)
        else:
            msg = "SelectBuilder must have a valid SELECT expression."
            raise SQLBuilderError(msg)
        return self


@trait
class UpdateTableClauseMixin:
    __slots__ = ()

    dialect: "DialectType | None"

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...

    def table(self, table_name: str, alias: str | None = None) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Update):
            self.set_expression(exp.Update(this=None, expressions=[]))
            current_expr = self.get_expression()

        assert current_expr is not None

        current_expr.set("this", _parse_dml_target(table_name, alias, dialect=self.dialect))
        return self


@trait
class UpdateSetClauseMixin:
    __slots__ = ()

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...
    def set(self, *args: Any, **kwargs: Any) -> Self:
        if not args and not kwargs:
            return self
        current_expr = self.get_expression()
        if current_expr is None:
            self.set_expression(exp.Update())
            current_expr = self.get_expression()

        if not isinstance(current_expr, exp.Update):
            msg = "Cannot add SET clause to non-UPDATE expression."
            raise SQLBuilderError(msg)

        assert current_expr is not None

        assignments: list[exp.Expr] = []
        if len(args) == ARG_PAIR_COUNT and not kwargs:
            col, val = args
            col_expr = col if isinstance(col, exp.Column) else exp.column(col)
            assignments.append(exp.EQ(this=col_expr, expression=self._process_update_value(val, col)))
        elif (len(args) == SINGLE_VALUE_COUNT and isinstance(args[0], Mapping)) or kwargs:
            all_values = dict(args[0] if args else {}, **kwargs)
            for col, val in all_values.items():
                assignments.append(exp.EQ(this=exp.column(col), expression=self._process_update_value(val, col)))
        else:
            msg = "Invalid arguments for set(): use (column, value), mapping, or kwargs."
            raise SQLBuilderError(msg)

        existing = current_expr.args.get("expressions", [])
        current_expr.set("expressions", existing + assignments)
        return self

    def set_from(self, data: Any, *, exclude_unset: bool = True) -> Self:
        """Set columns from a dict, dataclass, msgspec.Struct, Pydantic model, or attrs class.

        Schema instances are normalised via :func:`sqlspec.utils.serializers.schema_dump`
        with ``wire_format=False`` and dispatched to :meth:`set` via keyword unpack. The
        dict shape uses Python attribute names regardless of msgspec ``rename=`` or
        Pydantic ``Field(alias=...)``.

        Args:
            data: A dict, dataclass instance, ``msgspec.Struct``, ``pydantic.BaseModel``,
                or ``attrs``-decorated class instance.
            exclude_unset: If True, exclude fields that were never set (msgspec UNSET,
                Pydantic ``model_fields_set``, dataclass empty defaults). No-op for attrs.

        Returns:
            The current builder instance for method chaining.
        """
        payload = schema_dump(data, exclude_unset=exclude_unset, wire_format=False)
        return self.set(**payload)

    def _process_update_value(self, val: Any, col: Any) -> exp.Expr:
        column_name = col if isinstance(col, str) else str(col)
        if "." in column_name:
            column_name = column_name.split(".")[-1]
        resolved = resolve_value_expression(val, self, column_name or "set_src")
        if resolved is not None:
            return resolved
        sql_builder = cast("SQLBuilderProtocol", self)
        placeholder, _ = sql_builder.create_placeholder(val, column_name)
        return placeholder


@trait
class UpdateFromClauseMixin:
    __slots__ = ()

    dialect: "DialectType | None"

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...

    def from_(self, table: str | exp.Expr | Any, alias: str | None = None) -> Self:
        """Add a table or subquery to the UPDATE statement's FROM clause.

        Args:
            table: Target table name, expression, or builder instance.
            alias: Optional alias for the source table or subquery.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If called on a non-UPDATE expression or with an unsupported table type.
        """
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Update):
            msg = "Cannot add FROM clause to non-UPDATE expression. Set the main table first."
            raise SQLBuilderError(msg)

        table_expr: exp.Expr
        if isinstance(table, str):
            table_expr = _parse_dml_target(table, alias, dialect=self.dialect)
        elif isinstance(table, exp.Expr):
            if isinstance(table, (exp.Select, exp.SetOperation)):
                table_expr = exp.Subquery(this=table.copy())
                if alias:
                    table_expr = exp.alias_(table_expr, alias, table=True)
            else:
                table_expr = exp.alias_(table.copy(), alias, table=True) if alias else table.copy()
        elif (
            hasattr(table, "build")
            or hasattr(table, "to_statement")
            or hasattr(table, "get_expression")
            or hasattr(table, "_expression")
        ):
            raw_expression = None
            if hasattr(table, "_build_final_expression"):
                raw_expression = table._build_final_expression(copy=True)
            elif hasattr(table, "get_expression"):
                raw_expression = table.get_expression()
            elif hasattr(table, "_expression"):
                raw_expression = table._expression

            if raw_expression is None:
                msg = "Subquery builder has no expression to include in FROM clause."
                raise SQLBuilderError(msg)

            subquery_copy = (
                raw_expression
                if isinstance(table, QueryBuilder)
                else raw_expression.copy()
                if hasattr(raw_expression, "copy")
                else raw_expression
            )
            base_builder = cast("QueryBuilder", self)
            builder_alias = getattr(table, "alias_name", None) or getattr(table, "alias", None)
            if not isinstance(builder_alias, str):
                builder_alias = None
            if not builder_alias and hasattr(raw_expression, "alias_or_name"):
                builder_alias = raw_expression.alias_or_name
            effective_alias = alias or builder_alias or "subquery"

            subquery_params = getattr(table, "parameters", {})
            if subquery_params and isinstance(subquery_params, dict):
                param_mapping = base_builder._merge_cte_parameters(effective_alias, subquery_params)
                if param_mapping:
                    subquery_copy = base_builder._update_placeholders(subquery_copy, param_mapping)

            if isinstance(subquery_copy, exp.Values):
                if alias:
                    cols: list[str] = []
                    existing_alias = subquery_copy.args.get("alias")
                    source_columns = getattr(table, "columns", None)
                    if existing_alias and existing_alias.args.get("columns"):
                        cols = [c.name for c in existing_alias.args["columns"]]
                    elif isinstance(source_columns, (list, tuple)):
                        cols = [str(c) for c in source_columns]
                    table_expr = exp.alias_(subquery_copy, alias, table=cols or False)
                else:
                    table_expr = subquery_copy
            elif isinstance(subquery_copy, exp.Subquery):
                table_expr = exp.alias_(subquery_copy, alias, table=True) if alias else subquery_copy
            elif isinstance(subquery_copy, (exp.Select, exp.SetOperation)):
                table_expr = exp.Subquery(this=subquery_copy)
                if alias or builder_alias:
                    table_expr = exp.alias_(table_expr, alias or builder_alias, table=True)
            else:
                msg = "UPDATE FROM builder sources must be SELECT, VALUES, or subquery expressions."
                raise SQLBuilderError(msg)
        else:
            msg = f"Unsupported table type for FROM clause: {type(table)}"
            raise SQLBuilderError(msg)

        from_clause = current_expr.args.get("from_")
        if from_clause is None:
            current_expr.set("from_", exp.From(this=table_expr))
        else:
            from_table = from_clause.this
            from_table.append("joins", exp.Join(this=table_expr))

        return self


@trait
class ReturningClauseMixin:
    """Mixin providing RETURNING clause support for DML builders."""

    __slots__ = ()

    _expression: exp.Expr | None
    dialect: "DialectType | None"

    def returning(self, *columns: "str | exp.Expr | Column | ExpressionWrapper | Case") -> Self:
        """Add RETURNING clause to the DML statement.

        Each string is one SQL projection parsed with the builder dialect, so it must be trusted
        text, as with ``Select.select``. Other arguments must be SQLGlot nodes, SQLSpec expression
        wrappers, ``SQL`` objects, or query builders.

        Args:
            *columns: Columns or expressions to return.

        Returns:
            The builder instance for method chaining.

        Raises:
            SQLBuilderError: If expression not initialized or not DML, if no column is given, if a
                string is not exactly one projection, or if an argument cannot render as SQL.
        """
        if self._expression is None:
            msg = "Cannot add RETURNING: expression not initialized."
            raise SQLBuilderError(msg)
        if not isinstance(self._expression, (exp.Insert, exp.Update, exp.Delete)):
            msg = "RETURNING only supported for INSERT, UPDATE, DELETE statements."
            raise SQLBuilderError(msg)
        if not columns:
            msg = "RETURNING requires at least one column or expression."
            raise SQLBuilderError(msg)
        returning_exprs = [
            _parse_returning_column(col, self.dialect) if isinstance(col, str) else _resolve_returning_value(col, self)
            for col in columns
        ]
        self._expression.set("returning", exp.Returning(expressions=returning_exprs))
        return self


def _parse_returning_column(column: str, dialect: "DialectType | None") -> exp.Expr:
    """Parse one ``.returning()`` string into a single projection.

    Bare and dotted words stay columns, including keyword spellings such as ``user`` and words
    starting with a digit such as ``2fa``. A word of only digits is a number.

    Args:
        column: Projection text.
        dialect: Dialect whose tokenizer and parser read the text.

    Returns:
        The parsed projection.

    Raises:
        SQLBuilderError: If the text is not exactly one projection.
    """
    stripped = column.strip()
    if _SIMPLE_IDENTIFIER_RE.fullmatch(stripped):
        return _simple_column_expression(stripped)
    if _RETURNING_WORD_RE.fullmatch(stripped):
        return exp.column(*reversed(stripped.split(".")))
    msg = f"Invalid RETURNING expression {column!r}: pass one column or expression per argument."
    try:
        statements = sqlglot.parse(f"SELECT {stripped}", dialect=_normalize_dialect(dialect))
    except (ParseError, TokenError) as exc:
        raise SQLBuilderError(msg) from exc
    select = statements[0] if len(statements) == 1 else None
    if not isinstance(select, exp.Select) or len(select.expressions) != 1:
        raise SQLBuilderError(msg)
    if any(value for key, value in select.args.items() if key != "expressions"):
        raise SQLBuilderError(msg)
    projection: exp.Expr = select.expressions[0]
    return projection


def _resolve_returning_value(value: Any, builder: Any) -> exp.Expr:
    """Render a non-string ``.returning()`` argument, rejecting anything that is not a SQL carrier.

    Args:
        value: Argument passed to ``.returning()``.
        builder: Query builder receiving merged parameters.

    Returns:
        The projection expression.

    Raises:
        SQLBuilderError: If the value is not a SQLGlot node, expression wrapper, ``SQL`` object,
            or query builder.
    """
    resolved = resolve_value_expression(value, builder, "returning")
    if resolved is None:
        msg = f"Unsupported RETURNING value of type {type(value).__name__}."
        raise SQLBuilderError(msg)
    return resolved
