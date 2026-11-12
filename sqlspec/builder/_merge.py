"""MERGE statement builder.

Provides a fluent interface for building SQL MERGE queries with
parameter binding and validation.
"""

from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from itertools import starmap
from typing import TYPE_CHECKING, Any, Final, Self, cast
from uuid import UUID

from mypy_extensions import trait
from sqlglot import exp

from sqlspec.builder._base import QueryBuilder
from sqlspec.builder._explain import ExplainMixin
from sqlspec.builder._parsing_utils import (
    _coerce_column,
    _parse_table_reference,
    resolve_value_expression,
    unwrap_expression_source,
)
from sqlspec.exceptions import DialectNotSupportedError, SQLBuilderError
from sqlspec.utils.dispatch import TypeDispatcher
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.protocols import SQLBuilderProtocol

__all__ = ("Merge",)


MERGE_UNSUPPORTED_DIALECTS = frozenset({"mysql", "sqlite", "duckdb", "sybase"})
_POSTGRES_TYPE_DISPATCHER = TypeDispatcher[str]()
_ORACLE_TYPE_DISPATCHER = TypeDispatcher[str]()

_POSTGRES_TYPE_DISPATCHER.register(bool, "BOOLEAN")
_POSTGRES_TYPE_DISPATCHER.register(int, "INTEGER")
_POSTGRES_TYPE_DISPATCHER.register(float, "DOUBLE PRECISION")
_POSTGRES_TYPE_DISPATCHER.register(Decimal, "NUMERIC")
_POSTGRES_TYPE_DISPATCHER.register(dict, "JSONB")
_POSTGRES_TYPE_DISPATCHER.register(list, "JSONB")
_POSTGRES_TYPE_DISPATCHER.register(datetime, "TIMESTAMP")

_ORACLE_TYPE_DISPATCHER.register(bool, "NUMBER(1)")
_ORACLE_TYPE_DISPATCHER.register(int, "NUMBER")
_ORACLE_TYPE_DISPATCHER.register(float, "NUMBER")
_ORACLE_TYPE_DISPATCHER.register(Decimal, "NUMBER")
_ORACLE_TYPE_DISPATCHER.register(dict, "JSON")
_ORACLE_TYPE_DISPATCHER.register(list, "JSON")
_ORACLE_TYPE_DISPATCHER.register(datetime, "TIMESTAMP")

_TERADATA_DECIMAL_MAX_PRECISION: Final[int] = 38
_TERADATA_VARBYTE_MAX_LENGTH: Final[int] = 64000
_TERADATA_VARCHAR_MAX_LENGTH: Final[int] = 32000

_DB2_VARCHAR_MAX_LENGTH: Final[int] = 32672
_DB2_DEFAULT_TYPE: Final[str] = f"VARCHAR({_DB2_VARCHAR_MAX_LENGTH})"
_DB2_TYPE_DISPATCHER = TypeDispatcher[str]()
_DB2_TYPE_DISPATCHER.register(bool, "BOOLEAN")
_DB2_TYPE_DISPATCHER.register(int, "BIGINT")
_DB2_TYPE_DISPATCHER.register(float, "DOUBLE")
_DB2_TYPE_DISPATCHER.register(Decimal, "DECFLOAT(34)")
_DB2_TYPE_DISPATCHER.register(datetime, "TIMESTAMP")
_DB2_TYPE_DISPATCHER.register(date, "DATE")
_DB2_TYPE_DISPATCHER.register(time, "TIME")
_DB2_TYPE_DISPATCHER.register(bytes, f"VARBINARY({_DB2_VARCHAR_MAX_LENGTH})")
_DB2_TYPE_DISPATCHER.register(dict, "CLOB")
_DB2_TYPE_DISPATCHER.register(list, "CLOB")
_DB2_TYPE_DISPATCHER.register(UUID, "VARCHAR(36)")


@trait
class _MergeAssignmentMixin:
    """Shared assignment helpers for MERGE clause mixins."""

    __slots__ = ()

    def _process_assignment(self, target_column: str, value: Any) -> exp.Expr:
        column_identifier = _coerce_column(target_column)
        column_name = target_column if isinstance(target_column, str) else str(target_column)
        column_leaf = column_name.split(".")[-1]
        resolved = resolve_value_expression(value, self, column_leaf)
        if resolved is not None:
            return exp.EQ(this=column_identifier, expression=resolved)
        placeholder, _ = cast("QueryBuilder", self).create_placeholder(value, column_leaf)
        return exp.EQ(this=column_identifier, expression=placeholder)

    def _append_when(self, current_expr: exp.Merge, when_node: exp.When) -> None:
        whens = current_expr.args.get("whens")
        if not isinstance(whens, exp.Whens):
            whens = exp.Whens(expressions=[])
            current_expr.set("whens", whens)
        whens.append("expressions", when_node)


@trait
class MergeIntoClauseMixin:
    """Mixin providing INTO clause for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...

    def into(self, table: str | exp.Expr, alias: str | None = None) -> Self:
        current_expr = _ensure_merge_expression(cast("SQLBuilderProtocol", self))

        table_expr: exp.Expr
        if isinstance(table, str):
            table_expr = _parse_table_reference(table, alias, cast("QueryBuilder", self).dialect)
            cast("QueryBuilder", self)._merge_target_quoted = any(  # pyright: ignore[reportPrivateUsage]
                part.meta.get("sqlspec_explicit_table_quote") for part in table_expr.parts
            )
        else:
            table_expr = table

        current_expr.set("this", table_expr)
        return self


@trait
class MergeUsingClauseMixin(_MergeAssignmentMixin):
    """Mixin providing USING clause for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...
    def using(self, source: str | exp.Expr | Any, alias: str | None = None) -> Self:
        current_expr = _ensure_merge_expression(cast("SQLBuilderProtocol", self))
        builder = cast("QueryBuilder", self)
        is_teradata = builder.dialect_name == "teradata"
        source_expr: exp.Expr
        unwrapped_source = unwrap_expression_source(source, builder=cast("SQLBuilderProtocol", self))
        if isinstance(source, str):
            source_expr = _parse_table_reference(source, alias, builder.dialect)
        elif isinstance(source, (dict, list)):
            paren_expr = self._create_dict_source_expression(source, alias)
            if alias and isinstance(paren_expr, exp.Paren):
                source_expr = exp.Subquery(this=paren_expr.this, alias=exp.to_identifier(alias))
            else:
                source_expr = paren_expr
        elif isinstance(source, QueryBuilder):
            effective_alias = alias or ("src" if is_teradata else None)
            subquery_expression_source = builder._embed_subquery(source, alias_hint=effective_alias or "using_src")
            if isinstance(subquery_expression_source, exp.Subquery):
                if alias:
                    source_expr = subquery_expression_source.copy()
                    source_expr.set("alias", exp.to_identifier(alias))
                elif effective_alias and not subquery_expression_source.alias:
                    source_expr = subquery_expression_source.copy()
                    source_expr.set("alias", exp.to_identifier(effective_alias))
                else:
                    source_expr = subquery_expression_source
            elif effective_alias:
                source_expr = exp.Subquery(this=subquery_expression_source, alias=exp.to_identifier(effective_alias))
            else:
                source_expr = exp.paren(subquery_expression_source)
        elif unwrapped_source is not None:
            effective_alias = alias or ("src" if is_teradata else None)
            if isinstance(unwrapped_source, (exp.Select, exp.SetOperation)):
                source_expr = (
                    exp.Subquery(this=unwrapped_source, alias=exp.to_identifier(effective_alias))
                    if effective_alias
                    else exp.paren(unwrapped_source)
                )
            elif isinstance(unwrapped_source, exp.Paren) and (
                alias or (is_teradata and isinstance(unwrapped_source.this, (exp.Select, exp.SetOperation)))
            ):
                inner = unwrapped_source.this
                source_expr = exp.Subquery(this=inner, alias=exp.to_identifier(effective_alias or "src"))
            elif isinstance(unwrapped_source, exp.Subquery):
                if alias:
                    source_expr = unwrapped_source.copy()
                    source_expr.set("alias", exp.to_identifier(alias))
                elif is_teradata and not unwrapped_source.alias:
                    source_expr = unwrapped_source.copy()
                    source_expr.set("alias", exp.to_identifier("src"))
                else:
                    source_expr = unwrapped_source
            else:
                source_expr = exp.alias_(unwrapped_source, alias) if alias else unwrapped_source
        else:
            msg = f"Unsupported source type for USING clause: {type(source)}"
            raise SQLBuilderError(msg)

        current_expr.set("using", source_expr)
        return self

    def _create_dict_source_expression(
        self, source: "dict[str, Any] | list[dict[str, Any]]", alias: "str | None"
    ) -> "exp.Expr":
        """Create USING clause expression from dict or list of dicts.

        Uses JSON-based approach for type-safe bulk operations:
            - PostgreSQL: json_populate_recordset(NULL::table_name, $1::jsonb)
            - Oracle: JSON_TABLE(:payload, '$[*]' COLUMNS (...))
            - Db2: SELECT of CAST parameter markers per row, joined with UNION ALL
            - Others: Fall back to SELECT with parameterized values

        Args:
            source: Dict or list of dicts for USING clause
            alias: Optional alias for the source

        Returns:
            Expression for USING clause
        """
        data: list[dict[str, Any]]
        is_list: bool
        if isinstance(source, list):
            data = source
            is_list = True
        else:
            data = [source]
            is_list = False

        if not data:
            msg = "Cannot create USING clause from empty list"
            raise SQLBuilderError(msg)

        columns = list(data[0].keys())
        builder = cast("QueryBuilder", self)
        dialect = builder.dialect_name

        if dialect in {"postgres", "oracle"}:
            for record in data:
                for column, value in record.items():
                    if resolve_value_expression(value, self, str(column)) is not None:
                        msg = (
                            f"MERGE USING on {dialect} passes source rows as JSON, so column '{column}' "
                            "must hold a plain value. Use a SELECT source for expressions."
                        )
                        raise SQLBuilderError(msg)
        if dialect == "postgres":
            return self._create_postgres_json_source(data, columns, is_list, alias)
        if dialect == "oracle":
            return self._create_oracle_json_source(data, columns, alias)
        if dialect == "teradata":
            return self._create_teradata_select_source(data, alias)
        if dialect == "db2":
            return self._create_db2_select_source(data, columns, is_list, alias)

        return self._create_select_union_source(data, columns, is_list, alias)

    def _create_postgres_json_source(
        self, data: "list[dict[str, Any]]", columns: "list[str]", is_list: bool, alias: "str | None"
    ) -> "exp.Expr":
        """Create PostgreSQL jsonb_to_recordset source with explicit column definitions.

        Uses jsonb_to_recordset(jsonb) AS alias(col1 type1, col2 type2, ...) pattern
        which avoids composite type dependencies and provides explicit type definitions.

        Passes native Python list to driver for JSON serialization via driver-specific
        mechanisms (AsyncPG json_serializer, Psycopg/Psqlpy JSON codecs).
        """

        json_value = data if is_list else [data[0]]
        _, json_param_name = cast("QueryBuilder", self).create_placeholder(json_value, "json_data")

        sample_values: dict[str, Any] = {}
        for record in data:
            for column, value in record.items():
                if value is not None and column not in sample_values:
                    sample_values[column] = value

        alias_name = alias or "src"
        recordset_alias = f"{alias_name}_data"

        recordset_table = exp.Table(
            this=exp.Anonymous(
                this="jsonb_to_recordset",
                expressions=[
                    exp.Cast(
                        this=exp.Placeholder(this=json_param_name), to=exp.DataType.build("JSONB", dialect="postgres")
                    )
                ],
            ),
            alias=exp.TableAlias(
                this=exp.to_identifier(recordset_alias),
                columns=[
                    exp.ColumnDef(
                        this=exp.to_identifier(column),
                        kind=exp.DataType.build(
                            self._infer_postgres_type(sample_values.get(column)), dialect="postgres"
                        ),
                    )
                    for column in columns
                ],
            ),
        )

        parsed = exp.Select(
            expressions=[exp.column(column) for column in columns], from_=exp.From(this=recordset_table)
        )
        return exp.Subquery(
            this=parsed,
            alias=exp.TableAlias(
                this=exp.to_identifier(alias_name), columns=[exp.to_identifier(col) for col in columns]
            ),
        )

    def _create_oracle_json_source(
        self, data: "list[dict[str, Any]]", columns: "list[str]", alias: "str | None"
    ) -> "exp.Expr":
        """Create Oracle JSON_TABLE source (production-proven pattern from oracledb-vertexai-demo)."""
        json_value = to_json(data)
        _, json_param_name = cast("QueryBuilder", self).create_placeholder(json_value, "json_payload")

        sample_values: dict[str, Any] = {}
        for record in data:
            for column, value in record.items():
                if value is not None and column not in sample_values:
                    sample_values[column] = value

        alias_name = alias or "src"
        json_table = exp.Table(
            this=exp.JSONTable(
                this=exp.Placeholder(this=json_param_name),
                path=exp.Literal.string("$[*]"),
                schema=exp.JSONSchema(
                    expressions=[
                        exp.JSONColumnDef(
                            this=exp.to_identifier(column),
                            kind=exp.DataType.build(
                                self._infer_oracle_type(sample_values.get(column)), dialect="oracle"
                            ),
                            path=exp.Literal.string(f"$.{column}"),
                        )
                        for column in columns
                    ]
                ),
            )
        )
        parsed = exp.Select(expressions=[exp.column(column) for column in columns], from_=exp.From(this=json_table))
        return exp.Subquery(this=parsed, alias=exp.TableAlias(this=exp.to_identifier(alias_name)))

    def _infer_postgres_type(self, value: "Any") -> str:
        """Infer PostgreSQL column type from Python value.

        Maps Python types to PostgreSQL types for jsonb_to_recordset column definitions.

        Note: When value is None and we cannot infer the type from other records,
        we default to NUMERIC which is more permissive than TEXT for NULL values
        and commonly used for business data.
        """
        if value is None:
            return "NUMERIC"
        resolved_type = _POSTGRES_TYPE_DISPATCHER.get(value)
        if resolved_type is not None:
            return resolved_type
        return "TEXT"

    def _infer_oracle_type(self, value: "Any") -> str:
        """Infer Oracle column type for JSON_TABLE projection."""
        varchar2_max = 4000

        resolved_type = _ORACLE_TYPE_DISPATCHER.get(value)
        if resolved_type is not None:
            return resolved_type
        if value is not None and len(str(value)) > varchar2_max:
            return "CLOB"
        return f"VARCHAR2({varchar2_max})"

    def _infer_db2_type(self, value: "Any") -> str:
        """Infer the Db2 type used to CAST a USING-source parameter marker.

        Args:
            value: Sample value for the column.

        Returns:
            Db2 data type name.
        """
        resolved_type = _DB2_TYPE_DISPATCHER.get(value)
        if resolved_type is not None:
            return resolved_type
        if isinstance(value, str) and len(value) > _DB2_VARCHAR_MAX_LENGTH:
            return "CLOB"
        return _DB2_DEFAULT_TYPE

    def _infer_teradata_column_type(self, values: Sequence[Any]) -> str:
        """Choose one non-truncating type for every branch of a source column."""
        values = [value for value in values if value is not None]
        if not values:
            return "VARCHAR(32000)"
        if any(isinstance(value, (dict, list)) for value in values):
            return "CLOB"
        if all(isinstance(value, bool) for value in values):
            return "BYTEINT"
        decimals = [value for value in values if isinstance(value, Decimal)]
        if any(not value.is_finite() for value in decimals):
            msg = "Cannot infer Teradata DECIMAL type for non-finite Decimal value."
            raise SQLBuilderError(msg)
        if all(isinstance(value, (int, float, Decimal)) for value in values):
            if any(isinstance(value, float) for value in values):
                return "DOUBLE PRECISION"
            if decimals:
                decimals = [Decimal(value) for value in values]
                scale = max(max(0, -int(value.as_tuple().exponent)) for value in decimals)
                integer_digits = max(
                    max(1, len(value.as_tuple().digits) + int(value.as_tuple().exponent)) for value in decimals
                )
                precision = max(18, integer_digits + scale)
                if precision > _TERADATA_DECIMAL_MAX_PRECISION:
                    msg = "Teradata DECIMAL precision exceeds 38 digits."
                    raise SQLBuilderError(msg)
                return f"DECIMAL({precision}, {scale})"
            return "BIGINT"
        if all(isinstance(value, date) for value in values):
            datetimes = [value for value in values if isinstance(value, datetime)]
            if datetimes:
                return (
                    "TIMESTAMP WITH TIME ZONE" if any(value.tzinfo is not None for value in datetimes) else "TIMESTAMP"
                )
            return "DATE"
        if all(isinstance(value, time) for value in values):
            return "TIME"
        if all(isinstance(value, UUID) for value in values):
            return "VARCHAR(36)"
        if all(isinstance(value, (bytes, bytearray)) for value in values):
            size = max(1, *(len(value) for value in values))
            return "BLOB" if size > _TERADATA_VARBYTE_MAX_LENGTH else f"VARBYTE({size})"
        size = max(1, *(len(str(value)) for value in values))
        return "CLOB" if size > _TERADATA_VARCHAR_MAX_LENGTH else f"VARCHAR({size})"

    def _create_teradata_select_source(self, rows: list[dict[str, Any]], alias: str | None) -> exp.Expr:
        """Build tableless typed SELECT branches for Teradata MERGE sources."""
        builder = cast("QueryBuilder", self)
        columns = list(rows[0])
        column_types = {
            column: exp.DataType.build(
                self._infer_teradata_column_type([
                    row.get(column)
                    for row in rows
                    if resolve_value_expression(row.get(column), self, column.split(".")[-1]) is None
                ]),
                dialect="teradata",
            )
            for column in columns
        }
        query: exp.Expr | None = None
        for row in rows:
            projections: list[exp.Expr] = []
            for column in columns:
                value = row.get(column)
                leaf = column.split(".")[-1]
                resolved = resolve_value_expression(value, self, leaf)
                if resolved is None:
                    if isinstance(value, (dict, list)):
                        value = to_json(value)
                    elif isinstance(value, bytearray):
                        value = bytes(value)
                    placeholder, _ = builder.create_placeholder(value, leaf)
                    resolved = exp.Cast(this=placeholder, to=column_types[column].copy())
                projections.append(exp.alias_(resolved, column))
            branch = exp.Select(expressions=projections)
            query = branch if query is None else exp.Union(this=query, expression=branch, distinct=False)
        effective_alias = alias or "src"
        return exp.Subquery(
            this=query,
            alias=exp.TableAlias(
                this=exp.to_identifier(effective_alias), columns=[exp.to_identifier(column) for column in columns]
            ),
        )

    def _create_db2_select_source(
        self, data: "list[dict[str, Any]]", columns: "list[str]", is_list: bool, alias: "str | None"
    ) -> "exp.Expr":
        """Create a Db2 USING source whose parameter markers are CAST to value-derived types.

        Each row becomes a SELECT of ``CAST(:param AS <type>) AS column``; rows are joined with
        UNION ALL. Column types come from the first non-None value of each column across all
        rows. ``dict`` and ``list`` values are bound as JSON text.

        Args:
            data: Source rows.
            columns: Column names taken from the first row.
            is_list: Whether the source was supplied as a list of rows.
            alias: Optional alias for the source.

        Returns:
            Subquery aliased with its column list when ``alias`` is given, otherwise a
            parenthesized query.
        """
        builder = cast("QueryBuilder", self)
        rows = data if is_list else data[:1]
        sample_values: dict[str, Any] = {}
        for record in rows:
            for column, value in record.items():
                if value is not None and column not in sample_values:
                    sample_values[column] = value
        column_types = {
            column: exp.DataType.build(self._infer_db2_type(sample_values.get(column)), dialect="db2")
            for column in columns
        }

        source_expr: exp.Expr | None = None
        for row in rows:
            select_items: list[exp.Expr] = []
            for column in columns:
                value = row.get(column)
                if isinstance(value, (dict, list)):
                    value = to_json(value)
                column_name = str(column).split(".")[-1]
                resolved = resolve_value_expression(value, self, column_name)
                if resolved is not None:
                    select_items.append(exp.alias_(resolved, column))
                    continue
                placeholder, _ = builder.create_placeholder(value, column_name)
                cast_expr = exp.Cast(this=placeholder, to=column_types[column].copy())
                select_items.append(exp.alias_(cast_expr, column))
            select_expr = exp.Select(expressions=select_items)
            source_expr = (
                select_expr
                if source_expr is None
                else exp.Union(this=source_expr, expression=select_expr, distinct=False)
            )

        query = cast("exp.Expr", source_expr)
        if alias:
            return exp.Subquery(
                this=query,
                alias=exp.TableAlias(
                    this=exp.to_identifier(alias), columns=[exp.to_identifier(column) for column in columns]
                ),
            )
        return exp.paren(query)

    def _create_select_union_source(
        self, data: "list[dict[str, Any]]", columns: "list[str]", is_list: bool, alias: "str | None"
    ) -> "exp.Expr":
        """Create fallback SELECT UNION source for other databases."""
        parameterized_values: list[list[exp.Expr]] = []
        for row in data:
            row_params: list[exp.Expr] = []
            for column in columns:
                value = row.get(column)
                column_name = column if isinstance(column, str) else str(column)
                if "." in column_name:
                    column_name = column_name.split(".")[-1]
                resolved = resolve_value_expression(value, self, column_name)
                if resolved is not None:
                    row_params.append(resolved)
                    continue
                placeholder, _ = cast("QueryBuilder", self).create_placeholder(value, column_name)
                row_params.append(placeholder)
            parameterized_values.append(row_params)

        if is_list:
            union_selects: list[exp.Select] = []
            for row_params in parameterized_values:
                select_expr = exp.Select()
                select_expr.set(
                    "expressions", [exp.alias_(row_params[index], column) for index, column in enumerate(columns)]
                )
                union_selects.append(select_expr)

            source_expr: exp.Expr
            if len(union_selects) == 1:
                source_expr = union_selects[0]
            else:
                union_expr: exp.Expr = union_selects[0]
                for select in union_selects[1:]:
                    union_expr = exp.Union(this=union_expr, expression=select, distinct=False)
                source_expr = union_expr

            return exp.paren(source_expr)

        select_expr = exp.Select()
        select_expr.set(
            "expressions", [exp.alias_(parameterized_values[0][index], column) for index, column in enumerate(columns)]
        )

        return exp.paren(select_expr)


@trait
class MergeOnClauseMixin:
    """Mixin providing ON clause for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...

    def on(self, condition: str | exp.Expr) -> Self:
        current_expr = _ensure_merge_expression(cast("SQLBuilderProtocol", self))
        condition_expr = _parse_merge_condition(cast("SQLBuilderProtocol", self), condition, "ON condition", "ON")

        current_expr.set("on", exp.paren(condition_expr))
        return self


@trait
class MergeMatchedClauseMixin(_MergeAssignmentMixin):
    """Mixin providing WHEN MATCHED THEN ... clauses for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...

    def when_matched_then_update(
        self, set_values: dict[str, Any] | None = None, condition: str | exp.Expr | None = None, **assignments: Any
    ) -> Self:
        current_expr = _ensure_merge_expression(cast("SQLBuilderProtocol", self))
        combined_assignments: dict[str, Any] = {}
        if set_values:
            combined_assignments.update(set_values)
        if assignments:
            combined_assignments.update(assignments)

        if not combined_assignments:
            msg = "No update values provided. Use set_values or keyword arguments."
            raise SQLBuilderError(msg)

        set_expressions = list(starmap(self._process_assignment, combined_assignments.items()))
        update_expression = exp.Update(expressions=set_expressions)

        when_kwargs: dict[str, Any] = {"matched": True, "then": update_expression}
        if condition is not None:
            when_kwargs["condition"] = _parse_merge_condition(
                cast("SQLBuilderProtocol", self), condition, "WHEN clause condition", "WHEN"
            )

        self._append_when(current_expr, exp.When(**when_kwargs))
        return self

    def when_matched_then_delete(self, condition: str | exp.Expr | None = None) -> Self:
        current_expr = _ensure_merge_expression(cast("SQLBuilderProtocol", self))
        when_kwargs: dict[str, Any] = {"matched": True, "then": exp.Var(this="DELETE")}
        if condition is not None:
            when_kwargs["condition"] = _parse_merge_condition(
                cast("SQLBuilderProtocol", self), condition, "WHEN clause condition", "WHEN"
            )

        self._append_when(current_expr, exp.When(**when_kwargs))
        return self


@trait
class MergeNotMatchedClauseMixin(_MergeAssignmentMixin):
    """Mixin providing WHEN NOT MATCHED THEN ... clauses for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...

    def when_not_matched_then_insert(
        self,
        columns: Mapping[str, Any] | Sequence[str] | None = None,
        values: Sequence[Any] | None = None,
        **value_kwargs: Any,
    ) -> Self:
        current_expr = _ensure_merge_expression(cast("SQLBuilderProtocol", self))
        insert_expr = exp.Insert()
        column_names: list[str]
        column_values: list[Any]

        if isinstance(columns, Mapping):
            combined = dict(columns)
            if value_kwargs:
                combined.update(value_kwargs)
            column_names = list(combined.keys())
            column_values = list(combined.values())
        elif value_kwargs:
            column_names = list(value_kwargs.keys())
            column_values = list(value_kwargs.values())
        else:
            if columns is None:
                msg = "Columns must be provided when not using keyword arguments."
                raise SQLBuilderError(msg)
            column_names = [str(column) for column in columns]
            if values is None:
                using_alias: str | None = None
                using_expr = current_expr.args.get("using")
                if using_expr is not None and isinstance(using_expr, (exp.Subquery, exp.Table)):
                    using_alias = using_expr.alias or None
                elif using_expr is not None:
                    try:
                        using_alias = using_expr.alias or None
                    except AttributeError:
                        using_alias = None
                column_values = [
                    exp.column(col, table=using_alias) if using_alias else exp.column(col) for col in column_names
                ]
            else:
                column_values = list(values)
                if len(column_names) != len(column_values):
                    msg = "Number of columns must match number of values for MERGE insert"
                    raise SQLBuilderError(msg)

        insert_columns = [exp.column(name) for name in column_names]

        insert_values: list[exp.Expr] = []
        for column_name, value in zip(column_names, column_values, strict=True):
            resolved = resolve_value_expression(value, self, column_name.split(".")[-1])
            if resolved is not None:
                insert_values.append(resolved)
            else:
                placeholder, _ = cast("QueryBuilder", self).create_placeholder(value, column_name.split(".")[-1])
                insert_values.append(placeholder)

        insert_expr.set("this", exp.Tuple(expressions=insert_columns))
        insert_expr.set("expression", exp.Tuple(expressions=insert_values))
        self._append_when(current_expr, exp.When(matched=False, then=insert_expr))
        return self


@trait
class MergeNotMatchedBySourceClauseMixin(_MergeAssignmentMixin):
    """Mixin providing WHEN NOT MATCHED BY SOURCE THEN ... clauses."""

    __slots__ = ()

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...

    def when_not_matched_by_source_then_update(
        self, set_values: dict[str, Any] | None = None, **assignments: Any
    ) -> Self:
        current_expr = _ensure_merge_expression(cast("SQLBuilderProtocol", self))
        combined_assignments: dict[str, Any] = {}
        if set_values:
            combined_assignments.update(set_values)
        if assignments:
            combined_assignments.update(assignments)

        if not combined_assignments:
            msg = "No update values provided. Use set_values or keyword arguments."
            raise SQLBuilderError(msg)

        set_expressions = list(starmap(self._process_assignment, combined_assignments.items()))
        update_expr = exp.Update(expressions=set_expressions)
        self._append_when(current_expr, exp.When(matched=False, source=True, then=update_expr))
        return self

    def when_not_matched_by_source_then_delete(self) -> Self:
        current_expr = _ensure_merge_expression(cast("SQLBuilderProtocol", self))
        self._append_when(current_expr, exp.When(matched=False, source=True, then=exp.Delete()))
        return self


class Merge(
    QueryBuilder,
    MergeUsingClauseMixin,
    MergeOnClauseMixin,
    MergeMatchedClauseMixin,
    MergeNotMatchedClauseMixin,
    MergeIntoClauseMixin,
    MergeNotMatchedBySourceClauseMixin,
    ExplainMixin,
):
    """Builder for MERGE statements.

    Constructs SQL MERGE statements (also known as UPSERT in some databases)
    with parameter binding and validation.
    """

    __slots__ = ()
    _expression: exp.Expr | None
    _merge_target_quoted: bool

    def __init__(self, target_table: str | None = None, **kwargs: Any) -> None:
        """Initialize MERGE with optional target table.

        Args:
            target_table: Target table name
            **kwargs: Additional QueryBuilder arguments
        """
        if "enable_optimization" not in kwargs:
            kwargs["enable_optimization"] = False
        self._init_query_builder(kwargs)
        self._merge_target_quoted = False
        self._initialize_expression()

        if target_table:
            self.into(target_table)

    def build(self, dialect: "DialectType" = None) -> "Any":
        """Build MERGE statement with dialect validation.

        Args:
            dialect: Optional dialect override for SQL generation.

        Returns:
            Built statement object.
        """
        return super().build(dialect=dialect)

    def _prepare_expression_for_build(self, dialect: "DialectType" = None, *, copy: bool = True) -> exp.Expr:
        """Validate MERGE dialect support and apply dialect-specific expression lowering."""
        self._validate_dialect_support(dialect)
        expression = super()._prepare_expression_for_build(dialect=dialect, copy=copy)
        resolved_dialect = self._build_dialect(dialect)
        if resolved_dialect == "oracle":
            if not copy:
                expression = expression.copy()
            self._move_oracle_when_conditions_on_expr(expression)
        elif resolved_dialect == "teradata":
            if not copy:
                expression = expression.copy()
            self._ensure_teradata_using_alias_on_expr(expression)
        return expression

    def _ensure_teradata_using_alias_on_expr(self, merge_expr: exp.Expr) -> None:
        """Ensure subquery USING sources carry a correlation alias on Teradata."""
        if not isinstance(merge_expr, exp.Merge):
            return
        using_expr = merge_expr.args.get("using")
        if isinstance(using_expr, exp.Paren) and isinstance(using_expr.this, (exp.Select, exp.SetOperation)):
            merge_expr.set("using", exp.Subquery(this=using_expr.this, alias=exp.to_identifier("src")))
        elif isinstance(using_expr, exp.Subquery) and not using_expr.alias:
            using_expr.set("alias", exp.to_identifier("src"))

    def _create_base_expression(self) -> "exp.Merge":
        """Create a base MERGE expression.

        Returns:
            A new sqlglot Merge expression with empty clauses.
        """
        return exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))

    def _validate_dialect_support(self, dialect: "DialectType" = None) -> None:
        """Validate that the target dialect supports MERGE statements."""
        resolved_dialect = self._build_dialect(dialect)
        if resolved_dialect and resolved_dialect in MERGE_UNSUPPORTED_DIALECTS:
            self._raise_dialect_not_supported(resolved_dialect)

    def _raise_dialect_not_supported(self, dialect: str) -> None:
        """Raise error with helpful alternatives for unsupported dialects.

        Args:
            dialect: Name of the unsupported dialect.

        Raises:
            DialectNotSupportedError: Always raised with dialect-specific message.
        """
        alternatives = {
            "mysql": "INSERT ... ON DUPLICATE KEY UPDATE",
            "sqlite": "INSERT ... ON CONFLICT DO UPDATE",
            "duckdb": "INSERT ... ON CONFLICT DO UPDATE",
        }

        alternative = alternatives.get(dialect, "INSERT ... ON CONFLICT or equivalent")
        msg = (
            f"MERGE statements are not supported in {dialect.upper()}. "
            f"Use {alternative} instead. "
            f"See the SQLSpec documentation for examples."
        )
        raise DialectNotSupportedError(msg)

    def _move_oracle_when_conditions(self, dialect_name: str | None) -> None:
        """Normalize WHEN clause conditions for dialect quirks on the builder's expression."""
        if dialect_name != "oracle":
            return
        merge_expr = self.get_expression()
        if merge_expr is not None:
            self._move_oracle_when_conditions_on_expr(merge_expr)

    def _move_oracle_when_conditions_on_expr(self, merge_expr: exp.Expr) -> None:
        """Normalize WHEN clause conditions for Oracle on a target MERGE expression."""
        if not isinstance(merge_expr, exp.Merge):
            return

        whens = merge_expr.args.get("whens")
        if not isinstance(whens, exp.Whens):
            return

        for when_expr in list(whens.expressions):
            condition_expr = when_expr.args.get("condition")
            if condition_expr is None:
                continue
            then_expr = when_expr.args.get("then")
            if isinstance(then_expr, exp.Update):
                then_expr.set("where", exp.Where(this=condition_expr))
                when_expr.set("condition", None)


def _ensure_merge_expression(builder: "SQLBuilderProtocol") -> exp.Merge:
    """Return the builder's MERGE expression, initializing it when absent.

    Raises:
        SQLBuilderError: If the builder state cannot provide a MERGE expression.
    """
    expression = builder.get_expression()
    if expression is None or not isinstance(expression, exp.Merge):
        builder.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
        expression = builder.get_expression()
    if not isinstance(expression, exp.Merge):
        msg = "Merge builder is in an invalid state: expected a MERGE expression."
        raise SQLBuilderError(msg)
    return expression


def _parse_merge_condition(
    builder: "SQLBuilderProtocol", condition: "str | exp.Expr", condition_label: str, clause_label: str
) -> exp.Expr:
    """Parse a MERGE condition into a SQLGlot expression.

    Raises:
        SQLBuilderError: If a condition string cannot be parsed or the condition type is unsupported.
    """
    if isinstance(condition, str):
        parsed_condition: exp.Expr | None = exp.maybe_parse(condition, dialect=builder.dialect)
        if parsed_condition is None:
            msg = f"Could not parse {condition_label}: {condition}"
            raise SQLBuilderError(msg)
        return parsed_condition
    if isinstance(condition, exp.Expr):
        return condition
    msg = f"Unsupported condition type for {clause_label} clause: {type(condition)}"
    raise SQLBuilderError(msg)
