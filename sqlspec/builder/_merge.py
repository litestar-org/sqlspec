"""MERGE statement builder.

Provides a fluent interface for building SQL MERGE queries with
parameter binding and validation.
"""

import contextlib
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from itertools import starmap
from typing import TYPE_CHECKING, Any, cast

import sqlglot as sg
from mypy_extensions import trait
from sqlglot import exp
from sqlglot.errors import ParseError
from typing_extensions import Self

from sqlspec.builder._base import QueryBuilder
from sqlspec.builder._explain import ExplainMixin
from sqlspec.builder._parsing_utils import extract_sql_object_expression
from sqlspec.builder._select import is_explicitly_quoted
from sqlspec.core import SQLResult
from sqlspec.exceptions import DialectNotSupportedError, SQLBuilderError
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_guards import has_expression_and_sql

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = ("Merge",)

MERGE_UNSUPPORTED_DIALECTS = frozenset({"mysql", "sqlite", "duckdb"})


@trait
class _MergeAssignmentMixin:
    """Shared assignment helpers for MERGE clause mixins."""

    __slots__ = ()

    def _is_column_reference(self, value: str) -> bool:
        """Check if value is a SQL expression rather than a literal string.

        Returns True for qualified column references, SQL keywords, functions, and expressions.
        Returns False for plain literal strings that should be parameterized.
        """
        if not isinstance(value, str):
            return False

        builder = cast("QueryBuilder", self)
        with contextlib.suppress(ParseError):
            parsed: exp.Expression | None = exp.maybe_parse(value.strip(), dialect=builder.dialect)
            if parsed is None:
                return False

            if isinstance(parsed, exp.Column):
                return parsed.table is not None and bool(parsed.table)

            return isinstance(
                parsed,
                (
                    exp.Dot,
                    exp.Add,
                    exp.Sub,
                    exp.Mul,
                    exp.Div,
                    exp.Mod,
                    exp.Func,
                    exp.Anonymous,
                    exp.Null,
                    exp.CurrentTimestamp,
                    exp.CurrentDate,
                    exp.CurrentTime,
                    exp.Paren,
                    exp.Case,
                ),
            )
        return False

    def _process_assignment(self, target_column: str, value: Any) -> exp.Expression:
        column_identifier = exp.column(target_column) if isinstance(target_column, str) else target_column

        if has_expression_and_sql(value):
            value_expr = extract_sql_object_expression(value, builder=self)
            return exp.EQ(this=column_identifier, expression=value_expr)
        if isinstance(value, exp.Expression):
            return exp.EQ(this=column_identifier, expression=value)
        if isinstance(value, str) and self._is_column_reference(value):
            builder = cast("QueryBuilder", self)
            parsed_expression: exp.Expression | None = exp.maybe_parse(value, dialect=builder.dialect)
            if parsed_expression is None:
                msg = f"Could not parse assignment expression: {value}"
                raise SQLBuilderError(msg)
            return exp.EQ(this=column_identifier, expression=parsed_expression)

        column_name = target_column if isinstance(target_column, str) else str(target_column)
        column_leaf = column_name.split(".")[-1]
        placeholder, _ = cast("QueryBuilder", self).create_placeholder(value, column_leaf)
        return exp.EQ(this=column_identifier, expression=placeholder)


@trait
class MergeIntoClauseMixin:
    """Mixin providing INTO clause for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def into(self, table: str | exp.Expression, alias: str | None = None) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None

        table_expr: exp.Expression
        if isinstance(table, str):
            table_expr = exp.to_table(table)
            if is_explicitly_quoted(table):
                stripped = table.strip('"`')
                table_expr.set("quoted", True)
                table_expr.set("this", exp.to_identifier(stripped, quoted=True))
                cast("QueryBuilder", self)._merge_target_quoted = True  # pyright: ignore[reportPrivateUsage]
            else:
                cast("QueryBuilder", self)._merge_target_quoted = False  # pyright: ignore[reportPrivateUsage]
            if alias:
                table_expr = exp.alias_(table_expr, alias, table=True)
        else:
            table_expr = table

        current_expr.set("this", table_expr)
        return self


@trait
class MergeUsingClauseMixin(_MergeAssignmentMixin):
    """Mixin providing USING clause for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def _create_dict_source_expression(
        self, source: "dict[str, Any] | list[dict[str, Any]]", alias: "str | None"
    ) -> "exp.Expression":
        """Create USING clause expression from dict or list of dicts.

        Uses JSON-based approach for type-safe bulk operations:
        - PostgreSQL: json_populate_recordset(NULL::table_name, $1::jsonb)
        - Oracle: JSON_TABLE(:payload, '$[*]' COLUMNS (...))
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

        if dialect == "postgres":
            return self._create_postgres_json_source(data, columns, is_list, alias)
        if dialect == "oracle":
            return self._create_oracle_json_source(data, columns, alias)

        return self._create_select_union_source(data, columns, is_list, alias)

    def _create_postgres_json_source(
        self, data: "list[dict[str, Any]]", columns: "list[str]", is_list: bool, alias: "str | None"
    ) -> "exp.Expression":
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

        column_type_spec = ", ".join([f"{col} {self._infer_postgres_type(sample_values.get(col))}" for col in columns])
        column_selects = ", ".join(columns)
        from_sql = (
            f"SELECT {column_selects} FROM jsonb_to_recordset(:{json_param_name}::jsonb) AS "
            f"{recordset_alias}({column_type_spec})"
        )

        parsed = sg.parse_one(from_sql, dialect="postgres")
        return exp.Subquery(
            this=parsed,
            alias=exp.TableAlias(
                this=exp.to_identifier(alias_name), columns=[exp.to_identifier(col) for col in columns]
            ),
        )

    def _create_oracle_json_source(
        self, data: "list[dict[str, Any]]", columns: "list[str]", alias: "str | None"
    ) -> "exp.Expression":
        """Create Oracle JSON_TABLE source (production-proven pattern from oracledb-vertexai-demo)."""
        json_value = to_json(data)
        _, json_param_name = cast("QueryBuilder", self).create_placeholder(json_value, "json_payload")

        sample_values: dict[str, Any] = {}
        for record in data:
            for column, value in record.items():
                if value is not None and column not in sample_values:
                    sample_values[column] = value

        json_columns = [
            f"{column} {self._infer_oracle_type(sample_values.get(column))} PATH '$.{column}'" for column in columns
        ]

        alias_name = alias or "src"
        column_selects = ", ".join(columns)
        columns_clause = ", ".join(json_columns)

        from_sql = f"SELECT {column_selects} FROM JSON_TABLE(:{json_param_name}, '$[*]' COLUMNS ({columns_clause}))"

        parsed = sg.parse_one(from_sql, dialect="oracle")
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
        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, int):
            return "INTEGER"
        if isinstance(value, float):
            return "DOUBLE PRECISION"
        if isinstance(value, Decimal):
            return "NUMERIC"
        if isinstance(value, (dict, list)):
            return "JSONB"
        if isinstance(value, datetime):
            return "TIMESTAMP"
        return "TEXT"

    def _infer_oracle_type(self, value: "Any") -> str:
        """Infer Oracle column type for JSON_TABLE projection."""
        varchar2_max = 4000

        if isinstance(value, bool):
            return "NUMBER(1)"
        if isinstance(value, (int, float, Decimal)):
            return "NUMBER"
        if isinstance(value, (dict, list)):
            return "JSON"
        if isinstance(value, datetime):
            return "TIMESTAMP"
        if value is not None and len(str(value)) > varchar2_max:
            return "CLOB"
        return f"VARCHAR2({varchar2_max})"

    def _create_select_union_source(
        self, data: "list[dict[str, Any]]", columns: "list[str]", is_list: bool, alias: "str | None"
    ) -> "exp.Expression":
        """Create fallback SELECT UNION source for other databases."""
        parameterized_values: list[list[exp.Expression]] = []
        for row in data:
            row_params: list[exp.Expression] = []
            for column in columns:
                value = row.get(column)
                column_name = column if isinstance(column, str) else str(column)
                if "." in column_name:
                    column_name = column_name.split(".")[-1]
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

            source_expr: exp.Expression
            if len(union_selects) == 1:
                source_expr = union_selects[0]
            else:
                union_expr: exp.Expression = union_selects[0]
                for select in union_selects[1:]:
                    union_expr = exp.Union(this=union_expr, expression=select, distinct=False)
                source_expr = union_expr

            return exp.paren(source_expr)

        select_expr = exp.Select()
        select_expr.set(
            "expressions", [exp.alias_(parameterized_values[0][index], column) for index, column in enumerate(columns)]
        )

        return exp.paren(select_expr)

    def using(self, source: str | exp.Expression | Any, alias: str | None = None) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
        source_expr: exp.Expression
        if isinstance(source, str):
            source_expr = exp.to_table(source, alias=alias)
        elif isinstance(source, (dict, list)):
            paren_expr = self._create_dict_source_expression(source, alias)
            if alias and isinstance(paren_expr, exp.Paren):
                source_expr = exp.Subquery(this=paren_expr.this, alias=exp.to_identifier(alias))
            else:
                source_expr = paren_expr
        elif isinstance(source, QueryBuilder):
            builder = cast("QueryBuilder", self)
            for param_name, param_value in source.parameters.items():
                builder.add_parameter(param_value, name=param_name)
            subquery_expression_source = source.get_expression()
            if not isinstance(subquery_expression_source, exp.Expression):
                subquery_expression_source = exp.select()

            if alias:
                source_expr = exp.Subquery(this=subquery_expression_source, alias=exp.to_identifier(alias))
            else:
                source_expr = exp.paren(subquery_expression_source)
        elif isinstance(source, exp.Expression):
            # Handle different expression types for MERGE USING
            if isinstance(source, exp.Select):
                # Wrap SELECT in Subquery if alias provided
                source_expr = exp.Subquery(this=source, alias=exp.to_identifier(alias)) if alias else exp.paren(source)
            elif isinstance(source, exp.Paren) and alias:
                # Convert Paren to Subquery with alias
                inner = source.this
                source_expr = exp.Subquery(this=inner, alias=exp.to_identifier(alias))
            elif isinstance(source, exp.Subquery) and alias:
                # Update existing Subquery's alias
                source.set("alias", exp.to_identifier(alias))
                source_expr = source
            else:
                # Table name or other expression - use standard aliasing
                source_expr = exp.alias_(source, alias) if alias else source
        else:
            msg = f"Unsupported source type for USING clause: {type(source)}"
            raise SQLBuilderError(msg)

        current_expr.set("using", source_expr)
        return self


@trait
class MergeOnClauseMixin:
    """Mixin providing ON clause for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def on(self, condition: str | exp.Expression) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
        if isinstance(condition, str):
            builder = cast("QueryBuilder", self)
            parsed_condition: exp.Expression | None = exp.maybe_parse(condition, dialect=builder.dialect)
            if parsed_condition is None:
                msg = f"Could not parse ON condition: {condition}"
                raise SQLBuilderError(msg)
            condition_expr = parsed_condition
        elif isinstance(condition, exp.Expression):
            condition_expr = condition
        else:
            msg = f"Unsupported condition type for ON clause: {type(condition)}"
            raise SQLBuilderError(msg)

        current_expr.set("on", exp.paren(condition_expr))
        return self


@trait
class MergeMatchedClauseMixin(_MergeAssignmentMixin):
    """Mixin providing WHEN MATCHED THEN ... clauses for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def when_matched_then_update(
        self,
        set_values: dict[str, Any] | None = None,
        condition: str | exp.Expression | None = None,
        **assignments: Any,
    ) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
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
            if isinstance(condition, str):
                builder = cast("QueryBuilder", self)
                parsed_condition: exp.Expression | None = exp.maybe_parse(condition, dialect=builder.dialect)
                if parsed_condition is None:
                    msg = f"Could not parse WHEN clause condition: {condition}"
                    raise SQLBuilderError(msg)
                condition_expr = parsed_condition
            elif isinstance(condition, exp.Expression):
                condition_expr = condition
            else:
                msg = f"Unsupported condition type for WHEN clause: {type(condition)}"
                raise SQLBuilderError(msg)

            builder = cast("QueryBuilder", self)
            dialect_name = builder.dialect_name
            if dialect_name == "oracle":
                update_expression.set("where", condition_expr)
            else:
                when_kwargs["condition"] = condition_expr

        whens = current_expr.args.get("whens")
        if not isinstance(whens, exp.Whens):
            whens = exp.Whens(expressions=[])
            current_expr.set("whens", whens)
        whens.append("expressions", exp.When(**when_kwargs))
        return self

    def when_matched_then_delete(self, condition: str | exp.Expression | None = None) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
        when_kwargs: dict[str, Any] = {"matched": True, "then": exp.Var(this="DELETE")}
        if condition is not None:
            if isinstance(condition, str):
                builder = cast("QueryBuilder", self)
                parsed_condition: exp.Expression | None = exp.maybe_parse(condition, dialect=builder.dialect)
                if parsed_condition is None:
                    msg = f"Could not parse WHEN clause condition: {condition}"
                    raise SQLBuilderError(msg)
                when_kwargs["condition"] = parsed_condition
            elif isinstance(condition, exp.Expression):
                when_kwargs["condition"] = condition
            else:
                msg = f"Unsupported condition type for WHEN clause: {type(condition)}"
                raise SQLBuilderError(msg)

        whens = current_expr.args.get("whens")
        if not isinstance(whens, exp.Whens):
            whens = exp.Whens(expressions=[])
            current_expr.set("whens", whens)
        whens.append("expressions", exp.When(**when_kwargs))
        return self


@trait
class MergeNotMatchedClauseMixin(_MergeAssignmentMixin):
    """Mixin providing WHEN NOT MATCHED THEN ... clauses for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def when_not_matched_then_insert(
        self,
        columns: Mapping[str, Any] | Sequence[str] | None = None,
        values: Sequence[Any] | None = None,
        **value_kwargs: Any,
    ) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
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
                using_alias = None
                using_expr = current_expr.args.get("using")
                if using_expr is not None and isinstance(using_expr, (exp.Subquery, exp.Table)):
                    using_alias = using_expr.alias
                elif using_expr is not None:
                    try:
                        using_alias = using_expr.alias
                    except AttributeError:
                        using_alias = None
                column_values = [f"{using_alias}.{col}" for col in column_names] if using_alias else column_names
            else:
                column_values = list(values)
                if len(column_names) != len(column_values):
                    msg = "Number of columns must match number of values for MERGE insert"
                    raise SQLBuilderError(msg)

        insert_columns = [exp.column(name) for name in column_names]

        insert_values: list[exp.Expression] = []
        for column_name, value in zip(column_names, column_values, strict=True):
            if has_expression_and_sql(value):
                insert_values.append(extract_sql_object_expression(value, builder=self))
            elif isinstance(value, exp.Expression):
                insert_values.append(value)
            elif isinstance(value, str):
                if self._is_column_reference(value):
                    builder = cast("QueryBuilder", self)
                    parsed_value: exp.Expression | None = exp.maybe_parse(value, dialect=builder.dialect)
                    if parsed_value is None:
                        msg = f"Could not parse column reference: {value}"
                        raise SQLBuilderError(msg)
                    insert_values.append(parsed_value)
                else:
                    placeholder, _ = cast("QueryBuilder", self).create_placeholder(value, column_name.split(".")[-1])
                    insert_values.append(placeholder)
            else:
                placeholder, _ = cast("QueryBuilder", self).create_placeholder(value, column_name.split(".")[-1])
                insert_values.append(placeholder)

        insert_expr.set("this", exp.Tuple(expressions=insert_columns))
        insert_expr.set("expression", exp.Tuple(expressions=insert_values))
        whens = current_expr.args.get("whens")
        if not isinstance(whens, exp.Whens):
            whens = exp.Whens(expressions=[])
            current_expr.set("whens", whens)
        whens.append("expressions", exp.When(matched=False, then=insert_expr))
        return self


@trait
class MergeNotMatchedBySourceClauseMixin(_MergeAssignmentMixin):
    """Mixin providing WHEN NOT MATCHED BY SOURCE THEN ... clauses."""

    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def when_not_matched_by_source_then_update(
        self, set_values: dict[str, Any] | None = None, **assignments: Any
    ) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
        combined_assignments: dict[str, Any] = {}
        if set_values:
            combined_assignments.update(set_values)
        if assignments:
            combined_assignments.update(assignments)

        if not combined_assignments:
            msg = "No update values provided. Use set_values or keyword arguments."
            raise SQLBuilderError(msg)

        set_expressions: list[exp.Expression] = []
        for column_name, value in combined_assignments.items():
            column_identifier = exp.column(column_name)
            if has_expression_and_sql(value):
                value_expr = extract_sql_object_expression(value, builder=self)
            elif isinstance(value, exp.Expression):
                value_expr = value
            elif isinstance(value, str) and self._is_column_reference(value):
                builder = cast("QueryBuilder", self)
                parsed_value: exp.Expression | None = exp.maybe_parse(value, dialect=builder.dialect)
                if parsed_value is None:
                    msg = f"Could not parse assignment expression: {value}"
                    raise SQLBuilderError(msg)
                value_expr = parsed_value
            else:
                placeholder, _ = cast("QueryBuilder", self).create_placeholder(value, column_name)
                value_expr = placeholder
            set_expressions.append(exp.EQ(this=column_identifier, expression=value_expr))

        update_expr = exp.Update(expressions=set_expressions)
        whens = current_expr.args.get("whens")
        if not isinstance(whens, exp.Whens):
            whens = exp.Whens(expressions=[])
            current_expr.set("whens", whens)
        whens.append("expressions", exp.When(matched=False, source=True, then=update_expr))
        return self

    def when_not_matched_by_source_then_delete(self) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
        whens = current_expr.args.get("whens")
        if not isinstance(whens, exp.Whens):
            whens = exp.Whens(expressions=[])
            current_expr.set("whens", whens)
        whens.append("expressions", exp.When(matched=False, source=True, then=exp.Delete()))
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
    _expression: exp.Expression | None
    _merge_target_quoted: bool
    _lock_targets_quoted: bool

    def __init__(self, target_table: str | None = None, **kwargs: Any) -> None:
        """Initialize MERGE with optional target table.

        Args:
            target_table: Target table name
            **kwargs: Additional QueryBuilder arguments
        """
        if "enable_optimization" not in kwargs:
            kwargs["enable_optimization"] = False
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
        self._merge_target_quoted = False
        self._lock_targets_quoted = False
        self._initialize_expression()

        if target_table:
            self.into(target_table)

    @property
    def _expected_result_type(self) -> "type[SQLResult]":
        """Return the expected result type for this builder.

        Returns:
            The SQLResult type for MERGE statements.
        """
        return SQLResult

    def _create_base_expression(self) -> "exp.Merge":
        """Create a base MERGE expression.

        Returns:
            A new sqlglot Merge expression with empty clauses.
        """
        return exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))

    def _validate_dialect_support(self) -> None:
        """Validate that the current dialect supports MERGE statements.

        Raises:
            DialectNotSupportedError: If the dialect does not support MERGE.
        """
        dialect = self.dialect_name
        if dialect and dialect in MERGE_UNSUPPORTED_DIALECTS:
            self._raise_dialect_not_supported(dialect)

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

    def build(self, dialect: "DialectType" = None) -> "Any":
        """Build MERGE statement with dialect validation.

        Args:
            dialect: Optional dialect override for SQL generation.

        Returns:
            Built statement object.
        """
        self._validate_dialect_support()
        target_dialect = dialect or self.dialect
        if isinstance(target_dialect, str):
            dialect_name = target_dialect
        elif isinstance(target_dialect, type):
            dialect_name = target_dialect.__name__
        else:
            dialect_name = None
        if dialect_name:
            dialect_name = dialect_name.lower()
        self._normalize_merge_conditions_for_dialect(dialect_name)
        return super().build(dialect=dialect)

    def _normalize_merge_conditions_for_dialect(self, dialect_name: str | None) -> None:
        """Normalize WHEN clause conditions for dialect quirks (e.g., Oracle).

        Oracle requires conditional logic on UPDATE/DELETE branches to live in the
        clause-specific WHERE, not on the WHEN predicate. Move the condition down
        when present so generated SQL matches Oracle syntax.
        """
        if dialect_name != "oracle":
            return

        merge_expr = self.get_expression()
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
                then_expr.set("where", condition_expr)
                when_expr.set("condition", None)
