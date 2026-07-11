"""SQL factory for creating SQL builders and column expressions.

Provides statement builders (select, insert, update, etc.) and column expressions.
"""

import hashlib
import logging
from typing import TYPE_CHECKING, Any, TypeAlias, TypeVar, Union, cast, final

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import ParseError as SQLGlotParseError

from sqlspec.builder._base import QueryBuilder
from sqlspec.builder._column import Column
from sqlspec.builder._ddl import (
    AlterTable,
    CommentOn,
    CreateIndex,
    CreateMaterializedView,
    CreateSchema,
    CreateTable,
    CreateTableAsSelect,
    CreateView,
    DropIndex,
    DropMaterializedView,
    DropSchema,
    DropTable,
    DropView,
    RenameTable,
    Truncate,
)
from sqlspec.builder._delete import Delete
from sqlspec.builder._explain import Explain
from sqlspec.builder._expression_wrappers import (
    AggregateExpression,
    ConversionExpression,
    FunctionExpression,
    MathExpression,
    StringExpression,
)
from sqlspec.builder._insert import Insert
from sqlspec.builder._join import JoinBuilder, create_join_builder
from sqlspec.builder._merge import Merge
from sqlspec.builder._parsing_utils import (
    _coerce_column,
    _normalize_order_by,
    _normalize_partition_by,
    _resolve_dialect,
    extract_expression,
    to_expression,
)
from sqlspec.builder._select import Case, Select, SubqueryBuilder, WindowFunctionBuilder
from sqlspec.builder._update import Update
from sqlspec.core import SQL
from sqlspec.core.explain import ExplainFormat, ExplainOptions
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlspec.builder._expression_wrappers import ExpressionWrapper
    from sqlspec.protocols import SQLBuilderProtocol


__all__ = (
    "AlterTable",
    "Case",
    "Column",
    "CommentOn",
    "CreateIndex",
    "CreateMaterializedView",
    "CreateSchema",
    "CreateTable",
    "CreateTableAsSelect",
    "CreateView",
    "Delete",
    "DropIndex",
    "DropMaterializedView",
    "DropSchema",
    "DropTable",
    "DropView",
    "Explain",
    "Insert",
    "Merge",
    "RenameTable",
    "SQLFactory",
    "Select",
    "Truncate",
    "Update",
    "WindowFunctionBuilder",
    "build_copy_from_statement",
    "build_copy_statement",
    "build_copy_to_statement",
    "sql",
)

logger = get_logger("sqlspec.builder.factory")

BuilderT = TypeVar("BuilderT", bound=QueryBuilder)
ColumnLike: TypeAlias = Union[str, exp.Expr, "ExpressionWrapper", "Case", "Column"]

MIN_SQL_LIKE_STRING_LENGTH = 6
MIN_DECODE_ARGS = 2
SQL_STARTERS = {
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "WITH",
    "CALL",
    "DECLARE",
    "BEGIN",
    "END",
    "CREATE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "RENAME",
    "GRANT",
    "REVOKE",
    "SET",
    "SHOW",
    "USE",
    "EXPLAIN",
    "OPTIMIZE",
    "VACUUM",
    "COPY",
}


def _fingerprint_sql(sql: str) -> str:
    digest = hashlib.sha256(sql.encode("utf-8", errors="replace")).hexdigest()
    return digest[:12]


def _normalize_copy_dialect(dialect: DialectType | None) -> str:
    if dialect is None:
        return "postgres"
    if isinstance(dialect, str):
        return dialect
    return str(dialect)


def _to_copy_schema(table: str, columns: "Sequence[str] | None") -> exp.Expr:
    base = exp.table_(table)
    if not columns:
        return base
    column_nodes = [exp.column(column_name) for column_name in columns]
    return exp.Schema(this=base, expressions=column_nodes)


def _build_copy_expression(
    *, direction: str, table: str, location: str, columns: "Sequence[str] | None", options: "Mapping[str, Any] | None"
) -> exp.Copy:
    copy_args: dict[str, Any] = {"this": _to_copy_schema(table, columns), "files": [exp.Literal.string(location)]}

    if direction == "from":
        copy_args["kind"] = True
    elif direction == "to":
        copy_args["kind"] = False

    if options:
        params: list[exp.CopyParameter] = []
        for key, value in options.items():
            identifier = exp.Var(this=str(key).upper())
            value_expression: exp.Expr
            if isinstance(value, bool):
                value_expression = exp.Boolean(this=value)
            elif value is None:
                value_expression = exp.null()
            elif isinstance(value, (int, float)):
                value_expression = exp.Literal.number(value)
            elif isinstance(value, (list, tuple)):
                elements = [exp.Literal.string(str(item)) for item in value]
                value_expression = exp.Array(expressions=elements)
            else:
                value_expression = exp.Literal.string(str(value))
            params.append(exp.CopyParameter(this=identifier, expression=value_expression))
        copy_args["params"] = params

    return exp.Copy(**copy_args)


def build_copy_statement(
    *,
    direction: str,
    table: str,
    location: str,
    columns: "Sequence[str] | None" = None,
    options: "Mapping[str, Any] | None" = None,
    dialect: DialectType | None = None,
) -> SQL:
    expression = _build_copy_expression(
        direction=direction, table=table, location=location, columns=columns, options=options
    )
    rendered = expression.sql(dialect=_normalize_copy_dialect(dialect))
    return SQL(rendered)


def build_copy_from_statement(
    table: str,
    source: str,
    *,
    columns: "Sequence[str] | None" = None,
    options: "Mapping[str, Any] | None" = None,
    dialect: DialectType | None = None,
) -> SQL:
    return build_copy_statement(
        direction="from", table=table, location=source, columns=columns, options=options, dialect=dialect
    )


def build_copy_to_statement(
    table: str,
    target: str,
    *,
    columns: "Sequence[str] | None" = None,
    options: "Mapping[str, Any] | None" = None,
    dialect: DialectType | None = None,
) -> SQL:
    return build_copy_statement(
        direction="to", table=table, location=target, columns=columns, options=options, dialect=dialect
    )


@final
class SQLFactory:
    """Factory for creating SQL builders and column expressions."""

    __slots__ = ("dialect",)

    @staticmethod
    def _detect_type_from_expression(parsed_expr: exp.Expr) -> str:
        if parsed_expr.key:
            return parsed_expr.key.upper()
        command_type = type(parsed_expr).__name__.upper()
        if command_type == "COMMAND" and parsed_expr.this:
            return str(parsed_expr.this).upper()
        return command_type

    @staticmethod
    def _parse_sql_expression(sql: str, dialect: DialectType | None) -> "exp.Expr | None":
        try:
            return sqlglot.parse_one(sql, read=dialect)
        except SQLGlotParseError:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Failed to parse SQL for type detection",
                    extra={"sql_length": len(sql), "sql_hash": _fingerprint_sql(sql)},
                )
        except (ValueError, TypeError, AttributeError):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Unexpected error during SQL type detection",
                    exc_info=True,
                    extra={"sql_length": len(sql), "sql_hash": _fingerprint_sql(sql)},
                )
        return None

    def __init__(self, dialect: DialectType = None) -> None:
        """Initialize the SQL factory.

        Args:
            dialect: Default SQL dialect to use for all builders.
        """
        self.dialect = dialect

    def __call__(self, statement: str, dialect: DialectType = None) -> "Any":
        """Create a SelectBuilder from a SQL string, or SQL object for DML with RETURNING.

        Args:
            statement: The SQL statement string.
            dialect: Optional SQL dialect.

        Returns:
            SelectBuilder instance for SELECT/WITH statements,
            SQL object for DML statements with RETURNING clause.

        Raises:
            SQLBuilderError: If the SQL is not a SELECT/CTE/DML+RETURNING statement.
        """

        try:
            parsed_expr = sqlglot.parse_one(statement, read=_resolve_dialect(dialect, self.dialect))
        except Exception as e:
            msg = f"Failed to parse SQL: {e}"
            raise SQLBuilderError(msg) from e
        actual_type = type(parsed_expr).__name__.upper()
        expr_type_map = {
            "SELECT": "SELECT",
            "INSERT": "INSERT",
            "UPDATE": "UPDATE",
            "DELETE": "DELETE",
            "MERGE": "MERGE",
            "WITH": "WITH",
        }
        actual_type_str = expr_type_map.get(actual_type, actual_type)
        if actual_type_str == "SELECT" or (
            actual_type_str == "WITH" and parsed_expr.this and isinstance(parsed_expr.this, exp.Select)
        ):
            builder = Select(dialect=_resolve_dialect(dialect, self.dialect))
            builder.set_expression(parsed_expr)
            return builder

        if actual_type_str in {"INSERT", "UPDATE", "DELETE"} and parsed_expr.args.get("returning") is not None:
            return SQL(parsed_expr)

        msg = (
            f"sql(...) only supports SELECT statements or DML statements with RETURNING clause. "
            f"Detected type: {actual_type_str}. "
            f"Use sql.{actual_type_str.lower()}() instead."
        )
        raise SQLBuilderError(msg)

    def select(
        self, *columns_or_sql: Union[str, exp.Expr, Column, "SQL", "Case"], dialect: DialectType = None
    ) -> "Select":
        builder_dialect = _resolve_dialect(dialect, self.dialect)
        if len(columns_or_sql) == 1 and isinstance(columns_or_sql[0], str):
            sql_candidate = columns_or_sql[0].strip()
            if self._looks_like_sql(sql_candidate):
                parsed_expr = self._parse_sql_expression(sql_candidate, builder_dialect)
                detected = "COMMAND" if parsed_expr is None else self._detect_type_from_expression(parsed_expr)
                if detected not in {"SELECT", "WITH"}:
                    msg = (
                        f"sql.select() expects a SELECT or WITH statement, got {detected}. "
                        f"Use sql.{detected.lower()}() if a dedicated builder exists, or ensure the SQL is SELECT/WITH."
                    )
                    raise SQLBuilderError(msg)
                select_builder = Select(dialect=builder_dialect)
                return self._populate_select_from_sql(select_builder, sql_candidate, parsed_expr)
        select_builder = Select(dialect=builder_dialect)
        if columns_or_sql:
            select_builder.select(*columns_or_sql)
        return select_builder

    def insert(self, table_or_sql: str | None = None, dialect: DialectType = None) -> "Insert":
        builder_dialect = _resolve_dialect(dialect, self.dialect)
        builder = Insert(dialect=builder_dialect)
        if table_or_sql:
            if self._looks_like_sql(table_or_sql):
                parsed_expr = self._parse_sql_expression(table_or_sql, builder_dialect)
                detected = "COMMAND" if parsed_expr is None else self._detect_type_from_expression(parsed_expr)
                if detected not in {"INSERT", "SELECT"}:
                    msg = (
                        f"sql.insert() expects INSERT or SELECT (for insert-from-select), got {detected}. "
                        f"Use sql.{detected.lower()}() if a dedicated builder exists, "
                        f"or ensure the SQL is INSERT/SELECT."
                    )
                    raise SQLBuilderError(msg)
                return self._populate_insert_from_sql(builder, table_or_sql, parsed_expr)
            return builder.into(table_or_sql)
        return builder

    def update(self, table_or_sql: str | None = None, dialect: DialectType = None) -> "Update":
        builder_dialect = _resolve_dialect(dialect, self.dialect)
        builder = Update(dialect=builder_dialect)
        if table_or_sql:
            if self._looks_like_sql(table_or_sql):
                parsed_expr = self._parse_sql_expression(table_or_sql, builder_dialect)
                detected = "COMMAND" if parsed_expr is None else self._detect_type_from_expression(parsed_expr)
                if detected != "UPDATE":
                    msg = (
                        f"sql.update() expects UPDATE statement, got {detected}. "
                        f"Use sql.{detected.lower()}() if a dedicated builder exists."
                    )
                    raise SQLBuilderError(msg)
                return self._populate_update_from_sql(builder, table_or_sql, parsed_expr)
            return builder.table(table_or_sql)
        return builder

    def delete(self, table_or_sql: str | None = None, dialect: DialectType = None) -> "Delete":
        builder_dialect = _resolve_dialect(dialect, self.dialect)
        if table_or_sql and self._looks_like_sql(table_or_sql):
            builder = Delete(dialect=builder_dialect)
            parsed_expr = self._parse_sql_expression(table_or_sql, builder_dialect)
            detected = "COMMAND" if parsed_expr is None else self._detect_type_from_expression(parsed_expr)
            if detected != "DELETE":
                msg = (
                    f"sql.delete() expects DELETE statement, got {detected}. "
                    f"Use sql.{detected.lower()}() if a dedicated builder exists."
                )
                raise SQLBuilderError(msg)
            return self._populate_delete_from_sql(builder, table_or_sql, parsed_expr)

        return Delete(table_or_sql, dialect=builder_dialect) if table_or_sql else Delete(dialect=builder_dialect)

    def merge(self, table_or_sql: str | None = None, dialect: DialectType = None) -> "Merge":
        builder_dialect = _resolve_dialect(dialect, self.dialect)
        if table_or_sql and self._looks_like_sql(table_or_sql):
            builder = Merge(dialect=builder_dialect)
            parsed_expr = self._parse_sql_expression(table_or_sql, builder_dialect)
            detected = "COMMAND" if parsed_expr is None else self._detect_type_from_expression(parsed_expr)
            if detected != "MERGE":
                msg = (
                    f"sql.merge() expects MERGE statement, got {detected}. "
                    f"Use sql.{detected.lower()}() if a dedicated builder exists."
                )
                raise SQLBuilderError(msg)
            return self._populate_merge_from_sql(builder, table_or_sql, parsed_expr)

        return Merge(table_or_sql, dialect=builder_dialect) if table_or_sql else Merge(dialect=builder_dialect)

    def explain(
        self,
        statement: "str | exp.Expr | SQL | SQLBuilderProtocol",
        *,
        analyze: bool = False,
        verbose: bool = False,
        format: "ExplainFormat | str | None" = None,
        dialect: DialectType = None,
    ) -> "Explain":
        """Create an EXPLAIN builder for a SQL statement.

        Wraps any SQL statement in an EXPLAIN clause with dialect-aware
        syntax generation.

        Args:
            statement: SQL statement to explain (string, expression, SQL object, or builder)
            analyze: Execute the statement and show actual runtime statistics
            verbose: Show additional information
            format: Output format (TEXT, JSON, XML, YAML, TREE, TRADITIONAL)
            dialect: Optional SQL dialect override

        Returns:
            Explain builder for further configuration
        """
        builder_dialect = _resolve_dialect(dialect, self.dialect)

        fmt = None
        if format is not None:
            fmt = ExplainFormat(format.lower()) if isinstance(format, str) else format

        options = ExplainOptions(analyze=analyze, verbose=verbose, format=fmt)

        return Explain(statement, dialect=builder_dialect, options=options)

    @property
    def merge_(self) -> "Merge":
        """Create a new MERGE builder (property shorthand).

        Property that returns a new Merge builder instance using the factory's
        default dialect. Cleaner syntax alternative to merge() method.

        Returns:
            New Merge builder instance
        """
        return Merge(dialect=self.dialect)

    def upsert(self, table: str, dialect: DialectType = None) -> "Merge | Insert":
        """Create an upsert builder (MERGE or INSERT ON CONFLICT).

        Automatically selects the appropriate builder based on database dialect:
            - PostgreSQL 15+, Oracle, BigQuery: Returns MERGE builder
            - SQLite, DuckDB, MySQL: Returns INSERT builder with ON CONFLICT support

        Args:
            table: Target table name
            dialect: Optional SQL dialect (uses factory default if not provided)

        Returns:
            MERGE builder for supported databases, INSERT builder otherwise
        """
        builder_dialect = _resolve_dialect(dialect, self.dialect)
        dialect_str = str(builder_dialect).lower() if builder_dialect else None

        merge_supported = {"postgres", "postgresql", "oracle", "bigquery"}

        if dialect_str in merge_supported:
            return self.merge(table, dialect=builder_dialect)

        return self.insert(table, dialect=builder_dialect)

    def create_table(self, table_name: str, dialect: DialectType = None) -> "CreateTable":
        """Create a CREATE TABLE builder.

        Args:
            table_name: Name of the table to create
            dialect: Optional SQL dialect

        Returns:
            CreateTable builder instance
        """
        return CreateTable(table_name, dialect=_resolve_dialect(dialect, self.dialect))

    def create_table_as_select(self, dialect: DialectType = None) -> "CreateTableAsSelect":
        """Create a CREATE TABLE AS SELECT builder.

        Args:
            dialect: Optional SQL dialect

        Returns:
            CreateTableAsSelect builder instance
        """
        return CreateTableAsSelect(dialect=_resolve_dialect(dialect, self.dialect))

    def create_view(self, view_name: str, dialect: DialectType = None) -> "CreateView":
        """Create a CREATE VIEW builder.

        Args:
            view_name: Name of the view to create
            dialect: Optional SQL dialect

        Returns:
            CreateView builder instance
        """
        return CreateView(view_name, dialect=_resolve_dialect(dialect, self.dialect))

    def create_materialized_view(self, view_name: str, dialect: DialectType = None) -> "CreateMaterializedView":
        """Create a CREATE MATERIALIZED VIEW builder.

        Args:
            view_name: Name of the materialized view to create
            dialect: Optional SQL dialect

        Returns:
            CreateMaterializedView builder instance
        """
        return CreateMaterializedView(view_name, dialect=_resolve_dialect(dialect, self.dialect))

    def create_index(self, index_name: str, dialect: DialectType = None) -> "CreateIndex":
        """Create a CREATE INDEX builder.

        Args:
            index_name: Name of the index to create
            dialect: Optional SQL dialect

        Returns:
            CreateIndex builder instance
        """
        return CreateIndex(index_name, dialect=_resolve_dialect(dialect, self.dialect))

    def create_schema(self, schema_name: str, dialect: DialectType = None) -> "CreateSchema":
        """Create a CREATE SCHEMA builder.

        Args:
            schema_name: Name of the schema to create
            dialect: Optional SQL dialect

        Returns:
            CreateSchema builder instance
        """
        return CreateSchema(schema_name, dialect=_resolve_dialect(dialect, self.dialect))

    def drop_table(self, table_name: str, dialect: DialectType = None) -> "DropTable":
        """Create a DROP TABLE builder.

        Args:
            table_name: Name of the table to drop
            dialect: Optional SQL dialect

        Returns:
            DropTable builder instance
        """
        return DropTable(table_name, dialect=_resolve_dialect(dialect, self.dialect))

    def drop_view(self, view_name: str, dialect: DialectType = None) -> "DropView":
        """Create a DROP VIEW builder.

        Args:
            view_name: Name of the view to drop
            dialect: Optional SQL dialect

        Returns:
            DropView builder instance
        """
        return DropView(view_name, dialect=_resolve_dialect(dialect, self.dialect))

    def drop_materialized_view(self, view_name: str, dialect: DialectType = None) -> "DropMaterializedView":
        """Create a DROP MATERIALIZED VIEW builder.

        Args:
            view_name: Name of the materialized view to drop
            dialect: Optional SQL dialect

        Returns:
            DropMaterializedView builder instance
        """
        return DropMaterializedView(view_name, dialect=_resolve_dialect(dialect, self.dialect))

    def drop_index(self, index_name: str, dialect: DialectType = None) -> "DropIndex":
        """Create a DROP INDEX builder.

        Args:
            index_name: Name of the index to drop
            dialect: Optional SQL dialect

        Returns:
            DropIndex builder instance
        """
        return DropIndex(index_name, dialect=_resolve_dialect(dialect, self.dialect))

    def drop_schema(self, schema_name: str, dialect: DialectType = None) -> "DropSchema":
        """Create a DROP SCHEMA builder.

        Args:
            schema_name: Name of the schema to drop
            dialect: Optional SQL dialect

        Returns:
            DropSchema builder instance
        """
        return DropSchema(schema_name, dialect=_resolve_dialect(dialect, self.dialect))

    def alter_table(self, table_name: str, dialect: DialectType = None) -> "AlterTable":
        """Create an ALTER TABLE builder.

        Args:
            table_name: Name of the table to alter
            dialect: Optional SQL dialect

        Returns:
            AlterTable builder instance
        """
        return AlterTable(table_name, dialect=_resolve_dialect(dialect, self.dialect))

    def rename_table(self, old_name: str, dialect: DialectType = None) -> "RenameTable":
        """Create a RENAME TABLE builder.

        Args:
            old_name: Current name of the table
            dialect: Optional SQL dialect

        Returns:
            RenameTable builder instance
        """
        return RenameTable(old_name, dialect=_resolve_dialect(dialect, self.dialect))

    def comment_on(self, dialect: DialectType = None) -> "CommentOn":
        """Create a COMMENT ON builder.

        Args:
            dialect: Optional SQL dialect

        Returns:
            CommentOn builder instance
        """
        return CommentOn(dialect=_resolve_dialect(dialect, self.dialect))

    def copy_from(
        self,
        table: str,
        source: str,
        *,
        columns: "Sequence[str] | None" = None,
        options: "Mapping[str, Any] | None" = None,
        dialect: DialectType | None = None,
    ) -> SQL:
        """Build a COPY ... FROM statement."""

        effective_dialect = _resolve_dialect(dialect, self.dialect)
        return build_copy_from_statement(table, source, columns=columns, options=options, dialect=effective_dialect)

    def copy_to(
        self,
        table: str,
        target: str,
        *,
        columns: "Sequence[str] | None" = None,
        options: "Mapping[str, Any] | None" = None,
        dialect: DialectType | None = None,
    ) -> SQL:
        """Build a COPY ... TO statement."""

        effective_dialect = _resolve_dialect(dialect, self.dialect)
        return build_copy_to_statement(table, target, columns=columns, options=options, dialect=effective_dialect)

    def copy(
        self,
        table: str,
        *,
        source: str | None = None,
        target: str | None = None,
        columns: "Sequence[str] | None" = None,
        options: "Mapping[str, Any] | None" = None,
        dialect: DialectType | None = None,
    ) -> SQL:
        """Build a COPY statement, inferring direction from provided arguments."""

        if (source is None and target is None) or (source is not None and target is not None):
            msg = "Provide either 'source' or 'target' (but not both) to sql.copy()."
            raise SQLBuilderError(msg)

        if source is not None:
            return self.copy_from(table, source, columns=columns, options=options, dialect=dialect)

        target_value = cast("str", target)
        return self.copy_to(table, target_value, columns=columns, options=options, dialect=dialect)

    @staticmethod
    def _looks_like_sql(candidate: str, expected_type: str | None = None) -> bool:
        """Determine if a string looks like SQL.

        Args:
            candidate: String to check
            expected_type: Expected SQL statement type (SELECT, INSERT, etc.)

        Returns:
            True if the string appears to be SQL
        """
        if not candidate or len(candidate.strip()) < MIN_SQL_LIKE_STRING_LENGTH:
            return False

        candidate_upper = candidate.strip().upper()

        if expected_type:
            return candidate_upper.startswith(expected_type.upper())

        if any(candidate_upper.startswith(starter) for starter in SQL_STARTERS):
            return " " in candidate

        return False

    def _populate_builder_from_sql(
        self, builder: BuilderT, sql_string: str, expected_type: type[exp.Expr], parsed_expr: "exp.Expr | None" = None
    ) -> BuilderT:
        """Parse SQL string and populate a builder using SQLGlot directly."""
        builder_name = expected_type.__name__.lower()
        try:
            if parsed_expr is None:
                parsed_expr = exp.maybe_parse(sql_string, dialect=self.dialect)

            if expected_type is exp.Select and isinstance(parsed_expr, exp.With):
                base_expression = parsed_expr.this
                if isinstance(builder, Select) and isinstance(base_expression, exp.Select):
                    builder.set_expression(base_expression)
                    builder.load_ctes(list(parsed_expr.expressions))
                    return builder

            if isinstance(parsed_expr, expected_type):
                builder.set_expression(parsed_expr)
                return builder

            if expected_type is exp.Insert and isinstance(parsed_expr, exp.Select):
                logger.debug(
                    "Detected SELECT statement for INSERT; builder requires explicit target table",
                    extra={"builder": "insert"},
                )
                return builder

            logger.debug(
                "Cannot create %s from parsed statement type",
                builder_name.upper(),
                extra={"builder": builder_name, "parsed_type": type(parsed_expr).__name__},
            )

        except Exception:
            logger.debug(
                "Failed to parse %s SQL; falling back to traditional mode",
                builder_name.upper(),
                exc_info=True,
                extra={"builder": builder_name},
            )
        return builder

    def _populate_insert_from_sql(
        self, builder: "Insert", sql_string: str, parsed_expr: "exp.Expr | None" = None
    ) -> "Insert":
        """Parse SQL string and populate INSERT builder using SQLGlot directly."""
        return self._populate_builder_from_sql(builder, sql_string, exp.Insert, parsed_expr)

    def _populate_select_from_sql(
        self, builder: "Select", sql_string: str, parsed_expr: "exp.Expr | None" = None
    ) -> "Select":
        """Parse SQL string and populate SELECT builder using SQLGlot directly."""
        return self._populate_builder_from_sql(builder, sql_string, exp.Select, parsed_expr)

    def _populate_update_from_sql(
        self, builder: "Update", sql_string: str, parsed_expr: "exp.Expr | None" = None
    ) -> "Update":
        """Parse SQL string and populate UPDATE builder using SQLGlot directly."""
        return self._populate_builder_from_sql(builder, sql_string, exp.Update, parsed_expr)

    def _populate_delete_from_sql(
        self, builder: "Delete", sql_string: str, parsed_expr: "exp.Expr | None" = None
    ) -> "Delete":
        """Parse SQL string and populate DELETE builder using SQLGlot directly."""
        return self._populate_builder_from_sql(builder, sql_string, exp.Delete, parsed_expr)

    def _populate_merge_from_sql(
        self, builder: "Merge", sql_string: str, parsed_expr: "exp.Expr | None" = None
    ) -> "Merge":
        """Parse SQL string and populate MERGE builder using SQLGlot directly."""
        return self._populate_builder_from_sql(builder, sql_string, exp.Merge, parsed_expr)

    def column(self, name: str, table: str | None = None) -> Column:
        """Create a column reference.

        Args:
            name: Column name.
            table: Optional table name.

        Returns:
            Column object that supports method chaining and operator overloading.
        """
        return Column(name, table)

    @property
    def case_(self) -> "Case":
        """Create a CASE expression builder.

        Returns:
            Case builder instance for CASE expression building.
        """
        return Case()

    @property
    def row_number_(self) -> "WindowFunctionBuilder":
        """Create a ROW_NUMBER() window function builder."""
        return WindowFunctionBuilder("row_number")

    @property
    def rank_(self) -> "WindowFunctionBuilder":
        """Create a RANK() window function builder."""
        return WindowFunctionBuilder("rank")

    @property
    def dense_rank_(self) -> "WindowFunctionBuilder":
        """Create a DENSE_RANK() window function builder."""
        return WindowFunctionBuilder("dense_rank")

    @property
    def lag_(self) -> "WindowFunctionBuilder":
        """Create a LAG() window function builder."""
        return WindowFunctionBuilder("lag")

    @property
    def lead_(self) -> "WindowFunctionBuilder":
        """Create a LEAD() window function builder."""
        return WindowFunctionBuilder("lead")

    @property
    def count_over_(self) -> "WindowFunctionBuilder":
        """Create a COUNT(*) OVER() window function builder.

        Returns a WindowFunctionBuilder pre-configured with COUNT(*) for fluent chaining.
        Useful for pagination queries where you want to get the total count in the same query.

        Returns:
            WindowFunctionBuilder configured for COUNT(*) OVER()
        """
        return WindowFunctionBuilder("count", exp.Star())

    @property
    def sum_over_(self) -> "WindowFunctionBuilder":
        """Create a SUM() OVER() window function builder."""
        return WindowFunctionBuilder("sum")

    @property
    def avg_over_(self) -> "WindowFunctionBuilder":
        """Create an AVG() OVER() window function builder."""
        return WindowFunctionBuilder("avg")

    @property
    def max_over_(self) -> "WindowFunctionBuilder":
        """Create a MAX() OVER() window function builder."""
        return WindowFunctionBuilder("max")

    @property
    def min_over_(self) -> "WindowFunctionBuilder":
        """Create a MIN() OVER() window function builder."""
        return WindowFunctionBuilder("min")

    @property
    def exists_(self) -> "SubqueryBuilder":
        """Create an EXISTS subquery builder."""
        return SubqueryBuilder("exists")

    @property
    def in_(self) -> "SubqueryBuilder":
        """Create an IN subquery builder."""
        return SubqueryBuilder("in")

    @property
    def any_(self) -> "SubqueryBuilder":
        """Create an ANY subquery builder."""
        return SubqueryBuilder("any")

    @property
    def all_(self) -> "SubqueryBuilder":
        """Create an ALL subquery builder."""
        return SubqueryBuilder("all")

    @property
    def inner_join_(self) -> "JoinBuilder":
        """Create an INNER JOIN builder."""
        return create_join_builder("inner join")

    @property
    def left_join_(self) -> "JoinBuilder":
        """Create a LEFT JOIN builder."""
        return create_join_builder("left join")

    @property
    def right_join_(self) -> "JoinBuilder":
        """Create a RIGHT JOIN builder."""
        return create_join_builder("right join")

    @property
    def full_join_(self) -> "JoinBuilder":
        """Create a FULL OUTER JOIN builder."""
        return create_join_builder("full join")

    @property
    def cross_join_(self) -> "JoinBuilder":
        """Create a CROSS JOIN builder."""
        return create_join_builder("cross join")

    @property
    def lateral_join_(self) -> "JoinBuilder":
        """Create a LATERAL JOIN builder.

        Returns:
            JoinBuilder configured for LATERAL JOIN
        """
        return create_join_builder("lateral join", lateral=True)

    @property
    def left_lateral_join_(self) -> "JoinBuilder":
        """Create a LEFT LATERAL JOIN builder.

        Returns:
            JoinBuilder configured for LEFT LATERAL JOIN
        """
        return create_join_builder("left join", lateral=True)

    @property
    def cross_lateral_join_(self) -> "JoinBuilder":
        """Create a CROSS LATERAL JOIN builder.

        Returns:
            JoinBuilder configured for CROSS LATERAL JOIN
        """
        return create_join_builder("cross join", lateral=True)

    def __getattr__(self, name: str) -> "Column":
        """Dynamically create column references.

        Args:
            name: Column name.

        Returns:
            Column object for the given name.
        """
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        return Column(name)

    @staticmethod
    def raw(sql_fragment: str, **parameters: Any) -> "exp.Expr | SQL":
        """Create a raw SQL expression from a string fragment with optional parameters.

        Args:
            sql_fragment: Raw SQL string to parse into an expression.
            **parameters: Named parameters for parameter binding.

        Returns:
            SQLGlot expression from the parsed SQL fragment (if no parameters).
            SQL statement object (if parameters provided).

        Raises:
            SQLBuilderError: If the SQL fragment cannot be parsed.
        """
        if not parameters:
            try:
                parsed: exp.Expr = exp.maybe_parse(sql_fragment)
            except Exception as e:
                msg = f"Failed to parse raw SQL fragment '{sql_fragment}': {e}"
                raise SQLBuilderError(msg) from e
            return parsed

        return SQL(sql_fragment, parameters)

    def count(
        self, column: ColumnLike = "*", distinct: bool = False
    ) -> AggregateExpression:
        """Create a COUNT expression.

        Args:
            column: Column to count (default "*").
            distinct: Whether to use COUNT DISTINCT.

        Returns:
            COUNT expression.
        """
        if isinstance(column, str) and column == "*":
            if distinct:
                msg = "COUNT(DISTINCT *) is not valid SQL; pass a column to count distinct values."
                raise SQLBuilderError(msg)
            expr = exp.Count(this=exp.Star())
        else:
            col_expr = extract_expression(column)
            expr = exp.Count(this=exp.Distinct(expressions=[col_expr])) if distinct else exp.Count(this=col_expr)
        return AggregateExpression(expr)

    def count_distinct(self, column: ColumnLike) -> AggregateExpression:
        """Create a COUNT(DISTINCT column) expression.

        Args:
            column: Column to count distinct values.

        Returns:
            COUNT DISTINCT expression.
        """
        return self.count(column, distinct=True)

    def count_over(
        self,
        column: ColumnLike = "*",
        partition_by: str | list[str] | exp.Expr | None = None,
        order_by: str | list[str] | exp.Expr | None = None,
    ) -> FunctionExpression:
        """Create a COUNT() OVER() window function for inline total counts.

        This is particularly useful for pagination queries where you want to get
        the total count in the same query as the paginated results.

        Args:
            column: Column to count (default "*" for COUNT(*)).
            partition_by: Optional columns to partition by.
            order_by: Optional columns to order by.

        Returns:
            COUNT() OVER() window function expression.
        """
        if isinstance(column, str) and column == "*":
            count_expr = exp.Count(this=exp.Star())
        else:
            col_expr = extract_expression(column)
            count_expr = exp.Count(this=col_expr)

        over_args: dict[str, Any] = {}
        normalized_partition = _normalize_partition_by(partition_by)
        if normalized_partition is not None:
            over_args["partition_by"] = normalized_partition
        normalized_order = _normalize_order_by(order_by)
        if normalized_order is not None:
            over_args["order"] = normalized_order

        return FunctionExpression(exp.Window(this=count_expr, **over_args))

    def sum_over(
        self,
        column: ColumnLike,
        partition_by: str | list[str] | exp.Expr | None = None,
        order_by: str | list[str] | exp.Expr | None = None,
    ) -> FunctionExpression:
        """Create a SUM() OVER() window function.

        Args:
            column: Column to sum.
            partition_by: Columns to partition by.
            order_by: Columns to order by.

        Returns:
            SUM() OVER() window function expression.
        """
        col_expr = extract_expression(column)
        return self._create_window_function("SUM", [col_expr], partition_by, order_by)

    def avg_over(
        self,
        column: ColumnLike,
        partition_by: str | list[str] | exp.Expr | None = None,
        order_by: str | list[str] | exp.Expr | None = None,
    ) -> FunctionExpression:
        """Create an AVG() OVER() window function.

        Args:
            column: Column to average.
            partition_by: Columns to partition by.
            order_by: Columns to order by.

        Returns:
            AVG() OVER() window function expression.
        """
        col_expr = extract_expression(column)
        return self._create_window_function("AVG", [col_expr], partition_by, order_by)

    def max_over(
        self,
        column: ColumnLike,
        partition_by: str | list[str] | exp.Expr | None = None,
        order_by: str | list[str] | exp.Expr | None = None,
    ) -> FunctionExpression:
        """Create a MAX() OVER() window function.

        Args:
            column: Column to find maximum.
            partition_by: Columns to partition by.
            order_by: Columns to order by.

        Returns:
            MAX() OVER() window function expression.
        """
        col_expr = extract_expression(column)
        return self._create_window_function("MAX", [col_expr], partition_by, order_by)

    def min_over(
        self,
        column: ColumnLike,
        partition_by: str | list[str] | exp.Expr | None = None,
        order_by: str | list[str] | exp.Expr | None = None,
    ) -> FunctionExpression:
        """Create a MIN() OVER() window function.

        Args:
            column: Column to find minimum.
            partition_by: Columns to partition by.
            order_by: Columns to order by.

        Returns:
            MIN() OVER() window function expression.
        """
        col_expr = extract_expression(column)
        return self._create_window_function("MIN", [col_expr], partition_by, order_by)

    @staticmethod
    def sum(column: ColumnLike, distinct: bool = False) -> AggregateExpression:
        """Create a SUM expression.

        Args:
            column: Column to sum.
            distinct: Whether to use SUM DISTINCT.

        Returns:
            SUM expression.
        """
        col_expr = extract_expression(column)
        if distinct:
            return AggregateExpression(exp.Sum(this=exp.Distinct(expressions=[col_expr])))
        return AggregateExpression(exp.Sum(this=col_expr))

    @staticmethod
    def avg(column: ColumnLike) -> AggregateExpression:
        """Create an AVG expression.

        Args:
            column: Column to average.

        Returns:
            AVG expression.
        """
        col_expr = extract_expression(column)
        return AggregateExpression(exp.Avg(this=col_expr))

    @staticmethod
    def max(column: ColumnLike) -> AggregateExpression:
        """Create a MAX expression.

        Args:
            column: Column to find maximum.

        Returns:
            MAX expression.
        """
        col_expr = extract_expression(column)
        return AggregateExpression(exp.Max(this=col_expr))

    @staticmethod
    def min(column: ColumnLike) -> AggregateExpression:
        """Create a MIN expression.

        Args:
            column: Column to find minimum.

        Returns:
            MIN expression.
        """
        col_expr = extract_expression(column)
        return AggregateExpression(exp.Min(this=col_expr))

    @staticmethod
    def rollup(*columns: str | exp.Expr) -> FunctionExpression:
        """Create a ROLLUP expression for GROUP BY clauses.

        Args:
            *columns: Columns to include in the rollup.

        Returns:
            ROLLUP expression.
        """
        column_exprs = [_coerce_column(col) for col in columns]
        return FunctionExpression(exp.Rollup(expressions=column_exprs))

    @staticmethod
    def cube(*columns: str | exp.Expr) -> FunctionExpression:
        """Create a CUBE expression for GROUP BY clauses.

        Args:
            *columns: Columns to include in the cube.

        Returns:
            CUBE expression.
        """
        column_exprs = [_coerce_column(col) for col in columns]
        return FunctionExpression(exp.Cube(expressions=column_exprs))

    @staticmethod
    def grouping_sets(*column_sets: tuple[str, ...] | list[str]) -> FunctionExpression:
        """Create a GROUPING SETS expression for GROUP BY clauses.

        Args:
            *column_sets: Sets of columns to group by.

        Returns:
            GROUPING SETS expression.
        """
        set_expressions = []
        for column_set in column_sets:
            if isinstance(column_set, (tuple, list)):
                if len(column_set) == 0:
                    set_expressions.append(exp.Tuple(expressions=[]))
                else:
                    columns = [exp.column(col) for col in column_set]
                    set_expressions.append(exp.Tuple(expressions=columns))
            else:
                set_expressions.append(exp.column(column_set))

        return FunctionExpression(exp.GroupingSets(expressions=set_expressions))

    @staticmethod
    def any(values: list[Any] | exp.Expr | str) -> FunctionExpression:
        """Create an ANY expression for use with comparison operators.

        Args:
            values: Values, expression, or subquery for the ANY clause.

        Returns:
            ANY expression.
        """
        if isinstance(values, list):
            literals = [SQLFactory.to_literal(v).expression for v in values]
            return FunctionExpression(exp.Any(this=exp.Paren(this=exp.Array(expressions=literals))))
        if isinstance(values, str):
            parsed: exp.Expr = exp.maybe_parse(values)
            return FunctionExpression(exp.Any(this=parsed))
        return FunctionExpression(exp.Any(this=values))

    @staticmethod
    def not_any_(values: list[Any] | exp.Expr | str) -> FunctionExpression:
        """Create a NOT ANY expression for use with comparison operators.

        Args:
            values: Values, expression, or subquery for the NOT ANY clause.

        Returns:
            NOT ANY expression.
        """
        return FunctionExpression(exp.Not(this=SQLFactory.any(values).expression))

    @staticmethod
    def concat(*expressions: str | exp.Expr) -> StringExpression:
        """Create a CONCAT expression.

        Args:
            *expressions: Expressions to concatenate.

        Returns:
            CONCAT expression.
        """
        exprs = [_coerce_column(expr) for expr in expressions]
        return StringExpression(exp.Concat(expressions=exprs))

    @staticmethod
    def upper(column: ColumnLike) -> StringExpression:
        """Create an UPPER expression.

        Args:
            column: Column to convert to uppercase.

        Returns:
            UPPER expression.
        """
        col_expr = extract_expression(column)
        return StringExpression(exp.Upper(this=col_expr))

    @staticmethod
    def lower(column: ColumnLike) -> StringExpression:
        """Create a LOWER expression.

        Args:
            column: Column to convert to lowercase.

        Returns:
            LOWER expression.
        """
        col_expr = extract_expression(column)
        return StringExpression(exp.Lower(this=col_expr))

    @staticmethod
    def length(column: ColumnLike) -> StringExpression:
        """Create a LENGTH expression.

        Args:
            column: Column to get length of.

        Returns:
            LENGTH expression.
        """
        col_expr = extract_expression(column)
        return StringExpression(exp.Length(this=col_expr))

    @staticmethod
    def round(column: ColumnLike, decimals: int = 0) -> MathExpression:
        """Create a ROUND expression.

        Args:
            column: Column to round.
            decimals: Number of decimal places.

        Returns:
            ROUND expression.
        """
        col_expr = extract_expression(column)
        if decimals == 0:
            return MathExpression(exp.Round(this=col_expr))
        return MathExpression(exp.Round(this=col_expr, decimals=exp.Literal.number(decimals)))

    @staticmethod
    def to_literal(value: Any) -> FunctionExpression:
        """Convert a Python value to a SQLGlot literal expression.

        Uses SQLGlot's built-in exp.convert() function for literal creation.
        Handles all Python primitive types:
            - None -> exp.Null (renders as NULL)
            - bool -> exp.Boolean (renders as TRUE/FALSE or 1/0 based on dialect)
            - int/float -> exp.Literal with is_number=True
            - str -> exp.Literal with is_string=True
            - exp.Expr -> returned as-is (passthrough)

        Args:
            value: Python value or SQLGlot expression to convert.

        Returns:
            SQLGlot expression representing the literal value.
        """
        if isinstance(value, exp.Expr):
            return FunctionExpression(value)
        return FunctionExpression(exp.convert(value))

    @staticmethod
    def decode(column: ColumnLike, *args: str | exp.Expr | Any) -> FunctionExpression:
        """Create a DECODE expression (Oracle-style conditional logic).

        DECODE compares column to each search value and returns the corresponding result.
        If no match is found, returns the default value (if provided) or NULL.

        Args:
            column: Column to compare.
            *args: Alternating search values and results, with optional default at the end.
            Format: search1, result1, search2, result2, ..., [default]

        Raises:
            ValueError: If fewer than two search/result pairs are provided.

        Returns:
            CASE expression equivalent to DECODE.
        """
        col_expr = extract_expression(column)

        if len(args) < MIN_DECODE_ARGS:
            msg = "DECODE requires at least one search/result pair"
            raise ValueError(msg)

        conditions = []
        default = None
        paired_args = args
        if len(args) % 2 == 1:
            default = to_expression(args[-1])
            paired_args = args[:-1]

        for i in range(0, len(paired_args), 2):
            search_val = paired_args[i]
            result_val = paired_args[i + 1]

            search_expr = to_expression(search_val)
            result_expr = to_expression(result_val)

            condition = exp.EQ(this=col_expr, expression=search_expr)
            conditions.append(exp.If(this=condition, true=result_expr))

        return FunctionExpression(exp.Case(ifs=conditions, default=default))

    @staticmethod
    def cast(column: ColumnLike, data_type: str) -> ConversionExpression:
        """Create a CAST expression for type conversion.

        Args:
            column: Column or expression to cast.
            data_type: Target data type.

        Returns:
            CAST expression.
        """
        col_expr = extract_expression(column)
        return ConversionExpression(exp.Cast(this=col_expr, to=exp.DataType.build(data_type)))

    @staticmethod
    def coalesce(*expressions: str | exp.Expr) -> ConversionExpression:
        """Create a COALESCE expression.

        Args:
            *expressions: Expressions to coalesce.

        Returns:
            COALESCE expression.
        """
        exprs = [_coerce_column(expr) for expr in expressions]
        return ConversionExpression(exp.Coalesce(expressions=exprs))

    @staticmethod
    def nvl(column: ColumnLike, substitute_value: str | exp.Expr | Any) -> ConversionExpression:
        """Create an NVL (Oracle-style) expression using COALESCE.

        Args:
            column: Column to check for NULL.
            substitute_value: Value to use if column is NULL.

        Returns:
            COALESCE expression equivalent to NVL.
        """
        col_expr = extract_expression(column)
        sub_expr = to_expression(substitute_value)
        return ConversionExpression(exp.Coalesce(expressions=[col_expr, sub_expr]))

    @staticmethod
    def nvl2(
        column: ColumnLike, value_if_not_null: str | exp.Expr | Any, value_if_null: str | exp.Expr | Any
    ) -> ConversionExpression:
        """Create an NVL2 (Oracle-style) expression using CASE.

        NVL2 returns value_if_not_null if column is not NULL,
        otherwise returns value_if_null.

        Args:
            column: Column to check for NULL.
            value_if_not_null: Value to use if column is NOT NULL.
            value_if_null: Value to use if column is NULL.

        Returns:
            CASE expression equivalent to NVL2.
        """
        col_expr = extract_expression(column)
        not_null_expr = to_expression(value_if_not_null)
        null_expr = to_expression(value_if_null)

        is_null = exp.Is(this=col_expr, expression=exp.Null())
        condition = exp.Not(this=is_null)
        when_clause = exp.If(this=condition, true=not_null_expr)

        return ConversionExpression(exp.Case(ifs=[when_clause], default=null_expr))

    @staticmethod
    def bulk_insert(table_name: str, column_count: int, placeholder_style: str = "?") -> FunctionExpression:
        """Create bulk INSERT expression for executemany operations.

        For bulk loading operations like CSV ingestion where
        an INSERT expression with placeholders for executemany() is needed.

        Args:
            table_name: Name of the table to insert into
            column_count: Number of columns (for placeholder generation)
            placeholder_style: Placeholder style ("?" for SQLite/PostgreSQL, "%s" for MySQL, ":1" for Oracle)

        Returns:
            INSERT expression with placeholders for bulk operations
        """
        return FunctionExpression(
            exp.Insert(
                this=exp.Table(this=exp.to_identifier(table_name)),
                expression=exp.Values(
                    expressions=[
                        exp.Tuple(expressions=[exp.Placeholder(this=placeholder_style) for _ in range(column_count)])
                    ]
                ),
            )
        )

    def truncate(self, table_name: str) -> "Truncate":
        """Create a TRUNCATE TABLE builder.

        Args:
            table_name: Name of the table to truncate

        Returns:
            TruncateTable builder instance
        """
        return Truncate(table_name, dialect=self.dialect)

    @staticmethod
    def case() -> "Case":
        """Create a CASE expression builder.

        Returns:
            CaseExpressionBuilder for building CASE expressions.
        """
        return Case()

    def row_number(
        self, partition_by: str | list[str] | exp.Expr | None = None, order_by: str | list[str] | exp.Expr | None = None
    ) -> FunctionExpression:
        """Create a ROW_NUMBER() window function.

        Args:
            partition_by: Columns to partition by.
            order_by: Columns to order by.

        Returns:
            ROW_NUMBER window function expression.
        """
        return self._create_window_function("ROW_NUMBER", [], partition_by, order_by)

    def rank(
        self, partition_by: str | list[str] | exp.Expr | None = None, order_by: str | list[str] | exp.Expr | None = None
    ) -> FunctionExpression:
        """Create a RANK() window function.

        Args:
            partition_by: Columns to partition by.
            order_by: Columns to order by.

        Returns:
            RANK window function expression.
        """
        return self._create_window_function("RANK", [], partition_by, order_by)

    def dense_rank(
        self, partition_by: str | list[str] | exp.Expr | None = None, order_by: str | list[str] | exp.Expr | None = None
    ) -> FunctionExpression:
        """Create a DENSE_RANK() window function.

        Args:
            partition_by: Columns to partition by.
            order_by: Columns to order by.

        Returns:
            DENSE_RANK window function expression.
        """
        return self._create_window_function("DENSE_RANK", [], partition_by, order_by)

    def lag(
        self,
        column: ColumnLike,
        offset: int = 1,
        default: Any = None,
        partition_by: str | list[str] | exp.Expr | None = None,
        order_by: str | list[str] | exp.Expr | None = None,
    ) -> FunctionExpression:
        """Create a LAG() window function.

        LAG accesses data from a previous row in the same result set without using a self-join.

        Args:
            column: The column to get the lagged value from.
            offset: Number of rows to look back (default 1).
            default: Value to return when there is no row at the offset.
            partition_by: Columns to partition by.
            order_by: Columns to order by.

        Returns:
            LAG window function expression.
        """
        col_expr = extract_expression(column)
        func_args: list[exp.Expr] = [col_expr, exp.Literal.number(offset)]
        if default is not None:
            func_args.append(exp.convert(default))
        return self._create_window_function("LAG", func_args, partition_by, order_by)

    def lead(
        self,
        column: ColumnLike,
        offset: int = 1,
        default: Any = None,
        partition_by: str | list[str] | exp.Expr | None = None,
        order_by: str | list[str] | exp.Expr | None = None,
    ) -> FunctionExpression:
        """Create a LEAD() window function.

        LEAD accesses data from a subsequent row in the same result set without using a self-join.

        Args:
            column: The column to get the lead value from.
            offset: Number of rows to look forward (default 1).
            default: Value to return when there is no row at the offset.
            partition_by: Columns to partition by.
            order_by: Columns to order by.

        Returns:
            LEAD window function expression.
        """
        col_expr = extract_expression(column)
        func_args: list[exp.Expr] = [col_expr, exp.Literal.number(offset)]
        if default is not None:
            func_args.append(exp.convert(default))
        return self._create_window_function("LEAD", func_args, partition_by, order_by)

    @staticmethod
    def _create_window_function(
        func_name: str,
        func_args: list[exp.Expr],
        partition_by: str | list[str] | exp.Expr | None = None,
        order_by: str | list[str] | exp.Expr | None = None,
    ) -> FunctionExpression:
        """Helper to create window function expressions.

        Args:
            func_name: Name of the window function.
            func_args: Arguments to the function.
            partition_by: Columns to partition by.
            order_by: Columns to order by.

        Returns:
            Window function expression.
        """
        func_expr = exp.Anonymous(this=func_name, expressions=func_args)

        over_args: dict[str, Any] = {}

        normalized_partition = _normalize_partition_by(partition_by)
        if normalized_partition is not None:
            over_args["partition_by"] = normalized_partition
        normalized_order = _normalize_order_by(order_by)
        if normalized_order is not None:
            over_args["order"] = normalized_order

        return FunctionExpression(exp.Window(this=func_expr, **over_args))


sql = SQLFactory()
