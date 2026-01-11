"""SQL factory for creating SQL builders and column expressions.

Provides statement builders (select, insert, update, etc.) and column expressions.
"""

import hashlib
import logging
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Union, cast

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import ParseError as SQLGlotParseError

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
from sqlspec.builder._parsing_utils import extract_expression, to_expression
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


def _to_copy_schema(table: str, columns: "Sequence[str] | None") -> exp.Expression:
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
            value_expression: exp.Expression
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


class SQLFactory:
    """Factory for creating SQL builders and column expressions."""

    @staticmethod
    def _detect_type_from_expression(parsed_expr: exp.Expression) -> str:
        if parsed_expr.key:
            return parsed_expr.key.upper()
        command_type = type(parsed_expr).__name__.upper()
        if command_type == "COMMAND" and parsed_expr.this:
            return str(parsed_expr.this).upper()
        return command_type

    @staticmethod
    def _parse_sql_expression(sql: str, dialect: DialectType | None) -> "exp.Expression | None":
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

    @classmethod
    def detect_sql_type(cls, sql: str, dialect: DialectType = None) -> str:
        parsed_expr = cls._parse_sql_expression(sql, dialect)
        if parsed_expr is None:
            return "UNKNOWN"
        return cls._detect_type_from_expression(parsed_expr)

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
            parsed_expr = sqlglot.parse_one(statement, read=dialect or self.dialect)
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
            builder = Select(dialect=dialect or self.dialect)
            builder.set_expression(parsed_expr)
            return builder

        if actual_type_str in {"INSERT", "UPDATE", "DELETE"} and parsed_expr.args.get("returning") is not None:
            return SQL(statement)

        msg = (
            f"sql(...) only supports SELECT statements or DML statements with RETURNING clause. "
            f"Detected type: {actual_type_str}. "
            f"Use sql.{actual_type_str.lower()}() instead."
        )
        raise SQLBuilderError(msg)

    def select(
        self, *columns_or_sql: Union[str, exp.Expression, Column, "SQL", "Case"], dialect: DialectType = None
    ) -> "Select":
        builder_dialect = dialect or self.dialect
        if len(columns_or_sql) == 1 and isinstance(columns_or_sql[0], str):
            sql_candidate = columns_or_sql[0].strip()
            if self._looks_like_sql(sql_candidate):
                parsed_expr = self._parse_sql_expression(sql_candidate, builder_dialect)
                detected = "UNKNOWN" if parsed_expr is None else self._detect_type_from_expression(parsed_expr)
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
        builder_dialect = dialect or self.dialect
        builder = Insert(dialect=builder_dialect)
        if table_or_sql:
            if self._looks_like_sql(table_or_sql):
                parsed_expr = self._parse_sql_expression(table_or_sql, builder_dialect)
                detected = "UNKNOWN" if parsed_expr is None else self._detect_type_from_expression(parsed_expr)
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
        builder_dialect = dialect or self.dialect
        builder = Update(dialect=builder_dialect)
        if table_or_sql:
            if self._looks_like_sql(table_or_sql):
                parsed_expr = self._parse_sql_expression(table_or_sql, builder_dialect)
                detected = "UNKNOWN" if parsed_expr is None else self._detect_type_from_expression(parsed_expr)
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
        builder_dialect = dialect or self.dialect
        if table_or_sql and self._looks_like_sql(table_or_sql):
            builder = Delete(dialect=builder_dialect)
            parsed_expr = self._parse_sql_expression(table_or_sql, builder_dialect)
            detected = "UNKNOWN" if parsed_expr is None else self._detect_type_from_expression(parsed_expr)
            if detected != "DELETE":
                msg = (
                    f"sql.delete() expects DELETE statement, got {detected}. "
                    f"Use sql.{detected.lower()}() if a dedicated builder exists."
                )
                raise SQLBuilderError(msg)
            return self._populate_delete_from_sql(builder, table_or_sql, parsed_expr)

        return Delete(table_or_sql, dialect=builder_dialect) if table_or_sql else Delete(dialect=builder_dialect)

    def merge(self, table_or_sql: str | None = None, dialect: DialectType = None) -> "Merge":
        builder_dialect = dialect or self.dialect
        if table_or_sql and self._looks_like_sql(table_or_sql):
            builder = Merge(dialect=builder_dialect)
            parsed_expr = self._parse_sql_expression(table_or_sql, builder_dialect)
            detected = "UNKNOWN" if parsed_expr is None else self._detect_type_from_expression(parsed_expr)
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
        statement: "str | exp.Expression | SQL | SQLBuilderProtocol",
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

        Examples:
            Basic EXPLAIN:
                plan = sql.explain("SELECT * FROM users").build()

            With options:
                plan = (
                    sql.explain("SELECT * FROM users", analyze=True, format="json")
                    .buffers()
                    .timing()
                    .build()
                )

            From QueryBuilder:
                query = sql.select("*").from_("users").where("id = :id", id=1)
                plan = sql.explain(query, analyze=True).build()

            Chained configuration:
                plan = (
                    sql.explain(sql.select("*").from_("large_table"))
                    .analyze()
                    .format("json")
                    .buffers()
                    .timing()
                    .build()
                )
        """
        builder_dialect = dialect or self.dialect

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

        Examples:
            query = sql.merge_.into("products").using(data, alias="src")
            query = sql.merge_.into("products", alias="t").on("t.id = src.id")

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

        Examples:
            PostgreSQL/Oracle/BigQuery (uses MERGE):
                upsert_query = (
                    sql.upsert("products", dialect="postgres")
                    .using([{"id": 1, "name": "Product 1"}], alias="src")
                    .on("t.id = src.id")
                    .when_matched_then_update(name="src.name")
                    .when_not_matched_then_insert(id="src.id", name="src.name")
                )

            SQLite/DuckDB/MySQL (uses INSERT ON CONFLICT):
                upsert_query = (
                    sql.upsert("products", dialect="sqlite")
                    .values(id=1, name="Product 1")
                    .on_conflict("id")
                    .do_update(name="EXCLUDED.name")
                )
        """
        builder_dialect = dialect or self.dialect
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
        return CreateTable(table_name, dialect=dialect or self.dialect)

    def create_table_as_select(self, dialect: DialectType = None) -> "CreateTableAsSelect":
        """Create a CREATE TABLE AS SELECT builder.

        Args:
            dialect: Optional SQL dialect

        Returns:
            CreateTableAsSelect builder instance
        """
        return CreateTableAsSelect(dialect=dialect or self.dialect)

    def create_view(self, view_name: str, dialect: DialectType = None) -> "CreateView":
        """Create a CREATE VIEW builder.

        Args:
            view_name: Name of the view to create
            dialect: Optional SQL dialect

        Returns:
            CreateView builder instance
        """
        return CreateView(view_name, dialect=dialect or self.dialect)

    def create_materialized_view(self, view_name: str, dialect: DialectType = None) -> "CreateMaterializedView":
        """Create a CREATE MATERIALIZED VIEW builder.

        Args:
            view_name: Name of the materialized view to create
            dialect: Optional SQL dialect

        Returns:
            CreateMaterializedView builder instance
        """
        return CreateMaterializedView(view_name, dialect=dialect or self.dialect)

    def create_index(self, index_name: str, dialect: DialectType = None) -> "CreateIndex":
        """Create a CREATE INDEX builder.

        Args:
            index_name: Name of the index to create
            dialect: Optional SQL dialect

        Returns:
            CreateIndex builder instance
        """
        return CreateIndex(index_name, dialect=dialect or self.dialect)

    def create_schema(self, schema_name: str, dialect: DialectType = None) -> "CreateSchema":
        """Create a CREATE SCHEMA builder.

        Args:
            schema_name: Name of the schema to create
            dialect: Optional SQL dialect

        Returns:
            CreateSchema builder instance
        """
        return CreateSchema(schema_name, dialect=dialect or self.dialect)

    def drop_table(self, table_name: str, dialect: DialectType = None) -> "DropTable":
        """Create a DROP TABLE builder.

        Args:
            table_name: Name of the table to drop
            dialect: Optional SQL dialect

        Returns:
            DropTable builder instance
        """
        return DropTable(table_name, dialect=dialect or self.dialect)

    def drop_view(self, view_name: str, dialect: DialectType = None) -> "DropView":
        """Create a DROP VIEW builder.

        Args:
            view_name: Name of the view to drop
            dialect: Optional SQL dialect

        Returns:
            DropView builder instance
        """
        return DropView(view_name, dialect=dialect or self.dialect)

    def drop_index(self, index_name: str, dialect: DialectType = None) -> "DropIndex":
        """Create a DROP INDEX builder.

        Args:
            index_name: Name of the index to drop
            dialect: Optional SQL dialect

        Returns:
            DropIndex builder instance
        """
        return DropIndex(index_name, dialect=dialect or self.dialect)

    def drop_schema(self, schema_name: str, dialect: DialectType = None) -> "DropSchema":
        """Create a DROP SCHEMA builder.

        Args:
            schema_name: Name of the schema to drop
            dialect: Optional SQL dialect

        Returns:
            DropSchema builder instance
        """
        return DropSchema(schema_name, dialect=dialect or self.dialect)

    def alter_table(self, table_name: str, dialect: DialectType = None) -> "AlterTable":
        """Create an ALTER TABLE builder.

        Args:
            table_name: Name of the table to alter
            dialect: Optional SQL dialect

        Returns:
            AlterTable builder instance
        """
        return AlterTable(table_name, dialect=dialect or self.dialect)

    def rename_table(self, old_name: str, dialect: DialectType = None) -> "RenameTable":
        """Create a RENAME TABLE builder.

        Args:
            old_name: Current name of the table
            dialect: Optional SQL dialect

        Returns:
            RenameTable builder instance
        """
        return RenameTable(old_name, dialect=dialect or self.dialect)

    def comment_on(self, dialect: DialectType = None) -> "CommentOn":
        """Create a COMMENT ON builder.

        Args:
            dialect: Optional SQL dialect

        Returns:
            CommentOn builder instance
        """
        return CommentOn(dialect=dialect or self.dialect)

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

        effective_dialect = dialect or self.dialect
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

        effective_dialect = dialect or self.dialect
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

    def _populate_insert_from_sql(
        self, builder: "Insert", sql_string: str, parsed_expr: "exp.Expression | None" = None
    ) -> "Insert":
        """Parse SQL string and populate INSERT builder using SQLGlot directly."""
        try:
            if parsed_expr is None:
                parsed_expr = exp.maybe_parse(sql_string, dialect=self.dialect)

            if isinstance(parsed_expr, exp.Insert):
                builder.set_expression(parsed_expr)
                return builder

            if isinstance(parsed_expr, exp.Select):
                logger.debug(
                    "Detected SELECT statement for INSERT; builder requires explicit target table",
                    extra={"builder": "insert"},
                )
                return builder

            logger.debug(
                "Cannot create INSERT from parsed statement type",
                extra={"builder": "insert", "parsed_type": type(parsed_expr).__name__},
            )

        except Exception:
            logger.debug(
                "Failed to parse INSERT SQL; falling back to traditional mode",
                exc_info=True,
                extra={"builder": "insert"},
            )
        return builder

    def _populate_select_from_sql(
        self, builder: "Select", sql_string: str, parsed_expr: "exp.Expression | None" = None
    ) -> "Select":
        """Parse SQL string and populate SELECT builder using SQLGlot directly."""
        try:
            if parsed_expr is None:
                parsed_expr = exp.maybe_parse(sql_string, dialect=self.dialect)

            if isinstance(parsed_expr, exp.With):
                base_expression = parsed_expr.this
                if isinstance(base_expression, exp.Select):
                    builder.set_expression(base_expression)
                    builder.load_ctes(list(parsed_expr.expressions))
                    return builder
            if isinstance(parsed_expr, exp.Select):
                builder.set_expression(parsed_expr)
                return builder

            logger.debug(
                "Cannot create SELECT from parsed statement type",
                extra={"builder": "select", "parsed_type": type(parsed_expr).__name__},
            )

        except Exception:
            logger.debug(
                "Failed to parse SELECT SQL; falling back to traditional mode",
                exc_info=True,
                extra={"builder": "select"},
            )
        return builder

    def _populate_update_from_sql(
        self, builder: "Update", sql_string: str, parsed_expr: "exp.Expression | None" = None
    ) -> "Update":
        """Parse SQL string and populate UPDATE builder using SQLGlot directly."""
        try:
            if parsed_expr is None:
                parsed_expr = exp.maybe_parse(sql_string, dialect=self.dialect)

            if isinstance(parsed_expr, exp.Update):
                builder.set_expression(parsed_expr)
                return builder

            logger.debug(
                "Cannot create UPDATE from parsed statement type",
                extra={"builder": "update", "parsed_type": type(parsed_expr).__name__},
            )

        except Exception:
            logger.debug(
                "Failed to parse UPDATE SQL; falling back to traditional mode",
                exc_info=True,
                extra={"builder": "update"},
            )
        return builder

    def _populate_delete_from_sql(
        self, builder: "Delete", sql_string: str, parsed_expr: "exp.Expression | None" = None
    ) -> "Delete":
        """Parse SQL string and populate DELETE builder using SQLGlot directly."""
        try:
            if parsed_expr is None:
                parsed_expr = exp.maybe_parse(sql_string, dialect=self.dialect)

            if isinstance(parsed_expr, exp.Delete):
                builder.set_expression(parsed_expr)
                return builder

            logger.debug(
                "Cannot create DELETE from parsed statement type",
                extra={"builder": "delete", "parsed_type": type(parsed_expr).__name__},
            )

        except Exception:
            logger.debug(
                "Failed to parse DELETE SQL; falling back to traditional mode",
                exc_info=True,
                extra={"builder": "delete"},
            )
        return builder

    def _populate_merge_from_sql(
        self, builder: "Merge", sql_string: str, parsed_expr: "exp.Expression | None" = None
    ) -> "Merge":
        """Parse SQL string and populate MERGE builder using SQLGlot directly."""
        try:
            if parsed_expr is None:
                parsed_expr = exp.maybe_parse(sql_string, dialect=self.dialect)

            if isinstance(parsed_expr, exp.Merge):
                builder.set_expression(parsed_expr)
                return builder

            logger.debug(
                "Cannot create MERGE from parsed statement type",
                extra={"builder": "merge", "parsed_type": type(parsed_expr).__name__},
            )

        except Exception:
            logger.debug(
                "Failed to parse MERGE SQL; falling back to traditional mode", exc_info=True, extra={"builder": "merge"}
            )
        return builder

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

        Example:
            ```python
            case_expr = (
                sql.case_
                .when("x = 1", "one")
                .when("x = 2", "two")
                .else_("other")
                .end()
            )
            aliased_case = (
                sql.case_
                .when("status = 'active'", 1)
                .else_(0)
                .as_("is_active")
            )
            ```
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

        Example:
            ```python
            query = (
                sql
                .select("u.name", "arr.value")
                .from_("users u")
                .join(sql.lateral_join_("UNNEST(u.tags)").on("true"))
            )
            ```
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

        Note:
            Special SQL constructs like case_, row_number_, etc. are
            handled as properties for type safety.
        """
        return Column(name)

    @staticmethod
    def raw(sql_fragment: str, **parameters: Any) -> "exp.Expression | SQL":
        """Create a raw SQL expression from a string fragment with optional parameters.

        Args:
            sql_fragment: Raw SQL string to parse into an expression.
            **parameters: Named parameters for parameter binding.

        Returns:
            SQLGlot expression from the parsed SQL fragment (if no parameters).
            SQL statement object (if parameters provided).

        Raises:
            SQLBuilderError: If the SQL fragment cannot be parsed.

        Example:
            ```python
            expr = sql.raw("COALESCE(name, 'Unknown')")


            stmt = sql.raw(
                "LOWER(name) LIKE LOWER(:pattern)", pattern=f"%{query}%"
            )


            expr = sql.raw(
                "price BETWEEN :min_price AND :max_price",
                min_price=100,
                max_price=500,
            )


            query = sql.select(
                "name",
                sql.raw(
                    "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC)"
                ),
            ).from_("employees")
            ```
        """
        if not parameters:
            try:
                parsed: exp.Expression = exp.maybe_parse(sql_fragment)
            except Exception as e:
                msg = f"Failed to parse raw SQL fragment '{sql_fragment}': {e}"
                raise SQLBuilderError(msg) from e
            return parsed

        return SQL(sql_fragment, parameters)

    def count(
        self, column: Union[str, exp.Expression, "ExpressionWrapper", "Case", "Column"] = "*", distinct: bool = False
    ) -> AggregateExpression:
        """Create a COUNT expression.

        Args:
            column: Column to count (default "*").
            distinct: Whether to use COUNT DISTINCT.

        Returns:
            COUNT expression.
        """
        if isinstance(column, str) and column == "*":
            expr = exp.Count(this=exp.Star(), distinct=distinct)
        else:
            col_expr = extract_expression(column)
            expr = exp.Count(this=col_expr, distinct=distinct)
        return AggregateExpression(expr)

    def count_distinct(self, column: Union[str, exp.Expression, "ExpressionWrapper", "Case"]) -> AggregateExpression:
        """Create a COUNT(DISTINCT column) expression.

        Args:
            column: Column to count distinct values.

        Returns:
            COUNT DISTINCT expression.
        """
        return self.count(column, distinct=True)

    @staticmethod
    def sum(
        column: Union[str, exp.Expression, "ExpressionWrapper", "Case"], distinct: bool = False
    ) -> AggregateExpression:
        """Create a SUM expression.

        Args:
            column: Column to sum.
            distinct: Whether to use SUM DISTINCT.

        Returns:
            SUM expression.
        """
        col_expr = extract_expression(column)
        return AggregateExpression(exp.Sum(this=col_expr, distinct=distinct))

    @staticmethod
    def avg(column: Union[str, exp.Expression, "ExpressionWrapper", "Case"]) -> AggregateExpression:
        """Create an AVG expression.

        Args:
            column: Column to average.

        Returns:
            AVG expression.
        """
        col_expr = extract_expression(column)
        return AggregateExpression(exp.Avg(this=col_expr))

    @staticmethod
    def max(column: Union[str, exp.Expression, "ExpressionWrapper", "Case"]) -> AggregateExpression:
        """Create a MAX expression.

        Args:
            column: Column to find maximum.

        Returns:
            MAX expression.
        """
        col_expr = extract_expression(column)
        return AggregateExpression(exp.Max(this=col_expr))

    @staticmethod
    def min(column: Union[str, exp.Expression, "ExpressionWrapper", "Case"]) -> AggregateExpression:
        """Create a MIN expression.

        Args:
            column: Column to find minimum.

        Returns:
            MIN expression.
        """
        col_expr = extract_expression(column)
        return AggregateExpression(exp.Min(this=col_expr))

    @staticmethod
    def rollup(*columns: str | exp.Expression) -> FunctionExpression:
        """Create a ROLLUP expression for GROUP BY clauses.

        Args:
            *columns: Columns to include in the rollup.

        Returns:
            ROLLUP expression.

        Example:
            ```python
            query = (
                sql
                .select("product", "region", sql.sum("sales"))
                .from_("sales_data")
                .group_by(sql.rollup("product", "region"))
            )
            ```
        """
        column_exprs = [exp.column(col) if isinstance(col, str) else col for col in columns]
        return FunctionExpression(exp.Rollup(expressions=column_exprs))

    @staticmethod
    def cube(*columns: str | exp.Expression) -> FunctionExpression:
        """Create a CUBE expression for GROUP BY clauses.

        Args:
            *columns: Columns to include in the cube.

        Returns:
            CUBE expression.

        Example:
            ```python
            query = (
                sql
                .select("product", "region", sql.sum("sales"))
                .from_("sales_data")
                .group_by(sql.cube("product", "region"))
            )
            ```
        """
        column_exprs = [exp.column(col) if isinstance(col, str) else col for col in columns]
        return FunctionExpression(exp.Cube(expressions=column_exprs))

    @staticmethod
    def grouping_sets(*column_sets: tuple[str, ...] | list[str]) -> FunctionExpression:
        """Create a GROUPING SETS expression for GROUP BY clauses.

        Args:
            *column_sets: Sets of columns to group by.

        Returns:
            GROUPING SETS expression.

        Example:
            ```python
            query = (
                sql
                .select("product", "region", sql.sum("sales"))
                .from_("sales_data")
                .group_by(
                    sql.grouping_sets(("product",), ("region",), ())
                )
            )
            ```
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
    def any(values: list[Any] | exp.Expression | str) -> FunctionExpression:
        """Create an ANY expression for use with comparison operators.

        Args:
            values: Values, expression, or subquery for the ANY clause.

        Returns:
            ANY expression.

        Example:
            ```python
            subquery = sql.select("user_id").from_("active_users")
            query = (
                sql
                .select("*")
                .from_("users")
                .where(sql.id.eq(sql.any(subquery)))
            )
            ```
        """
        if isinstance(values, list):
            literals = [SQLFactory.to_literal(v) for v in values]
            return FunctionExpression(exp.Any(this=exp.Array(expressions=literals)))
        if isinstance(values, str):
            parsed: exp.Expression = exp.maybe_parse(values)
            return FunctionExpression(exp.Any(this=parsed))
        return FunctionExpression(exp.Any(this=values))

    @staticmethod
    def not_any_(values: list[Any] | exp.Expression | str) -> FunctionExpression:
        """Create a NOT ANY expression for use with comparison operators.

        Args:
            values: Values, expression, or subquery for the NOT ANY clause.

        Returns:
            NOT ANY expression.

        Example:
            ```python
            subquery = sql.select("user_id").from_("blocked_users")
            query = (
                sql
                .select("*")
                .from_("users")
                .where(sql.id.neq(sql.not_any(subquery)))
            )
            ```
        """
        return SQLFactory.any(values)

    @staticmethod
    def concat(*expressions: str | exp.Expression) -> StringExpression:
        """Create a CONCAT expression.

        Args:
            *expressions: Expressions to concatenate.

        Returns:
            CONCAT expression.
        """
        exprs = [exp.column(expr) if isinstance(expr, str) else expr for expr in expressions]
        return StringExpression(exp.Concat(expressions=exprs))

    @staticmethod
    def upper(column: str | exp.Expression) -> StringExpression:
        """Create an UPPER expression.

        Args:
            column: Column to convert to uppercase.

        Returns:
            UPPER expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        return StringExpression(exp.Upper(this=col_expr))

    @staticmethod
    def lower(column: str | exp.Expression) -> StringExpression:
        """Create a LOWER expression.

        Args:
            column: Column to convert to lowercase.

        Returns:
            LOWER expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        return StringExpression(exp.Lower(this=col_expr))

    @staticmethod
    def length(column: str | exp.Expression) -> StringExpression:
        """Create a LENGTH expression.

        Args:
            column: Column to get length of.

        Returns:
            LENGTH expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        return StringExpression(exp.Length(this=col_expr))

    @staticmethod
    def round(column: str | exp.Expression, decimals: int = 0) -> MathExpression:
        """Create a ROUND expression.

        Args:
            column: Column to round.
            decimals: Number of decimal places.

        Returns:
            ROUND expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        if decimals == 0:
            return MathExpression(exp.Round(this=col_expr))
        return MathExpression(exp.Round(this=col_expr, expression=exp.Literal.number(decimals)))

    @staticmethod
    def to_literal(value: Any) -> FunctionExpression:
        """Convert a Python value to a SQLGlot literal expression.

        Uses SQLGlot's built-in exp.convert() function for literal creation.
        Handles all Python primitive types:
        - None -> exp.Null (renders as NULL)
        - bool -> exp.Boolean (renders as TRUE/FALSE or 1/0 based on dialect)
        - int/float -> exp.Literal with is_number=True
        - str -> exp.Literal with is_string=True
        - exp.Expression -> returned as-is (passthrough)

        Args:
            value: Python value or SQLGlot expression to convert.

        Returns:
            SQLGlot expression representing the literal value.
        """
        if isinstance(value, exp.Expression):
            return FunctionExpression(value)
        return FunctionExpression(exp.convert(value))

    @staticmethod
    def decode(column: str | exp.Expression, *args: str | exp.Expression | Any) -> FunctionExpression:
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

        Example:
            ```python
            sql.decode(
                "status", "A", "Active", "I", "Inactive", "Unknown"
            )
            ```
        """
        col_expr = exp.column(column) if isinstance(column, str) else column

        if len(args) < MIN_DECODE_ARGS:
            msg = "DECODE requires at least one search/result pair"
            raise ValueError(msg)

        conditions = []
        default = None

        for i in range(0, len(args) - 1, 2):
            if i + 1 >= len(args):
                default = to_expression(args[i])
                break

            search_val = args[i]
            result_val = args[i + 1]

            search_expr = to_expression(search_val)
            result_expr = to_expression(result_val)

            condition = exp.EQ(this=col_expr, expression=search_expr)
            conditions.append(exp.If(this=condition, true=result_expr))

        return FunctionExpression(exp.Case(ifs=conditions, default=default))

    @staticmethod
    def cast(column: str | exp.Expression, data_type: str) -> ConversionExpression:
        """Create a CAST expression for type conversion.

        Args:
            column: Column or expression to cast.
            data_type: Target data type (e.g., 'INT', 'VARCHAR(100)', 'DECIMAL(10,2)').

        Returns:
            CAST expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        return ConversionExpression(exp.Cast(this=col_expr, to=exp.DataType.build(data_type)))

    @staticmethod
    def coalesce(*expressions: str | exp.Expression) -> ConversionExpression:
        """Create a COALESCE expression.

        Args:
            *expressions: Expressions to coalesce.

        Returns:
            COALESCE expression.
        """
        exprs = [exp.column(expr) if isinstance(expr, str) else expr for expr in expressions]
        return ConversionExpression(exp.Coalesce(expressions=exprs))

    @staticmethod
    def nvl(column: str | exp.Expression, substitute_value: str | exp.Expression | Any) -> ConversionExpression:
        """Create an NVL (Oracle-style) expression using COALESCE.

        Args:
            column: Column to check for NULL.
            substitute_value: Value to use if column is NULL.

        Returns:
            COALESCE expression equivalent to NVL.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        sub_expr = to_expression(substitute_value)
        return ConversionExpression(exp.Coalesce(expressions=[col_expr, sub_expr]))

    @staticmethod
    def nvl2(
        column: str | exp.Expression,
        value_if_not_null: str | exp.Expression | Any,
        value_if_null: str | exp.Expression | Any,
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

        Example:
            ```python
            sql.nvl2("salary", "Has Salary", "No Salary")
            ```
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
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

        Example:
            ```python
            from sqlspec import sql


            insert_expr = sql.bulk_insert("my_table", 3)


            insert_expr = sql.bulk_insert(
                "my_table", 3, placeholder_style="%s"
            )


            insert_expr = sql.bulk_insert(
                "my_table", 3, placeholder_style=":1"
            )
            ```
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

        Example:
            ```python
            from sqlspec import sql


            truncate_sql = sql.truncate_table("my_table").build().sql


            truncate_sql = (
                sql
                .truncate_table("my_table")
                .cascade()
                .restart_identity()
                .build()
                .sql
            )
            ```
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
        self,
        partition_by: str | list[str] | exp.Expression | None = None,
        order_by: str | list[str] | exp.Expression | None = None,
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
        self,
        partition_by: str | list[str] | exp.Expression | None = None,
        order_by: str | list[str] | exp.Expression | None = None,
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
        self,
        partition_by: str | list[str] | exp.Expression | None = None,
        order_by: str | list[str] | exp.Expression | None = None,
    ) -> FunctionExpression:
        """Create a DENSE_RANK() window function.

        Args:
            partition_by: Columns to partition by.
            order_by: Columns to order by.

        Returns:
            DENSE_RANK window function expression.
        """
        return self._create_window_function("DENSE_RANK", [], partition_by, order_by)

    @staticmethod
    def _create_window_function(
        func_name: str,
        func_args: list[exp.Expression],
        partition_by: str | list[str] | exp.Expression | None = None,
        order_by: str | list[str] | exp.Expression | None = None,
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

        if partition_by:
            if isinstance(partition_by, str):
                over_args["partition_by"] = [exp.column(partition_by)]
            elif isinstance(partition_by, list):
                over_args["partition_by"] = [exp.column(col) for col in partition_by]
            elif isinstance(partition_by, exp.Expression):
                over_args["partition_by"] = [partition_by]

        if order_by:
            if isinstance(order_by, str):
                over_args["order"] = exp.Order(expressions=[exp.column(order_by).asc()])
            elif isinstance(order_by, list):
                over_args["order"] = exp.Order(expressions=[exp.column(col).asc() for col in order_by])
            elif isinstance(order_by, exp.Expression):
                over_args["order"] = exp.Order(expressions=[order_by])

        return FunctionExpression(exp.Window(this=func_expr, **over_args))


sql = SQLFactory()
