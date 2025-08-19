"""SQL factory for creating SQL builders and column expressions.

Provides statement builders (select, insert, update, etc.) and column expressions.
"""

import logging
from typing import TYPE_CHECKING, Any, Optional, Union

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import ParseError as SQLGlotParseError

from sqlspec.builder import (
    AlterTable,
    Column,
    CommentOn,
    CreateIndex,
    CreateMaterializedView,
    CreateSchema,
    CreateTable,
    CreateTableAsSelect,
    CreateView,
    Delete,
    DropIndex,
    DropSchema,
    DropTable,
    DropView,
    Insert,
    Merge,
    RenameTable,
    Select,
    Truncate,
    Update,
)
from sqlspec.builder._expression_wrappers import (
    AggregateExpression,
    ConversionExpression,
    FunctionExpression,
    MathExpression,
    StringExpression,
)
from sqlspec.builder.mixins._join_operations import JoinBuilder
from sqlspec.builder.mixins._select_operations import Case, SubqueryBuilder, WindowFunctionBuilder
from sqlspec.core.statement import SQL
from sqlspec.exceptions import SQLBuilderError

if TYPE_CHECKING:
    from sqlspec.builder._expression_wrappers import ExpressionWrapper


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
    "Insert",
    "Merge",
    "RenameTable",
    "SQLFactory",
    "Select",
    "Truncate",
    "Update",
    "WindowFunctionBuilder",
    "sql",
)

logger = logging.getLogger("sqlspec")

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


class SQLFactory:
    """Factory for creating SQL builders and column expressions."""

    @classmethod
    def detect_sql_type(cls, sql: str, dialect: DialectType = None) -> str:
        try:
            parsed_expr = sqlglot.parse_one(sql, read=dialect)
            if parsed_expr and parsed_expr.key:
                return parsed_expr.key.upper()
            if parsed_expr:
                command_type = type(parsed_expr).__name__.upper()
                if command_type == "COMMAND" and parsed_expr.this:
                    return str(parsed_expr.this).upper()
                return command_type
        except SQLGlotParseError:
            logger.debug("Failed to parse SQL for type detection: %s", sql[:100])
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning("Unexpected error during SQL type detection for '%s...': %s", sql[:50], e)
        return "UNKNOWN"

    def __init__(self, dialect: DialectType = None) -> None:
        """Initialize the SQL factory.

        Args:
            dialect: Default SQL dialect to use for all builders.
        """
        self.dialect = dialect

    def __call__(self, statement: str, dialect: DialectType = None) -> "Any":
        """Create a SelectBuilder from a SQL string, only allowing SELECT/CTE queries.

        Args:
            statement: The SQL statement string.
            parameters: Optional parameters for the query.
            *filters: Optional filters.
            config: Optional config.
            dialect: Optional SQL dialect.
            **kwargs: Additional parameters.

        Returns:
            SelectBuilder instance.

        Raises:
            SQLBuilderError: If the SQL is not a SELECT/CTE statement.
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
            builder._expression = parsed_expr
            return builder
        msg = (
            f"sql(...) only supports SELECT statements. Detected type: {actual_type_str}. "
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
                detected = self.detect_sql_type(sql_candidate, dialect=builder_dialect)
                if detected not in {"SELECT", "WITH"}:
                    msg = (
                        f"sql.select() expects a SELECT or WITH statement, got {detected}. "
                        f"Use sql.{detected.lower()}() if a dedicated builder exists, or ensure the SQL is SELECT/WITH."
                    )
                    raise SQLBuilderError(msg)
                select_builder = Select(dialect=builder_dialect)
                return self._populate_select_from_sql(select_builder, sql_candidate)
        select_builder = Select(dialect=builder_dialect)
        if columns_or_sql:
            select_builder.select(*columns_or_sql)
        return select_builder

    def insert(self, table_or_sql: Optional[str] = None, dialect: DialectType = None) -> "Insert":
        builder_dialect = dialect or self.dialect
        builder = Insert(dialect=builder_dialect)
        if table_or_sql:
            if self._looks_like_sql(table_or_sql):
                detected = self.detect_sql_type(table_or_sql, dialect=builder_dialect)
                if detected not in {"INSERT", "SELECT"}:
                    msg = (
                        f"sql.insert() expects INSERT or SELECT (for insert-from-select), got {detected}. "
                        f"Use sql.{detected.lower()}() if a dedicated builder exists, "
                        f"or ensure the SQL is INSERT/SELECT."
                    )
                    raise SQLBuilderError(msg)
                return self._populate_insert_from_sql(builder, table_or_sql)
            return builder.into(table_or_sql)
        return builder

    def update(self, table_or_sql: Optional[str] = None, dialect: DialectType = None) -> "Update":
        builder_dialect = dialect or self.dialect
        builder = Update(dialect=builder_dialect)
        if table_or_sql:
            if self._looks_like_sql(table_or_sql):
                detected = self.detect_sql_type(table_or_sql, dialect=builder_dialect)
                if detected != "UPDATE":
                    msg = (
                        f"sql.update() expects UPDATE statement, got {detected}. "
                        f"Use sql.{detected.lower()}() if a dedicated builder exists."
                    )
                    raise SQLBuilderError(msg)
                return self._populate_update_from_sql(builder, table_or_sql)
            return builder.table(table_or_sql)
        return builder

    def delete(self, table_or_sql: Optional[str] = None, dialect: DialectType = None) -> "Delete":
        builder_dialect = dialect or self.dialect
        builder = Delete(dialect=builder_dialect)
        if table_or_sql and self._looks_like_sql(table_or_sql):
            detected = self.detect_sql_type(table_or_sql, dialect=builder_dialect)
            if detected != "DELETE":
                msg = (
                    f"sql.delete() expects DELETE statement, got {detected}. "
                    f"Use sql.{detected.lower()}() if a dedicated builder exists."
                )
                raise SQLBuilderError(msg)
            return self._populate_delete_from_sql(builder, table_or_sql)
        return builder

    def merge(self, table_or_sql: Optional[str] = None, dialect: DialectType = None) -> "Merge":
        builder_dialect = dialect or self.dialect
        builder = Merge(dialect=builder_dialect)
        if table_or_sql:
            if self._looks_like_sql(table_or_sql):
                detected = self.detect_sql_type(table_or_sql, dialect=builder_dialect)
                if detected != "MERGE":
                    msg = (
                        f"sql.merge() expects MERGE statement, got {detected}. "
                        f"Use sql.{detected.lower()}() if a dedicated builder exists."
                    )
                    raise SQLBuilderError(msg)
                return self._populate_merge_from_sql(builder, table_or_sql)
            return builder.into(table_or_sql)
        return builder

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

    @staticmethod
    def _looks_like_sql(candidate: str, expected_type: Optional[str] = None) -> bool:
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

    def _populate_insert_from_sql(self, builder: "Insert", sql_string: str) -> "Insert":
        """Parse SQL string and populate INSERT builder using SQLGlot directly."""
        try:
            parsed_expr: exp.Expression = exp.maybe_parse(sql_string, dialect=self.dialect)

            if isinstance(parsed_expr, exp.Insert):
                builder._expression = parsed_expr
                return builder

            if isinstance(parsed_expr, exp.Select):
                logger.info("Detected SELECT statement for INSERT - may need target table specification")
                return builder

            logger.warning("Cannot create INSERT from %s statement", type(parsed_expr).__name__)

        except Exception as e:
            logger.warning("Failed to parse INSERT SQL, falling back to traditional mode: %s", e)
        return builder

    def _populate_select_from_sql(self, builder: "Select", sql_string: str) -> "Select":
        """Parse SQL string and populate SELECT builder using SQLGlot directly."""
        try:
            parsed_expr: exp.Expression = exp.maybe_parse(sql_string, dialect=self.dialect)

            if isinstance(parsed_expr, exp.Select):
                builder._expression = parsed_expr
                return builder

            logger.warning("Cannot create SELECT from %s statement", type(parsed_expr).__name__)

        except Exception as e:
            logger.warning("Failed to parse SELECT SQL, falling back to traditional mode: %s", e)
        return builder

    def _populate_update_from_sql(self, builder: "Update", sql_string: str) -> "Update":
        """Parse SQL string and populate UPDATE builder using SQLGlot directly."""
        try:
            parsed_expr: exp.Expression = exp.maybe_parse(sql_string, dialect=self.dialect)

            if isinstance(parsed_expr, exp.Update):
                builder._expression = parsed_expr
                return builder

            logger.warning("Cannot create UPDATE from %s statement", type(parsed_expr).__name__)

        except Exception as e:
            logger.warning("Failed to parse UPDATE SQL, falling back to traditional mode: %s", e)
        return builder

    def _populate_delete_from_sql(self, builder: "Delete", sql_string: str) -> "Delete":
        """Parse SQL string and populate DELETE builder using SQLGlot directly."""
        try:
            parsed_expr: exp.Expression = exp.maybe_parse(sql_string, dialect=self.dialect)

            if isinstance(parsed_expr, exp.Delete):
                builder._expression = parsed_expr
                return builder

            logger.warning("Cannot create DELETE from %s statement", type(parsed_expr).__name__)

        except Exception as e:
            logger.warning("Failed to parse DELETE SQL, falling back to traditional mode: %s", e)
        return builder

    def _populate_merge_from_sql(self, builder: "Merge", sql_string: str) -> "Merge":
        """Parse SQL string and populate MERGE builder using SQLGlot directly."""
        try:
            parsed_expr: exp.Expression = exp.maybe_parse(sql_string, dialect=self.dialect)

            if isinstance(parsed_expr, exp.Merge):
                builder._expression = parsed_expr
                return builder

            logger.warning("Cannot create MERGE from %s statement", type(parsed_expr).__name__)

        except Exception as e:
            logger.warning("Failed to parse MERGE SQL, falling back to traditional mode: %s", e)
        return builder

    def column(self, name: str, table: Optional[str] = None) -> Column:
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
                sql.case_.when("x = 1", "one")
                .when("x = 2", "two")
                .else_("other")
                .end()
            )
            aliased_case = (
                sql.case_.when("status = 'active'", 1)
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
        return JoinBuilder("inner join")

    @property
    def left_join_(self) -> "JoinBuilder":
        """Create a LEFT JOIN builder."""
        return JoinBuilder("left join")

    @property
    def right_join_(self) -> "JoinBuilder":
        """Create a RIGHT JOIN builder."""
        return JoinBuilder("right join")

    @property
    def full_join_(self) -> "JoinBuilder":
        """Create a FULL OUTER JOIN builder."""
        return JoinBuilder("full join")

    @property
    def cross_join_(self) -> "JoinBuilder":
        """Create a CROSS JOIN builder."""
        return JoinBuilder("cross join")

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
    def raw(sql_fragment: str, **parameters: Any) -> "Union[exp.Expression, SQL]":
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
                return parsed
                if sql_fragment.strip().replace("_", "").replace(".", "").isalnum():
                    return exp.to_identifier(sql_fragment)
                return exp.Literal.string(sql_fragment)
            except Exception as e:
                msg = f"Failed to parse raw SQL fragment '{sql_fragment}': {e}"
                raise SQLBuilderError(msg) from e

        return SQL(sql_fragment, parameters)

    @staticmethod
    def count(
        column: Union[str, exp.Expression, "ExpressionWrapper", "Case", "Column"] = "*", distinct: bool = False
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
            col_expr = SQLFactory._extract_expression(column)
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
        col_expr = SQLFactory._extract_expression(column)
        return AggregateExpression(exp.Sum(this=col_expr, distinct=distinct))

    @staticmethod
    def avg(column: Union[str, exp.Expression, "ExpressionWrapper", "Case"]) -> AggregateExpression:
        """Create an AVG expression.

        Args:
            column: Column to average.

        Returns:
            AVG expression.
        """
        col_expr = SQLFactory._extract_expression(column)
        return AggregateExpression(exp.Avg(this=col_expr))

    @staticmethod
    def max(column: Union[str, exp.Expression, "ExpressionWrapper", "Case"]) -> AggregateExpression:
        """Create a MAX expression.

        Args:
            column: Column to find maximum.

        Returns:
            MAX expression.
        """
        col_expr = SQLFactory._extract_expression(column)
        return AggregateExpression(exp.Max(this=col_expr))

    @staticmethod
    def min(column: Union[str, exp.Expression, "ExpressionWrapper", "Case"]) -> AggregateExpression:
        """Create a MIN expression.

        Args:
            column: Column to find minimum.

        Returns:
            MIN expression.
        """
        col_expr = SQLFactory._extract_expression(column)
        return AggregateExpression(exp.Min(this=col_expr))

    @staticmethod
    def rollup(*columns: Union[str, exp.Expression]) -> FunctionExpression:
        """Create a ROLLUP expression for GROUP BY clauses.

        Args:
            *columns: Columns to include in the rollup.

        Returns:
            ROLLUP expression.

        Example:
            ```python
            query = (
                sql.select("product", "region", sql.sum("sales"))
                .from_("sales_data")
                .group_by(sql.rollup("product", "region"))
            )
            ```
        """
        column_exprs = [exp.column(col) if isinstance(col, str) else col for col in columns]
        return FunctionExpression(exp.Rollup(expressions=column_exprs))

    @staticmethod
    def cube(*columns: Union[str, exp.Expression]) -> FunctionExpression:
        """Create a CUBE expression for GROUP BY clauses.

        Args:
            *columns: Columns to include in the cube.

        Returns:
            CUBE expression.

        Example:
            ```python
            query = (
                sql.select("product", "region", sql.sum("sales"))
                .from_("sales_data")
                .group_by(sql.cube("product", "region"))
            )
            ```
        """
        column_exprs = [exp.column(col) if isinstance(col, str) else col for col in columns]
        return FunctionExpression(exp.Cube(expressions=column_exprs))

    @staticmethod
    def grouping_sets(*column_sets: Union[tuple[str, ...], list[str]]) -> FunctionExpression:
        """Create a GROUPING SETS expression for GROUP BY clauses.

        Args:
            *column_sets: Sets of columns to group by.

        Returns:
            GROUPING SETS expression.

        Example:
            ```python
            query = (
                sql.select("product", "region", sql.sum("sales"))
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
    def any(values: Union[list[Any], exp.Expression, str]) -> FunctionExpression:
        """Create an ANY expression for use with comparison operators.

        Args:
            values: Values, expression, or subquery for the ANY clause.

        Returns:
            ANY expression.

        Example:
            ```python
            subquery = sql.select("user_id").from_("active_users")
            query = (
                sql.select("*")
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
    def not_any_(values: Union[list[Any], exp.Expression, str]) -> FunctionExpression:
        """Create a NOT ANY expression for use with comparison operators.

        Args:
            values: Values, expression, or subquery for the NOT ANY clause.

        Returns:
            NOT ANY expression.

        Example:
            ```python
            subquery = sql.select("user_id").from_("blocked_users")
            query = (
                sql.select("*")
                .from_("users")
                .where(sql.id.neq(sql.not_any(subquery)))
            )
            ```
        """
        return SQLFactory.any(values)

    @staticmethod
    def concat(*expressions: Union[str, exp.Expression]) -> StringExpression:
        """Create a CONCAT expression.

        Args:
            *expressions: Expressions to concatenate.

        Returns:
            CONCAT expression.
        """
        exprs = [exp.column(expr) if isinstance(expr, str) else expr for expr in expressions]
        return StringExpression(exp.Concat(expressions=exprs))

    @staticmethod
    def upper(column: Union[str, exp.Expression]) -> StringExpression:
        """Create an UPPER expression.

        Args:
            column: Column to convert to uppercase.

        Returns:
            UPPER expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        return StringExpression(exp.Upper(this=col_expr))

    @staticmethod
    def lower(column: Union[str, exp.Expression]) -> StringExpression:
        """Create a LOWER expression.

        Args:
            column: Column to convert to lowercase.

        Returns:
            LOWER expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        return StringExpression(exp.Lower(this=col_expr))

    @staticmethod
    def length(column: Union[str, exp.Expression]) -> StringExpression:
        """Create a LENGTH expression.

        Args:
            column: Column to get length of.

        Returns:
            LENGTH expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        return StringExpression(exp.Length(this=col_expr))

    @staticmethod
    def round(column: Union[str, exp.Expression], decimals: int = 0) -> MathExpression:
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
    def _to_expression(value: Any) -> exp.Expression:
        """Convert a Python value to a raw SQLGlot expression.

        Args:
            value: Python value or SQLGlot expression to convert.

        Returns:
            Raw SQLGlot expression.
        """
        if isinstance(value, exp.Expression):
            return value
        return exp.convert(value)

    @staticmethod
    def _extract_expression(value: Any) -> exp.Expression:
        """Extract SQLGlot expression from value, handling our wrapper types.

        Args:
            value: String, SQLGlot expression, or our wrapper type.

        Returns:
            Raw SQLGlot expression.
        """
        from sqlspec.builder._expression_wrappers import ExpressionWrapper
        from sqlspec.builder.mixins._select_operations import Case

        if isinstance(value, str):
            return exp.column(value)
        if isinstance(value, Column):
            return value._expression
        if isinstance(value, ExpressionWrapper):
            return value.expression
        if isinstance(value, Case):
            return exp.Case(ifs=value._conditions, default=value._default)
        if isinstance(value, exp.Expression):
            return value
        return exp.convert(value)

    @staticmethod
    def decode(column: Union[str, exp.Expression], *args: Union[str, exp.Expression, Any]) -> FunctionExpression:
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
                default = SQLFactory._to_expression(args[i])
                break

            search_val = args[i]
            result_val = args[i + 1]

            search_expr = SQLFactory._to_expression(search_val)
            result_expr = SQLFactory._to_expression(result_val)

            condition = exp.EQ(this=col_expr, expression=search_expr)
            conditions.append(exp.If(this=condition, true=result_expr))

        return FunctionExpression(exp.Case(ifs=conditions, default=default))

    @staticmethod
    def cast(column: Union[str, exp.Expression], data_type: str) -> ConversionExpression:
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
    def coalesce(*expressions: Union[str, exp.Expression]) -> ConversionExpression:
        """Create a COALESCE expression.

        Args:
            *expressions: Expressions to coalesce.

        Returns:
            COALESCE expression.
        """
        exprs = [exp.column(expr) if isinstance(expr, str) else expr for expr in expressions]
        return ConversionExpression(exp.Coalesce(expressions=exprs))

    @staticmethod
    def nvl(
        column: Union[str, exp.Expression], substitute_value: Union[str, exp.Expression, Any]
    ) -> ConversionExpression:
        """Create an NVL (Oracle-style) expression using COALESCE.

        Args:
            column: Column to check for NULL.
            substitute_value: Value to use if column is NULL.

        Returns:
            COALESCE expression equivalent to NVL.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        sub_expr = SQLFactory._to_expression(substitute_value)
        return ConversionExpression(exp.Coalesce(expressions=[col_expr, sub_expr]))

    @staticmethod
    def nvl2(
        column: Union[str, exp.Expression],
        value_if_not_null: Union[str, exp.Expression, Any],
        value_if_null: Union[str, exp.Expression, Any],
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
        not_null_expr = SQLFactory._to_expression(value_if_not_null)
        null_expr = SQLFactory._to_expression(value_if_null)

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
                sql.truncate_table("my_table")
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
        partition_by: Optional[Union[str, list[str], exp.Expression]] = None,
        order_by: Optional[Union[str, list[str], exp.Expression]] = None,
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
        partition_by: Optional[Union[str, list[str], exp.Expression]] = None,
        order_by: Optional[Union[str, list[str], exp.Expression]] = None,
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
        partition_by: Optional[Union[str, list[str], exp.Expression]] = None,
        order_by: Optional[Union[str, list[str], exp.Expression]] = None,
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
        partition_by: Optional[Union[str, list[str], exp.Expression]] = None,
        order_by: Optional[Union[str, list[str], exp.Expression]] = None,
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
