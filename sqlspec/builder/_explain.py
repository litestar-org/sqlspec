"""EXPLAIN statement builder.

Provides a fluent interface for building EXPLAIN statements with
dialect-aware SQL generation.
"""

from typing import TYPE_CHECKING, Any, ClassVar

from mypy_extensions import trait
from typing_extensions import Self

from sqlspec.core import SQL, StatementConfig
from sqlspec.core.explain import ExplainFormat, ExplainOptions
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import has_expression_and_sql, has_parameter_builder, is_expression

if TYPE_CHECKING:
    from sqlglot import exp
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.protocols import SQLBuilderProtocol


__all__ = (
    "Explain",
    "ExplainMixin",
    "build_bigquery_explain",
    "build_duckdb_explain",
    "build_explain_sql",
    "build_generic_explain",
    "build_mysql_explain",
    "build_oracle_explain",
    "build_postgres_explain",
    "build_sqlite_explain",
    "normalize_dialect_name",
)


POSTGRES_DIALECTS = frozenset({"postgres", "postgresql", "redshift"})
MYSQL_DIALECTS = frozenset({"mysql", "mariadb"})
SQLITE_DIALECTS = frozenset({"sqlite"})
DUCKDB_DIALECTS = frozenset({"duckdb"})
ORACLE_DIALECTS = frozenset({"oracle"})
BIGQUERY_DIALECTS = frozenset({"bigquery"})
SPANNER_DIALECTS = frozenset({"spanner"})


def normalize_dialect_name(dialect: "DialectType | None") -> str | None:
    """Normalize dialect to lowercase string.

    Args:
        dialect: Dialect type, string, or None

    Returns:
        Lowercase string representation of dialect or None
    """
    if dialect is None:
        return None
    if isinstance(dialect, str):
        return dialect.lower()
    return dialect.__class__.__name__.lower()


def build_postgres_explain(statement_sql: str, options: "ExplainOptions") -> str:
    """Build PostgreSQL EXPLAIN statement.

    PostgreSQL uses the syntax: EXPLAIN (OPTIONS) statement

    Args:
        statement_sql: The SQL statement to explain
        options: ExplainOptions configuration

    Returns:
        Complete EXPLAIN SQL string
    """
    option_parts: list[str] = []

    if options.analyze:
        option_parts.append("ANALYZE")
    if options.verbose:
        option_parts.append("VERBOSE")
    if options.costs is not None:
        option_parts.append(f"COSTS {'TRUE' if options.costs else 'FALSE'}")
    if options.buffers is not None:
        option_parts.append(f"BUFFERS {'TRUE' if options.buffers else 'FALSE'}")
    if options.timing is not None:
        option_parts.append(f"TIMING {'TRUE' if options.timing else 'FALSE'}")
    if options.summary is not None:
        option_parts.append(f"SUMMARY {'TRUE' if options.summary else 'FALSE'}")
    if options.memory is not None:
        option_parts.append(f"MEMORY {'TRUE' if options.memory else 'FALSE'}")
    if options.settings is not None:
        option_parts.append(f"SETTINGS {'TRUE' if options.settings else 'FALSE'}")
    if options.wal is not None:
        option_parts.append(f"WAL {'TRUE' if options.wal else 'FALSE'}")
    if options.generic_plan is not None:
        option_parts.append(f"GENERIC_PLAN {'TRUE' if options.generic_plan else 'FALSE'}")
    if options.format is not None:
        option_parts.append(f"FORMAT {options.format.value.upper()}")

    if option_parts:
        options_str = ", ".join(option_parts)
        return f"EXPLAIN ({options_str}) {statement_sql}"
    return f"EXPLAIN {statement_sql}"


def build_mysql_explain(statement_sql: str, options: "ExplainOptions") -> str:
    """Build MySQL EXPLAIN statement.

    MySQL uses:
    - EXPLAIN [FORMAT = TRADITIONAL|JSON|TREE] statement
    - EXPLAIN ANALYZE statement (always TREE format)

    Args:
        statement_sql: The SQL statement to explain
        options: ExplainOptions configuration

    Returns:
        Complete EXPLAIN SQL string
    """
    if options.analyze:
        return f"EXPLAIN ANALYZE {statement_sql}"

    if options.format is not None:
        format_map = {
            ExplainFormat.JSON: "JSON",
            ExplainFormat.TREE: "TREE",
            ExplainFormat.TRADITIONAL: "TRADITIONAL",
            ExplainFormat.TEXT: "TRADITIONAL",
        }
        fmt = format_map.get(options.format, "TRADITIONAL")
        return f"EXPLAIN FORMAT = {fmt} {statement_sql}"

    return f"EXPLAIN {statement_sql}"


def build_sqlite_explain(statement_sql: str, options: "ExplainOptions") -> str:
    """Build SQLite EXPLAIN statement.

    SQLite only supports EXPLAIN QUERY PLAN (no additional options).
    Raw EXPLAIN returns virtual machine opcodes which is rarely useful.

    Args:
        statement_sql: The SQL statement to explain
        options: ExplainOptions configuration (mostly ignored for SQLite)

    Returns:
        Complete EXPLAIN SQL string
    """
    return f"EXPLAIN QUERY PLAN {statement_sql}"


def build_duckdb_explain(statement_sql: str, options: "ExplainOptions") -> str:
    """Build DuckDB EXPLAIN statement.

    DuckDB supports:
    - EXPLAIN statement
    - EXPLAIN ANALYZE statement
    - EXPLAIN (FORMAT JSON) statement

    Args:
        statement_sql: The SQL statement to explain
        options: ExplainOptions configuration

    Returns:
        Complete EXPLAIN SQL string
    """
    if options.analyze:
        return f"EXPLAIN ANALYZE {statement_sql}"

    if options.format is not None and options.format == ExplainFormat.JSON:
        return f"EXPLAIN (FORMAT JSON) {statement_sql}"

    return f"EXPLAIN {statement_sql}"


def build_oracle_explain(statement_sql: str, options: "ExplainOptions") -> str:
    """Build Oracle EXPLAIN statement.

    Oracle requires a two-step process:
    1. EXPLAIN PLAN FOR statement
    2. SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY())

    This function returns only the first step. The driver must handle
    executing both statements.

    Args:
        statement_sql: The SQL statement to explain
        options: ExplainOptions configuration (mostly ignored for Oracle)

    Returns:
        EXPLAIN PLAN FOR SQL string
    """
    return f"EXPLAIN PLAN FOR {statement_sql}"


def build_bigquery_explain(statement_sql: str, options: "ExplainOptions") -> str:
    """Build BigQuery EXPLAIN statement.

    BigQuery supports:
    - EXPLAIN statement
    - EXPLAIN ANALYZE statement (incurs query execution costs!)

    Args:
        statement_sql: The SQL statement to explain
        options: ExplainOptions configuration

    Returns:
        Complete EXPLAIN SQL string
    """
    if options.analyze:
        return f"EXPLAIN ANALYZE {statement_sql}"
    return f"EXPLAIN {statement_sql}"


def build_generic_explain(statement_sql: str, options: "ExplainOptions") -> str:
    """Build generic EXPLAIN statement for unknown dialects.

    Args:
        statement_sql: The SQL statement to explain
        options: ExplainOptions configuration

    Returns:
        Complete EXPLAIN SQL string
    """
    if options.analyze:
        return f"EXPLAIN ANALYZE {statement_sql}"
    return f"EXPLAIN {statement_sql}"


def build_explain_sql(statement_sql: str, options: "ExplainOptions", dialect: "DialectType | None" = None) -> str:
    """Build dialect-specific EXPLAIN SQL.

    Args:
        statement_sql: The SQL statement to explain
        options: ExplainOptions configuration
        dialect: Target SQL dialect

    Returns:
        Complete EXPLAIN SQL string for the target dialect
    """
    dialect_name = normalize_dialect_name(dialect)

    if dialect_name in POSTGRES_DIALECTS:
        return build_postgres_explain(statement_sql, options)
    if dialect_name in MYSQL_DIALECTS:
        return build_mysql_explain(statement_sql, options)
    if dialect_name in SQLITE_DIALECTS:
        return build_sqlite_explain(statement_sql, options)
    if dialect_name in DUCKDB_DIALECTS:
        return build_duckdb_explain(statement_sql, options)
    if dialect_name in ORACLE_DIALECTS:
        return build_oracle_explain(statement_sql, options)
    if dialect_name in BIGQUERY_DIALECTS:
        return build_bigquery_explain(statement_sql, options)

    return build_generic_explain(statement_sql, options)


class Explain:
    """Builder for EXPLAIN statements with dialect-aware rendering.

    Provides a fluent API for constructing EXPLAIN statements with
    various options that are translated to dialect-specific syntax.

    Examples:
        Basic usage:
            explain = Explain("SELECT * FROM users").build()

        With options:
            explain = (
                Explain("SELECT * FROM users", dialect="postgres")
                .analyze()
                .format("json")
                .buffers()
                .build()
            )

        From QueryBuilder:
            explain = (
                Explain(select_builder, dialect="postgres")
                .analyze()
                .verbose()
                .build()
            )
    """

    __slots__: ClassVar[tuple[str, ...]] = ("_dialect", "_options", "_parameters", "_statement", "_statement_sql")

    def __init__(
        self,
        statement: "str | exp.Expression | SQL | SQLBuilderProtocol",
        dialect: "DialectType | None" = None,
        options: "ExplainOptions | None" = None,
    ) -> None:
        """Initialize ExplainBuilder.

        Args:
            statement: SQL statement to explain (string, expression, SQL object, or builder)
            dialect: Target SQL dialect
            options: Initial ExplainOptions (or None for defaults)
        """
        self._dialect = dialect
        self._options = options if options is not None else ExplainOptions()
        self._statement = statement
        self._parameters: dict[str, Any] = {}

        self._statement_sql = self._resolve_statement_sql(statement)

    def _resolve_statement_sql(self, statement: "str | exp.Expression | SQL | SQLBuilderProtocol") -> str:
        """Resolve statement to SQL string.

        Args:
            statement: The statement to resolve

        Returns:
            SQL string representation of the statement
        """
        if isinstance(statement, str):
            return statement

        if isinstance(statement, SQL):
            self._parameters.update(statement.named_parameters)
            return statement.raw_sql

        if is_expression(statement):
            dialect_str = normalize_dialect_name(self._dialect)
            return statement.sql(dialect=dialect_str)

        if has_parameter_builder(statement):
            safe_query = statement.build(dialect=self._dialect)
            if safe_query.parameters:
                self._parameters.update(safe_query.parameters)
            return str(safe_query.sql)

        if has_expression_and_sql(statement):
            return statement.sql

        msg = f"Cannot resolve statement to SQL: {type(statement).__name__}"
        raise SQLBuilderError(msg)

    def analyze(self, enabled: bool = True) -> Self:
        """Enable ANALYZE option (execute statement for real statistics).

        Args:
            enabled: Whether to enable ANALYZE

        Returns:
            Self for method chaining
        """
        self._options = self._options.copy(analyze=enabled)
        return self

    def verbose(self, enabled: bool = True) -> Self:
        """Enable VERBOSE option (show additional information).

        Args:
            enabled: Whether to enable VERBOSE

        Returns:
            Self for method chaining
        """
        self._options = self._options.copy(verbose=enabled)
        return self

    def format(self, fmt: "ExplainFormat | str") -> Self:
        """Set output format.

        Args:
            fmt: Output format (TEXT, JSON, XML, YAML, TREE, TRADITIONAL)

        Returns:
            Self for method chaining
        """
        if isinstance(fmt, str):
            fmt = ExplainFormat(fmt.lower())
        self._options = self._options.copy(format=fmt)
        return self

    def costs(self, enabled: bool = True) -> Self:
        """Enable COSTS option (show estimated costs).

        Args:
            enabled: Whether to show costs

        Returns:
            Self for method chaining
        """
        self._options = self._options.copy(costs=enabled)
        return self

    def buffers(self, enabled: bool = True) -> Self:
        """Enable BUFFERS option (show buffer usage).

        Args:
            enabled: Whether to show buffer usage

        Returns:
            Self for method chaining
        """
        self._options = self._options.copy(buffers=enabled)
        return self

    def timing(self, enabled: bool = True) -> Self:
        """Enable TIMING option (show actual timing).

        Args:
            enabled: Whether to show timing

        Returns:
            Self for method chaining
        """
        self._options = self._options.copy(timing=enabled)
        return self

    def summary(self, enabled: bool = True) -> Self:
        """Enable SUMMARY option (show summary information).

        Args:
            enabled: Whether to show summary

        Returns:
            Self for method chaining
        """
        self._options = self._options.copy(summary=enabled)
        return self

    def memory(self, enabled: bool = True) -> Self:
        """Enable MEMORY option (show memory usage, PostgreSQL 17+).

        Args:
            enabled: Whether to show memory usage

        Returns:
            Self for method chaining
        """
        self._options = self._options.copy(memory=enabled)
        return self

    def settings(self, enabled: bool = True) -> Self:
        """Enable SETTINGS option (show configuration parameters, PostgreSQL 12+).

        Args:
            enabled: Whether to show settings

        Returns:
            Self for method chaining
        """
        self._options = self._options.copy(settings=enabled)
        return self

    def wal(self, enabled: bool = True) -> Self:
        """Enable WAL option (show WAL usage, PostgreSQL 13+).

        Args:
            enabled: Whether to show WAL usage

        Returns:
            Self for method chaining
        """
        self._options = self._options.copy(wal=enabled)
        return self

    def generic_plan(self, enabled: bool = True) -> Self:
        """Enable GENERIC_PLAN option (ignore parameter values, PostgreSQL 16+).

        Args:
            enabled: Whether to use generic plan

        Returns:
            Self for method chaining
        """
        self._options = self._options.copy(generic_plan=enabled)
        return self

    def with_options(self, options: "ExplainOptions") -> Self:
        """Replace all options with the provided ExplainOptions.

        Args:
            options: New options to use

        Returns:
            Self for method chaining
        """
        self._options = options
        return self

    @property
    def options(self) -> "ExplainOptions":
        """Get current ExplainOptions."""
        return self._options

    @property
    def dialect(self) -> "DialectType | None":
        """Get current dialect."""
        return self._dialect

    @property
    def parameters(self) -> dict[str, Any]:
        """Get parameters from the underlying statement."""
        return self._parameters.copy()

    def build(self, dialect: "DialectType | None" = None) -> "SQL":
        """Build the EXPLAIN statement as a SQL object.

        Args:
            dialect: Optional dialect override

        Returns:
            SQL object containing the EXPLAIN statement
        """
        target_dialect = dialect or self._dialect
        explain_sql = build_explain_sql(self._statement_sql, self._options, target_dialect)
        statement_config = StatementConfig(dialect=target_dialect) if target_dialect is not None else None

        if self._parameters:
            if statement_config is None:
                return SQL(explain_sql, self._parameters)
            return SQL(explain_sql, self._parameters, statement_config=statement_config)
        if statement_config is None:
            return SQL(explain_sql)
        return SQL(explain_sql, statement_config=statement_config)

    def to_sql(self, dialect: "DialectType | None" = None) -> str:
        """Build and return just the SQL string.

        Args:
            dialect: Optional dialect override

        Returns:
            EXPLAIN SQL string
        """
        target_dialect = dialect or self._dialect
        return build_explain_sql(self._statement_sql, self._options, target_dialect)

    def __repr__(self) -> str:
        """String representation."""
        return f"Explain({self._statement_sql!r}, dialect={self._dialect!r}, options={self._options!r})"


@trait
class ExplainMixin:
    """Mixin to add .explain() method to QueryBuilder subclasses.

    This mixin can be added to any QueryBuilder subclass to provide
    EXPLAIN plan functionality.

    Examples:
        class Select(QueryBuilder, ExplainMixin):
            pass

        query = Select().select("*").from_("users")
        explain = query.explain().analyze().format("json").build()
    """

    __slots__ = ()

    dialect: "DialectType | None"

    def explain(
        self, analyze: bool = False, verbose: bool = False, format: "ExplainFormat | str | None" = None
    ) -> "Explain":
        """Create an EXPLAIN builder for this query.

        Args:
            analyze: Execute the statement for real statistics
            verbose: Show additional information
            format: Output format (TEXT, JSON, XML, YAML, TREE)

        Returns:
            Explain builder for further configuration
        """
        fmt = None
        if format is not None:
            fmt = ExplainFormat(format.lower()) if isinstance(format, str) else format

        options = ExplainOptions(analyze=analyze, verbose=verbose, format=fmt)

        return Explain(self, dialect=self.dialect, options=options)  # type: ignore[arg-type]
