# DDL builders for SQLSpec: DROP, CREATE INDEX, TRUNCATE, etc.

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional, Union

from sqlglot import exp
from sqlglot.dialects.dialect import DialectType
from typing_extensions import Self

from sqlspec.statement.builder.base import QueryBuilder, SafeQuery

if TYPE_CHECKING:
    from sqlspec.statement.sql import SQL, SQLConfig

__all__ = (
    "CreateIndexBuilder",
    "CreateSchemaBuilder",
    "CreateTableAsSelectBuilder",
    "DDLBuilder",
    "DropIndexBuilder",
    "DropSchemaBuilder",
    "DropTableBuilder",
    "DropViewBuilder",
    "TruncateTableBuilder",
)


@dataclass
class DDLBuilder(QueryBuilder[Any]):
    """Base class for DDL builders (CREATE, DROP, ALTER, etc)."""

    dialect: Optional[DialectType] = None
    _expression: Optional[exp.Expression] = field(default=None, init=False, repr=False, compare=False, hash=False)

    def __post_init__(self) -> None:
        # Override to prevent QueryBuilder from calling _create_base_expression prematurely
        pass

    def _create_base_expression(self) -> exp.Expression:
        msg = "Subclasses must implement _create_base_expression."
        raise NotImplementedError(msg)

    @property
    def _expected_result_type(self) -> "type[object]":
        # DDL typically returns no rows; use object for now.
        return object

    def build(self) -> "SafeQuery":
        if self._expression is None:
            self._expression = self._create_base_expression()
        return super().build()

    def to_statement(self, config: "Optional[SQLConfig]" = None) -> "SQL":
        return super().to_statement(config=config)


# --- DROP TABLE ---
@dataclass
class DropTableBuilder(DDLBuilder):
    """Builder for DROP TABLE [IF EXISTS] ... [CASCADE|RESTRICT]."""

    _table_name: Optional[str] = None
    _if_exists: bool = False
    _cascade: Optional[bool] = None  # True: CASCADE, False: RESTRICT, None: not set

    def table(self, name: str) -> Self:
        self._table_name = name
        return self

    def if_exists(self) -> Self:
        self._if_exists = True
        return self

    def cascade(self) -> Self:
        self._cascade = True
        return self

    def restrict(self) -> Self:
        self._cascade = False
        return self

    def _create_base_expression(self) -> exp.Expression:
        if not self._table_name:
            self._raise_sql_builder_error("Table name must be set for DROP TABLE.")
        return exp.Drop(
            kind="TABLE",
            this=exp.to_table(self._table_name),
            exists=self._if_exists,
            cascade=self._cascade,
        )


# --- DROP INDEX ---
@dataclass
class DropIndexBuilder(DDLBuilder):
    """Builder for DROP INDEX [IF EXISTS] ... [ON table] [CASCADE|RESTRICT]."""

    _index_name: Optional[str] = None
    _table_name: Optional[str] = None
    _if_exists: bool = False
    _cascade: Optional[bool] = None

    def name(self, index_name: str) -> Self:
        self._index_name = index_name
        return self

    def on_table(self, table_name: str) -> Self:
        self._table_name = table_name
        return self

    def if_exists(self) -> Self:
        self._if_exists = True
        return self

    def cascade(self) -> Self:
        self._cascade = True
        return self

    def restrict(self) -> Self:
        self._cascade = False
        return self

    def _create_base_expression(self) -> exp.Expression:
        if not self._index_name:
            self._raise_sql_builder_error("Index name must be set for DROP INDEX.")
        return exp.Drop(
            kind="INDEX",
            this=exp.to_identifier(self._index_name),
            table=exp.to_table(self._table_name) if self._table_name else None,
            exists=self._if_exists,
            cascade=self._cascade,
        )


# --- DROP VIEW ---
@dataclass
class DropViewBuilder(DDLBuilder):
    """Builder for DROP VIEW [IF EXISTS] ... [CASCADE|RESTRICT]."""

    _view_name: Optional[str] = None
    _if_exists: bool = False
    _cascade: Optional[bool] = None

    def name(self, view_name: str) -> Self:
        self._view_name = view_name
        return self

    def if_exists(self) -> Self:
        self._if_exists = True
        return self

    def cascade(self) -> Self:
        self._cascade = True
        return self

    def restrict(self) -> Self:
        self._cascade = False
        return self

    def _create_base_expression(self) -> exp.Expression:
        if not self._view_name:
            self._raise_sql_builder_error("View name must be set for DROP VIEW.")
        return exp.Drop(
            kind="VIEW",
            this=exp.to_identifier(self._view_name),
            exists=self._if_exists,
            cascade=self._cascade,
        )


# --- DROP SCHEMA ---
@dataclass
class DropSchemaBuilder(DDLBuilder):
    """Builder for DROP SCHEMA [IF EXISTS] ... [CASCADE|RESTRICT]."""

    _schema_name: Optional[str] = None
    _if_exists: bool = False
    _cascade: Optional[bool] = None

    def name(self, schema_name: str) -> Self:
        self._schema_name = schema_name
        return self

    def if_exists(self) -> Self:
        self._if_exists = True
        return self

    def cascade(self) -> Self:
        self._cascade = True
        return self

    def restrict(self) -> Self:
        self._cascade = False
        return self

    def _create_base_expression(self) -> exp.Expression:
        if not self._schema_name:
            self._raise_sql_builder_error("Schema name must be set for DROP SCHEMA.")
        return exp.Drop(
            kind="SCHEMA",
            this=exp.to_identifier(self._schema_name),
            exists=self._if_exists,
            cascade=self._cascade,
        )


# --- CREATE INDEX ---
@dataclass
class CreateIndexBuilder(DDLBuilder):
    """Builder for CREATE [UNIQUE] INDEX [IF NOT EXISTS] ... ON ... (...).

    Supports columns, expressions, ordering, using, and where.
    """

    _index_name: Optional[str] = None
    _table_name: Optional[str] = None
    _columns: list[Union[str, exp.Ordered, exp.Expression]] = field(default_factory=list)
    _unique: bool = False
    _if_not_exists: bool = False
    _using: Optional[str] = None
    _where: Optional[Union[str, exp.Expression]] = None

    def name(self, index_name: str) -> Self:
        self._index_name = index_name
        return self

    def on_table(self, table_name: str) -> Self:
        self._table_name = table_name
        return self

    def columns(self, *cols: Union[str, exp.Ordered, exp.Expression]) -> Self:
        self._columns.extend(cols)
        return self

    def expressions(self, *exprs: Union[str, exp.Expression]) -> Self:
        self._columns.extend(exprs)
        return self

    def unique(self) -> Self:
        self._unique = True
        return self

    def if_not_exists(self) -> Self:
        self._if_not_exists = True
        return self

    def using(self, method: str) -> Self:
        self._using = method
        return self

    def where(self, condition: Union[str, exp.Expression]) -> Self:
        self._where = condition
        return self

    def _create_base_expression(self) -> exp.Expression:
        if not self._index_name or not self._table_name:
            self._raise_sql_builder_error("Index name and table name must be set for CREATE INDEX.")
        exprs = []
        for col in self._columns:
            if isinstance(col, str):
                exprs.append(exp.column(col))
            else:
                exprs.append(col)
        where_expr = None
        if self._where:
            where_expr = exp.condition(self._where) if isinstance(self._where, str) else self._where
        # Use exp.Create for CREATE INDEX
        return exp.Create(
            kind="INDEX",
            this=exp.to_identifier(self._index_name),
            table=exp.to_table(self._table_name),
            expressions=exprs,
            unique=self._unique,
            exists=self._if_not_exists,
            using=exp.to_identifier(self._using) if self._using else None,
            where=where_expr,
        )


# --- TRUNCATE TABLE ---
@dataclass
class TruncateTableBuilder(DDLBuilder):
    """Builder for TRUNCATE TABLE ... [CASCADE|RESTRICT] [RESTART IDENTITY|CONTINUE IDENTITY]."""

    _table_name: Optional[str] = None
    _cascade: Optional[bool] = None
    _identity: Optional[str] = None  # "RESTART" or "CONTINUE"

    def table(self, name: str) -> Self:
        self._table_name = name
        return self

    def cascade(self) -> Self:
        self._cascade = True
        return self

    def restrict(self) -> Self:
        self._cascade = False
        return self

    def restart_identity(self) -> Self:
        self._identity = "RESTART"
        return self

    def continue_identity(self) -> Self:
        self._identity = "CONTINUE"
        return self

    def _create_base_expression(self) -> exp.Expression:
        if not self._table_name:
            self._raise_sql_builder_error("Table name must be set for TRUNCATE TABLE.")
        identity_expr = exp.Var(this=self._identity) if self._identity else None
        return exp.TruncateTable(
            this=exp.to_table(self._table_name),
            cascade=self._cascade,
            identity=identity_expr,
        )


# --- CREATE SCHEMA ---
@dataclass
class CreateSchemaBuilder(DDLBuilder):
    """Builder for CREATE SCHEMA [IF NOT EXISTS] schema_name [AUTHORIZATION user_name]."""

    _schema_name: Optional[str] = None
    _if_not_exists: bool = False
    _authorization: Optional[str] = None

    def name(self, schema_name: str) -> Self:
        self._schema_name = schema_name
        return self

    def if_not_exists(self) -> Self:
        self._if_not_exists = True
        return self

    def authorization(self, user_name: str) -> Self:
        self._authorization = user_name
        return self

    def _create_base_expression(self) -> exp.Expression:
        if not self._schema_name:
            self._raise_sql_builder_error("Schema name must be set for CREATE SCHEMA.")
        props = []
        if self._authorization:
            props.append(
                exp.Property(this=exp.to_identifier("AUTHORIZATION"), value=exp.to_identifier(self._authorization))
            )
        properties_node = exp.Properties(expressions=props) if props else None
        return exp.Create(
            kind="SCHEMA",
            this=exp.to_identifier(self._schema_name),
            exists=self._if_not_exists,
            properties=properties_node,
        )


@dataclass
class CreateTableAsSelectBuilder(DDLBuilder):
    """Builder for CREATE TABLE [IF NOT EXISTS] ... AS SELECT ... (CTAS).

    Supports optional column list and parameterized SELECT sources.

    Example:
        builder = (
            CreateTableAsSelectBuilder()
            .name("my_table")
            .if_not_exists()
            .columns("id", "name")
            .as_select(select_builder)
        )
        sql = builder.build().sql

    Methods:
        - name(table_name: str): Set the table name.
        - if_not_exists(): Add IF NOT EXISTS.
        - columns(*cols: str): Set explicit column list (optional).
        - as_select(select_query): Set the SELECT source (SQL, SelectBuilder, or str).
    """

    _table_name: Optional[str] = None
    _if_not_exists: bool = False
    _columns: list[str] = field(default_factory=list)
    _select_query: Optional[object] = None  # SQL, SelectBuilder, or str

    def name(self, table_name: str) -> Self:
        self._table_name = table_name
        return self

    def if_not_exists(self) -> Self:
        self._if_not_exists = True
        return self

    def columns(self, *cols: str) -> Self:
        self._columns = list(cols)
        return self

    def as_select(self, select_query: object) -> Self:
        self._select_query = select_query
        return self

    def _create_base_expression(self) -> exp.Expression:
        if not self._table_name:
            self._raise_sql_builder_error("Table name must be set for CREATE TABLE AS SELECT.")
        if self._select_query is None:
            self._raise_sql_builder_error("SELECT query must be set for CREATE TABLE AS SELECT.")

        # Determine the SELECT expression and parameters
        select_expr = None
        select_params = None
        from sqlspec.statement.builder.select import SelectBuilder
        from sqlspec.statement.sql import SQL

        if isinstance(self._select_query, SQL):
            select_expr = self._select_query.expression
            select_params = getattr(self._select_query, "parameters", None)
        elif isinstance(self._select_query, SelectBuilder):
            select_expr = getattr(self._select_query, "_expression", None)
            select_params = getattr(self._select_query, "_parameters", None)
        elif isinstance(self._select_query, str):
            select_expr = exp.maybe_parse(self._select_query)
            select_params = None
        else:
            self._raise_sql_builder_error("Unsupported type for SELECT query in CTAS.")
        if select_expr is None or not isinstance(select_expr, exp.Select):
            self._raise_sql_builder_error("SELECT query must be a valid SELECT expression.")

        # Merge parameters from SELECT if present
        if select_params:
            for p_name, p_value in select_params.items():
                self.add_parameter(p_value, name=p_name)

        # Build schema/column list if provided
        schema_expr = None
        if self._columns:
            schema_expr = exp.Schema(expressions=[exp.column(c) for c in self._columns])

        return exp.Create(
            kind="TABLE",
            this=exp.to_table(self._table_name),
            exists=self._if_not_exists,
            expression=select_expr,
            schema=schema_expr,
        )
