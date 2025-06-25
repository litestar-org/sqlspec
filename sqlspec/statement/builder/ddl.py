# DDL builders for SQLSpec: DROP, CREATE INDEX, TRUNCATE, etc.

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional, Union

from sqlglot import exp
from sqlglot.dialects.dialect import DialectType
from typing_extensions import Self

from sqlspec.statement.builder._ddl_utils import build_column_expression, build_constraint_expression
from sqlspec.statement.builder.base import QueryBuilder, SafeQuery
from sqlspec.statement.result import SQLResult

if TYPE_CHECKING:
    from sqlspec.statement.sql import SQL, SQLConfig

__all__ = (
    "AlterOperation",
    "AlterTableBuilder",
    "ColumnDefinition",
    "CommentOnBuilder",
    "ConstraintDefinition",
    "CreateIndexBuilder",
    "CreateSchemaBuilder",
    "CreateTableAsSelectBuilder",
    "CreateTableBuilder",
    "DDLBuilder",
    "DropIndexBuilder",
    "DropSchemaBuilder",
    "DropTableBuilder",
    "DropViewBuilder",
    "RenameTableBuilder",
    "TruncateTableBuilder",
)


@dataclass
class DDLBuilder(QueryBuilder[Any]):
    """Base class for DDL builders (CREATE, DROP, ALTER, etc)."""

    dialect: DialectType = None
    _expression: Optional[exp.Expression] = field(default=None, init=False, repr=False, compare=False, hash=False)

    def __post_init__(self) -> None:
        # Override to prevent QueryBuilder from calling _create_base_expression prematurely
        pass

    def _create_base_expression(self) -> exp.Expression:
        msg = "Subclasses must implement _create_base_expression."
        raise NotImplementedError(msg)

    @property
    def _expected_result_type(self) -> "type[SQLResult[Any]]":
        # DDL typically returns no rows; use object for now.
        return SQLResult

    def build(self) -> "SafeQuery":
        if self._expression is None:
            self._expression = self._create_base_expression()
        return super().build()

    def to_statement(self, config: "Optional[SQLConfig]" = None) -> "SQL":
        return super().to_statement(config=config)


# --- Data Structures for CREATE TABLE ---
@dataclass
class ColumnDefinition:
    """Column definition for CREATE TABLE."""

    name: str
    dtype: str
    default: "Optional[Any]" = None
    not_null: bool = False
    primary_key: bool = False
    unique: bool = False
    auto_increment: bool = False
    comment: "Optional[str]" = None
    check: "Optional[str]" = None
    generated: "Optional[str]" = None  # For computed columns
    collate: "Optional[str]" = None


@dataclass
class ConstraintDefinition:
    """Constraint definition for CREATE TABLE."""

    constraint_type: str  # 'PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK'
    name: "Optional[str]" = None
    columns: "list[str]" = field(default_factory=list)
    references_table: "Optional[str]" = None
    references_columns: "list[str]" = field(default_factory=list)
    condition: "Optional[str]" = None
    on_delete: "Optional[str]" = None
    on_update: "Optional[str]" = None
    deferrable: bool = False
    initially_deferred: bool = False


# --- CREATE TABLE ---
@dataclass
class CreateTableBuilder(DDLBuilder):
    """Builder for CREATE TABLE statements with columns and constraints.

    Example:
        builder = (
            CreateTableBuilder("users")
            .column("id", "SERIAL", primary_key=True)
            .column("email", "VARCHAR(255)", not_null=True, unique=True)
            .column("created_at", "TIMESTAMP", default="CURRENT_TIMESTAMP")
            .foreign_key_constraint("org_id", "organizations", "id")
        )
        sql = builder.build().sql
    """

    _table_name: str = field(default="", init=False)
    _if_not_exists: bool = False
    _temporary: bool = False
    _columns: "list[ColumnDefinition]" = field(default_factory=list)
    _constraints: "list[ConstraintDefinition]" = field(default_factory=list)
    _table_options: "dict[str, Any]" = field(default_factory=dict)
    _schema: "Optional[str]" = None
    _tablespace: "Optional[str]" = None
    _like_table: "Optional[str]" = None
    _partition_by: "Optional[str]" = None

    def __init__(self, table_name: str) -> None:
        super().__init__()
        self._table_name = table_name

    def in_schema(self, schema_name: str) -> "Self":
        """Set the schema for the table."""
        self._schema = schema_name
        return self

    def if_not_exists(self) -> "Self":
        """Add IF NOT EXISTS clause."""
        self._if_not_exists = True
        return self

    def temporary(self) -> "Self":
        """Create a temporary table."""
        self._temporary = True
        return self

    def like(self, source_table: str) -> "Self":
        """Create table LIKE another table."""
        self._like_table = source_table
        return self

    def tablespace(self, name: str) -> "Self":
        """Set tablespace for the table."""
        self._tablespace = name
        return self

    def partition_by(self, partition_spec: str) -> "Self":
        """Set partitioning specification."""
        self._partition_by = partition_spec
        return self

    def column(
        self,
        name: str,
        dtype: str,
        default: "Optional[Any]" = None,
        not_null: bool = False,
        primary_key: bool = False,
        unique: bool = False,
        auto_increment: bool = False,
        comment: "Optional[str]" = None,
        check: "Optional[str]" = None,
        generated: "Optional[str]" = None,
        collate: "Optional[str]" = None,
    ) -> "Self":
        """Add a column definition to the table."""
        if not name:
            self._raise_sql_builder_error("Column name must be a non-empty string")

        if not dtype:
            self._raise_sql_builder_error("Column type must be a non-empty string")

        # Check for duplicate column names
        if any(col.name == name for col in self._columns):
            self._raise_sql_builder_error(f"Column '{name}' already defined")

        # Create column definition
        column_def = ColumnDefinition(
            name=name,
            dtype=dtype,
            default=default,
            not_null=not_null,
            primary_key=primary_key,
            unique=unique,
            auto_increment=auto_increment,
            comment=comment,
            check=check,
            generated=generated,
            collate=collate,
        )

        self._columns.append(column_def)

        # If primary key is specified on column, also add a constraint
        if primary_key and not any(c.constraint_type == "PRIMARY KEY" for c in self._constraints):
            self.primary_key_constraint([name])

        return self

    def primary_key_constraint(self, columns: "Union[str, list[str]]", name: "Optional[str]" = None) -> "Self":
        """Add a primary key constraint."""
        # Normalize column list
        col_list = [columns] if isinstance(columns, str) else list(columns)

        # Validation
        if not col_list:
            self._raise_sql_builder_error("Primary key must include at least one column")

        # Check if primary key already exists
        existing_pk = next((c for c in self._constraints if c.constraint_type == "PRIMARY KEY"), None)
        if existing_pk:
            # Update existing primary key to include new columns
            for col in col_list:
                if col not in existing_pk.columns:
                    existing_pk.columns.append(col)
        else:
            # Create new primary key constraint
            constraint = ConstraintDefinition(constraint_type="PRIMARY KEY", name=name, columns=col_list)
            self._constraints.append(constraint)

        return self

    def foreign_key_constraint(
        self,
        columns: "Union[str, list[str]]",
        references_table: str,
        references_columns: "Union[str, list[str]]",
        name: "Optional[str]" = None,
        on_delete: "Optional[str]" = None,
        on_update: "Optional[str]" = None,
        deferrable: bool = False,
        initially_deferred: bool = False,
    ) -> "Self":
        """Add a foreign key constraint."""
        # Normalize inputs
        col_list = [columns] if isinstance(columns, str) else list(columns)

        ref_col_list = [references_columns] if isinstance(references_columns, str) else list(references_columns)

        # Validation
        if len(col_list) != len(ref_col_list):
            self._raise_sql_builder_error("Foreign key columns and referenced columns must have same length")

        # Validate actions
        valid_actions = {"CASCADE", "SET NULL", "SET DEFAULT", "RESTRICT", "NO ACTION", None}
        if on_delete and on_delete.upper() not in valid_actions:
            self._raise_sql_builder_error(f"Invalid ON DELETE action: {on_delete}")
        if on_update and on_update.upper() not in valid_actions:
            self._raise_sql_builder_error(f"Invalid ON UPDATE action: {on_update}")

        constraint = ConstraintDefinition(
            constraint_type="FOREIGN KEY",
            name=name,
            columns=col_list,
            references_table=references_table,
            references_columns=ref_col_list,
            on_delete=on_delete.upper() if on_delete else None,
            on_update=on_update.upper() if on_update else None,
            deferrable=deferrable,
            initially_deferred=initially_deferred,
        )

        self._constraints.append(constraint)
        return self

    def unique_constraint(self, columns: "Union[str, list[str]]", name: "Optional[str]" = None) -> "Self":
        """Add a unique constraint."""
        # Normalize column list
        col_list = [columns] if isinstance(columns, str) else list(columns)

        if not col_list:
            self._raise_sql_builder_error("Unique constraint must include at least one column")

        constraint = ConstraintDefinition(constraint_type="UNIQUE", name=name, columns=col_list)

        self._constraints.append(constraint)
        return self

    def check_constraint(self, condition: str, name: "Optional[str]" = None) -> "Self":
        """Add a check constraint."""
        if not condition:
            self._raise_sql_builder_error("Check constraint must have a condition")

        constraint = ConstraintDefinition(constraint_type="CHECK", name=name, condition=condition)

        self._constraints.append(constraint)
        return self

    def engine(self, engine_name: str) -> "Self":
        """Set storage engine (MySQL/MariaDB)."""
        self._table_options["engine"] = engine_name
        return self

    def charset(self, charset_name: str) -> "Self":
        """Set character set."""
        self._table_options["charset"] = charset_name
        return self

    def collate(self, collation: str) -> "Self":
        """Set table collation."""
        self._table_options["collate"] = collation
        return self

    def comment(self, comment_text: str) -> "Self":
        """Set table comment."""
        self._table_options["comment"] = comment_text
        return self

    def with_option(self, key: str, value: "Any") -> "Self":
        """Add custom table option."""
        self._table_options[key] = value
        return self

    def _create_base_expression(self) -> "exp.Expression":
        """Create the SQLGlot expression for CREATE TABLE."""
        if not self._columns and not self._like_table:
            self._raise_sql_builder_error("Table must have at least one column or use LIKE clause")

        # Build table identifier with schema if provided
        if self._schema:
            table = exp.Table(this=exp.to_identifier(self._table_name), db=exp.to_identifier(self._schema))
        else:
            table = exp.to_table(self._table_name)

        # Build column expressions
        column_defs: list[exp.Expression] = []
        for col in self._columns:
            col_expr = build_column_expression(col)
            column_defs.append(col_expr)

        # Build constraint expressions
        for constraint in self._constraints:
            # Skip PRIMARY KEY constraints that are already defined on columns
            if constraint.constraint_type == "PRIMARY KEY" and len(constraint.columns) == 1:
                col_name = constraint.columns[0]
                if any(c.name == col_name and c.primary_key for c in self._columns):
                    continue

            constraint_expr = build_constraint_expression(constraint)
            if constraint_expr:
                column_defs.append(constraint_expr)

        # Build properties for table options
        props: list[exp.Property] = []
        if self._table_options.get("engine"):
            props.append(
                exp.Property(
                    this=exp.to_identifier("ENGINE"), value=exp.to_identifier(self._table_options.get("engine"))
                )
            )
        if self._tablespace:
            props.append(exp.Property(this=exp.to_identifier("TABLESPACE"), value=exp.to_identifier(self._tablespace)))
        if self._partition_by:
            props.append(
                exp.Property(this=exp.to_identifier("PARTITION BY"), value=exp.Literal.string(self._partition_by))
            )

        # Add other table options
        for key, value in self._table_options.items():
            if key != "engine":  # Skip already handled options
                if isinstance(value, str):
                    props.append(exp.Property(this=exp.to_identifier(key.upper()), value=exp.Literal.string(value)))
                else:
                    props.append(exp.Property(this=exp.to_identifier(key.upper()), value=exp.Literal.number(value)))

        properties_node = exp.Properties(expressions=props) if props else None

        # Build schema expression
        schema_expr = exp.Schema(expressions=column_defs) if column_defs else None

        # Handle LIKE clause
        like_expr = None
        if self._like_table:
            like_expr = exp.to_table(self._like_table)

        # Create the CREATE expression
        return exp.Create(
            kind="TABLE",
            this=table,
            exists=self._if_not_exists,
            temporary=self._temporary,
            expression=schema_expr,
            properties=properties_node,
            like=like_expr,
        )

    @staticmethod
    def _build_column_expression(col: "ColumnDefinition") -> "exp.Expression":
        """Build SQLGlot expression for a column definition."""
        return build_column_expression(col)

    @staticmethod
    def _build_constraint_expression(constraint: "ConstraintDefinition") -> "Optional[exp.Expression]":
        """Build SQLGlot expression for a table constraint."""
        return build_constraint_expression(constraint)


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
            kind="TABLE", this=exp.to_table(self._table_name), exists=self._if_exists, cascade=self._cascade
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
            kind="VIEW", this=exp.to_identifier(self._view_name), exists=self._if_exists, cascade=self._cascade
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
            kind="SCHEMA", this=exp.to_identifier(self._schema_name), exists=self._if_exists, cascade=self._cascade
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
        exprs: list[exp.Expression] = []
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
        return exp.TruncateTable(this=exp.to_table(self._table_name), cascade=self._cascade, identity=identity_expr)


# --- ALTER TABLE ---
@dataclass
class AlterOperation:
    """Represents a single ALTER TABLE operation."""

    operation_type: str
    column_name: "Optional[str]" = None
    column_definition: "Optional[ColumnDefinition]" = None
    constraint_name: "Optional[str]" = None
    constraint_definition: "Optional[ConstraintDefinition]" = None
    new_type: "Optional[str]" = None
    new_name: "Optional[str]" = None
    after_column: "Optional[str]" = None
    first: bool = False
    using_expression: "Optional[str]" = None


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
        props: list[exp.Property] = []
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
                # Always preserve the original parameter name
                # The SELECT query already has unique parameter names
                self._parameters[p_name] = p_value

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


@dataclass
class CreateMaterializedViewBuilder(DDLBuilder):
    """Builder for CREATE MATERIALIZED VIEW [IF NOT EXISTS] ... AS SELECT ...

    Supports optional column list, parameterized SELECT sources, and dialect-specific options.
    """

    _view_name: Optional[str] = None
    _if_not_exists: bool = False
    _columns: list[str] = field(default_factory=list)
    _select_query: Optional[object] = None  # SQL, SelectBuilder, or str
    _with_data: Optional[bool] = None  # True: WITH DATA, False: NO DATA, None: not set
    _refresh_mode: Optional[str] = None
    _storage_parameters: dict[str, Any] = field(default_factory=dict)
    _tablespace: Optional[str] = None
    _using_index: Optional[str] = None
    _hints: list[str] = field(default_factory=list)

    def name(self, view_name: str) -> Self:
        self._view_name = view_name
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

    def with_data(self) -> Self:
        self._with_data = True
        return self

    def no_data(self) -> Self:
        self._with_data = False
        return self

    def refresh_mode(self, mode: str) -> Self:
        self._refresh_mode = mode
        return self

    def storage_parameter(self, key: str, value: Any) -> Self:
        self._storage_parameters[key] = value
        return self

    def tablespace(self, name: str) -> Self:
        self._tablespace = name
        return self

    def using_index(self, index_name: str) -> Self:
        self._using_index = index_name
        return self

    def with_hint(self, hint: str) -> Self:
        self._hints.append(hint)
        return self

    def _create_base_expression(self) -> exp.Expression:
        if not self._view_name:
            self._raise_sql_builder_error("View name must be set for CREATE MATERIALIZED VIEW.")
        if self._select_query is None:
            self._raise_sql_builder_error("SELECT query must be set for CREATE MATERIALIZED VIEW.")

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
            self._raise_sql_builder_error("Unsupported type for SELECT query in materialized view.")
        if select_expr is None or not isinstance(select_expr, exp.Select):
            self._raise_sql_builder_error("SELECT query must be a valid SELECT expression.")

        # Merge parameters from SELECT if present
        if select_params:
            for p_name, p_value in select_params.items():
                # Always preserve the original parameter name
                # The SELECT query already has unique parameter names
                self._parameters[p_name] = p_value

        # Build schema/column list if provided
        schema_expr = None
        if self._columns:
            schema_expr = exp.Schema(expressions=[exp.column(c) for c in self._columns])

        # Build properties for dialect-specific options
        props: list[exp.Property] = []
        if self._refresh_mode:
            props.append(
                exp.Property(this=exp.to_identifier("REFRESH_MODE"), value=exp.Literal.string(self._refresh_mode))
            )
        if self._tablespace:
            props.append(exp.Property(this=exp.to_identifier("TABLESPACE"), value=exp.to_identifier(self._tablespace)))
        if self._using_index:
            props.append(
                exp.Property(this=exp.to_identifier("USING_INDEX"), value=exp.to_identifier(self._using_index))
            )
        for k, v in self._storage_parameters.items():
            props.append(exp.Property(this=exp.to_identifier(k), value=exp.Literal.string(str(v))))
        if self._with_data is not None:
            props.append(exp.Property(this=exp.to_identifier("WITH_DATA" if self._with_data else "NO_DATA")))
        props.extend(
            exp.Property(this=exp.to_identifier("HINT"), value=exp.Literal.string(hint)) for hint in self._hints
        )
        properties_node = exp.Properties(expressions=props) if props else None

        return exp.Create(
            kind="MATERIALIZED_VIEW",
            this=exp.to_identifier(self._view_name),
            exists=self._if_not_exists,
            expression=select_expr,
            schema=schema_expr,
            properties=properties_node,
        )


@dataclass
class CreateViewBuilder(DDLBuilder):
    """Builder for CREATE VIEW [IF NOT EXISTS] ... AS SELECT ...

    Supports optional column list, parameterized SELECT sources, and hints.
    """

    _view_name: Optional[str] = None
    _if_not_exists: bool = False
    _columns: list[str] = field(default_factory=list)
    _select_query: Optional[object] = None  # SQL, SelectBuilder, or str
    _hints: list[str] = field(default_factory=list)

    def name(self, view_name: str) -> Self:
        self._view_name = view_name
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

    def with_hint(self, hint: str) -> Self:
        self._hints.append(hint)
        return self

    def _create_base_expression(self) -> exp.Expression:
        if not self._view_name:
            self._raise_sql_builder_error("View name must be set for CREATE VIEW.")
        if self._select_query is None:
            self._raise_sql_builder_error("SELECT query must be set for CREATE VIEW.")

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
            self._raise_sql_builder_error("Unsupported type for SELECT query in view.")
        if select_expr is None or not isinstance(select_expr, exp.Select):
            self._raise_sql_builder_error("SELECT query must be a valid SELECT expression.")

        # Merge parameters from SELECT if present
        if select_params:
            for p_name, p_value in select_params.items():
                # Always preserve the original parameter name
                # The SELECT query already has unique parameter names
                self._parameters[p_name] = p_value

        # Build schema/column list if provided
        schema_expr = None
        if self._columns:
            schema_expr = exp.Schema(expressions=[exp.column(c) for c in self._columns])

        # Build properties for hints
        props: list[exp.Property] = [
            exp.Property(this=exp.to_identifier("HINT"), value=exp.Literal.string(h)) for h in self._hints
        ]
        properties_node = exp.Properties(expressions=props) if props else None

        return exp.Create(
            kind="VIEW",
            this=exp.to_identifier(self._view_name),
            exists=self._if_not_exists,
            expression=select_expr,
            schema=schema_expr,
            properties=properties_node,
        )


@dataclass
class AlterTableBuilder(DDLBuilder):
    """Builder for ALTER TABLE with granular operations.

    Supports column operations (add, drop, alter type, rename) and constraint operations.

    Example:
        builder = (
            AlterTableBuilder("users")
            .add_column("email", "VARCHAR(255)", not_null=True)
            .drop_column("old_field")
            .add_constraint("check_age", "CHECK (age >= 18)")
        )
    """

    _table_name: str = field(default="", init=False)
    _operations: "list[AlterOperation]" = field(default_factory=list)
    _schema: "Optional[str]" = None
    _if_exists: bool = False

    def __init__(self, table_name: str) -> None:
        super().__init__()
        self._table_name = table_name
        self._operations = []
        self._schema = None
        self._if_exists = False

    def if_exists(self) -> "Self":
        """Add IF EXISTS clause."""
        self._if_exists = True
        return self

    def add_column(
        self,
        name: str,
        dtype: str,
        default: "Optional[Any]" = None,
        not_null: bool = False,
        unique: bool = False,
        comment: "Optional[str]" = None,
        after: "Optional[str]" = None,
        first: bool = False,
    ) -> "Self":
        """Add a new column to the table."""
        if not name:
            self._raise_sql_builder_error("Column name must be a non-empty string")

        if not dtype:
            self._raise_sql_builder_error("Column type must be a non-empty string")

        column_def = ColumnDefinition(
            name=name, dtype=dtype, default=default, not_null=not_null, unique=unique, comment=comment
        )

        operation = AlterOperation(
            operation_type="ADD COLUMN", column_definition=column_def, after_column=after, first=first
        )

        self._operations.append(operation)
        return self

    def drop_column(self, name: str, cascade: bool = False) -> "Self":
        """Drop a column from the table."""
        if not name:
            self._raise_sql_builder_error("Column name must be a non-empty string")

        operation = AlterOperation(operation_type="DROP COLUMN CASCADE" if cascade else "DROP COLUMN", column_name=name)

        self._operations.append(operation)
        return self

    def alter_column_type(self, name: str, new_type: str, using: "Optional[str]" = None) -> "Self":
        """Change the type of an existing column."""
        if not name:
            self._raise_sql_builder_error("Column name must be a non-empty string")

        if not new_type:
            self._raise_sql_builder_error("New type must be a non-empty string")

        operation = AlterOperation(
            operation_type="ALTER COLUMN TYPE", column_name=name, new_type=new_type, using_expression=using
        )

        self._operations.append(operation)
        return self

    def rename_column(self, old_name: str, new_name: str) -> "Self":
        """Rename a column."""
        if not old_name:
            self._raise_sql_builder_error("Old column name must be a non-empty string")

        if not new_name:
            self._raise_sql_builder_error("New column name must be a non-empty string")

        operation = AlterOperation(operation_type="RENAME COLUMN", column_name=old_name, new_name=new_name)

        self._operations.append(operation)
        return self

    def add_constraint(
        self,
        constraint_type: str,
        columns: "Optional[Union[str, list[str]]]" = None,
        name: "Optional[str]" = None,
        references_table: "Optional[str]" = None,
        references_columns: "Optional[Union[str, list[str]]]" = None,
        condition: "Optional[str]" = None,
        on_delete: "Optional[str]" = None,
        on_update: "Optional[str]" = None,
    ) -> "Self":
        """Add a constraint to the table.

        Args:
            constraint_type: Type of constraint ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK')
            columns: Column(s) for the constraint (not needed for CHECK)
            name: Optional constraint name
            references_table: Table referenced by foreign key
            references_columns: Columns referenced by foreign key
            condition: CHECK constraint condition
            on_delete: Foreign key ON DELETE action
            on_update: Foreign key ON UPDATE action
        """
        valid_types = {"PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK"}
        if constraint_type.upper() not in valid_types:
            self._raise_sql_builder_error(f"Invalid constraint type: {constraint_type}")

        # Normalize columns
        col_list = None
        if columns is not None:
            col_list = [columns] if isinstance(columns, str) else list(columns)

        # Normalize reference columns
        ref_col_list = None
        if references_columns is not None:
            ref_col_list = [references_columns] if isinstance(references_columns, str) else list(references_columns)

        constraint_def = ConstraintDefinition(
            constraint_type=constraint_type.upper(),
            name=name,
            columns=col_list or [],
            references_table=references_table,
            references_columns=ref_col_list or [],
            condition=condition,
            on_delete=on_delete,
            on_update=on_update,
        )

        operation = AlterOperation(operation_type="ADD CONSTRAINT", constraint_definition=constraint_def)

        self._operations.append(operation)
        return self

    def drop_constraint(self, name: str, cascade: bool = False) -> "Self":
        """Drop a constraint from the table."""
        if not name:
            self._raise_sql_builder_error("Constraint name must be a non-empty string")

        operation = AlterOperation(
            operation_type="DROP CONSTRAINT CASCADE" if cascade else "DROP CONSTRAINT", constraint_name=name
        )

        self._operations.append(operation)
        return self

    def set_not_null(self, column: str) -> "Self":
        """Set a column to NOT NULL."""
        operation = AlterOperation(operation_type="ALTER COLUMN SET NOT NULL", column_name=column)

        self._operations.append(operation)
        return self

    def drop_not_null(self, column: str) -> "Self":
        """Remove NOT NULL constraint from a column."""
        operation = AlterOperation(operation_type="ALTER COLUMN DROP NOT NULL", column_name=column)

        self._operations.append(operation)
        return self

    def set_default(self, column: str, default: "Any") -> "Self":
        """Set default value for a column."""
        operation = AlterOperation(
            operation_type="ALTER COLUMN SET DEFAULT",
            column_name=column,
            column_definition=ColumnDefinition(name=column, dtype="", default=default),
        )

        self._operations.append(operation)
        return self

    def drop_default(self, column: str) -> "Self":
        """Remove default value from a column."""
        operation = AlterOperation(operation_type="ALTER COLUMN DROP DEFAULT", column_name=column)

        self._operations.append(operation)
        return self

    def _create_base_expression(self) -> "exp.Expression":
        """Create the SQLGlot expression for ALTER TABLE."""
        if not self._operations:
            self._raise_sql_builder_error("At least one operation must be specified for ALTER TABLE")

        if self._schema:
            table = exp.Table(this=exp.to_identifier(self._table_name), db=exp.to_identifier(self._schema))
        else:
            table = exp.to_table(self._table_name)

        actions: list[exp.Expression] = [self._build_operation_expression(op) for op in self._operations]

        return exp.Alter(this=table, kind="TABLE", actions=actions, exists=self._if_exists)

    def _build_operation_expression(self, op: "AlterOperation") -> exp.Expression:
        """Build a structured SQLGlot expression for a single alter operation."""
        op_type = op.operation_type.upper()

        if op_type == "ADD COLUMN":
            if not op.column_definition:
                self._raise_sql_builder_error("Column definition required for ADD COLUMN")
            # SQLGlot expects a ColumnDef directly for ADD COLUMN actions
            # Note: SQLGlot doesn't support AFTER/FIRST positioning in standard ALTER TABLE ADD COLUMN
            # These would need to be handled at the dialect level
            return build_column_expression(op.column_definition)

        if op_type == "DROP COLUMN":
            return exp.Drop(this=exp.to_identifier(op.column_name), kind="COLUMN", exists=True)

        if op_type == "DROP COLUMN CASCADE":
            return exp.Drop(this=exp.to_identifier(op.column_name), kind="COLUMN", cascade=True, exists=True)

        if op_type == "ALTER COLUMN TYPE":
            if not op.new_type:
                self._raise_sql_builder_error("New type required for ALTER COLUMN TYPE")
            return exp.AlterColumn(
                this=exp.to_identifier(op.column_name),
                dtype=exp.DataType.build(op.new_type),
                using=exp.maybe_parse(op.using_expression) if op.using_expression else None,
            )

        if op_type == "RENAME COLUMN":
            return exp.RenameColumn(this=exp.to_identifier(op.column_name), to=exp.to_identifier(op.new_name))

        if op_type == "ADD CONSTRAINT":
            if not op.constraint_definition:
                self._raise_sql_builder_error("Constraint definition required for ADD CONSTRAINT")
            constraint_expr = build_constraint_expression(op.constraint_definition)
            return exp.AddConstraint(this=constraint_expr)

        if op_type == "DROP CONSTRAINT":
            return exp.Drop(this=exp.to_identifier(op.constraint_name), kind="CONSTRAINT", exists=True)

        if op_type == "DROP CONSTRAINT CASCADE":
            return exp.Drop(this=exp.to_identifier(op.constraint_name), kind="CONSTRAINT", cascade=True, exists=True)

        if op_type == "ALTER COLUMN SET NOT NULL":
            return exp.AlterColumn(this=exp.to_identifier(op.column_name), allow_null=False)

        if op_type == "ALTER COLUMN DROP NOT NULL":
            return exp.AlterColumn(this=exp.to_identifier(op.column_name), drop=True, allow_null=True)

        if op_type == "ALTER COLUMN SET DEFAULT":
            if not op.column_definition or op.column_definition.default is None:
                self._raise_sql_builder_error("Default value required for SET DEFAULT")
            default_val = op.column_definition.default
            # Handle different default value types
            default_expr: Optional[exp.Expression]
            if isinstance(default_val, str):
                # Check if it's a function/expression or a literal string
                if default_val.upper() in {"CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"} or "(" in default_val:
                    default_expr = exp.maybe_parse(default_val)
                else:
                    default_expr = exp.Literal.string(default_val)
            elif isinstance(default_val, (int, float)):
                default_expr = exp.Literal.number(default_val)
            elif default_val is True:
                default_expr = exp.true()
            elif default_val is False:
                default_expr = exp.false()
            else:
                default_expr = exp.Literal.string(str(default_val))
            return exp.AlterColumn(this=exp.to_identifier(op.column_name), default=default_expr)

        if op_type == "ALTER COLUMN DROP DEFAULT":
            return exp.AlterColumn(this=exp.to_identifier(op.column_name), kind="DROP DEFAULT")

        self._raise_sql_builder_error(f"Unknown operation type: {op.operation_type}")
        raise AssertionError  # This line is unreachable but satisfies the linter


@dataclass
class CommentOnBuilder(DDLBuilder):
    """Builder for COMMENT ON ... IS ... statements.

    Supports COMMENT ON TABLE and COMMENT ON COLUMN.
    """

    _target_type: Optional[str] = None  # 'TABLE' or 'COLUMN'
    _table: Optional[str] = None
    _column: Optional[str] = None
    _comment: Optional[str] = None

    def on_table(self, table: str) -> Self:
        self._target_type = "TABLE"
        self._table = table
        self._column = None
        return self

    def on_column(self, table: str, column: str) -> Self:
        self._target_type = "COLUMN"
        self._table = table
        self._column = column
        return self

    def is_(self, comment: str) -> Self:
        self._comment = comment
        return self

    def _create_base_expression(self) -> exp.Expression:
        if self._target_type == "TABLE" and self._table and self._comment is not None:
            # Create a proper Comment expression
            return exp.Comment(
                this=exp.to_table(self._table), kind="TABLE", expression=exp.Literal.string(self._comment)
            )
        if self._target_type == "COLUMN" and self._table and self._column and self._comment is not None:
            # Create a proper Comment expression for column
            return exp.Comment(
                this=exp.Column(table=self._table, this=self._column),
                kind="COLUMN",
                expression=exp.Literal.string(self._comment),
            )
        self._raise_sql_builder_error("Must specify target and comment for COMMENT ON statement.")
        raise AssertionError  # This line is unreachable but satisfies the linter


@dataclass
class RenameTableBuilder(DDLBuilder):
    """Builder for ALTER TABLE ... RENAME TO ... statements.

    Supports renaming a table.
    """

    _old_name: Optional[str] = None
    _new_name: Optional[str] = None

    def table(self, old_name: str) -> Self:
        self._old_name = old_name
        return self

    def to(self, new_name: str) -> Self:
        self._new_name = new_name
        return self

    def _create_base_expression(self) -> exp.Expression:
        if not self._old_name or not self._new_name:
            self._raise_sql_builder_error("Both old and new table names must be set for RENAME TABLE.")
        # Create ALTER TABLE with RENAME TO action
        return exp.Alter(
            this=exp.to_table(self._old_name),
            kind="TABLE",
            actions=[exp.AlterRename(this=exp.to_identifier(self._new_name))],
        )
