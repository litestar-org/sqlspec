"""DDL statement builders.

Provides builders for DDL operations including CREATE, DROP, ALTER,
TRUNCATE, and other schema manipulation statements.
"""

from typing import TYPE_CHECKING, Any, ClassVar, Self, cast

from mypy_extensions import trait
from sqlglot import exp
from sqlglot.errors import ParseError

from sqlspec.builder._base import QueryBuilder
from sqlspec.builder._parsing_utils import _mark_explicit_table_quotes, _normalize_dialect
from sqlspec.core import SQL
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import has_sqlglot_expression

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.builder._column import ColumnExpression

__all__ = (
    "AlterOperation",
    "AlterTable",
    "ColumnDefinition",
    "CommentOn",
    "ConstraintDefinition",
    "CreateIndex",
    "CreateMaterializedView",
    "CreateSchema",
    "CreateTable",
    "CreateTableAsSelect",
    "CreateView",
    "DDLBuilder",
    "DropIndex",
    "DropMaterializedView",
    "DropSchema",
    "DropTable",
    "DropView",
    "RenameTable",
    "Truncate",
)

CONSTRAINT_TYPE_PRIMARY_KEY = "PRIMARY KEY"
CONSTRAINT_TYPE_FOREIGN_KEY = "FOREIGN KEY"
CONSTRAINT_TYPE_UNIQUE = "UNIQUE"
CONSTRAINT_TYPE_CHECK = "CHECK"

FOREIGN_KEY_ACTION_CASCADE = "CASCADE"
FOREIGN_KEY_ACTION_SET_NULL = "SET NULL"
FOREIGN_KEY_ACTION_SET_DEFAULT = "SET DEFAULT"
FOREIGN_KEY_ACTION_RESTRICT = "RESTRICT"
FOREIGN_KEY_ACTION_NO_ACTION = "NO ACTION"

VALID_FOREIGN_KEY_ACTIONS = {
    FOREIGN_KEY_ACTION_CASCADE,
    FOREIGN_KEY_ACTION_SET_NULL,
    FOREIGN_KEY_ACTION_SET_DEFAULT,
    FOREIGN_KEY_ACTION_RESTRICT,
    FOREIGN_KEY_ACTION_NO_ACTION,
}

VALID_CONSTRAINT_TYPES = {
    CONSTRAINT_TYPE_PRIMARY_KEY,
    CONSTRAINT_TYPE_FOREIGN_KEY,
    CONSTRAINT_TYPE_UNIQUE,
    CONSTRAINT_TYPE_CHECK,
}

CURRENT_TIMESTAMP_KEYWORD = "CURRENT_TIMESTAMP"
CURRENT_DATE_KEYWORD = "CURRENT_DATE"
CURRENT_TIME_KEYWORD = "CURRENT_TIME"


def build_column_expression(
    col: "ColumnDefinition", dialect: "DialectType | None" = None, *, include_primary_key: bool = True
) -> "exp.Expr":
    """Build SQLGlot expression for a column definition."""
    col_def = exp.ColumnDef(
        this=_parse_ddl_identifier(col.name, dialect=dialect), kind=_parse_column_type(col.name, col.dtype, dialect)
    )

    constraints: list[exp.ColumnConstraint] = []

    if col.not_null:
        constraints.append(exp.ColumnConstraint(kind=exp.NotNullColumnConstraint()))

    if col.primary_key and include_primary_key:
        constraints.append(exp.ColumnConstraint(kind=exp.PrimaryKeyColumnConstraint()))

    if col.unique:
        constraints.append(exp.ColumnConstraint(kind=exp.UniqueColumnConstraint()))

    if col.auto_increment:
        constraints.append(exp.ColumnConstraint(kind=exp.AutoIncrementColumnConstraint(this=False)))

    if col.default is not None:
        default_expr: exp.Expr | None = None
        if isinstance(col.default, str):
            default_upper = col.default.upper()
            if default_upper == CURRENT_TIMESTAMP_KEYWORD:
                default_expr = exp.CurrentTimestamp()
            elif default_upper == CURRENT_DATE_KEYWORD:
                default_expr = exp.CurrentDate()
            elif default_upper == CURRENT_TIME_KEYWORD:
                default_expr = exp.CurrentTime()
            elif "(" in col.default:
                default_expr = exp.maybe_parse(col.default)
            else:
                default_expr = exp.convert(col.default)
        else:
            default_expr = exp.convert(col.default)

        constraints.append(exp.ColumnConstraint(kind=exp.DefaultColumnConstraint(this=default_expr)))

    if col.check:
        constraints.append(exp.ColumnConstraint(kind=exp.Check(this=exp.maybe_parse(col.check))))

    if col.comment:
        constraints.append(exp.ColumnConstraint(kind=exp.CommentColumnConstraint(this=exp.convert(col.comment))))

    if col.generated:
        constraints.append(
            exp.ColumnConstraint(kind=exp.GeneratedAsIdentityColumnConstraint(this=exp.maybe_parse(col.generated)))
        )

    if col.collate:
        constraints.append(
            exp.ColumnConstraint(
                kind=exp.CollateColumnConstraint(this=_parse_ddl_identifier(col.collate, dialect=dialect))
            )
        )

    if constraints:
        col_def.set("constraints", constraints)

    return col_def


def build_constraint_expression(
    constraint: "ConstraintDefinition", dialect: "DialectType | None" = None
) -> "exp.Expr | None":
    """Build SQLGlot expression for a table constraint."""
    if constraint.constraint_type == CONSTRAINT_TYPE_PRIMARY_KEY:
        pk_constraint = exp.PrimaryKey(
            expressions=[_parse_ddl_identifier(col, dialect=dialect) for col in constraint.columns]
        )

        if constraint.name:
            return exp.Constraint(
                this=_parse_ddl_identifier(constraint.name, dialect=dialect), expressions=[pk_constraint]
            )
        return pk_constraint

    if constraint.constraint_type == CONSTRAINT_TYPE_FOREIGN_KEY:
        fk_options: list[str] = []
        if constraint.deferrable:
            if constraint.initially_deferred:
                fk_options.append("DEFERRABLE INITIALLY DEFERRED")
            else:
                fk_options.append("DEFERRABLE INITIALLY IMMEDIATE")

        reference_options: list[str] = []
        if constraint.on_delete:
            reference_options.append(f"ON DELETE {constraint.on_delete}")
        if constraint.on_update:
            reference_options.append(f"ON UPDATE {constraint.on_update}")

        fk_constraint = exp.ForeignKey(
            expressions=[_parse_ddl_identifier(col, dialect=dialect) for col in constraint.columns],
            reference=exp.Reference(
                this=_parse_ddl_table(constraint.references_table, dialect=dialect)
                if constraint.references_table
                else None,
                expressions=[_parse_ddl_identifier(col, dialect=dialect) for col in constraint.references_columns],
                options=reference_options or None,
            ),
            options=fk_options or None,
        )

        if constraint.name:
            return exp.Constraint(
                this=_parse_ddl_identifier(constraint.name, dialect=dialect), expressions=[fk_constraint]
            )
        return fk_constraint

    if constraint.constraint_type == CONSTRAINT_TYPE_UNIQUE:
        unique_constraint = exp.UniqueKeyProperty(
            expressions=[_parse_ddl_identifier(col, dialect=dialect) for col in constraint.columns]
        )

        if constraint.name:
            return exp.Constraint(
                this=_parse_ddl_identifier(constraint.name, dialect=dialect), expressions=[unique_constraint]
            )
        return unique_constraint

    if constraint.constraint_type == CONSTRAINT_TYPE_CHECK:
        check_expr = exp.Check(
            this=constraint.condition_expr
            if constraint.condition_expr is not None
            else exp.maybe_parse(constraint.condition, dialect=dialect)
            if constraint.condition
            else None
        )

        if constraint.name:
            return exp.Constraint(
                this=_parse_ddl_identifier(constraint.name, dialect=dialect), expressions=[check_expr]
            )
        return check_expr

    return None


class DDLBuilder(QueryBuilder):
    """Base class for DDL builders (CREATE, DROP, ALTER, etc)."""

    __slots__ = ("_expression_dialect",)

    def __init__(self, dialect: "DialectType" = None) -> None:
        super().__init__(dialect=dialect)
        self._expression: exp.Expr | None = None
        self._expression_dialect: DialectType | None = None

    def _prepare_expression_for_build(self, dialect: "DialectType" = None, *, copy: bool = True) -> exp.Expr:
        target_dialect = _normalize_dialect(dialect or self.dialect)
        self._expression_dialect = target_dialect
        self.set_parameters({})
        self._expression = self._create_base_expression()
        return super()._prepare_expression_for_build(dialect=target_dialect, copy=copy)

    def _create_base_expression(self) -> exp.Expr:
        msg = "Subclasses must implement _create_base_expression."
        raise NotImplementedError(msg)

    def _require(self, value: object, message: str) -> None:
        if not value:
            self._raise_builder_error(message)

    def _resolve_select_query(self, query: object, context: str, *, require_select_type: bool = True) -> exp.Expr:
        effective_dialect = self._expression_dialect or self.dialect_name
        select_parameters: dict[str, Any] | None = None

        if isinstance(query, SQL):
            select_expr = (
                query.expression.copy()
                if query.expression is not None
                else exp.maybe_parse(query.sql, dialect=effective_dialect)
            )
            select_parameters = query.parameters
        elif isinstance(query, QueryBuilder):
            select_expr = query._prepare_expression_for_build(dialect=effective_dialect, copy=True)
            select_parameters = query.parameters
        elif isinstance(query, str):
            select_expr = exp.maybe_parse(query, dialect=effective_dialect)
        elif isinstance(query, exp.Expr):
            select_expr = query.copy()
        else:
            self._raise_builder_error(f"Unsupported type for SELECT query in {context}.")

        if select_expr is None or (require_select_type and not isinstance(select_expr, (exp.Select, exp.SetOperation))):
            self._raise_builder_error("SELECT query must be a valid SELECT expression.")

        if select_parameters:
            self.load_parameters(select_parameters)

        return select_expr


@trait
class _IfExistsDDLMixin:
    __slots__ = ()

    _if_exists: bool

    def if_exists(self) -> Self:
        cast("Any", self)._if_exists = True
        return self


@trait
class _CascadeRestrictDDLMixin:
    __slots__ = ()

    _cascade: "bool | None"

    def cascade(self) -> Self:
        cast("Any", self)._cascade = True
        return self

    def restrict(self) -> Self:
        cast("Any", self)._cascade = False
        return self


@trait
class _IfNotExistsDDLMixin:
    __slots__ = ()

    _if_not_exists: bool

    def if_not_exists(self) -> Self:
        cast("Any", self)._if_not_exists = True
        return self


class ColumnDefinition:
    """Column definition for CREATE TABLE."""

    __slots__ = (
        "auto_increment",
        "check",
        "collate",
        "comment",
        "default",
        "dtype",
        "generated",
        "name",
        "not_null",
        "primary_key",
        "unique",
    )

    def __init__(
        self,
        name: str,
        dtype: str,
        default: "Any | None" = None,
        not_null: bool = False,
        primary_key: bool = False,
        unique: bool = False,
        auto_increment: bool = False,
        comment: "str | None" = None,
        check: "str | None" = None,
        generated: "str | None" = None,
        collate: "str | None" = None,
    ) -> None:
        self.name = name
        self.dtype = dtype
        self.default = default
        self.not_null = not_null
        self.primary_key = primary_key
        self.unique = unique
        self.auto_increment = auto_increment
        self.comment = comment
        self.check = check
        self.generated = generated
        self.collate = collate


class ConstraintDefinition:
    """Constraint definition for CREATE TABLE."""

    __slots__ = (
        "columns",
        "condition",
        "condition_expr",
        "constraint_type",
        "deferrable",
        "initially_deferred",
        "name",
        "on_delete",
        "on_update",
        "references_columns",
        "references_table",
    )

    def __init__(
        self,
        constraint_type: str,
        name: "str | None" = None,
        columns: "list[str] | None" = None,
        references_table: "str | None" = None,
        references_columns: "list[str] | None" = None,
        condition: "str | None" = None,
        condition_expr: "exp.Expr | None" = None,
        on_delete: "str | None" = None,
        on_update: "str | None" = None,
        deferrable: bool = False,
        initially_deferred: bool = False,
    ) -> None:
        self.constraint_type = constraint_type
        self.name = name
        self.columns = columns or []
        self.references_table = references_table
        self.references_columns = references_columns or []
        self.condition = condition
        self.condition_expr = condition_expr
        self.on_delete = on_delete
        self.on_update = on_update
        self.deferrable = deferrable
        self.initially_deferred = initially_deferred


class CreateTable(DDLBuilder, _IfNotExistsDDLMixin):
    """Builder for CREATE TABLE statements with columns and constraints."""

    __slots__ = (
        "_columns",
        "_constraints",
        "_if_not_exists",
        "_like_table",
        "_partition_by",
        "_schema",
        "_table_name",
        "_table_options",
        "_tablespace",
        "_temporary",
    )

    def __init__(self, table_name: str, dialect: "DialectType" = None) -> None:
        super().__init__(dialect=dialect)
        self._table_name = table_name
        self._if_not_exists = False
        self._temporary = False
        self._columns: list[ColumnDefinition] = []
        self._constraints: list[ConstraintDefinition] = []
        self._table_options: dict[str, Any] = {}
        self._schema: str | None = None
        self._tablespace: str | None = None
        self._like_table: str | None = None
        self._partition_by: str | None = None

    @property
    def columns(self) -> "list[ColumnDefinition]":
        """Get the list of column definitions for this table.

        Returns:
            List of ColumnDefinition objects.
        """
        return self._columns

    def in_schema(self, schema_name: str) -> "Self":
        """Set the schema for the table."""
        self._schema = schema_name
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
        default: "Any | None" = None,
        not_null: bool = False,
        primary_key: bool = False,
        unique: bool = False,
        auto_increment: bool = False,
        comment: "str | None" = None,
        check: "str | None" = None,
        generated: "str | None" = None,
        collate: "str | None" = None,
    ) -> "Self":
        """Add a column definition to the table."""
        if not name:
            self._raise_builder_error("Column name must be a non-empty string")

        if not dtype:
            self._raise_builder_error("Column type must be a non-empty string")

        if any(col.name == name for col in self._columns):
            self._raise_builder_error(f"Column '{name}' already defined")

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

        if primary_key:
            self.primary_key_constraint([name])

        return self

    def primary_key_constraint(self, columns: "str | list[str]", name: "str | None" = None) -> "Self":
        """Add a primary key constraint."""
        col_list = [columns] if isinstance(columns, str) else list(columns)

        if not col_list:
            self._raise_builder_error("Primary key must include at least one column")

        existing_pk = self._find_primary_key_constraint()
        if existing_pk:
            if name:
                existing_pk.name = name
            for col in col_list:
                if col not in existing_pk.columns:
                    existing_pk.columns.append(col)
        else:
            constraint = ConstraintDefinition(constraint_type=CONSTRAINT_TYPE_PRIMARY_KEY, name=name, columns=col_list)
            self._constraints.append(constraint)

        return self

    def foreign_key_constraint(
        self,
        columns: "str | list[str]",
        references_table: str,
        references_columns: "str | list[str]",
        name: "str | None" = None,
        on_delete: "str | None" = None,
        on_update: "str | None" = None,
        deferrable: bool = False,
        initially_deferred: bool = False,
    ) -> "Self":
        """Add a foreign key constraint."""
        col_list = [columns] if isinstance(columns, str) else list(columns)

        ref_col_list = [references_columns] if isinstance(references_columns, str) else list(references_columns)

        if len(col_list) != len(ref_col_list):
            self._raise_builder_error("Foreign key columns and referenced columns must have same length")

        self._validate_foreign_key_action(on_delete, "ON DELETE")
        self._validate_foreign_key_action(on_update, "ON UPDATE")

        constraint = ConstraintDefinition(
            constraint_type=CONSTRAINT_TYPE_FOREIGN_KEY,
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

    def unique_constraint(self, columns: "str | list[str]", name: "str | None" = None) -> "Self":
        """Add a unique constraint."""
        col_list = [columns] if isinstance(columns, str) else list(columns)

        if not col_list:
            self._raise_builder_error("Unique constraint must include at least one column")

        constraint = ConstraintDefinition(constraint_type=CONSTRAINT_TYPE_UNIQUE, name=name, columns=col_list)

        self._constraints.append(constraint)
        return self

    def check_constraint(self, condition: "str | ColumnExpression", name: "str | None" = None) -> "Self":
        """Add a check constraint."""
        if condition is None or (isinstance(condition, str) and not condition):
            self._raise_builder_error("Check constraint must have a condition")

        condition_expr: exp.Expr | None = None
        condition_str: str | None = None
        if has_sqlglot_expression(condition):
            sqlglot_expr = condition.sqlglot_expression
            condition_expr = sqlglot_expr if isinstance(sqlglot_expr, exp.Expr) else None
        else:
            condition_str = str(condition)

        constraint = ConstraintDefinition(
            constraint_type=CONSTRAINT_TYPE_CHECK, name=name, condition=condition_str, condition_expr=condition_expr
        )

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

    def _create_base_expression(self) -> "exp.Expr":
        """Create the SQLGlot expression for CREATE TABLE."""
        self._require(self._columns or self._like_table, "Table must have at least one column or use LIKE clause")

        effective_dialect = self._expression_dialect or self.dialect_name
        has_table_level_pk = any(
            c.constraint_type == CONSTRAINT_TYPE_PRIMARY_KEY and not self._is_redundant_single_column_primary_key(c)
            for c in self._constraints
        )
        column_defs: list[exp.Expr] = []
        for col in self._columns:
            col_expr = build_column_expression(
                col, dialect=effective_dialect, include_primary_key=not has_table_level_pk
            )
            column_defs.append(col_expr)

        for constraint in self._constraints:
            if self._is_redundant_single_column_primary_key(constraint):
                continue

            constraint_expr = build_constraint_expression(constraint, dialect=effective_dialect)
            if constraint_expr:
                column_defs.append(constraint_expr)

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
            partition_expr: exp.Expr = exp.maybe_parse(self._partition_by, dialect=effective_dialect) or exp.var(
                self._partition_by
            )
            props.append(exp.PartitionedByProperty(this=partition_expr))

        for key, value in self._table_options.items():
            if key != "engine":
                props.append(exp.Property(this=exp.to_identifier(key.upper()), value=exp.convert(value)))

        table_identifier = _parse_ddl_table(self._table_name, schema=self._schema, dialect=effective_dialect)

        schema_expr = exp.Schema(this=table_identifier, expressions=column_defs)

        if self._temporary:
            props.append(exp.TemporaryProperty())
        if self._like_table:
            props.append(exp.LikeProperty(this=_parse_ddl_table(self._like_table, dialect=effective_dialect)))
        properties_node = _wrap_properties(props)

        create_target: exp.Expr = table_identifier if self._like_table and not column_defs else schema_expr
        return exp.Create(kind="TABLE", this=create_target, exists=self._if_not_exists, properties=properties_node)

    def _has_primary_key_constraint(self) -> bool:
        """Check if table already has a primary key constraint."""
        return any(c.constraint_type == CONSTRAINT_TYPE_PRIMARY_KEY for c in self._constraints)

    def _find_primary_key_constraint(self) -> "ConstraintDefinition | None":
        """Find existing primary key constraint."""
        return next((c for c in self._constraints if c.constraint_type == CONSTRAINT_TYPE_PRIMARY_KEY), None)

    def _validate_foreign_key_action(self, action: "str | None", action_type: str) -> None:
        """Validate foreign key action (ON DELETE or ON UPDATE)."""
        if action and action.upper() not in VALID_FOREIGN_KEY_ACTIONS:
            self._raise_builder_error(f"Invalid {action_type} action: {action}")

    def _is_redundant_single_column_primary_key(self, constraint: "ConstraintDefinition") -> bool:
        """Check if constraint is a redundant single-column primary key."""
        if (
            constraint.constraint_type != CONSTRAINT_TYPE_PRIMARY_KEY
            or len(constraint.columns) != 1
            or constraint.name is not None
        ):
            return False

        col_name = constraint.columns[0]
        return any(c.name == col_name and c.primary_key for c in self._columns)


class _SingleObjectDropBuilder(DDLBuilder, _IfExistsDDLMixin, _CascadeRestrictDDLMixin):
    """Template base for DROP builders targeting a single named object.

    Subclasses set ``_drop_kind`` (the SQL object kind) and ``_object_label``
    (the human-readable noun used in error messages), and may override
    ``_build_drop_this`` or ``_drop_expression_args`` to customize the
    generated ``exp.Drop`` expression.
    """

    __slots__ = ("_cascade", "_if_exists", "_name")

    _drop_kind: "ClassVar[str]" = ""
    _object_label: "ClassVar[str]" = ""

    def __init__(self, name: str, dialect: "DialectType" = None) -> None:
        super().__init__(dialect=dialect)
        self._name = name
        self._if_exists = False
        self._cascade: bool | None = None

    def _build_drop_this(self) -> exp.Expr:
        effective_dialect = self._expression_dialect or self.dialect_name
        if self._drop_kind == "SCHEMA":
            return _parse_ddl_identifier(self._name, dialect=effective_dialect)
        return _parse_ddl_table(self._name, dialect=effective_dialect)

    def _drop_expression_args(self) -> "dict[str, Any]":
        return {}

    def _create_base_expression(self) -> exp.Expr:
        self._require(self._name, f"{self._object_label} name must be set for DROP {self._drop_kind}.")
        return exp.Drop(
            kind=self._drop_kind,
            tables=[self._build_drop_this()],
            exists=self._if_exists,
            cascade=self._cascade,
            **self._drop_expression_args(),
        )


class DropTable(_SingleObjectDropBuilder):
    """Builder for DROP TABLE [IF EXISTS] ... [CASCADE|RESTRICT]."""

    __slots__ = ()

    _drop_kind: "ClassVar[str]" = "TABLE"
    _object_label: "ClassVar[str]" = "Table"

    def __init__(self, table_name: str, dialect: "DialectType" = None) -> None:
        """Initialize DROP TABLE with table name.

        Args:
            table_name: Name of the table to drop
            dialect: SQL dialect to use
        """
        super().__init__(table_name, dialect=dialect)

    def table(self, name: str) -> Self:
        self._name = name
        return self


class DropIndex(_SingleObjectDropBuilder):
    """Builder for ``DROP INDEX [IF EXISTS] ... [ON table] [CASCADE|RESTRICT]``."""

    __slots__ = ("_table_name",)

    _drop_kind: "ClassVar[str]" = "INDEX"
    _object_label: "ClassVar[str]" = "Index"

    def __init__(self, index_name: str, dialect: "DialectType" = None) -> None:
        """Initialize DROP INDEX with index name.

        Args:
            index_name: Name of the index to drop
            dialect: SQL dialect to use
        """
        super().__init__(index_name, dialect=dialect)
        self._table_name: str | None = None

    def name(self, index_name: str) -> Self:
        self._name = index_name
        return self

    def on_table(self, table_name: str) -> Self:
        self._table_name = table_name
        return self

    def _drop_expression_args(self) -> "dict[str, Any]":
        effective_dialect = self._expression_dialect or self.dialect_name
        return {
            "cluster": exp.OnProperty(this=_parse_ddl_table(self._table_name, dialect=effective_dialect))
            if self._table_name
            else None
        }


class DropView(_SingleObjectDropBuilder):
    """Builder for DROP VIEW [IF EXISTS] ... [CASCADE|RESTRICT]."""

    __slots__ = ()

    _drop_kind: "ClassVar[str]" = "VIEW"
    _object_label: "ClassVar[str]" = "View"

    def __init__(self, view_name: str, dialect: "DialectType" = None) -> None:
        """Initialize DROP VIEW with view name.

        Args:
            view_name: Name of the view to drop
            dialect: SQL dialect to use
        """
        super().__init__(view_name, dialect=dialect)

    def name(self, view_name: str) -> Self:
        self._name = view_name
        return self


class DropSchema(_SingleObjectDropBuilder):
    """Builder for DROP SCHEMA [IF EXISTS] ... [CASCADE|RESTRICT]."""

    __slots__ = ()

    _drop_kind: "ClassVar[str]" = "SCHEMA"
    _object_label: "ClassVar[str]" = "Schema"

    def __init__(self, schema_name: str, dialect: "DialectType" = None) -> None:
        """Initialize DROP SCHEMA with schema name.

        Args:
            schema_name: Name of the schema to drop
            dialect: SQL dialect to use
        """
        super().__init__(schema_name, dialect=dialect)

    def name(self, schema_name: str) -> Self:
        self._name = schema_name
        return self


class DropMaterializedView(_SingleObjectDropBuilder):
    """Builder for DROP MATERIALIZED VIEW [IF EXISTS] ... [CASCADE|RESTRICT]."""

    __slots__ = ()

    _drop_kind: "ClassVar[str]" = "MATERIALIZED VIEW"
    _object_label: "ClassVar[str]" = "View"

    def __init__(self, view_name: str, dialect: "DialectType" = None) -> None:
        """Initialize DROP MATERIALIZED VIEW with view name.

        Args:
            view_name: Name of the materialized view to drop
            dialect: SQL dialect to use
        """
        super().__init__(view_name, dialect=dialect)

    def name(self, view_name: str) -> Self:
        """Set the materialized view name."""
        self._name = view_name
        return self


class CreateIndex(DDLBuilder, _IfNotExistsDDLMixin):
    """Builder for CREATE [UNIQUE] INDEX [IF NOT EXISTS] ... ON ... (...)."""

    __slots__ = ("_columns", "_if_not_exists", "_index_name", "_table_name", "_unique", "_using", "_where")

    def __init__(self, index_name: str, dialect: "DialectType" = None) -> None:
        """Initialize CREATE INDEX with index name.

        Args:
            index_name: Name of the index to create
            dialect: SQL dialect to use
        """
        super().__init__(dialect=dialect)
        self._index_name = index_name
        self._table_name: str | None = None
        self._columns: list[str | exp.Ordered | exp.Expr] = []
        self._unique = False
        self._if_not_exists = False
        self._using: str | None = None
        self._where: str | exp.Expr | None = None

    def name(self, index_name: str) -> Self:
        self._index_name = index_name
        return self

    def on_table(self, table_name: str) -> Self:
        self._table_name = table_name
        return self

    def columns(self, *cols: str | exp.Ordered | exp.Expr) -> Self:
        self._columns.extend(cols)
        return self

    def expressions(self, *exprs: str | exp.Expr) -> Self:
        self._columns.extend(exprs)
        return self

    def unique(self) -> Self:
        self._unique = True
        return self

    def using(self, method: str) -> Self:
        self._using = method
        return self

    def where(self, condition: str | exp.Expr) -> Self:
        self._where = condition
        return self

    def _create_base_expression(self) -> exp.Expr:
        """Build the CREATE INDEX expression for this builder.

        Columns are turned into raw expressions (not ``Ordered``) to preserve natural NULL ordering,
        string ``where`` clauses become expressions, and the final ``exp.Index`` is wrapped in an ``exp.Create`` with the configured flags.
        """
        self._require(self._index_name and self._table_name, "Index name and table name must be set for CREATE INDEX.")

        effective_dialect = self._expression_dialect or self.dialect_name
        cols: list[exp.Expr] = []
        for col in self._columns:
            if isinstance(col, str):
                cols.append(exp.Column(this=_parse_ddl_identifier(col, dialect=effective_dialect)))
            else:
                cols.append(col)

        where_expr = None
        if self._where:
            where_expr = exp.condition(self._where) if isinstance(self._where, str) else self._where

        index_params = exp.IndexParameters(columns=cols) if cols else None

        if self._using:
            if index_params is None:
                index_params = exp.IndexParameters()
            index_params.set("using", exp.Var(this=self._using))

        if where_expr:
            if index_params is None:
                index_params = exp.IndexParameters()
            index_params.set("where", exp.Where(this=where_expr))

        index_expr = exp.Index(
            this=_parse_ddl_identifier(self._index_name, dialect=effective_dialect),
            table=_parse_ddl_table(cast("str", self._table_name), dialect=effective_dialect),
            params=index_params,
        )

        return exp.Create(kind="INDEX", this=index_expr, unique=self._unique, exists=self._if_not_exists)


class Truncate(DDLBuilder, _CascadeRestrictDDLMixin):
    """Builder for ``TRUNCATE TABLE ... [CASCADE|RESTRICT] [RESTART IDENTITY|CONTINUE IDENTITY]``."""

    __slots__ = ("_cascade", "_identity", "_table_name")

    def __init__(self, table_name: str, dialect: "DialectType" = None) -> None:
        """Initialize TRUNCATE with table name.

        Args:
            table_name: Name of the table to truncate
            dialect: SQL dialect to use
        """
        super().__init__(dialect=dialect)
        self._table_name = table_name
        self._cascade: bool | None = None
        self._identity: str | None = None

    def table(self, name: str) -> Self:
        self._table_name = name
        return self

    def restart_identity(self) -> Self:
        self._identity = "RESTART"
        return self

    def continue_identity(self) -> Self:
        self._identity = "CONTINUE"
        return self

    def _create_base_expression(self) -> exp.Expr:
        self._require(self._table_name, "Table name must be set for TRUNCATE TABLE.")
        effective_dialect = self._expression_dialect or self.dialect_name
        identity_expr = exp.Var(this=self._identity) if self._identity else None
        option_expr = (
            exp.Var(this="CASCADE")
            if self._cascade is True
            else exp.Var(this="RESTRICT")
            if self._cascade is False
            else None
        )
        return exp.TruncateTable(
            expressions=[_parse_ddl_table(self._table_name, dialect=effective_dialect)],
            option=option_expr,
            identity=identity_expr,
        )


class AlterOperation:
    """Represents a single ALTER TABLE operation."""

    __slots__ = (
        "after_column",
        "column_definition",
        "column_name",
        "constraint_definition",
        "constraint_name",
        "first",
        "new_name",
        "new_type",
        "operation_type",
        "using_expression",
    )

    def __init__(
        self,
        operation_type: str,
        column_name: "str | None" = None,
        column_definition: "ColumnDefinition | None" = None,
        constraint_name: "str | None" = None,
        constraint_definition: "ConstraintDefinition | None" = None,
        new_type: "str | None" = None,
        new_name: "str | None" = None,
        after_column: "str | None" = None,
        first: bool = False,
        using_expression: "str | None" = None,
    ) -> None:
        self.operation_type = operation_type
        self.column_name = column_name
        self.column_definition = column_definition
        self.constraint_name = constraint_name
        self.constraint_definition = constraint_definition
        self.new_type = new_type
        self.new_name = new_name
        self.after_column = after_column
        self.first = first
        self.using_expression = using_expression


class CreateSchema(DDLBuilder, _IfNotExistsDDLMixin):
    """Builder for CREATE SCHEMA [IF NOT EXISTS] schema_name [AUTHORIZATION user_name]."""

    __slots__ = ("_authorization", "_if_not_exists", "_schema_name")

    def __init__(self, schema_name: str, dialect: "DialectType" = None) -> None:
        """Initialize CREATE SCHEMA with schema name.

        Args:
            schema_name: Name of the schema to create
            dialect: SQL dialect to use
        """
        super().__init__(dialect=dialect)
        self._schema_name = schema_name
        self._if_not_exists = False
        self._authorization: str | None = None

    def name(self, schema_name: str) -> Self:
        self._schema_name = schema_name
        return self

    def authorization(self, user_name: str) -> Self:
        self._authorization = user_name
        return self

    def _create_base_expression(self) -> exp.Expr:
        self._require(self._schema_name, "Schema name must be set for CREATE SCHEMA.")
        effective_dialect = self._expression_dialect or self.dialect_name
        props: list[exp.Property] = []
        if self._authorization:
            props.append(
                exp.Property(
                    this=exp.to_identifier("AUTHORIZATION"),
                    value=_parse_ddl_identifier(self._authorization, dialect=effective_dialect),
                )
            )
        properties_node = _wrap_properties(props)
        return exp.Create(
            kind="SCHEMA",
            this=_parse_ddl_identifier(self._schema_name, dialect=effective_dialect),
            exists=self._if_not_exists,
            properties=properties_node,
        )


class CreateTableAsSelect(DDLBuilder, _IfNotExistsDDLMixin):
    """Builder for CREATE TABLE [IF NOT EXISTS] ... AS SELECT ... (CTAS).

    Methods:
        - name(table_name: str): Set the table name.
        - if_not_exists(): Add IF NOT EXISTS.
        - columns(*cols: str): Set explicit column list (optional).
        - as_select(select_query): Set the SELECT source (SQL, SelectBuilder, or str).
    """

    __slots__ = ("_columns", "_if_not_exists", "_select_query", "_table_name")

    def __init__(self, dialect: "DialectType" = None) -> None:
        super().__init__(dialect=dialect)
        self._table_name: str | None = None
        self._if_not_exists = False
        self._columns: list[str] = []
        self._select_query: object | None = None

    def name(self, table_name: str) -> Self:
        self._table_name = table_name
        return self

    def columns(self, *cols: str) -> Self:
        self._columns = list(cols)
        return self

    def as_select(self, select_query: "str | exp.Expr | Any") -> Self:
        self._select_query = select_query
        return self

    def _create_base_expression(self) -> exp.Expr:
        self._require(self._table_name, "Table name must be set for CREATE TABLE AS SELECT.")
        if self._select_query is None:
            self._raise_builder_error("SELECT query must be set for CREATE TABLE AS SELECT.")

        effective_dialect = self._expression_dialect or self.dialect_name
        select_expr = self._resolve_select_query(self._select_query, "CTAS", require_select_type=False)

        create_target: exp.Expr = _parse_ddl_table(cast("str", self._table_name), dialect=effective_dialect)
        if self._columns:
            create_target = exp.Schema(
                this=create_target,
                expressions=[_parse_ddl_identifier(c, dialect=effective_dialect) for c in self._columns],
            )

        return exp.Create(kind="TABLE", this=create_target, exists=self._if_not_exists, expression=select_expr)


class CreateMaterializedView(DDLBuilder, _IfNotExistsDDLMixin):
    """Builder for CREATE MATERIALIZED VIEW [IF NOT EXISTS] ... AS SELECT ..."""

    __slots__ = (
        "_columns",
        "_hints",
        "_if_not_exists",
        "_refresh_mode",
        "_select_query",
        "_storage_parameters",
        "_tablespace",
        "_using_index",
        "_view_name",
        "_with_data",
    )

    def __init__(self, view_name: str, dialect: "DialectType" = None) -> None:
        """Initialize CREATE MATERIALIZED VIEW with view name.

        Args:
            view_name: Name of the materialized view to create
            dialect: SQL dialect to use
        """
        super().__init__(dialect=dialect)
        self._view_name = view_name
        self._if_not_exists = False
        self._columns: list[str] = []
        self._select_query: Any = None
        self._with_data: bool | None = None
        self._refresh_mode: str | None = None
        self._storage_parameters: dict[str, Any] = {}
        self._tablespace: str | None = None
        self._using_index: str | None = None
        self._hints: list[str] = []

    def name(self, view_name: str) -> Self:
        self._view_name = view_name
        return self

    def columns(self, *cols: str) -> Self:
        self._columns = list(cols)
        return self

    def as_select(self, select_query: "str | exp.Expr | Any") -> Self:
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

    def _create_base_expression(self) -> exp.Expr:
        self._require(self._view_name, "View name must be set for CREATE MATERIALIZED VIEW.")
        if self._select_query is None:
            self._raise_builder_error("SELECT query must be set for CREATE MATERIALIZED VIEW.")

        effective_dialect = self._expression_dialect or self.dialect_name
        select_expr = self._resolve_select_query(self._select_query, "materialized view")

        schema_expr = _build_optional_column_schema(self._columns, dialect=effective_dialect)

        props: list[exp.Property] = []
        if self._refresh_mode:
            props.append(exp.Property(this=exp.to_identifier("REFRESH_MODE"), value=exp.convert(self._refresh_mode)))
        if self._tablespace:
            props.append(exp.Property(this=exp.to_identifier("TABLESPACE"), value=exp.to_identifier(self._tablespace)))
        if self._using_index:
            props.append(
                exp.Property(this=exp.to_identifier("USING_INDEX"), value=exp.to_identifier(self._using_index))
            )
        for k, v in self._storage_parameters.items():
            props.append(exp.Property(this=exp.to_identifier(k), value=exp.convert(str(v))))
        if self._with_data is not None:
            props.append(exp.WithDataProperty(no=not self._with_data))
        props.extend(exp.Property(this=exp.to_identifier("HINT"), value=exp.convert(hint)) for hint in self._hints)
        properties_node = _wrap_properties(props)

        create_target: exp.Expr = _parse_ddl_table(self._view_name, dialect=effective_dialect)
        if schema_expr is not None:
            create_target = exp.Schema(this=create_target, expressions=schema_expr.expressions)

        return exp.Create(
            kind="MATERIALIZED_VIEW",
            this=create_target,
            exists=self._if_not_exists,
            expression=select_expr,
            properties=properties_node,
        )


class CreateView(DDLBuilder, _IfNotExistsDDLMixin):
    """Builder for CREATE VIEW [IF NOT EXISTS] ... AS SELECT ..."""

    __slots__ = ("_columns", "_hints", "_if_not_exists", "_select_query", "_view_name")

    def __init__(self, view_name: str, dialect: "DialectType" = None) -> None:
        """Initialize CREATE VIEW with view name.

        Args:
            view_name: Name of the view to create
            dialect: SQL dialect to use
        """
        super().__init__(dialect=dialect)
        self._view_name = view_name
        self._if_not_exists = False
        self._columns: list[str] = []
        self._select_query: Any = None
        self._hints: list[str] = []

    def name(self, view_name: str) -> Self:
        self._view_name = view_name
        return self

    def columns(self, *cols: str) -> Self:
        self._columns = list(cols)
        return self

    def as_select(self, select_query: "str | exp.Expr | Any") -> Self:
        self._select_query = select_query
        return self

    def with_hint(self, hint: str) -> Self:
        self._hints.append(hint)
        return self

    def _create_base_expression(self) -> exp.Expr:
        self._require(self._view_name, "View name must be set for CREATE VIEW.")
        if self._select_query is None:
            self._raise_builder_error("SELECT query must be set for CREATE VIEW.")

        effective_dialect = self._expression_dialect or self.dialect_name
        select_expr = self._resolve_select_query(self._select_query, "view")

        schema_expr = _build_optional_column_schema(self._columns, dialect=effective_dialect)

        props: list[exp.Property] = [
            exp.Property(this=exp.to_identifier("HINT"), value=exp.convert(h)) for h in self._hints
        ]
        properties_node = _wrap_properties(props)

        create_target: exp.Expr = _parse_ddl_table(self._view_name, dialect=effective_dialect)
        if schema_expr is not None:
            create_target = exp.Schema(this=create_target, expressions=schema_expr.expressions)

        return exp.Create(
            kind="VIEW",
            this=create_target,
            exists=self._if_not_exists,
            expression=select_expr,
            properties=properties_node,
        )


class AlterTable(DDLBuilder, _IfExistsDDLMixin):
    """Builder for ALTER TABLE operations."""

    __slots__ = ("_if_exists", "_operations", "_schema", "_table_name")

    def __init__(self, table_name: str, dialect: "DialectType" = None) -> None:
        super().__init__(dialect=dialect)
        self._table_name = table_name
        self._operations: list[AlterOperation] = []
        self._schema: str | None = None
        self._if_exists = False

    def add_column(
        self,
        name: str,
        dtype: str,
        default: "Any | None" = None,
        not_null: bool = False,
        unique: bool = False,
        comment: "str | None" = None,
        after: "str | None" = None,
        first: bool = False,
    ) -> "Self":
        """Add a new column to the table."""
        if not name:
            self._raise_builder_error("Column name must be a non-empty string")

        if not dtype:
            self._raise_builder_error("Column type must be a non-empty string")

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
            self._raise_builder_error("Column name must be a non-empty string")

        operation = AlterOperation(operation_type="DROP COLUMN CASCADE" if cascade else "DROP COLUMN", column_name=name)

        self._operations.append(operation)
        return self

    def alter_column_type(self, name: str, new_type: str, using: "str | None" = None) -> "Self":
        """Change the type of an existing column."""
        if not name:
            self._raise_builder_error("Column name must be a non-empty string")

        if not new_type:
            self._raise_builder_error("New type must be a non-empty string")

        operation = AlterOperation(
            operation_type="ALTER COLUMN TYPE", column_name=name, new_type=new_type, using_expression=using
        )

        self._operations.append(operation)
        return self

    def rename_column(self, old_name: str, new_name: str) -> "Self":
        """Rename a column."""
        if not old_name:
            self._raise_builder_error("Old column name must be a non-empty string")

        if not new_name:
            self._raise_builder_error("New column name must be a non-empty string")

        operation = AlterOperation(operation_type="RENAME COLUMN", column_name=old_name, new_name=new_name)

        self._operations.append(operation)
        return self

    def add_constraint(
        self,
        constraint_type: str,
        columns: "str | list[str] | None" = None,
        name: "str | None" = None,
        references_table: "str | None" = None,
        references_columns: "str | list[str] | None" = None,
        condition: "str | ColumnExpression | None" = None,
        on_delete: "str | None" = None,
        on_update: "str | None" = None,
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
        if constraint_type.upper() not in VALID_CONSTRAINT_TYPES:
            self._raise_builder_error(f"Invalid constraint type: {constraint_type}")

        col_list = None
        if columns is not None:
            col_list = [columns] if isinstance(columns, str) else list(columns)

        ref_col_list = None
        if references_columns is not None:
            ref_col_list = [references_columns] if isinstance(references_columns, str) else list(references_columns)

        condition_str: str | None = None
        if condition is not None:
            if has_sqlglot_expression(condition):
                sqlglot_expr = condition.sqlglot_expression
                condition_str = sqlglot_expr.sql(dialect=self.dialect) if sqlglot_expr else str(condition)
            else:
                condition_str = str(condition)

        constraint_def = ConstraintDefinition(
            constraint_type=constraint_type.upper(),
            name=name,
            columns=col_list or [],
            references_table=references_table,
            references_columns=ref_col_list or [],
            condition=condition_str,
            on_delete=on_delete,
            on_update=on_update,
        )

        operation = AlterOperation(operation_type="ADD CONSTRAINT", constraint_definition=constraint_def)

        self._operations.append(operation)
        return self

    def drop_constraint(self, name: str, cascade: bool = False) -> "Self":
        """Drop a constraint from the table."""
        if not name:
            self._raise_builder_error("Constraint name must be a non-empty string")

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

    def in_schema(self, schema_name: str) -> "Self":
        """Set the schema for the table."""
        self._schema = schema_name
        return self

    def set_column_default(self, column: str, value: "Any") -> "Self":
        """Set the default value for a column."""
        if not column:
            self._raise_builder_error("Column name must be a non-empty string")

        column_def = ColumnDefinition(name=column, dtype="", default=value)
        operation = AlterOperation(
            operation_type="ALTER COLUMN SET DEFAULT", column_name=column, column_definition=column_def
        )
        self._operations.append(operation)
        return self

    def drop_column_default(self, column: str) -> "Self":
        """Remove the default value from a column."""
        if not column:
            self._raise_builder_error("Column name must be a non-empty string")

        operation = AlterOperation(operation_type="ALTER COLUMN DROP DEFAULT", column_name=column)
        self._operations.append(operation)
        return self

    def _create_base_expression(self) -> "exp.Expr":
        """Create the SQLGlot expression for ALTER TABLE."""
        self._require(self._operations, "At least one operation must be specified for ALTER TABLE")

        effective_dialect = self._expression_dialect or self.dialect_name
        table = _parse_ddl_table(self._table_name, schema=self._schema, dialect=effective_dialect)

        actions: list[exp.Expr] = [self._build_operation_expression(op) for op in self._operations]

        return exp.Alter(this=table, kind="TABLE", actions=actions, exists=self._if_exists)

    def _build_operation_expression(self, op: "AlterOperation") -> exp.Expr:
        """Build a structured SQLGlot expression for a single alter operation."""
        op_type = op.operation_type.upper()
        effective_dialect = self._expression_dialect or self.dialect_name

        if op_type == "ADD COLUMN":
            if not op.column_definition:
                self._raise_builder_error("Column definition required for ADD COLUMN")
            col_expr = build_column_expression(op.column_definition, dialect=effective_dialect)
            if isinstance(col_expr, exp.ColumnDef) and (op.after_column or op.first):
                constraints = list(col_expr.args.get("constraints") or [])
                if op.after_column:
                    constraints.append(
                        exp.ColumnConstraint(
                            kind=exp.ColumnPosition(
                                this=_parse_ddl_identifier(op.after_column, dialect=effective_dialect), position="AFTER"
                            )
                        )
                    )
                elif op.first:
                    constraints.append(exp.ColumnConstraint(kind=exp.ColumnPosition(position="FIRST")))
                col_expr.set("constraints", constraints)
            return col_expr

        if op_type == "DROP COLUMN":
            return exp.Drop(
                tables=[_parse_ddl_identifier(cast("str", op.column_name), dialect=effective_dialect)],
                kind="COLUMN",
                exists=True,
            )

        if op_type == "DROP COLUMN CASCADE":
            return exp.Drop(
                tables=[_parse_ddl_identifier(cast("str", op.column_name), dialect=effective_dialect)],
                kind="COLUMN",
                cascade=True,
                exists=True,
            )

        if op_type == "ALTER COLUMN TYPE":
            if not op.new_type:
                self._raise_builder_error("New type required for ALTER COLUMN TYPE")
            return exp.AlterColumn(
                this=_parse_ddl_identifier(cast("str", op.column_name), dialect=effective_dialect),
                dtype=_parse_column_type(op.column_name, op.new_type, effective_dialect),
                using=exp.maybe_parse(op.using_expression, dialect=effective_dialect) if op.using_expression else None,
            )

        if op_type == "RENAME COLUMN":
            return exp.RenameColumn(
                this=_parse_ddl_identifier(cast("str", op.column_name), dialect=effective_dialect),
                to=_parse_ddl_identifier(cast("str", op.new_name), dialect=effective_dialect),
            )

        if op_type == "ADD CONSTRAINT":
            if not op.constraint_definition:
                self._raise_builder_error("Constraint definition required for ADD CONSTRAINT")
            constraint_expr = build_constraint_expression(op.constraint_definition, dialect=effective_dialect)
            return exp.AddConstraint(expressions=[constraint_expr])

        if op_type == "DROP CONSTRAINT":
            return exp.Drop(
                tables=[_parse_ddl_identifier(cast("str", op.constraint_name), dialect=effective_dialect)],
                kind="CONSTRAINT",
                exists=True,
            )

        if op_type == "DROP CONSTRAINT CASCADE":
            return exp.Drop(
                tables=[_parse_ddl_identifier(cast("str", op.constraint_name), dialect=effective_dialect)],
                kind="CONSTRAINT",
                cascade=True,
                exists=True,
            )

        if op_type == "ALTER COLUMN SET NOT NULL":
            return exp.AlterColumn(
                this=_parse_ddl_identifier(cast("str", op.column_name), dialect=effective_dialect), allow_null=False
            )

        if op_type == "ALTER COLUMN DROP NOT NULL":
            return exp.AlterColumn(
                this=_parse_ddl_identifier(cast("str", op.column_name), dialect=effective_dialect),
                drop=True,
                allow_null=True,
            )

        if op_type == "ALTER COLUMN SET DEFAULT":
            if not op.column_definition or op.column_definition.default is None:
                self._raise_builder_error("Default value required for SET DEFAULT")
            default_val = op.column_definition.default
            default_expr: exp.Expr | None
            if isinstance(default_val, str):
                if self._is_sql_function_default(default_val):
                    default_expr = exp.maybe_parse(default_val, dialect=effective_dialect)
                else:
                    default_expr = exp.convert(default_val)
            elif isinstance(default_val, (int, float)):
                default_expr = exp.convert(default_val)
            elif default_val is True:
                default_expr = exp.true()
            elif default_val is False:
                default_expr = exp.false()
            else:
                default_expr = exp.convert(str(default_val))
            return exp.AlterColumn(
                this=_parse_ddl_identifier(cast("str", op.column_name), dialect=effective_dialect), default=default_expr
            )

        if op_type == "ALTER COLUMN DROP DEFAULT":
            return exp.AlterColumn(
                this=_parse_ddl_identifier(cast("str", op.column_name), dialect=effective_dialect), drop=True
            )

        self._raise_builder_error(f"Unknown operation type: {op.operation_type}")
        raise AssertionError

    def _is_sql_function_default(self, default_val: str) -> bool:
        """Check if default value is a SQL function or expression."""
        default_upper = default_val.upper()
        return (
            default_upper in {CURRENT_TIMESTAMP_KEYWORD, CURRENT_DATE_KEYWORD, CURRENT_TIME_KEYWORD}
            or "(" in default_val
        )


class CommentOn(DDLBuilder):
    """Builder for COMMENT ON ... IS ... statements."""

    __slots__ = ("_column", "_comment", "_table", "_target_type")

    def __init__(self, dialect: "DialectType" = None) -> None:
        """Initialize COMMENT ON builder.

        Args:
            dialect: SQL dialect to use
        """
        super().__init__(dialect=dialect)
        self._target_type: str | None = None
        self._table: str | None = None
        self._column: str | None = None
        self._comment: str | None = None

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

    def _create_base_expression(self) -> exp.Expr:
        effective_dialect = self._expression_dialect or self.dialect_name
        if self._target_type == "TABLE" and self._table and self._comment is not None:
            return exp.Comment(
                this=_parse_ddl_table(self._table, dialect=effective_dialect),
                kind="TABLE",
                expression=exp.convert(self._comment),
            )
        if self._target_type == "COLUMN" and self._table and self._column and self._comment is not None:
            table_expr = _parse_ddl_table(self._table, dialect=effective_dialect)
            col_expr = exp.Column(
                this=_parse_ddl_identifier(self._column, dialect=effective_dialect),
                table=table_expr.args.get("this"),
                db=table_expr.args.get("db"),
                catalog=table_expr.args.get("catalog"),
            )
            return exp.Comment(this=col_expr, kind="COLUMN", expression=exp.convert(self._comment))
        self._raise_builder_error("Must specify target and comment for COMMENT ON statement.")
        raise AssertionError


class RenameTable(DDLBuilder):
    """Builder for ALTER TABLE ... RENAME TO ... statements."""

    __slots__ = ("_new_name", "_old_name")

    def __init__(self, old_name: str, dialect: "DialectType" = None) -> None:
        """Initialize RENAME TABLE with old name.

        Args:
            old_name: Current name of the table
            dialect: SQL dialect to use
        """
        super().__init__(dialect=dialect)
        self._old_name = old_name
        self._new_name: str | None = None

    def table(self, old_name: str) -> Self:
        self._old_name = old_name
        return self

    def to(self, new_name: str) -> Self:
        self._new_name = new_name
        return self

    def _create_base_expression(self) -> exp.Expr:
        self._require(self._old_name and self._new_name, "Both old and new table names must be set for RENAME TABLE.")
        effective_dialect = self._expression_dialect or self.dialect_name
        return exp.Alter(
            this=_parse_ddl_table(self._old_name, dialect=effective_dialect),
            kind="TABLE",
            actions=[exp.AlterRename(this=_parse_ddl_table(cast("str", self._new_name), dialect=effective_dialect))],
        )


def _build_optional_column_schema(columns: list[str], dialect: "DialectType | None" = None) -> exp.Schema | None:
    if not columns:
        return None
    return exp.Schema(
        expressions=[exp.Column(this=_parse_ddl_identifier(column, dialect=dialect)) for column in columns]
    )


def _wrap_properties(properties: list[exp.Property]) -> exp.Properties | None:
    return exp.Properties(expressions=properties) if properties else None


def _parse_column_type(name: str | None, dtype: str, dialect: "DialectType | None") -> exp.DataType:
    target_dialect = dialect
    norm_dialect = _normalize_dialect(dialect) if dialect else None
    if norm_dialect in ("tsql", "mssql") and dtype.strip().upper().startswith("TIMESTAMP"):
        target_dialect = None
    try:
        return exp.DataType.build(dtype, dialect=target_dialect)
    except ParseError as exc:
        msg = f"Column {name!r}: cannot parse type {dtype!r} for dialect {dialect!r}"
        raise SQLBuilderError(msg) from exc


def _parse_ddl_identifier(name: str, dialect: "DialectType | None" = None) -> exp.Identifier:
    """Parse a DDL identifier, preserving explicit SQL quoting without double-wrapping."""
    stripped = name.strip()
    if not stripped:
        return exp.to_identifier(name)
    parsed = _mark_explicit_table_quotes(exp.to_table(stripped, dialect=dialect), stripped)
    if isinstance(parsed.this, exp.Identifier) and not parsed.args.get("db"):
        return parsed.this
    return exp.to_identifier(name)


def _parse_ddl_table(table_name: str, schema: "str | None" = None, dialect: "DialectType | None" = None) -> exp.Table:
    """Parse a DDL table reference, preserving quoted table and schema identifiers."""
    table = _mark_explicit_table_quotes(exp.to_table(table_name, dialect=dialect), table_name)
    if schema:
        table.set("db", _parse_ddl_identifier(schema, dialect=dialect))
    return table
