"""DDL statement builders.

Provides builders for DDL operations including CREATE, DROP, ALTER,
TRUNCATE, and other schema manipulation statements.
"""

import contextlib
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, Final, Self, cast
from uuid import UUID

import sqlglot
from mypy_extensions import mypyc_attr, trait
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy
from sqlglot.errors import ParseError, TokenError
from sqlglot.tokenizer_core import TokenType

from sqlspec.builder._base import QueryBuilder
from sqlspec.builder._parsing_utils import (
    _is_simple_identifier,
    _mark_explicit_table_quotes,
    _normalize_dialect,
    _truthy_metadata_flag,
    parse_order_expression,
)
from sqlspec.core import SQL
from sqlspec.data_dictionary._types import DDLResult, MetadataFidelity, MetadataSource, MetadataSupport, ObjectIdentity
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.arrow_helpers import sql_type_for_category, sql_type_from_arrow_type
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
    "CreateSequence",
    "CreateTable",
    "CreateTableAsSelect",
    "CreateView",
    "DDLBuilder",
    "DropIndex",
    "DropMaterializedView",
    "DropSchema",
    "DropSequence",
    "DropTable",
    "DropView",
    "RenameTable",
    "Truncate",
    "compile_table_ddl_results",
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

_IDENTITY_KEYWORDS = frozenset({
    "ALWAYS",
    "BY DEFAULT",
    "ALWAYS AS IDENTITY",
    "BY DEFAULT AS IDENTITY",
    "BY DEFAULT ON NULL",
    "BY DEFAULT ON NULL AS IDENTITY",
})
_IDENTITY_SEED_INC_RE = re.compile(r"^IDENTITY\(\s*(-?\d+)\s*,\s*(-?\d+)\s*\)$", re.IGNORECASE)
_ORDER_DIRECTION_SUFFIX_RE = re.compile(
    r"\s+(?:ASC|DESC|NULLS\s+(?:FIRST|LAST))(?:\s+NULLS\s+(?:FIRST|LAST))?\s*$", re.IGNORECASE
)
_PAREN_COLS_RE = re.compile(r"\(([^)]+)\)")
_FK_DEF_RE = re.compile(
    r"FOREIGN\s+KEY\s*\((?P<cols>[^)]+)\)\s*REFERENCES\s+(?P<ref_table>[^\s(]+)\s*\((?P<ref_cols>[^)]+)\)",
    re.IGNORECASE,
)
_FK_ON_DELETE_RE = re.compile(
    r"\bON\s+DELETE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION)\b", re.IGNORECASE
)
_FK_ON_UPDATE_RE = re.compile(
    r"\bON\s+UPDATE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION)\b", re.IGNORECASE
)
_UUID_STR_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_ISO_DATETIME_STR_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$", re.IGNORECASE
)
_ISO_DATE_STR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO_OFFSET_SUFFIX_RE = re.compile(r"(?:Z|[+-]\d{2}:?\d{2})$", re.IGNORECASE)
_DEFAULT_INFERRED_DECIMAL_PRECISION = 38
_DEFAULT_INFERRED_DECIMAL_SCALE = 9
_ORACLE_NUMBER_MAX_PRECISION = 38
_BARE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TYPE_NAME_PART = r'(?:"(?:[^"]|"")+"|`(?:[^`]|``)+`|\[(?:[^\]]|\]\])+\]|[A-Za-z_][A-Za-z0-9_$]*)'
_QUALIFIED_TYPE_NAME_RE = re.compile(rf"^{_TYPE_NAME_PART}(?:\s*\.\s*{_TYPE_NAME_PART}){{0,2}}$")
_INLINE_ENUM_TYPE_RE = re.compile(r"^\s*ENUM\s*\(", re.IGNORECASE)
_INLINE_ENUM_DIALECTS = frozenset({"duckdb", "mysql", "clickhouse"})
_VERBATIM_TYPE_NAME_RE = re.compile(
    r"^[A-Za-z_]\w*(?:\s+[A-Za-z_]\w*)*(?:\s*\(\s*(?:\d+|MAX)(?:\s*,\s*\d+)?\s*\))?$", re.IGNORECASE
)
_QUOTED_IDENTIFIER_RE = re.compile(r'^(?:"(?:[^"]|"")+"|`(?:[^`]|``)+`|\[(?:[^\]]|\]\])+\])$')
_INDEX_EXPRESSION_CHARS_RE = re.compile(r"[()+\-*/%|<>=!:,']")
_INDEX_PLACEHOLDER_RE = re.compile(r"^<[^<>]*>$")
_NATIVE_INDEX_SQL_RE = re.compile(r"^\s*CREATE\s+(?:UNIQUE\s+)?(?:\w+\s+)*INDEX\b", re.IGNORECASE)
_NUMERIC_LITERAL_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
_MYSQL_KEYWORD_DEFAULT_RE = re.compile(
    r"^(?:CURRENT_TIMESTAMP|NOW|LOCALTIME|LOCALTIMESTAMP|CURRENT_DATE|CURRENT_TIME|CURDATE|CURTIME|UTC_TIMESTAMP)"
    r"(?:\(\s*\d*\s*\))?$",
    re.IGNORECASE,
)
_MYSQL_BIT_HEX_LITERAL_RE = re.compile(r"^[bBxX]'[0-9a-fA-F]*'$")
_UNPARSEABLE_DEFAULT: Final[object] = object()
_RESERVED_IDENTIFIER_WORDS = frozenset({
    "ACCESS",
    "ADD",
    "ALL",
    "ALTER",
    "AND",
    "ANY",
    "AS",
    "ASC",
    "AUDIT",
    "BETWEEN",
    "BY",
    "CHAR",
    "CHECK",
    "CLUSTER",
    "COLUMN",
    "COMMENT",
    "COMPRESS",
    "CONNECT",
    "CREATE",
    "CURRENT",
    "DATE",
    "DECIMAL",
    "DEFAULT",
    "DELETE",
    "DESC",
    "DISTINCT",
    "DROP",
    "ELSE",
    "EXCLUSIVE",
    "EXISTS",
    "FILE",
    "FLOAT",
    "FOR",
    "FROM",
    "GRANT",
    "GROUP",
    "HAVING",
    "IDENTIFIED",
    "IMMEDIATE",
    "IN",
    "INCREMENT",
    "INDEX",
    "INITIAL",
    "INSERT",
    "INTEGER",
    "INTERSECT",
    "INTO",
    "IS",
    "LEVEL",
    "LIKE",
    "LOCK",
    "LONG",
    "MAXEXTENTS",
    "MINUS",
    "MLSLABEL",
    "MODE",
    "MODIFY",
    "NOAUDIT",
    "NOCOMPRESS",
    "NOT",
    "NOWAIT",
    "NULL",
    "NUMBER",
    "OF",
    "OFFLINE",
    "ON",
    "ONLINE",
    "OPTION",
    "OR",
    "ORDER",
    "PCTFREE",
    "PRIOR",
    "PUBLIC",
    "RAW",
    "RENAME",
    "RESOURCE",
    "REVOKE",
    "ROW",
    "ROWID",
    "ROWNUM",
    "ROWS",
    "SELECT",
    "SESSION",
    "SET",
    "SHARE",
    "SIZE",
    "SMALLINT",
    "START",
    "SUCCESSFUL",
    "SYNONYM",
    "SYSDATE",
    "TABLE",
    "THEN",
    "TO",
    "TRIGGER",
    "UID",
    "UNION",
    "UNIQUE",
    "UPDATE",
    "USER",
    "VALIDATE",
    "VALUES",
    "VARCHAR",
    "VARCHAR2",
    "VIEW",
    "WHENEVER",
    "WHERE",
    "WITH",
})
_CHAR_LENGTH_TYPES = frozenset({
    "binary",
    "bit",
    "bit varying",
    "bytes",
    "char",
    "char varying",
    "character",
    "character varying",
    "national character",
    "national character varying",
    "nchar",
    "nvarchar",
    "nvarchar2",
    "raw",
    "string",
    "varbinary",
    "varbit",
    "varchar",
    "varchar2",
})
_UNICODE_CHAR_TYPES = frozenset({"nchar", "nvarchar", "national character", "national character varying"})
_DECIMAL_TYPES = frozenset({"dec", "decimal", "number", "numeric"})
_TEMPORAL_SCALE_TYPES = frozenset({"datetime2", "datetimeoffset", "time"})
_ORACLE_ON_DELETE_ACTIONS = frozenset({FOREIGN_KEY_ACTION_CASCADE, FOREIGN_KEY_ACTION_SET_NULL})
_NO_PARTIAL_INDEX_DIALECTS = frozenset({"duckdb"})
_NO_CASCADE_SEQUENCE_DIALECTS = frozenset({"tsql", "oracle", "spanner", "bigquery", "db2"})
_COMMENT_ON_DIALECTS = frozenset({"postgres", "oracle", "db2", "duckdb", "snowflake"})
_INCLUDE_INDEX_DIALECTS = frozenset({"postgres", "tsql", "db2"})
_STORING_INDEX_DIALECTS = frozenset({"spanner", "spangres"})
_PARENTHESIZED_DEFAULT_DIALECTS = frozenset({"sqlite", "mysql", "spanner"})
_SELF_PARENTHESIZED_GENERATED_DIALECTS = frozenset({"postgres", "mysql", "sqlite"})
_GENERIC_NORMALIZATION_DIALECTS = frozenset({"oracle", "db2", "snowflake"})
_AUTO_INCREMENT_IDENTITY_DIALECTS = frozenset({"mysql", "sqlite", "duckdb"})
_VIRTUAL_ONLY_GENERATED_DIALECTS = frozenset({"duckdb", "oracle"})
_STORED_GENERATED_DEFAULT_DIALECTS = frozenset({"postgres", "spanner"})
_MSSQL_STORAGE_INDEX_TYPES = frozenset({
    "CLUSTERED",
    "NONCLUSTERED",
    "HEAP",
    "XML",
    "SPATIAL",
    "CLUSTERED COLUMNSTORE",
    "NONCLUSTERED COLUMNSTORE",
    "CLUSTERED_COLUMNSTORE",
    "NONCLUSTERED_COLUMNSTORE",
})


def build_column_expression(
    col: "ColumnDefinition", dialect: "DialectType | None" = None, *, include_primary_key: bool = True
) -> "exp.Expr":
    """Build SQLGlot expression for a column definition.

    Raises:
        SQLBuilderError: When the column uses an identity or auto-increment feature the
            target dialect cannot express.
    """
    norm_dialect = _normalize_dialect(dialect) if dialect else None
    col_def = exp.ColumnDef(
        this=_parse_ddl_identifier(col.name, dialect=dialect), kind=_parse_column_type(col.name, col.dtype, dialect)
    )
    inline_primary_key = col.primary_key and include_primary_key

    constraints: list[exp.ColumnConstraint] = []

    if col.not_null:
        constraints.append(exp.ColumnConstraint(kind=exp.NotNullColumnConstraint()))

    if inline_primary_key:
        constraints.append(exp.ColumnConstraint(kind=exp.PrimaryKeyColumnConstraint()))

    if col.unique:
        constraints.append(exp.ColumnConstraint(kind=exp.UniqueColumnConstraint()))

    identity_constraint = _build_identity_column_constraint(col, dialect=dialect)
    use_auto_increment = col.auto_increment or (
        identity_constraint is not None and norm_dialect in _AUTO_INCREMENT_IDENTITY_DIALECTS
    )
    if use_auto_increment:
        if norm_dialect == "duckdb":
            msg = (
                f"Column {col.name!r}: DuckDB has no AUTO_INCREMENT or identity columns; "
                "create a sequence with CreateSequence and use DEFAULT nextval('<sequence>') instead"
            )
            raise SQLBuilderError(msg)
        if norm_dialect == "sqlite" and not inline_primary_key:
            msg = f"Column {col.name!r}: SQLite AUTOINCREMENT requires an inline INTEGER PRIMARY KEY column"
            raise SQLBuilderError(msg)
        constraints.append(exp.ColumnConstraint(kind=exp.AutoIncrementColumnConstraint(this=False)))
    elif identity_constraint is not None:
        constraints.append(identity_constraint)

    if col.default is not None:
        constraints.append(
            exp.ColumnConstraint(kind=exp.DefaultColumnConstraint(this=_build_default_expression(col.default, dialect)))
        )

    if col.check:
        constraints.append(exp.ColumnConstraint(kind=exp.Check(this=exp.maybe_parse(col.check, dialect=norm_dialect))))

    if col.comment:
        constraints.append(exp.ColumnConstraint(kind=exp.CommentColumnConstraint(this=exp.convert(col.comment))))

    computed_constraint = _build_computed_column_constraint(col, dialect=dialect)
    if computed_constraint is not None:
        constraints.append(computed_constraint)

    if col.collate:
        constraints.append(_build_collate_column_constraint(col.collate, dialect=dialect))

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
        on_delete, on_update = _dialect_foreign_key_actions(constraint, dialect)
        if on_delete:
            reference_options.append(f"ON DELETE {on_delete}")
        if on_update:
            reference_options.append(f"ON UPDATE {on_update}")

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
        unique_constraint = exp.UniqueColumnConstraint(
            this=exp.Tuple(expressions=[_parse_ddl_identifier(col, dialect=dialect) for col in constraint.columns])
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

    def _optimize_expression(
        self, expression: exp.Expr, *, force: bool = False, dialect: "DialectType" = None
    ) -> exp.Expr:
        """Normalize DDL identifiers without disturbing explicitly quoted names.

        Statements without an embedded query only need identifier normalization. SQLGlot
        folds even quoted identifiers on case-insensitive dialects (T-SQL, SQLite, DuckDB),
        which would rename catalog objects, so identifiers carrying an explicit quote marker
        are left exactly as supplied. Statements embedding a query (CTAS, views) keep the
        full query optimizer.
        """
        if not force and not self.enable_optimization:
            return expression
        if expression.find(exp.Query) is not None:
            return super()._optimize_expression(expression, force=force, dialect=dialect)
        target_dialect = _normalize_dialect(dialect) if dialect is not None else self.dialect_name
        normalizer = Dialect.get_or_raise(None if target_dialect in _GENERIC_NORMALIZATION_DIALECTS else target_dialect)
        return expression.transform(lambda node: _normalize_ddl_identifier(node, normalizer), copy=True)

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
        "generated_expression",
        "identity",
        "name",
        "not_null",
        "persisted",
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
        generated: "str | bool | None" = None,
        collate: "str | None" = None,
        identity: "str | bool | None" = None,
        generated_expression: "str | exp.Expr | None" = None,
        persisted: "bool | None" = None,
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
        self.identity = identity
        self.generated_expression = generated_expression
        self.persisted = persisted

    def to_sqlglot(self, dialect: "DialectType | None" = None, *, include_primary_key: bool = True) -> "exp.Expr":
        """Build a SQLGlot ColumnDef expression for this column definition."""
        return build_column_expression(self, dialect=dialect, include_primary_key=include_primary_key)


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
        generated: "str | bool | None" = None,
        collate: "str | None" = None,
        identity: "str | bool | None" = None,
        generated_expression: "str | exp.Expr | None" = None,
        persisted: "bool | None" = None,
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
            identity=identity,
            generated_expression=generated_expression,
            persisted=persisted,
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

    @classmethod
    def from_metadata(
        cls,
        table_name: str,
        columns: "Sequence[Mapping[str, Any] | Any]",
        constraints: "Sequence[Mapping[str, Any] | Any] | None" = None,
        foreign_keys: "Sequence[Mapping[str, Any] | Any] | None" = None,
        schema: "str | None" = None,
        if_not_exists: bool = False,
        dialect: "DialectType" = None,
        *,
        source_dialect: "DialectType" = None,
        warnings: "list[str] | None" = None,
    ) -> "CreateTable":
        """Build a CreateTable builder from structured data_dictionary metadata rows.

        Names are raw catalog identifiers and are quoted whenever the bare spelling would
        not resolve to the exact catalog name in ``dialect``. Catalog SQL fragments
        (defaults, CHECK and generated expressions) are parsed with ``source_dialect``
        (defaults to ``dialect``).

        Args:
            table_name: Raw catalog table name.
            columns: Column metadata rows.
            constraints: Constraint metadata rows.
            foreign_keys: Foreign key metadata rows.
            schema: Raw catalog schema name.
            if_not_exists: Whether to emit ``IF NOT EXISTS``.
            dialect: Target dialect for the generated DDL.
            source_dialect: Dialect the catalog SQL fragments are written in.
            warnings: Collector for metadata that cannot be reproduced faithfully. When
                omitted, any such loss raises instead of being dropped.

        Returns:
            Configured CreateTable builder.

        Raises:
            SQLBuilderError: When metadata cannot be reproduced and no ``warnings`` collector is given.
        """
        collected: list[str] = []
        fragment_dialect = source_dialect if source_dialect is not None else dialect
        builder = cls(_catalog_name(table_name, dialect, fragment_dialect), dialect=dialect)
        if schema:
            builder.in_schema(_catalog_name(schema, dialect, fragment_dialect))
        if if_not_exists:
            builder.if_not_exists()

        norm_columns = [
            row for row in (_normalize_metadata_row(col) for col in columns) if not _is_hidden_metadata_column(row)
        ]
        if any(row.get("ordinal_position") is not None for row in norm_columns):
            indexed_cols = list(enumerate(norm_columns))
            indexed_cols.sort(key=lambda item: (_safe_int(item[1].get("ordinal_position"), item[0] + 1), item[0]))
            norm_columns = [item[1] for item in indexed_cols]

        norm_constraints = [_normalize_metadata_row(c) for c in constraints or ()]
        norm_fks = [_normalize_metadata_row(fk) for fk in foreign_keys or ()]

        default_constraint_map: dict[str, Any] = {}
        for c_row in norm_constraints:
            c_type_raw = str(_first_present(c_row, "constraint_type", "type") or "").strip().upper()
            if c_type_raw in {"DEFAULT", "DEFAULT_CONSTRAINT"}:
                c_col = _first_non_empty_str(c_row, "column_name", "name")
                c_def = _first_present(c_row, "definition", "default", "column_default")
                if c_col and c_def is not None:
                    default_constraint_map[c_col] = c_def

        pk_cols, pk_name = _extract_metadata_primary_key(norm_columns, norm_constraints)
        use_inline_pk = len(pk_cols) == 1 and pk_name is None

        unique_specs: list[tuple[str | None, list[str]]] = []
        seen_unique_cols: set[tuple[str, ...]] = set()
        for c_row in norm_constraints:
            if _normalize_constraint_type(c_row) != CONSTRAINT_TYPE_UNIQUE:
                continue
            uq_name = _first_non_empty_str(c_row, "constraint_name", "name")
            uq_cols = _extract_constraint_columns(c_row)
            if not uq_cols:
                collected.append(f"UNIQUE constraint {uq_name or '<unnamed>'} has no catalog columns and was omitted")
                continue
            if tuple(uq_cols) == tuple(pk_cols) or tuple(uq_cols) in seen_unique_cols:
                continue
            seen_unique_cols.add(tuple(uq_cols))
            unique_specs.append((uq_name, uq_cols))

        for row in norm_columns:
            col_name = _first_non_empty_str(row, "column_name", "name")
            if not col_name:
                continue
            builder._add_metadata_column(
                row,
                col_name,
                dialect=dialect,
                fragment_dialect=fragment_dialect,
                primary_key=use_inline_pk and col_name == pk_cols[0],
                unique=_truthy_metadata_flag(_first_present(row, "is_unique", "unique"))
                and col_name not in pk_cols
                and (col_name,) not in seen_unique_cols,
                fallback_default=default_constraint_map.get(col_name),
                warnings=collected,
            )

        if pk_cols and not use_inline_pk:
            builder.primary_key_constraint(
                [_catalog_name(col, dialect, fragment_dialect) for col in pk_cols],
                name=_catalog_name(pk_name, dialect, fragment_dialect) if pk_name else None,
            )

        for uq_name, uq_cols in unique_specs:
            builder.unique_constraint(
                [_catalog_name(col, dialect, fragment_dialect) for col in uq_cols],
                name=_catalog_name(uq_name, dialect, fragment_dialect) if uq_name else None,
            )

        seen_checks: set[str] = set()
        for c_row in norm_constraints:
            if _normalize_constraint_type(c_row) != CONSTRAINT_TYPE_CHECK:
                continue
            chk_name = _first_non_empty_str(c_row, "constraint_name", "name")
            raw_cond = _first_non_empty_str(c_row, "check_clause", "condition", "definition", "expression")
            cond = _strip_check_wrapper(raw_cond) if raw_cond else None
            if not cond:
                collected.append(
                    f"CHECK constraint {chk_name or '<unnamed>'} has no catalog expression and was omitted"
                )
                continue
            dedup_key = chk_name or cond
            if dedup_key in seen_checks:
                continue
            seen_checks.add(dedup_key)
            cond_expr = _parse_catalog_fragment(cond, fragment_dialect)
            if cond_expr is None:
                collected.append(
                    f"CHECK constraint {chk_name or '<unnamed>'} could not be parsed and was omitted: {cond}"
                )
                continue
            builder._constraints.append(
                ConstraintDefinition(
                    constraint_type=CONSTRAINT_TYPE_CHECK,
                    name=_catalog_name(chk_name, dialect, fragment_dialect) if chk_name else None,
                    condition_expr=cond_expr,
                )
            )

        for fk_spec in _extract_metadata_foreign_keys(
            norm_constraints,
            norm_fks,
            table_schema=schema,
            dialect=dialect,
            source_dialect=fragment_dialect,
            warnings=collected,
        ):
            on_delete, on_update = _supported_foreign_key_actions(
                fk_spec["on_delete"], fk_spec["on_update"], dialect, name=fk_spec["name"], warnings=collected
            )
            builder.foreign_key_constraint(
                columns=fk_spec["columns"],
                references_table=fk_spec["ref_table"],
                references_columns=fk_spec["ref_columns"],
                name=fk_spec["name"],
                on_delete=on_delete,
                on_update=on_update,
                deferrable=fk_spec["deferrable"],
                initially_deferred=fk_spec["initially_deferred"],
            )

        _deliver_metadata_warnings(collected, warnings, context=f"table {table_name!r}")
        return builder

    @classmethod
    def from_arrow_schema(
        cls,
        table_name: str,
        arrow_schema: Any,
        *,
        schema: "str | None" = None,
        primary_key: "str | Sequence[str] | None" = None,
        if_not_exists: bool = False,
        dialect: "DialectType" = None,
    ) -> "CreateTable":
        """Build a CreateTable builder from a pyarrow.Schema or pyarrow.Table."""
        resolved_schema = getattr(arrow_schema, "schema", arrow_schema)
        fields = list(resolved_schema)
        if not fields:
            msg = "Cannot infer table schema from empty Arrow schema"
            raise SQLBuilderError(msg)

        builder = cls(table_name, dialect=dialect)
        if schema:
            builder.in_schema(schema)
        if if_not_exists:
            builder.if_not_exists()

        pk_cols = [primary_key] if isinstance(primary_key, str) else [str(col) for col in (primary_key or ()) if col]
        use_inline_pk = len(pk_cols) == 1
        dialect_str = _normalize_dialect(dialect) if dialect else None
        text_type = sql_type_for_category("text", dialect_str)

        for field in fields:
            col_name = str(field.name)
            dtype = sql_type_from_arrow_type(field.type, dialect=dialect_str)
            is_pk_member = col_name in pk_cols
            if is_pk_member and dtype == text_type:
                dtype = sql_type_for_category("key_text", dialect_str)
            not_null = is_pk_member or not bool(getattr(field, "nullable", True))
            builder.column(
                name=_record_field_name(col_name, dialect_str),
                dtype=dtype,
                not_null=not_null,
                primary_key=use_inline_pk and is_pk_member,
            )

        if len(pk_cols) > 1:
            builder.primary_key_constraint([_record_field_name(col, dialect_str) for col in pk_cols])

        return builder

    @classmethod
    def from_records(
        cls,
        table_name: str,
        records: "Sequence[Mapping[str, Any] | Any]",
        *,
        schema: "str | None" = None,
        primary_key: "str | Sequence[str] | None" = None,
        if_not_exists: bool = False,
        dialect: "DialectType" = None,
    ) -> "CreateTable":
        """Infer a CreateTable builder from a sequence of mapping or struct records."""
        if not records:
            msg = "Cannot infer table schema from empty records"
            raise SQLBuilderError(msg)

        norm_records = [row if isinstance(row, Mapping) else _normalize_metadata_row(row) for row in records]
        ordered_columns: list[str] = []
        seen_columns: set[str] = set()
        for row in norm_records:
            for key in row:
                col_str = str(key)
                if col_str not in seen_columns:
                    seen_columns.add(col_str)
                    ordered_columns.append(col_str)

        if not ordered_columns:
            msg = "Cannot infer table schema from empty records"
            raise SQLBuilderError(msg)

        builder = cls(table_name, dialect=dialect)
        if schema:
            builder.in_schema(schema)
        if if_not_exists:
            builder.if_not_exists()

        pk_cols = [primary_key] if isinstance(primary_key, str) else [str(col) for col in (primary_key or ()) if col]
        use_inline_pk = len(pk_cols) == 1
        dialect_str = _normalize_dialect(dialect) if dialect else None

        for col_name in ordered_columns:
            has_null = False
            values: list[Any] = []
            for row in norm_records:
                if col_name not in row or row[col_name] is None:
                    has_null = True
                else:
                    values.append(row[col_name])

            is_pk_member = col_name in pk_cols
            not_null = is_pk_member or not has_null
            dtype = _infer_sql_type_from_values(values, dialect=dialect_str, key=is_pk_member)
            builder.column(
                name=_record_field_name(col_name, dialect_str),
                dtype=dtype,
                not_null=not_null,
                primary_key=use_inline_pk and is_pk_member,
            )

        if len(pk_cols) > 1:
            builder.primary_key_constraint([_record_field_name(col, dialect_str) for col in pk_cols])

        return builder

    def _create_base_expression(self) -> "exp.Expr":
        """Create the SQLGlot expression for CREATE TABLE."""
        self._require(self._columns or self._like_table, "Table must have at least one column or use LIKE clause")

        effective_dialect = self._expression_dialect or self.dialect_name
        primary_key_after_schema = _normalize_dialect(effective_dialect) == "spanner"
        has_table_level_pk = primary_key_after_schema or any(
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
            if primary_key_after_schema and constraint.constraint_type == CONSTRAINT_TYPE_PRIMARY_KEY:
                continue
            if self._is_redundant_single_column_primary_key(constraint):
                continue

            constraint_expr = build_constraint_expression(constraint, dialect=effective_dialect)
            if constraint_expr:
                column_defs.append(constraint_expr)

        props: list[exp.Expr] = []
        primary_key_constraint = self._find_primary_key_constraint()
        if primary_key_after_schema and primary_key_constraint is not None:
            props.append(
                exp.PrimaryKey(
                    expressions=[
                        exp.Ordered(this=_parse_ddl_identifier(col, dialect=effective_dialect), nulls_first=True)
                        for col in primary_key_constraint.columns
                    ]
                )
            )
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

    def _add_metadata_column(
        self,
        row: "Mapping[str, Any]",
        col_name: str,
        *,
        dialect: "DialectType",
        fragment_dialect: "DialectType",
        primary_key: bool,
        unique: bool,
        fallback_default: Any,
        warnings: "list[str]",
    ) -> None:
        """Add one catalog column, recording any attribute that cannot be reproduced in ``warnings``."""
        computed_source, persisted, is_generated = _resolve_metadata_computed(row)
        computed_expr: exp.Expr | None = None
        if computed_source is not None:
            computed_expr = (
                computed_source
                if isinstance(computed_source, exp.Expr)
                else _parse_catalog_fragment(computed_source, fragment_dialect)
            )
            if computed_expr is None:
                warnings.append(
                    f"Column {col_name!r} generated expression could not be parsed; emitted as a plain column: "
                    f"{computed_source}"
                )
        elif is_generated:
            warnings.append(
                f"Column {col_name!r} is a generated column but the catalog did not report its expression; "
                "emitted as a plain column"
            )
        norm_dialect = _normalize_dialect(dialect) if dialect else None
        if computed_expr is not None and persisted and norm_dialect in _VIRTUAL_ONLY_GENERATED_DIALECTS:
            warnings.append(
                f"Column {col_name!r} is a stored generated column; {norm_dialect} supports only virtual generated "
                "columns, so it is emitted as virtual"
            )
            persisted = False
        is_computed = computed_expr is not None or is_generated
        identity_val = None if is_computed else _resolve_metadata_identity(row)
        auto_increment = False if (is_computed or identity_val is not None) else _resolve_metadata_auto_increment(row)
        if (identity_val is not None or auto_increment) and (
            norm_dialect in {"duckdb", "bigquery"} or (norm_dialect == "sqlite" and not primary_key)
        ):
            warnings.append(
                f"Column {col_name!r} is an identity/auto-increment column; {norm_dialect} cannot express it on this "
                "column, so it is emitted without automatic value generation"
            )
            identity_val = None
            auto_increment = False
        raw_default = (
            None
            if (is_computed or identity_val is not None or auto_increment)
            else (_first_present(row, "column_default", "default", "default_value") or fallback_default)
        )
        default_val: Any = None
        if raw_default is not None:
            default_val = _parse_metadata_default(
                raw_default, source_dialect=fragment_dialect, extra=str(row.get("extra") or "")
            )
            if default_val is _UNPARSEABLE_DEFAULT:
                warnings.append(f"Column {col_name!r} default could not be parsed and was omitted: {raw_default}")
                default_val = None
        dtype = _translate_catalog_type(
            _resolve_metadata_column_type(row, dialect=fragment_dialect), fragment_dialect, dialect
        )
        if _INLINE_ENUM_TYPE_RE.match(dtype) and norm_dialect not in _INLINE_ENUM_DIALECTS:
            warnings.append(
                f"Column {col_name!r} uses an inline ENUM type that {norm_dialect or 'the target dialect'} cannot "
                "declare; emitted as text"
            )
            dtype = sql_type_for_category("text", norm_dialect)
        col_comment = (
            _first_non_empty_str(row, "comment", "column_comment", "description") if norm_dialect == "mysql" else None
        )
        self.column(
            name=_catalog_name(col_name, dialect, fragment_dialect),
            dtype=dtype,
            default=default_val,
            not_null=_resolve_metadata_not_null(row),
            primary_key=primary_key,
            unique=unique,
            auto_increment=auto_increment,
            comment=col_comment,
            check=_first_non_empty_str(row, "check"),
            collate=_first_non_empty_str(row, "collate", "collation", "collation_name"),
            identity=identity_val,
            generated_expression=computed_expr,
            persisted=persisted,
        )


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

    __slots__ = (
        "_clustered",
        "_columns",
        "_if_not_exists",
        "_include_columns",
        "_index_name",
        "_schema",
        "_table_name",
        "_unique",
        "_using",
        "_where",
    )

    def __init__(
        self,
        index_name: str,
        dialect: "DialectType" = None,
        include: "Sequence[str | exp.Expr] | None" = None,
        schema: "str | None" = None,
    ) -> None:
        """Initialize CREATE INDEX with index name.

        Args:
            index_name: Name of the index to create
            dialect: SQL dialect to use
            include: Optional sequence of covering columns for INCLUDE (...)
            schema: Optional schema name for the target table
        """
        super().__init__(dialect=dialect)
        self._index_name = index_name
        self._table_name: str | None = None
        self._schema: str | None = schema
        self._columns: list[str | exp.Ordered | exp.Expr] = []
        self._include_columns: list[str | exp.Expr] = list(include) if include is not None else []
        self._unique = False
        self._if_not_exists = False
        self._using: str | None = None
        self._where: str | exp.Expr | None = None
        self._clustered: bool | None = None

    def name(self, index_name: str) -> Self:
        self._index_name = index_name
        return self

    def in_schema(self, schema_name: str) -> Self:
        """Set the schema for the indexed table."""
        self._schema = schema_name
        return self

    def on(self, table_name: str, *cols: str | exp.Ordered | exp.Expr) -> Self:
        """Set the target table (and optional index columns) for the index."""
        self._table_name = table_name
        if cols:
            self._columns.extend(cols)
        return self

    def on_table(self, table_name: str) -> Self:
        self._table_name = table_name
        return self

    def columns(self, *cols: str | exp.Ordered | exp.Expr) -> Self:
        self._columns.extend(cols)
        return self

    def include(self, *columns: str | exp.Expr) -> Self:
        """Add covering columns for the INCLUDE (...) clause."""
        self._include_columns.extend(columns)
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

    def clustered(self, value: bool = True) -> Self:
        """Mark the index ``CLUSTERED`` (or ``NONCLUSTERED`` when ``value`` is false) on T-SQL.

        Other dialects have no clustered-index DDL and ignore the setting.
        """
        self._clustered = value
        return self

    @classmethod
    def from_metadata(
        cls,
        index: "Mapping[str, Any] | Any",
        *,
        table_name: "str | None" = None,
        schema: "str | None" = None,
        dialect: "DialectType" = None,
        source_dialect: "DialectType" = None,
        known_columns: "Sequence[str] | None" = None,
        warnings: "list[str] | None" = None,
    ) -> "CreateIndex | None":
        """Build a CreateIndex builder from a structured index metadata row or IndexDetails object.

        Native index SQL reported by the catalog (``native_sql`` or a ``CREATE ... INDEX``
        ``definition``) is parsed for key expressions, sort order, and the partial-index
        predicate. Otherwise the catalog column list is used; entries that match
        ``known_columns`` or are not expression-like are treated as identifiers.

        Args:
            index: Index metadata row.
            table_name: Raw catalog table name (defaults to the row's table name).
            schema: Raw catalog schema name (defaults to the row's schema).
            dialect: Target dialect for the generated DDL.
            source_dialect: Dialect of catalog SQL fragments (defaults to ``dialect``).
            known_columns: Raw names of the table's columns.
            warnings: Collector for indexes that cannot be reproduced faithfully. When
                omitted, such an index raises instead of being dropped.

        Returns:
            Configured builder, or ``None`` for primary-key, unique-constraint, and
            internal indexes (and for unreproducible indexes when ``warnings`` is given).

        Raises:
            SQLBuilderError: When the index cannot be reproduced and no ``warnings`` collector is given.
        """
        collected: list[str] = []
        builder = cls._from_metadata_row(
            _normalize_metadata_row(index),
            table_name=table_name,
            schema=schema,
            dialect=dialect,
            source_dialect=source_dialect,
            known_columns=known_columns,
            warnings=collected,
        )
        _deliver_metadata_warnings(collected, warnings, context="index")
        return builder

    @classmethod
    def _from_metadata_row(
        cls,
        row: "Mapping[str, Any]",
        *,
        table_name: "str | None",
        schema: "str | None",
        dialect: "DialectType",
        source_dialect: "DialectType",
        known_columns: "Sequence[str] | None",
        warnings: "list[str]",
    ) -> "CreateIndex | None":
        if _truthy_metadata_flag(_first_present(row, "is_primary", "is_primary_key", "primary_key")):
            return None
        if _truthy_metadata_flag(row.get("is_unique_constraint")):
            return None

        index_name = _first_non_empty_str(row, "index_name", "name")
        if not index_name or index_name.lower().startswith("sqlite_autoindex_"):
            return None

        resolved_table = table_name or _first_non_empty_str(row, "table_name", "table")
        resolved_schema = (
            schema if schema is not None else _first_non_empty_str(row, "schema_name", "schema", "table_schema")
        )
        if not resolved_table:
            return None

        norm_dialect = _normalize_dialect(dialect) if dialect else None
        if norm_dialect == "bigquery":
            warnings.append(
                f"BigQuery does not support secondary CREATE INDEX statements; index '{index_name}' omitted"
            )
            return None

        fragment_dialect = source_dialect if source_dialect is not None else dialect
        storage_type = str(row.get("type_desc") or "").strip().upper().replace("_", " ")
        if storage_type and storage_type not in {"CLUSTERED", "NONCLUSTERED", "HEAP"}:
            warnings.append(f"Index {index_name}: {storage_type} indexes are not reproduced; index omitted")
            return None

        key_exprs: list[exp.Expr] | None = None
        where_expr: exp.Expr | None = None
        native_sql = _native_index_sql(row)
        if native_sql is not None:
            native_parts = _parse_native_index_sql(native_sql, fragment_dialect)
            if native_parts is not None:
                key_exprs, where_expr = native_parts

        if key_exprs is None:
            key_exprs = _metadata_index_key_expressions(
                row,
                index_name,
                dialect=dialect,
                fragment_dialect=fragment_dialect,
                known_columns=known_columns,
                warnings=warnings,
            )
            if key_exprs is None:
                return None
            where_text = _first_non_empty_str(row, "where", "filter_definition", "predicate")
            has_filter_val = row.get("has_filter")
            is_partial = _truthy_metadata_flag(has_filter_val) or _truthy_metadata_flag(row.get("is_partial"))
            if where_text and (has_filter_val is None or _truthy_metadata_flag(has_filter_val)):
                where_expr = _parse_catalog_fragment(where_text, fragment_dialect)
                if where_expr is None:
                    warnings.append(f"Index {index_name}: predicate could not be parsed ({where_text}); index omitted")
                    return None
            elif is_partial:
                warnings.append(f"Index {index_name} is partial but the catalog did not report its predicate; omitted")
                return None

        if where_expr is not None and norm_dialect in _NO_PARTIAL_INDEX_DIALECTS:
            warnings.append(f"DuckDB does not support partial indexes; index '{index_name}' omitted")
            return None

        builder = cls(_catalog_name(index_name, dialect, fragment_dialect), dialect=dialect)
        if resolved_schema:
            builder.in_schema(_catalog_name(resolved_schema, dialect, fragment_dialect))
        builder.on(_catalog_name(resolved_table, dialect, fragment_dialect), *key_exprs)

        if _truthy_metadata_flag(_first_present(row, "is_unique", "unique")):
            builder.unique()

        inc_cols = _split_metadata_columns(_first_present(row, "included_columns", "include", "include_columns"))
        if inc_cols:
            if (
                norm_dialect is None
                or norm_dialect in _INCLUDE_INDEX_DIALECTS
                or norm_dialect in _STORING_INDEX_DIALECTS
            ):
                builder.include(*(_catalog_name(col, dialect, fragment_dialect) for col in inc_cols))
            else:
                warnings.append(
                    f"Index {index_name}: covering INCLUDE columns are not supported on {norm_dialect} and were omitted"
                )

        if where_expr is not None:
            builder.where(where_expr)

        if storage_type in {"CLUSTERED", "NONCLUSTERED"}:
            builder.clustered(storage_type == "CLUSTERED")

        using_method = _first_non_empty_str(row, "using", "index_type", "access_method")
        if using_method:
            using_upper = using_method.strip().upper().replace("_", " ")
            if using_upper not in _MSSQL_STORAGE_INDEX_TYPES and not (
                using_upper == "BTREE" and norm_dialect in {"tsql", "mssql", "sqlite"}
            ):
                builder.using(using_method.strip())

        return builder

    def _create_base_expression(self) -> exp.Expr:
        """Build the CREATE INDEX expression for this builder.

        String columns become identifiers unless they are expression-like (parentheses,
        operators, literals) or carry an explicit ASC/DESC suffix; names that are not bare
        identifiers (for example ``Customer Id``) are quoted rather than parsed. String
        ``where`` clauses are parsed with the target dialect.
        """
        self._require(self._index_name and self._table_name, "Index name and table name must be set for CREATE INDEX.")

        effective_dialect = self._expression_dialect or self.dialect_name
        norm_dialect = _normalize_dialect(effective_dialect) if effective_dialect else None
        cols: list[exp.Expr] = [
            _index_column_expression(col, effective_dialect) if isinstance(col, str) else col.copy()
            for col in self._columns
        ]

        include_cols: list[exp.Expr] = []
        for inc_col in self._include_columns:
            if isinstance(inc_col, str):
                include_cols.append(_parse_ddl_identifier(inc_col, dialect=effective_dialect))
            else:
                include_cols.append(inc_col.copy())

        where_expr: exp.Expr | None = None
        if self._where is not None:
            if norm_dialect in _NO_PARTIAL_INDEX_DIALECTS:
                self._raise_builder_error(
                    "DuckDB does not support partial indexes; a full index would change the index semantics."
                )
            where_expr = (
                exp.condition(self._where, dialect=norm_dialect) if isinstance(self._where, str) else self._where.copy()
            )

        index_params = exp.IndexParameters(columns=cols) if cols else None

        if include_cols:
            if index_params is None:
                index_params = exp.IndexParameters()
            index_params.set("include", include_cols)

        if self._using:
            if index_params is None:
                index_params = exp.IndexParameters()
            index_params.set("using", exp.Var(this=self._using))

        if where_expr is not None:
            if index_params is None:
                index_params = exp.IndexParameters()
            index_params.set("where", exp.Where(this=where_expr))

        index_expr = exp.Index(
            this=_parse_ddl_identifier(self._index_name, dialect=effective_dialect),
            table=_parse_ddl_table(cast("str", self._table_name), schema=self._schema, dialect=effective_dialect),
            params=index_params,
        )

        kind = "INDEX"
        if self._clustered is not None and norm_dialect == "tsql":
            kind = "CLUSTERED INDEX" if self._clustered else "NONCLUSTERED INDEX"
        return exp.Create(kind=kind, this=index_expr, unique=self._unique, exists=self._if_not_exists)

    def _optimize_expression(
        self, expression: exp.Expr, *, force: bool = False, dialect: "DialectType" = None
    ) -> exp.Expr:
        """Optimize the CREATE INDEX expression while preserving explicit ASC ordering."""
        explicit_asc_positions: list[int] = []
        index_params = expression.find(exp.IndexParameters)
        if index_params is not None:
            for idx, col_expr in enumerate(index_params.args.get("columns") or []):
                if isinstance(col_expr, exp.Ordered) and col_expr.args.get("desc") is False:
                    explicit_asc_positions.append(idx)

        optimized = super()._optimize_expression(expression, force=force, dialect=dialect)
        if explicit_asc_positions:
            optimized = optimized.copy()
            opt_params = optimized.find(exp.IndexParameters)
            if opt_params is not None:
                opt_columns = opt_params.args.get("columns") or []
                for idx in explicit_asc_positions:
                    if idx < len(opt_columns) and isinstance(opt_columns[idx], exp.Ordered):
                        opt_columns[idx].set("desc", False)
        return optimized

    def _has_sql_postprocessing(self) -> bool:
        return bool(self._include_columns)

    def _postprocess_built_sql(self, sql_string: str, dialect: "DialectType" = None) -> str:
        target_dialect = _normalize_dialect(dialect) if dialect is not None else self.dialect_name
        if self._include_columns and target_dialect in _STORING_INDEX_DIALECTS:
            return sql_string.replace(" INCLUDE (", " STORING (", 1)
        return sql_string


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

    __slots__ = ("_column", "_comment", "_schema", "_table", "_target_type")

    def __init__(self, dialect: "DialectType" = None) -> None:
        """Initialize COMMENT ON builder.

        Args:
            dialect: SQL dialect to use
        """
        super().__init__(dialect=dialect)
        self._target_type: str | None = None
        self._table: str | None = None
        self._schema: str | None = None
        self._column: str | None = None
        self._comment: str | None = None

    def in_schema(self, schema_name: str) -> Self:
        """Set the schema for the target table or column."""
        self._schema = schema_name
        return self

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
                this=_parse_ddl_table(self._table, schema=self._schema, dialect=effective_dialect),
                kind="TABLE",
                expression=exp.convert(self._comment),
            )
        if self._target_type == "COLUMN" and self._table and self._column and self._comment is not None:
            table_expr = _parse_ddl_table(self._table, schema=self._schema, dialect=effective_dialect)
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


@mypyc_attr(allow_interpreted_subclasses=False)
class CreateSequence(DDLBuilder):
    """Builder for CREATE SEQUENCE statements."""

    __slots__ = (
        "_cache",
        "_cycle",
        "_if_not_exists",
        "_increment_by",
        "_max_value",
        "_min_value",
        "_schema",
        "_sequence_name",
        "_start_with",
    )

    def __init__(
        self,
        sequence_name: str,
        schema: "str | None" = None,
        if_not_exists: bool = False,
        start_with: "int | None" = None,
        increment_by: "int | None" = None,
        min_value: "int | None" = None,
        max_value: "int | None" = None,
        cycle: bool = False,
        cache: "int | None" = None,
        dialect: "DialectType" = None,
    ) -> None:
        super().__init__(dialect=dialect)
        self._sequence_name = sequence_name
        self._schema = schema
        self._if_not_exists = if_not_exists
        self._start_with = start_with
        self._increment_by = increment_by
        self._min_value = min_value
        self._max_value = max_value
        self._cycle = cycle
        self._cache = cache

    def name(self, sequence_name: str) -> Self:
        """Set the sequence name."""
        self._sequence_name = sequence_name
        return self

    def in_schema(self, schema_name: str) -> Self:
        """Set the schema for the sequence."""
        self._schema = schema_name
        return self

    def if_not_exists(self) -> Self:
        """Add IF NOT EXISTS to the CREATE SEQUENCE statement."""
        self._if_not_exists = True
        return self

    def start_with(self, value: int) -> Self:
        """Set the START WITH value for the sequence."""
        self._start_with = value
        return self

    def increment_by(self, value: int) -> Self:
        """Set the INCREMENT BY value for the sequence."""
        self._increment_by = value
        return self

    def min_value(self, value: int) -> Self:
        """Set the MINVALUE for the sequence."""
        self._min_value = value
        return self

    def max_value(self, value: int) -> Self:
        """Set the MAXVALUE for the sequence."""
        self._max_value = value
        return self

    def cycle(self) -> Self:
        """Enable CYCLE on the sequence."""
        self._cycle = True
        return self

    def no_cycle(self) -> Self:
        """Disable CYCLE on the sequence."""
        self._cycle = False
        return self

    def cache(self, value: int) -> Self:
        """Set the CACHE size for the sequence."""
        self._cache = value
        return self

    def _create_base_expression(self) -> exp.Expr:
        """Build the SQLGlot Create expression for this sequence.

        Raises:
            SQLBuilderError: On SQLite (no sequences), or when Spanner is asked for options
                other than a start counter (Spanner sequences are bit-reversed and accept only
                ``start_with_counter``).
        """
        self._require(self._sequence_name, "Sequence name must be set for CREATE SEQUENCE.")
        effective_dialect = self._expression_dialect or self.dialect_name
        norm_dialect = _normalize_dialect(effective_dialect) if effective_dialect else None
        if norm_dialect == "sqlite":
            self._raise_builder_error("SQLite has no sequences; use an INTEGER PRIMARY KEY column instead.")
        table_expr = _parse_ddl_table(self._sequence_name, schema=self._schema, dialect=effective_dialect)

        if norm_dialect == "spanner":
            unsupported = [
                option
                for option, value in (
                    ("INCREMENT BY", self._increment_by),
                    ("MINVALUE", self._min_value),
                    ("MAXVALUE", self._max_value),
                    ("CACHE", self._cache),
                )
                if value is not None
            ]
            if self._cycle:
                unsupported.append("CYCLE")
            if unsupported:
                self._raise_builder_error(
                    f"Spanner sequences do not support {', '.join(unsupported)}; only start_with is available."
                )
            spanner_options: list[exp.Expr] = [
                exp.Property(this=exp.Var(this="sequence_kind"), value=exp.Literal.string("bit_reversed_positive"))
            ]
            if self._start_with is not None:
                spanner_options.append(
                    exp.Property(this=exp.Var(this="start_with_counter"), value=exp.Literal.number(self._start_with))
                )
            return exp.Create(
                this=table_expr,
                kind="SEQUENCE",
                exists=self._if_not_exists,
                properties=exp.Properties(expressions=spanner_options),
            )

        options: list[exp.Expr] = [exp.Var(this="CYCLE")] if self._cycle else []
        has_seq_props = any(
            v is not None for v in (self._start_with, self._increment_by, self._min_value, self._max_value, self._cache)
        ) or bool(options)

        properties_node: exp.Properties | None = None
        if has_seq_props:
            seq_props = exp.SequenceProperties(
                start=exp.Literal.number(self._start_with) if self._start_with is not None else None,
                increment=exp.Literal.number(self._increment_by) if self._increment_by is not None else None,
                minvalue=exp.Literal.number(self._min_value) if self._min_value is not None else None,
                maxvalue=exp.Literal.number(self._max_value) if self._max_value is not None else None,
                cache=exp.Literal.number(self._cache) if self._cache is not None else None,
                options=options or None,
            )
            properties_node = exp.Properties(expressions=[seq_props])

        return exp.Create(this=table_expr, kind="SEQUENCE", exists=self._if_not_exists, properties=properties_node)


@mypyc_attr(allow_interpreted_subclasses=False)
class DropSequence(DDLBuilder):
    """Builder for DROP SEQUENCE statements."""

    __slots__ = ("_cascade", "_if_exists", "_schema", "_sequence_name")

    def __init__(
        self,
        sequence_name: str,
        schema: "str | None" = None,
        if_exists: bool = False,
        cascade: bool = False,
        dialect: "DialectType" = None,
    ) -> None:
        super().__init__(dialect=dialect)
        self._sequence_name = sequence_name
        self._schema = schema
        self._if_exists = if_exists
        self._cascade = cascade

    def name(self, sequence_name: str) -> Self:
        """Set the sequence name."""
        self._sequence_name = sequence_name
        return self

    def in_schema(self, schema_name: str) -> Self:
        """Set the schema for the sequence."""
        self._schema = schema_name
        return self

    def if_exists(self) -> Self:
        """Add IF EXISTS to the DROP SEQUENCE statement."""
        self._if_exists = True
        return self

    def cascade(self) -> Self:
        """Add CASCADE to the DROP SEQUENCE statement."""
        self._cascade = True
        return self

    def _create_base_expression(self) -> exp.Expr:
        """Build the SQLGlot Drop expression for this sequence.

        Raises:
            SQLBuilderError: On SQLite (no sequences), or when ``CASCADE`` is requested on a
                dialect whose ``DROP SEQUENCE`` has no ``CASCADE`` option.
        """
        self._require(self._sequence_name, "Sequence name must be set for DROP SEQUENCE.")
        effective_dialect = self._expression_dialect or self.dialect_name
        norm_dialect = _normalize_dialect(effective_dialect) if effective_dialect else None
        if norm_dialect == "sqlite":
            self._raise_builder_error("SQLite has no sequences.")
        if self._cascade and norm_dialect in _NO_CASCADE_SEQUENCE_DIALECTS:
            self._raise_builder_error(f"DROP SEQUENCE ... CASCADE is not supported by {norm_dialect}.")
        table_expr = _parse_ddl_table(self._sequence_name, schema=self._schema, dialect=effective_dialect)
        return exp.Drop(
            this=table_expr, tables=[table_expr.copy()], kind="SEQUENCE", exists=self._if_exists, cascade=self._cascade
        )


def compile_table_ddl_results(
    table_name: str,
    columns: "Sequence[Mapping[str, Any] | Any]",
    *,
    schema: "str | None" = None,
    constraints: "Sequence[Mapping[str, Any] | Any] | None" = None,
    foreign_keys: "Sequence[Mapping[str, Any] | Any] | None" = None,
    indexes: "Sequence[Mapping[str, Any] | Any] | None" = None,
    table_comment: "str | None" = None,
    column_comments: "Mapping[str, str] | None" = None,
    dialect: "DialectType" = None,
    source_dialect: "DialectType" = None,
    object_type: str = "table",
    warnings: tuple[str, ...] = (),
) -> DDLResult:
    """Compile structured catalog metadata into a generated DDLResult for a table.

    Catalog attributes that cannot be reproduced on ``dialect`` (unreported generated
    expressions, partial-index predicates, expression index keys, unsupported referential
    actions, unparsable defaults) are listed in ``DDLResult.warnings`` and the result's
    fidelity becomes ``LOSSY`` instead of ``GENERATED``.

    Args:
        table_name: Raw catalog table name.
        columns: Column metadata rows.
        schema: Raw catalog schema name.
        constraints: Constraint metadata rows.
        foreign_keys: Foreign key metadata rows.
        indexes: Index metadata rows.
        table_comment: Optional table comment.
        column_comments: Optional column comments keyed by raw column name.
        dialect: Target dialect for the generated DDL.
        source_dialect: Dialect of catalog SQL fragments (defaults to ``dialect``).
        object_type: Object type recorded on the result identity.
        warnings: Caller warnings to carry on the result.

    Returns:
        Generated DDL result.
    """
    dialect_str = _normalize_dialect(dialect) if dialect else None
    identity = ObjectIdentity(
        name=table_name, object_type=object_type, schema=schema, dialect=dialect_str, source=MetadataSource.GENERATED
    )
    if not columns:
        return DDLResult.unsupported(
            identity,
            source=MetadataSource.GENERATED,
            warnings=warnings or (f"No catalog columns found for {schema or '<default>'}.{table_name}",),
        )

    lossy: list[str] = []
    normalized_columns = [_normalize_metadata_row(col) for col in columns]
    fragment_dialect = source_dialect if source_dialect is not None else dialect

    merged_column_comments: dict[str, str] = {}
    for row in normalized_columns:
        col_name = _first_non_empty_str(row, "column_name", "name")
        col_comment = _first_non_empty_str(row, "comment", "column_comment", "description")
        if col_name and col_comment:
            merged_column_comments[col_name] = col_comment
    if column_comments:
        merged_column_comments.update({
            col_name: col_comment for col_name, col_comment in column_comments.items() if col_name and col_comment
        })

    table_builder = CreateTable.from_metadata(
        table_name,
        normalized_columns,
        constraints=constraints,
        foreign_keys=foreign_keys,
        schema=schema,
        dialect=dialect,
        source_dialect=source_dialect,
        warnings=lossy,
    )
    if dialect_str == "mysql":
        if table_comment:
            table_builder.comment(table_comment)
        if merged_column_comments:
            by_builder_name = {
                _catalog_name(col_name, dialect, fragment_dialect): col_comment
                for col_name, col_comment in merged_column_comments.items()
            }
            for col_def in table_builder._columns:
                col_comment_val = by_builder_name.get(col_def.name) or merged_column_comments.get(col_def.name)
                if col_comment_val:
                    col_def.comment = col_comment_val
    statements: list[str] = [table_builder.build(dialect=dialect).sql.rstrip(";").strip()]

    known_columns = [
        col_name
        for col_name in (_first_non_empty_str(row, "column_name", "name") for row in normalized_columns)
        if col_name
    ]
    constraint_names: set[str] = {
        c.name
        for c in table_builder._constraints
        if c.name and c.constraint_type in {CONSTRAINT_TYPE_PRIMARY_KEY, CONSTRAINT_TYPE_UNIQUE}
    }
    constrained_column_tuples: set[tuple[str, ...]] = {
        tuple(c.columns)
        for c in table_builder._constraints
        if c.columns and c.constraint_type in {CONSTRAINT_TYPE_PRIMARY_KEY, CONSTRAINT_TYPE_UNIQUE}
    }
    for col_def in table_builder._columns:
        if col_def.primary_key or col_def.unique:
            constrained_column_tuples.add((col_def.name,))

    for index_row in _group_metadata_index_rows(indexes or ()):
        raw_idx_name = _first_non_empty_str(index_row, "index_name", "name")
        if raw_idx_name and _catalog_name(raw_idx_name, dialect, fragment_dialect) in constraint_names:
            continue
        idx_builder = CreateIndex._from_metadata_row(
            index_row,
            table_name=table_name,
            schema=schema or "",
            dialect=dialect,
            source_dialect=source_dialect,
            known_columns=known_columns,
            warnings=lossy,
        )
        if idx_builder is not None:
            if (
                idx_builder._unique
                and not idx_builder._where
                and not idx_builder._include_columns
                and all(isinstance(col_expr, exp.Column) for col_expr in idx_builder._columns)
            ):
                idx_cols = tuple(
                    _catalog_name(col, dialect, fragment_dialect)
                    for col in _split_metadata_columns(
                        _first_present(index_row, "columns", "column_names", "column_name")
                    )
                )
                if idx_cols and idx_cols in constrained_column_tuples:
                    continue
            statements.append(idx_builder.build(dialect=dialect).sql.rstrip(";").strip())

    if dialect_str is None or dialect_str in _COMMENT_ON_DIALECTS:
        quoted_table = _catalog_name(table_name, dialect, source_dialect)
        quoted_schema = _catalog_name(schema, dialect, source_dialect) if schema else None
        if table_comment:
            tbl_comment = CommentOn(dialect=dialect).on_table(quoted_table).is_(table_comment)
            if quoted_schema:
                tbl_comment.in_schema(quoted_schema)
            statements.append(tbl_comment.build(dialect=dialect).sql.rstrip(";").strip())

        for col_name, col_comment in merged_column_comments.items():
            col_comment_builder = (
                CommentOn(dialect=dialect)
                .on_column(quoted_table, _catalog_name(col_name, dialect, source_dialect))
                .is_(col_comment)
            )
            if quoted_schema:
                col_comment_builder.in_schema(quoted_schema)
            statements.append(col_comment_builder.build(dialect=dialect).sql.rstrip(";").strip())
    elif dialect_str != "mysql":
        if table_comment:
            lossy.append(f"Table comment is not supported by {dialect_str}; comment omitted")
        lossy.extend(
            f"Column comment on {col_name!r} is not supported by {dialect_str}; comment omitted"
            for col_name in merged_column_comments
        )

    ddl_text = ";\n\n".join(stmt for stmt in statements if stmt) + ";"
    return DDLResult(
        identity=identity,
        status=MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.LOSSY if lossy else MetadataFidelity.GENERATED,
        source=MetadataSource.GENERATED,
        ddl=ddl_text,
        warnings=(*warnings, *lossy),
    )


def _normalize_metadata_row(item: Any) -> dict[str, Any]:
    """Normalize a metadata mapping, ObjectMetadata, or ForeignKeyMetadata into a dict."""
    if type(item) is dict and "attributes" not in item and "identity" not in item and all(type(k) is str for k in item):
        return item
    if isinstance(item, Mapping):
        result: dict[str, Any] = {str(k): v for k, v in item.items()}
        nested_attrs = result.get("attributes")
        if isinstance(nested_attrs, Mapping):
            for k, v in nested_attrs.items():
                result.setdefault(str(k), v)
        nested_ident = result.get("identity")
        if isinstance(nested_ident, Mapping):
            if nested_ident.get("name") is not None:
                result.setdefault("name", nested_ident["name"])
            if nested_ident.get("schema") is not None:
                result.setdefault("schema_name", nested_ident["schema"])
        elif isinstance(nested_ident, ObjectIdentity):
            result.setdefault("name", nested_ident.name)
            if nested_ident.schema is not None:
                result.setdefault("schema_name", nested_ident.schema)
        return result

    ident = getattr(item, "identity", None)
    attrs = getattr(item, "attributes", None)
    if isinstance(ident, ObjectIdentity) and isinstance(attrs, Mapping):
        result = {str(k): v for k, v in attrs.items()}
        result.setdefault("name", ident.name)
        if ident.schema is not None:
            result.setdefault("schema_name", ident.schema)
        return result

    asdict_fn = getattr(item, "_asdict", None)
    if callable(asdict_fn):
        asdict_res = asdict_fn()
        if isinstance(asdict_res, Mapping):
            return {str(k): v for k, v in asdict_res.items()}

    field_names = getattr(type(item), "__struct_fields__", None) or getattr(type(item), "__slots__", None)
    if isinstance(field_names, (tuple, list)):
        return {key: getattr(item, key) for key in field_names if isinstance(key, str) and hasattr(item, key)}

    msg = f"Unsupported catalog metadata row type: {type(item).__name__}"
    raise SQLBuilderError(msg)


def _first_present(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        val = row.get(key)
        if val is not None:
            return val
    return None


def _first_non_empty_str(row: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        val = row.get(key)
        if val is not None:
            text = str(val).strip()
            if text:
                return text
    return None


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _split_metadata_columns(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [
            part.strip().strip('"[]`')
            if _is_simple_identifier(part.strip().strip('"[]`')) and " " not in part.strip()
            else part.strip()
            for part in value.split(",")
            if part.strip()
        ]
    if isinstance(value, Sequence):
        return [str(part).strip() for part in value if str(part).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _resolve_metadata_column_type(row: Mapping[str, Any], dialect: "DialectType | None" = None) -> str:
    data_type_val = _first_non_empty_str(row, "data_type", "type")
    column_type_val = _first_non_empty_str(row, "column_type")
    if column_type_val and "(" in column_type_val and (not data_type_val or "(" not in data_type_val):
        raw_type = column_type_val
    else:
        norm_dialect = _normalize_dialect(dialect) if dialect else None
        fallback_type = "sql_variant" if norm_dialect in {"tsql", "mssql"} else "TEXT"
        raw_type = data_type_val or column_type_val or fallback_type

    if "(" in raw_type or "[" in raw_type:
        return raw_type

    normalized = raw_type.strip().lower()
    norm_dialect = _normalize_dialect(dialect) if dialect else None
    char_len = _first_present(row, "character_maximum_length", "char_length")
    max_length = char_len if char_len is not None else row.get("max_length")
    precision = _first_present(row, "numeric_precision", "precision")
    scale = _first_present(row, "numeric_scale", "scale")

    if normalized in _UNICODE_CHAR_TYPES:
        if max_length in {-1, "-1"} or (isinstance(max_length, str) and max_length.strip().upper() == "MAX"):
            return f"{raw_type}(MAX)" if norm_dialect in {"tsql", "mssql", None} else "TEXT"
        if max_length is not None:
            raw_len = _safe_int(max_length, 0)
            if raw_len > 0:
                is_mssql_bytes = _truthy_metadata_flag(row.get("from_mssql_byte_length")) or (
                    char_len is None
                    and (
                        norm_dialect in {"tsql", "mssql"}
                        or "identity_seed" in row
                        or "is_computed" in row
                        or "is_persisted_computed" in row
                    )
                )
                effective_len = max(1, raw_len // 2) if is_mssql_bytes else raw_len
                return f"{raw_type}({effective_len})"

    if normalized in _CHAR_LENGTH_TYPES:
        if max_length in {-1, "-1"} or (isinstance(max_length, str) and max_length.strip().upper() == "MAX"):
            if norm_dialect in {"tsql", "mssql"}:
                return f"{raw_type}(MAX)"
            return "TEXT" if "char" in normalized else raw_type
        if max_length is not None:
            raw_len = _safe_int(max_length, 0)
            if raw_len > 0:
                return f"{raw_type}({raw_len})"

    if normalized in _DECIMAL_TYPES:
        if precision is not None and scale is not None:
            return f"{raw_type}({_safe_int(precision)},{_safe_int(scale)})"
        if precision is not None:
            return f"{raw_type}({_safe_int(precision)})"
        if normalized == "number" and scale is not None and _safe_int(scale, -1) == 0:
            return f"{raw_type}({_ORACLE_NUMBER_MAX_PRECISION},0)"

    if normalized in _TEMPORAL_SCALE_TYPES and scale is not None:
        return f"{raw_type}({_safe_int(scale)})"

    return raw_type


def _translate_catalog_type(
    dtype: str, source_dialect: "DialectType | None", target_dialect: "DialectType | None"
) -> str:
    """Render a catalog type written in ``source_dialect`` as type text for ``target_dialect``."""
    norm_source = _normalize_dialect(source_dialect) if source_dialect else None
    norm_target = _normalize_dialect(target_dialect) if target_dialect else None
    if norm_source is None or norm_source == norm_target:
        return dtype
    with contextlib.suppress(ParseError):
        built = exp.DataType.build(dtype, dialect=norm_source)
        rendered = built.sql(dialect=norm_target)
        if norm_target == "sqlite" and _sqlite_type_affinity(rendered) != _sqlite_type_affinity(dtype):
            return dtype
        return rendered
    return dtype


def _resolve_metadata_not_null(row: Mapping[str, Any]) -> bool:
    if row.get("not_null") is not None:
        return _truthy_metadata_flag(row["not_null"])
    nullable_val = _first_present(row, "is_nullable", "nullable")
    if nullable_val is None:
        return False
    if isinstance(nullable_val, bool):
        return not nullable_val
    if isinstance(nullable_val, (int, float)):
        return nullable_val == 0
    if isinstance(nullable_val, str):
        norm = nullable_val.strip().lower()
        if norm in {"no", "n", "false", "f", "0"}:
            return True
        if norm in {"yes", "y", "true", "t", "1"}:
            return False
    return not bool(nullable_val)


def _resolve_metadata_computed(row: Mapping[str, Any]) -> tuple[str | exp.Expr | None, bool | None, bool]:
    """Return the generated-column expression, storage (``None`` when unknown), and generated flag."""
    generated_kind = str(row.get("generated_kind") or "").strip().lower()
    extra = str(row.get("extra") or "").strip().lower()
    persisted_val = _first_present(row, "is_persisted_computed", "is_stored", "persisted")
    persisted: bool | None
    if persisted_val is not None:
        persisted = _truthy_metadata_flag(persisted_val)
    elif generated_kind in {"v", "virtual"} or "virtual generated" in extra:
        persisted = False
    elif generated_kind in {"s", "stored"} or "stored generated" in extra:
        persisted = True
    else:
        persisted = None

    is_generated = (
        _truthy_metadata_flag(row.get("is_computed"))
        or _truthy_metadata_flag(row.get("is_generated"))
        or _truthy_metadata_flag(row.get("virtual_column"))
        or generated_kind in {"s", "v", "stored", "virtual"}
        or extra in {"generated", "virtual generated", "stored generated"}
    )

    expr_val = _first_present(row, "computed_definition", "generation_expression", "generated_expression")
    if expr_val is not None and str(expr_val).strip():
        return (expr_val if isinstance(expr_val, exp.Expr) else str(expr_val).strip(), persisted, True)

    gen_val = row.get("generated")
    if isinstance(gen_val, str) and gen_val.strip() and gen_val.strip().upper() not in _IDENTITY_KEYWORDS:
        return (gen_val.strip(), persisted, True)

    if is_generated:
        fallback_expr = _first_non_empty_str(row, "column_default", "default")
        if fallback_expr:
            return (fallback_expr, persisted, True)

    return (None, persisted, is_generated)


def _is_hidden_metadata_column(row: Mapping[str, Any]) -> bool:
    """Return whether a catalog column is a system-generated hidden column (Oracle ``SYS_NC*`` columns)."""
    if not _truthy_metadata_flag(row.get("hidden_column")):
        return False
    name = _first_non_empty_str(row, "column_name", "name") or ""
    return name.upper().startswith("SYS_")


def _resolve_metadata_identity(row: Mapping[str, Any]) -> str | None:
    raw_identity = _first_present(row, "identity", "identity_generation")
    if isinstance(raw_identity, bool):
        if raw_identity:
            return "ALWAYS"
    elif isinstance(raw_identity, str):
        ident_clean = raw_identity.strip().upper()
        if ident_clean in {"D", "BY DEFAULT", "GENERATED BY DEFAULT", "BY DEFAULT AS IDENTITY"}:
            return "BY DEFAULT"
        if ident_clean in {"A", "Y", "YES", "ALWAYS", "GENERATED ALWAYS", "ALWAYS AS IDENTITY", "TRUE", "1"}:
            return "ALWAYS"
        if ident_clean not in {"", "N", "NO", "NONE", "FALSE", "0", "NOT_APPLICABLE"}:
            return ident_clean

    if _truthy_metadata_flag(row.get("is_identity")) or _truthy_metadata_flag(row.get("identity_column")):
        seed = row.get("identity_seed")
        inc = row.get("identity_increment")
        if seed is not None and inc is not None:
            seed_i = _safe_int(seed, 1)
            inc_i = _safe_int(inc, 1)
            if (seed_i, inc_i) != (1, 1):
                return f"IDENTITY({seed_i},{inc_i})"
        return "ALWAYS"

    return None


def _resolve_metadata_auto_increment(row: Mapping[str, Any]) -> bool:
    if _truthy_metadata_flag(row.get("auto_increment")):
        return True
    extra = str(row.get("extra") or "").lower()
    return "auto_increment" in extra or "autoincrement" in extra


def _parse_metadata_default(value: Any, *, source_dialect: "DialectType | None" = None, extra: str = "") -> Any:
    """Convert a raw catalog default into a builder default value.

    MySQL ``information_schema`` reports literal defaults unquoted (``hello world``,
    ``2024-01-01``), so on MySQL raw defaults are string literals unless they are numeric,
    ``NULL``, a ``CURRENT_TIMESTAMP``-family keyword, a bit/hex literal, or the column's
    ``EXTRA`` marks an expression default (``DEFAULT_GENERATED``). Other engines report
    SQL text, which is parsed; non-literal expressions are parenthesized because engines
    such as SQLite and MySQL require ``DEFAULT (expr)`` (added when the column is rendered).

    Returns:
        ``None`` for no default, a keyword string, an expression, or ``_UNPARSEABLE_DEFAULT``.
    """
    if value is None or isinstance(value, exp.Expr):
        return value
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped or stripped.upper() == "NULL":
        return None
    normalized_kw = re.sub(r"\s+", "_", stripped.upper())
    if normalized_kw in {CURRENT_TIMESTAMP_KEYWORD, CURRENT_DATE_KEYWORD, CURRENT_TIME_KEYWORD}:
        return normalized_kw

    raw_source = source_dialect.lower() if isinstance(source_dialect, str) else None
    norm_source = _normalize_dialect(source_dialect) if source_dialect else None
    if norm_source == "mysql" and raw_source != "mariadb" and "default_generated" not in extra.lower():
        if _NUMERIC_LITERAL_RE.fullmatch(stripped):
            return exp.Literal.number(stripped)
        if not _MYSQL_KEYWORD_DEFAULT_RE.fullmatch(stripped) and not _MYSQL_BIT_HEX_LITERAL_RE.fullmatch(stripped):
            return exp.Literal.string(stripped)

    parsed = _parse_catalog_fragment(stripped, source_dialect)
    if parsed is None:
        return _UNPARSEABLE_DEFAULT
    while isinstance(parsed, exp.Paren):
        parsed = parsed.this
    if isinstance(parsed, exp.Column) and not parsed.table:
        return exp.Literal.string(parsed.name)
    return parsed


def _is_literal_default(expression: "exp.Expr | None") -> bool:
    """Return whether a default expression is a literal or keyword that needs no parentheses."""
    if isinstance(expression, exp.Neg):
        expression = expression.this
    return isinstance(
        expression,
        (
            exp.Literal,
            exp.Null,
            exp.Boolean,
            exp.BitString,
            exp.HexString,
            exp.CurrentTimestamp,
            exp.CurrentDate,
            exp.CurrentTime,
        ),
    )


def _parse_catalog_fragment(text: str, dialect: "DialectType | None") -> exp.Expr | None:
    """Parse a catalog SQL fragment, returning ``None`` when it is not valid SQL for ``dialect``.

    Catalogs wrap stored expressions in redundant parentheses (T-SQL reports
    ``([Total]>=(0))``); those around the whole fragment and around single literals or
    columns are removed. Identifiers keep the catalog's spelling: quoted identifiers, and
    unquoted identifiers whose spelling differs from the source dialect's folded form
    (DuckDB and T-SQL store ``Amount > 0`` for column ``"Amount"``), are marked explicitly
    quoted so case normalization cannot rename them.
    """
    norm_dialect = _normalize_dialect(dialect) if dialect else None
    try:
        parsed = sqlglot.parse_one(text, read=norm_dialect)
    except (ParseError, TokenError):
        return None
    if isinstance(parsed, exp.Paren):
        parsed = parsed.unnest()
    return parsed.transform(lambda node: _simplify_catalog_fragment_node(node, norm_dialect), copy=False)


def _simplify_catalog_fragment_node(node: exp.Expr, dialect: str | None) -> exp.Expr:
    if isinstance(node, exp.Paren) and isinstance(node.this, (exp.Literal, exp.Column, exp.Null, exp.Boolean)):
        return node.this
    if isinstance(node, exp.Identifier) and (node.quoted or _fold_identifier(node.name, dialect) != node.name):
        node.set("quoted", True)
        node.meta["sqlspec_explicit_table_quote"] = True
    return node


def _deliver_metadata_warnings(collected: "list[str]", warnings: "list[str] | None", *, context: str) -> None:
    """Hand lossy-metadata findings to the caller's collector, or raise when none was supplied."""
    if not collected:
        return
    if warnings is None:
        msg = f"Cannot reproduce catalog metadata for {context} faithfully: " + "; ".join(collected)
        raise SQLBuilderError(msg)
    warnings.extend(collected)


def _normalize_constraint_type(row: Mapping[str, Any]) -> str | None:
    raw = str(_first_present(row, "constraint_type", "type") or "").strip().upper().replace("_", " ")
    if raw in {"PRIMARY KEY", "P", "PRIMARY KEY CONSTRAINT"}:
        return CONSTRAINT_TYPE_PRIMARY_KEY
    if raw in {"UNIQUE", "U", "UNIQUE CONSTRAINT"}:
        return CONSTRAINT_TYPE_UNIQUE
    if raw in {"CHECK", "C", "K", "CHECK CONSTRAINT"}:
        return CONSTRAINT_TYPE_CHECK
    if raw in {"FOREIGN KEY", "F", "R", "FOREIGN KEY CONSTRAINT"}:
        return CONSTRAINT_TYPE_FOREIGN_KEY
    return None


def _extract_constraint_columns(row: Mapping[str, Any]) -> list[str]:
    cols = _split_metadata_columns(_first_present(row, "columns", "column_names", "column_name"))
    if cols:
        return cols
    defn = _first_non_empty_str(row, "definition")
    if defn:
        match = _PAREN_COLS_RE.search(defn)
        if match:
            return _split_metadata_columns(match.group(1))
    return []


def _extract_metadata_primary_key(
    columns: Sequence[Mapping[str, Any]], constraints: Sequence[Mapping[str, Any]]
) -> tuple[list[str], str | None]:
    pk_name_from_constraint: str | None = None
    pk_cols_from_constraint: list[str] = []
    for c_row in constraints:
        if _normalize_constraint_type(c_row) == CONSTRAINT_TYPE_PRIMARY_KEY:
            pk_name_from_constraint = pk_name_from_constraint or _first_non_empty_str(c_row, "constraint_name", "name")
            c_cols = _extract_constraint_columns(c_row)
            for col in c_cols:
                if col not in pk_cols_from_constraint:
                    pk_cols_from_constraint.append(col)

    pk_candidates: list[tuple[int, int, str]] = []
    pk_name_from_columns: str | None = None
    for idx, row in enumerate(columns):
        if not pk_name_from_columns:
            pk_name_from_columns = _first_non_empty_str(row, "primary_key_name")
        col_name = _first_non_empty_str(row, "column_name", "name")
        if not col_name:
            continue
        pk_ord = row.get("primary_key_ordinal")
        if pk_ord is not None and str(pk_ord).strip() not in {"", "0"}:
            pk_candidates.append((_safe_int(pk_ord, idx + 1), idx, col_name))
            continue
        is_pk_val = _first_present(row, "is_primary", "is_primary_key", "primary_key")
        if isinstance(is_pk_val, int) and not isinstance(is_pk_val, bool) and is_pk_val > 0:
            pk_candidates.append((is_pk_val, idx, col_name))
        elif _truthy_metadata_flag(is_pk_val) or str(row.get("column_key") or "").strip().upper() == "PRI":
            pk_candidates.append((idx + 1, idx, col_name))

    pk_candidates.sort(key=lambda item: (item[0], item[1]))
    pk_cols_from_columns: list[str] = []
    for _, _, col_name in pk_candidates:
        if col_name not in pk_cols_from_columns:
            pk_cols_from_columns.append(col_name)

    return (pk_cols_from_constraint or pk_cols_from_columns, pk_name_from_constraint or pk_name_from_columns)


def _strip_check_wrapper(definition: str) -> str:
    stripped = definition.strip()
    if stripped.upper().startswith("CHECK"):
        remainder = stripped[5:].strip()
        if remainder.startswith("(") and remainder.endswith(")"):
            return remainder[1:-1].strip()
        return remainder
    return stripped


def _normalize_fk_action(action: str | None) -> str | None:
    if not action:
        return None
    cleaned = re.sub(r"[\s_]+", " ", action.strip().upper())
    if cleaned == FOREIGN_KEY_ACTION_NO_ACTION:
        return None
    return cleaned if cleaned in VALID_FOREIGN_KEY_ACTIONS else None


def _supported_foreign_key_actions(
    on_delete: str | None,
    on_update: str | None,
    dialect: "DialectType | None",
    *,
    name: str | None,
    warnings: "list[str]",
) -> tuple[str | None, str | None]:
    """Drop referential actions the target dialect rejects, recording each semantic loss."""
    norm_dialect = _normalize_dialect(dialect) if dialect else None
    label = name or "<unnamed>"
    if norm_dialect == "oracle":
        resolved_delete = on_delete if on_delete in _ORACLE_ON_DELETE_ACTIONS else None
        if on_delete and resolved_delete is None and on_delete != FOREIGN_KEY_ACTION_RESTRICT:
            warnings.append(f"Foreign key {label}: Oracle does not support ON DELETE {on_delete}; action omitted")
        if on_update and on_update != FOREIGN_KEY_ACTION_RESTRICT:
            warnings.append(f"Foreign key {label}: Oracle does not support ON UPDATE {on_update}; action omitted")
        return resolved_delete, None
    if norm_dialect in {"duckdb", "bigquery"}:
        label_dialect = "BigQuery" if norm_dialect == "bigquery" else "DuckDB"
        implicit_actions = {FOREIGN_KEY_ACTION_RESTRICT}
        if on_delete and on_delete not in implicit_actions:
            warnings.append(
                f"Foreign key {label}: {label_dialect} does not support ON DELETE {on_delete}; action omitted"
            )
        if on_update and on_update not in implicit_actions:
            warnings.append(
                f"Foreign key {label}: {label_dialect} does not support ON UPDATE {on_update}; action omitted"
            )
        return None, None
    if norm_dialect == "tsql":
        return (
            None if on_delete == FOREIGN_KEY_ACTION_RESTRICT else on_delete,
            None if on_update == FOREIGN_KEY_ACTION_RESTRICT else on_update,
        )
    return on_delete, on_update


def _metadata_fk_actions(row: Mapping[str, Any], definition: str | None) -> tuple[str | None, str | None]:
    """Return the ON DELETE / ON UPDATE actions from FK metadata keys, falling back to its definition text."""
    on_delete = _normalize_fk_action(_first_non_empty_str(row, "on_delete", "delete_rule"))
    on_update = _normalize_fk_action(_first_non_empty_str(row, "on_update", "update_rule"))
    if definition:
        if on_delete is None:
            del_match = _FK_ON_DELETE_RE.search(definition)
            on_delete = _normalize_fk_action(del_match.group(1)) if del_match else None
        if on_update is None:
            upd_match = _FK_ON_UPDATE_RE.search(definition)
            on_update = _normalize_fk_action(upd_match.group(1)) if upd_match else None
    return on_delete, on_update


def _find_matching_metadata_fk(
    merged: list[dict[str, Any]],
    *,
    quoted_name: str | None,
    fk_group_id: Any,
    ref_table: str,
    quoted_cols: Sequence[str],
    quoted_ref_cols: Sequence[str],
    on_delete: str | None = None,
    on_update: str | None = None,
) -> dict[str, Any] | None:
    """Find an existing merged FK entry that belongs to the same constraint group."""
    for existing in merged:
        if quoted_name and existing["name"] == quoted_name:
            return existing
        if (
            fk_group_id is not None
            and existing.get("_fk_group_id") == fk_group_id
            and existing["ref_table"] == ref_table
        ):
            return existing
        if (
            tuple(existing["columns"]) == tuple(quoted_cols)
            and existing["ref_table"] == ref_table
            and tuple(existing["ref_columns"]) == tuple(quoted_ref_cols)
        ):
            return existing
        if (
            quoted_name is None
            and fk_group_id is None
            and existing["name"] is None
            and existing.get("_fk_group_id") is None
            and existing["ref_table"] == ref_table
            and existing["on_delete"] == on_delete
            and existing["on_update"] == on_update
            and len(quoted_cols) == 1
            and len(quoted_ref_cols) == 1
            and quoted_cols[0] not in existing["columns"]
            and quoted_ref_cols[0] not in existing["ref_columns"]
        ):
            return existing
    return None


def _extract_metadata_foreign_keys(
    constraints: Sequence[Mapping[str, Any]],
    foreign_keys: Sequence[Mapping[str, Any]],
    *,
    table_schema: str | None = None,
    dialect: "DialectType | None" = None,
    source_dialect: "DialectType | None" = None,
    warnings: "list[str] | None" = None,
) -> list[dict[str, Any]]:
    """Merge FK metadata rows into specs whose names are already quoted for ``dialect``."""
    fk_names = {
        name for fk_row in foreign_keys if (name := _first_non_empty_str(fk_row, "constraint_name", "name")) is not None
    }
    raw_items: list[Mapping[str, Any]] = []
    for c_row in constraints:
        if _normalize_constraint_type(c_row) != CONSTRAINT_TYPE_FOREIGN_KEY:
            continue
        c_name = _first_non_empty_str(c_row, "constraint_name", "name")
        has_ref = bool(
            _first_non_empty_str(
                c_row, "referenced_table", "references_table", "referenced_table_name", "table", "definition"
            )
        )
        if not has_ref and ((c_name and c_name in fk_names) or bool(foreign_keys)):
            continue
        raw_items.append(c_row)
    raw_items.extend(foreign_keys)

    merged: list[dict[str, Any]] = []
    for row in raw_items:
        name = _first_non_empty_str(row, "constraint_name", "name")
        fk_group_id = row.get("id") if name is None else None
        cols = _split_metadata_columns(_first_present(row, "columns", "column_names", "column_name", "from"))
        ref_table = _first_non_empty_str(row, "referenced_table", "references_table", "referenced_table_name", "table")
        ref_cols = _split_metadata_columns(
            _first_present(
                row, "referenced_columns", "references_columns", "referenced_column", "referenced_column_name", "to"
            )
        )
        ref_is_catalog_name = ref_table is not None
        defn = _first_non_empty_str(row, "definition")
        if defn and (not cols or not ref_table or not ref_cols):
            fk_match = _FK_DEF_RE.search(defn)
            if fk_match:
                if not cols:
                    cols = _split_metadata_columns(fk_match.group("cols"))
                if not ref_table:
                    ref_table = fk_match.group("ref_table").strip()
                if not ref_cols:
                    ref_cols = _split_metadata_columns(fk_match.group("ref_cols"))

        if not cols or not ref_table or not ref_cols:
            if warnings is not None:
                warnings.append(f"Foreign key {name or '<unnamed>'} metadata is incomplete and was omitted")
            continue

        explicit_ref_schema = _first_non_empty_str(
            row, "referenced_schema", "references_schema", "referenced_table_schema"
        )
        norm_target = _normalize_dialect(dialect) if dialect else None
        if (
            explicit_ref_schema
            and table_schema is None
            and norm_target in {"sqlite", "duckdb"}
            and explicit_ref_schema.lower() == "main"
        ):
            explicit_ref_schema = None
        ref_schema = explicit_ref_schema or table_schema
        qualify_ref = bool(explicit_ref_schema or (ref_schema and table_schema))
        if ref_is_catalog_name:
            ref_table = _catalog_qualified_name(ref_table, ref_schema if qualify_ref else None, dialect, source_dialect)
        elif qualify_ref and ref_schema and "." not in ref_table:
            ref_table = f"{_catalog_name(ref_schema, dialect, source_dialect)}.{ref_table}"

        on_delete, on_update = _metadata_fk_actions(row, defn)
        deferrable = _truthy_metadata_flag(_first_present(row, "deferrable", "is_deferrable"))
        initially_deferred = _truthy_metadata_flag(row.get("initially_deferred"))
        quoted_cols = [_catalog_name(col, dialect, source_dialect) for col in cols]
        quoted_ref_cols = (
            [_catalog_name(col, dialect, source_dialect) for col in ref_cols] if ref_is_catalog_name else ref_cols
        )
        quoted_name = _catalog_name(name, dialect, source_dialect) if name else None

        matched_existing = _find_matching_metadata_fk(
            merged,
            quoted_name=quoted_name,
            fk_group_id=fk_group_id,
            ref_table=ref_table,
            quoted_cols=quoted_cols,
            quoted_ref_cols=quoted_ref_cols,
            on_delete=on_delete,
            on_update=on_update,
        )

        if matched_existing is not None:
            if (
                len(quoted_cols) == 1
                and len(quoted_ref_cols) == 1
                and quoted_cols[0] not in matched_existing["columns"]
            ):
                matched_existing["columns"].append(quoted_cols[0])
                matched_existing["ref_columns"].append(quoted_ref_cols[0])
            if not matched_existing["name"] and quoted_name:
                matched_existing["name"] = quoted_name
            if not matched_existing["on_delete"] and on_delete:
                matched_existing["on_delete"] = on_delete
            if not matched_existing["on_update"] and on_update:
                matched_existing["on_update"] = on_update
            if deferrable:
                matched_existing["deferrable"] = True
            if initially_deferred:
                matched_existing["initially_deferred"] = True
            continue

        merged.append({
            "name": quoted_name,
            "columns": list(quoted_cols),
            "ref_table": ref_table,
            "ref_columns": list(quoted_ref_cols),
            "on_delete": on_delete,
            "on_update": on_update,
            "deferrable": deferrable,
            "initially_deferred": initially_deferred,
            "_fk_group_id": fk_group_id,
        })

    return [item for item in merged if len(item["columns"]) == len(item["ref_columns"])]


def _native_index_sql(row: Mapping[str, Any]) -> str | None:
    """Return the catalog's native ``CREATE INDEX`` text when the row carries it."""
    native = _first_non_empty_str(row, "native_sql", "index_sql")
    if native is not None:
        return native
    definition = _first_non_empty_str(row, "definition", "indexdef")
    if definition is not None and _NATIVE_INDEX_SQL_RE.match(definition):
        return definition
    return None


def _parse_native_index_sql(
    native_sql: str, dialect: "DialectType | None"
) -> tuple[list[exp.Expr], exp.Expr | None] | None:
    """Return the key expressions and predicate of a native ``CREATE INDEX`` statement."""
    parsed = _parse_catalog_fragment(native_sql.strip().rstrip(";"), dialect)
    if parsed is None and dialect is not None:
        parsed = _parse_catalog_fragment(native_sql.strip().rstrip(";"), None)
    if not isinstance(parsed, exp.Create) or not isinstance(parsed.this, exp.Index):
        return None
    params = parsed.this.args.get("params")
    if not isinstance(params, exp.IndexParameters):
        return None
    key_exprs = [_strip_default_ordering(col) for col in params.args.get("columns") or []]
    if not key_exprs:
        return None
    where_node = params.args.get("where")
    where_expr = where_node.this if isinstance(where_node, exp.Where) else None
    return key_exprs, where_expr


def _strip_default_ordering(column: exp.Expr) -> exp.Expr:
    """Drop the implicit ascending wrapper and redundant parentheses around column or function keys, keeping DESC."""
    inner: exp.Expr = column.this if isinstance(column, exp.Ordered) and not column.args.get("desc") else column
    if isinstance(inner, exp.Paren) and isinstance(inner.this, (exp.Column, exp.Func)):
        unwrapped: exp.Expr = inner.this
        return unwrapped
    return inner


def _metadata_index_key_expressions(
    row: Mapping[str, Any],
    index_name: str,
    *,
    dialect: "DialectType | None",
    fragment_dialect: "DialectType | None",
    known_columns: "Sequence[str] | None",
    warnings: "list[str]",
) -> list[exp.Expr] | None:
    """Build index key expressions from the catalog column list and per-column sort order."""
    cols = _split_metadata_columns(_first_present(row, "columns", "column_names", "column_name"))
    if not cols:
        warnings.append(f"Index {index_name} has no catalog key columns; index omitted")
        return None
    known = set(known_columns or ())
    descending = _metadata_index_descending_flags(row, len(cols))
    key_exprs: list[exp.Expr] = []
    for position, col in enumerate(cols):
        key_expr: exp.Expr
        if col in known:
            key_expr = exp.Column(this=_explicit_or_bare_identifier(col, dialect, fragment_dialect))
        elif _ORDER_DIRECTION_SUFFIX_RE.search(col) and not _INDEX_EXPRESSION_CHARS_RE.search(col):
            base_name = _ORDER_DIRECTION_SUFFIX_RE.sub("", col).strip()
            descending[position] = descending[position] or "DESC" in col[len(base_name) :].upper()
            key_expr = exp.Column(this=_explicit_or_bare_identifier(base_name, dialect, fragment_dialect))
        elif _INDEX_PLACEHOLDER_RE.fullmatch(col):
            warnings.append(f"Index {index_name} uses an expression the catalog did not report; index omitted")
            return None
        elif _INDEX_EXPRESSION_CHARS_RE.search(col):
            parsed_key = _parse_catalog_fragment(col, fragment_dialect)
            if parsed_key is None:
                warnings.append(f"Index {index_name}: key expression could not be parsed ({col}); index omitted")
                return None
            key_expr = parsed_key
        else:
            key_expr = exp.Column(this=_explicit_or_bare_identifier(col, dialect, fragment_dialect))
        key_exprs.append(_descending_key(key_expr, dialect) if descending[position] else key_expr)
    return key_exprs


def _metadata_index_descending_flags(row: Mapping[str, Any], count: int) -> list[bool]:
    """Return per-key descending flags from ``column_orders`` (``A``/``D``) or ``is_descending`` metadata."""
    raw_orders = _first_present(row, "column_orders", "descending", "is_descending")
    if raw_orders is None:
        return [False] * count
    values: list[Any] = (
        raw_orders.split(",")
        if isinstance(raw_orders, str)
        else list(raw_orders)
        if isinstance(raw_orders, Sequence)
        else [raw_orders]
    )
    flags = [
        str(value).strip().upper() in {"D", "DESC"} or (not isinstance(value, str) and _truthy_metadata_flag(value))
        for value in values
    ]
    return (flags + [False] * count)[:count]


def _descending_key(expression: exp.Expr, dialect: "DialectType | None") -> exp.Ordered:
    """Wrap an index key as DESC using the dialect's default null ordering (no explicit NULLS clause)."""
    null_ordering = Dialect.get_or_raise(_normalize_dialect(dialect) if dialect else None).NULL_ORDERING
    return exp.Ordered(
        this=expression, desc=True, nulls_first=null_ordering not in {"nulls_are_small", "nulls_are_last"}
    )


def _explicit_or_bare_identifier(
    name: str, dialect: "DialectType | None", source_dialect: "DialectType | None" = None
) -> exp.Identifier:
    """Return an identifier for a raw catalog name, explicitly quoted when its spelling requires it."""
    if not _catalog_name_needs_quotes(name, dialect, source_dialect):
        return exp.to_identifier(name)
    norm_dialect = _normalize_dialect(dialect)
    case_dialect = norm_dialect if source_dialect is None else _normalize_dialect(source_dialect)
    target_name = name
    if _BARE_IDENTIFIER_RE.fullmatch(name) and _fold_identifier(name, case_dialect) == name:
        target_name = _fold_identifier(name, norm_dialect)
    return _explicit_identifier(target_name)


def _explicit_identifier(name: str) -> exp.Identifier:
    """Return a quoted identifier marked so normalization and unquoting passes keep it verbatim."""
    identifier = exp.Identifier(this=name, quoted=True)
    identifier.meta["sqlspec_explicit_table_quote"] = True
    return identifier


def _index_column_expression(text: str, dialect: "DialectType | None") -> exp.Expr:
    """Turn a user-supplied index column string into an identifier, ordered key, or expression."""
    stripped = text.strip()
    norm_dialect = _normalize_dialect(dialect) if dialect else None
    base = _ORDER_DIRECTION_SUFFIX_RE.sub("", stripped).strip()
    has_order_suffix = base != stripped
    base_is_name = _is_simple_identifier(base) or bool(_QUOTED_IDENTIFIER_RE.fullmatch(base))
    if has_order_suffix and (base_is_name or _INDEX_EXPRESSION_CHARS_RE.search(base)):
        ordered: exp.Expr = parse_order_expression(stripped)
        return ordered
    if _is_simple_identifier(stripped) or _QUOTED_IDENTIFIER_RE.fullmatch(stripped):
        return exp.Column(this=_parse_ddl_identifier(stripped, dialect=dialect))
    if _INDEX_EXPRESSION_CHARS_RE.search(stripped):
        parsed: exp.Expr | None = exp.maybe_parse(stripped, dialect=norm_dialect)
        if parsed is not None:
            return parsed
    if has_order_suffix:
        suffix = stripped[len(base) :].upper()
        column = exp.Column(this=_explicit_identifier(base))
        return _descending_key(column, dialect) if "DESC" in suffix else column
    return exp.Column(this=_explicit_identifier(stripped))


def _group_metadata_index_rows(indexes: Sequence[Mapping[str, Any] | Any]) -> list[dict[str, Any]]:
    grouped: list[dict[str, Any]] = []
    by_name: dict[str, dict[str, Any]] = {}
    for item in indexes:
        row = _normalize_metadata_row(item)
        idx_name = _first_non_empty_str(row, "index_name", "name")
        if not idx_name:
            continue
        has_col_list = row.get("columns") is not None or row.get("column_names") is not None
        single_col = _first_non_empty_str(row, "column_name")
        if not has_col_list and single_col:
            existing = by_name.get(idx_name)
            pos = _safe_int(_first_present(row, "column_position", "ordinal_position", "key_ordinal"), 0)
            if existing is None:
                entry = dict(row)
                entry["_col_pairs"] = [(pos, single_col)]
                by_name[idx_name] = entry
                grouped.append(entry)
            else:
                existing["_col_pairs"].append((pos, single_col))
        elif idx_name not in by_name:
            by_name[idx_name] = row
            grouped.append(row)

    result: list[dict[str, Any]] = []
    for entry in grouped:
        pairs = entry.pop("_col_pairs", None)
        if pairs is not None:
            pairs.sort(key=lambda pair: pair[0])
            entry["columns"] = [col for _, col in pairs]
        result.append(entry)
    return result


def _build_optional_column_schema(columns: list[str], dialect: "DialectType | None" = None) -> exp.Schema | None:
    if not columns:
        return None
    return exp.Schema(
        expressions=[exp.Column(this=_parse_ddl_identifier(column, dialect=dialect)) for column in columns]
    )


def _wrap_properties(properties: "Sequence[exp.Expr]") -> exp.Properties | None:
    return exp.Properties(expressions=list(properties)) if properties else None


def _parse_column_type(name: str | None, dtype: str, dialect: "DialectType | None") -> exp.DataType:
    """Parse a column type for ``dialect``.

    SQLite derives column affinity from the declared type name, so SQLite keeps the type
    text verbatim (``NUMERIC(12,2)`` must not become ``REAL``). Types SQLGlot cannot parse
    (for example Oracle ``RAW(16)``) are kept verbatim as user-defined types.
    """
    target_dialect = dialect
    norm_dialect = _normalize_dialect(dialect) if dialect else None
    if norm_dialect == "sqlite":
        declared = dtype.strip()
        with contextlib.suppress(ParseError):
            built = exp.DataType.build(declared, dialect=target_dialect)
            if _sqlite_type_affinity(built.sql(dialect="sqlite")) == _sqlite_type_affinity(declared):
                return built
        return exp.DataType(this=exp.DataType.Type.USERDEFINED, kind=declared)
    if norm_dialect in ("tsql", "mssql") and dtype.strip().upper().startswith("TIMESTAMP"):
        target_dialect = None
    with contextlib.suppress(ParseError):
        return exp.DataType.build(dtype, dialect=target_dialect)
    if _QUALIFIED_TYPE_NAME_RE.fullmatch(dtype.strip()):
        type_name = _mark_explicit_table_quotes(exp.to_table(dtype.strip(), dialect=norm_dialect), dtype.strip())
        return exp.DataType(this=exp.DataType.Type.USERDEFINED, kind=type_name.sql(dialect=norm_dialect))
    if target_dialect is not None:
        with contextlib.suppress(ParseError):
            return exp.DataType.build(dtype)
        if _VERBATIM_TYPE_NAME_RE.fullmatch(dtype.strip()):
            return exp.DataType(this=exp.DataType.Type.USERDEFINED, kind=dtype.strip())
    msg = f"Column {name!r}: cannot parse type {dtype!r} for dialect {dialect!r}"
    raise SQLBuilderError(msg)


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


def _sqlite_type_affinity(type_text: str) -> str:
    """Return the SQLite column affinity for a declared type name (SQLite datatype rules 3.1)."""
    upper = type_text.upper()
    if "INT" in upper:
        return "INTEGER"
    if "CHAR" in upper or "CLOB" in upper or "TEXT" in upper:
        return "TEXT"
    if "BLOB" in upper or not upper.strip():
        return "BLOB"
    if "REAL" in upper or "FLOA" in upper or "DOUB" in upper:
        return "REAL"
    return "NUMERIC"


def _catalog_name(name: str, dialect: "DialectType | None", source_dialect: "DialectType | None" = None) -> str:
    """Return SQL text that resolves to exactly the catalog-supplied identifier ``name``.

    Catalog identifiers are stored with their real case. When the bare spelling would be
    folded by the dialect's normalization strategy, collides with a keyword, or contains
    characters outside the bare identifier alphabet, the name is returned in the
    dialect's quoted spelling so the builder marks it as explicitly quoted.
    """
    if not _catalog_name_needs_quotes(name, dialect, source_dialect):
        return name
    norm_dialect = _normalize_dialect(dialect)
    case_dialect = norm_dialect if source_dialect is None else _normalize_dialect(source_dialect)
    target_name = name
    if _BARE_IDENTIFIER_RE.fullmatch(name) and _fold_identifier(name, case_dialect) == name:
        target_name = _fold_identifier(name, norm_dialect)
    return exp.Identifier(this=target_name, quoted=True).sql(dialect=norm_dialect)


def _fold_identifier(name: str, dialect: str | None) -> str:
    """Return ``name`` as ``dialect`` folds an unquoted identifier."""
    strategy = Dialect.get_or_raise(dialect).normalization_strategy
    if strategy in {NormalizationStrategy.UPPERCASE, NormalizationStrategy.CASE_INSENSITIVE_UPPERCASE}:
        return name.upper()
    if strategy is NormalizationStrategy.CASE_SENSITIVE:
        return name
    return name.lower()


def _catalog_name_needs_quotes(
    name: str, dialect: "DialectType | None", source_dialect: "DialectType | None" = None
) -> bool:
    """Return whether a raw catalog identifier must be quoted to keep its exact spelling.

    Case is judged against the catalog's own (``source_dialect``) folding: a name already in
    the source's folded form was created unquoted and stays unquoted in the target, so it
    follows the target's folding. Keywords are judged against the target dialect.
    """
    if not _BARE_IDENTIFIER_RE.fullmatch(name):
        return True
    norm_dialect = _normalize_dialect(dialect)
    sqlglot_dialect = Dialect.get_or_raise(norm_dialect)
    case_dialect = norm_dialect if source_dialect is None else _normalize_dialect(source_dialect)
    if _fold_identifier(name, case_dialect) != name:
        return True
    if name.lower() in sqlglot_dialect.generator_class.RESERVED_KEYWORDS or name.upper() in _RESERVED_IDENTIFIER_WORDS:
        return True
    tokens = sqlglot_dialect.tokenize(name)
    return len(tokens) != 1 or tokens[0].token_type is not TokenType.VAR


def _record_field_name(name: str, dialect: "DialectType | None") -> str:
    """Return SQL text for a record or Arrow field name.

    Single-case bare names (``id``, ``ID``) are left unquoted so they follow the
    dialect's normal case folding; mixed-case names, keywords, and names with other
    characters are quoted so ``UserName`` stays ``UserName``.
    """
    if _BARE_IDENTIFIER_RE.fullmatch(name) and (name.islower() or name.isupper()):
        bare_spelling = name.lower() if name.islower() else name.upper()
        if not _catalog_name_needs_quotes(bare_spelling, None) and name.upper() not in _RESERVED_IDENTIFIER_WORDS:
            return name
    return _catalog_name(name, dialect)


def _catalog_qualified_name(
    name: str, schema: "str | None", dialect: "DialectType | None", source_dialect: "DialectType | None" = None
) -> str:
    """Return a schema-qualified SQL reference built from raw catalog identifiers."""
    quoted_name = _catalog_name(name, dialect, source_dialect)
    return f"{_catalog_name(schema, dialect, source_dialect)}.{quoted_name}" if schema else quoted_name


def _build_identity_column_constraint(
    col: "ColumnDefinition", dialect: "DialectType | None" = None
) -> exp.ColumnConstraint | None:
    """Build an identity ColumnConstraint for a ColumnDefinition when configured.

    Raises:
        SQLBuilderError: When identity is requested on SQLite, which has no identity columns.
    """
    constraint = _identity_constraint_from_spec(col)
    if constraint is None:
        return None
    norm_dialect = _normalize_dialect(dialect) if dialect else None
    if norm_dialect == "sqlite" and not col.primary_key:
        msg = (
            f"Column {col.name!r}: SQLite has no identity columns; "
            "use an INTEGER PRIMARY KEY column (optionally AUTOINCREMENT) instead"
        )
        raise SQLBuilderError(msg)
    return constraint


def _identity_constraint_from_spec(col: "ColumnDefinition") -> exp.ColumnConstraint | None:
    """Translate the identity / generated identity settings of a column into a constraint node."""
    if col.identity is not None and col.identity is not False:
        start_lit: exp.Expr | None = None
        inc_lit: exp.Expr | None = None
        if isinstance(col.identity, str):
            ident_upper = col.identity.strip().upper()
            ident_match = _IDENTITY_SEED_INC_RE.match(ident_upper)
            if ident_match is not None:
                seed_num = int(ident_match.group(1))
                inc_num = int(ident_match.group(2))
                is_always = True
                on_null = False
                if (seed_num, inc_num) != (1, 1):
                    start_lit = exp.Literal.number(seed_num)
                    inc_lit = exp.Literal.number(inc_num)
            else:
                is_always = not ident_upper.startswith("BY DEFAULT")
                on_null = "ON NULL" in ident_upper
        else:
            is_always = True
            on_null = False
        return exp.ColumnConstraint(
            kind=exp.GeneratedAsIdentityColumnConstraint(
                this=is_always, on_null=on_null or None, start=start_lit, increment=inc_lit
            )
        )

    if col.generated is True or (
        isinstance(col.generated, str) and col.generated.strip().upper() in _IDENTITY_KEYWORDS
    ):
        if isinstance(col.generated, str):
            gen_upper = col.generated.strip().upper()
            is_always = not gen_upper.startswith("BY DEFAULT")
            on_null = "ON NULL" in gen_upper
        else:
            is_always = True
            on_null = False
        return exp.ColumnConstraint(
            kind=exp.GeneratedAsIdentityColumnConstraint(this=is_always, on_null=on_null or None)
        )

    return None


def _build_computed_column_constraint(
    col: "ColumnDefinition", dialect: "DialectType | None" = None
) -> exp.ColumnConstraint | None:
    """Build a generated (computed) column constraint using the dialect's valid syntax.

    PostgreSQL, MySQL, and SQLite render ``GENERATED ALWAYS AS (expr) STORED|VIRTUAL``;
    DuckDB and Oracle render ``GENERATED ALWAYS AS (expr)`` (virtual only); T-SQL renders
    ``AS (expr) [PERSISTED]``; Spanner renders ``AS (expr) STORED``. ``persisted=None``
    selects the dialect default storage.

    Raises:
        SQLBuilderError: When a stored generated column is requested on a dialect that only
            supports virtual generated columns.
    """
    computed_source: str | exp.Expr | None = col.generated_expression
    if (
        computed_source is None
        and isinstance(col.generated, str)
        and col.generated.strip().upper() not in _IDENTITY_KEYWORDS
    ):
        computed_source = col.generated

    if computed_source is None:
        return None

    norm_dialect = _normalize_dialect(dialect) if dialect else None
    if isinstance(computed_source, exp.Expr):
        parsed_computed: exp.Expr = computed_source.copy()
    else:
        parsed_computed = sqlglot.parse_one(str(computed_source), read=norm_dialect)
    parsed_computed = parsed_computed.unnest() if isinstance(parsed_computed, exp.Paren) else parsed_computed
    persisted = col.persisted if col.persisted is not None else norm_dialect in _STORED_GENERATED_DEFAULT_DIALECTS

    if norm_dialect in _VIRTUAL_ONLY_GENERATED_DIALECTS:
        if persisted:
            msg = f"Column {col.name!r}: {norm_dialect} supports only virtual generated columns, not STORED"
            raise SQLBuilderError(msg)
        return exp.ColumnConstraint(kind=exp.GeneratedAsIdentityColumnConstraint(this=True, expression=parsed_computed))
    if norm_dialect == "spanner":
        persisted = True
    computed_body = (
        parsed_computed if norm_dialect in _SELF_PARENTHESIZED_GENERATED_DIALECTS else exp.Paren(this=parsed_computed)
    )
    return exp.ColumnConstraint(kind=exp.ComputedColumnConstraint(this=computed_body, persisted=persisted))


def _build_default_expression(default: Any, dialect: "DialectType | None") -> exp.Expr:
    """Convert a column default (literal value, keyword string, SQL text, or expression) to an expression.

    Expression defaults are parenthesized on engines that only accept ``DEFAULT (expr)``
    for non-literal defaults (SQLite, MySQL, Spanner).
    """
    norm_dialect = _normalize_dialect(dialect) if dialect else None
    if isinstance(default, exp.Expr):
        default_expr: exp.Expr = default.copy()
    elif not isinstance(default, str):
        return exp.convert(default)
    elif default.upper() == CURRENT_TIMESTAMP_KEYWORD:
        return exp.CurrentTimestamp()
    elif default.upper() == CURRENT_DATE_KEYWORD:
        return exp.CurrentDate()
    elif default.upper() == CURRENT_TIME_KEYWORD:
        return exp.CurrentTime()
    elif "(" in default:
        default_expr = exp.maybe_parse(default, dialect=norm_dialect)
    else:
        return exp.convert(default)
    if (
        norm_dialect in _PARENTHESIZED_DEFAULT_DIALECTS
        and not isinstance(default_expr, exp.Paren)
        and not _is_literal_default(default_expr)
    ):
        return exp.Paren(this=default_expr)
    return default_expr


def _build_collate_column_constraint(collate: str, dialect: "DialectType | None" = None) -> exp.ColumnConstraint:
    """Build a COLLATE ColumnConstraint, preserving explicit case on case-folding dialects."""
    collate_ident = _parse_ddl_identifier(collate, dialect=dialect)
    if any(ch.isupper() for ch in collate) and (
        _normalize_dialect(dialect) in {"postgres", "postgresql", "cockroach", "cockroachdb", "duckdb"}
    ):
        collate_ident.set("quoted", True)
        collate_ident.meta["_explicit_quoted"] = True
        collate_ident.meta["sqlspec_explicit_table_quote"] = True
    return exp.ColumnConstraint(kind=exp.CollateColumnConstraint(this=collate_ident))


def _is_uuid_string(value: Any) -> bool:
    return isinstance(value, str) and bool(_UUID_STR_RE.match(value.strip()))


def _is_iso_datetime_string(value: Any) -> bool:
    if not isinstance(value, str) or not _ISO_DATETIME_STR_RE.match(value.strip()):
        return False
    normalized = value.strip()
    if normalized.endswith(("Z", "z")):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        datetime.fromisoformat(normalized)
    except ValueError:
        return False
    return True


def _is_iso_date_string(value: Any) -> bool:
    if not isinstance(value, str) or not _ISO_DATE_STR_RE.match(value.strip()):
        return False
    try:
        date.fromisoformat(value.strip())
    except ValueError:
        return False
    return True


def _infer_sql_type_from_values(values: "Sequence[Any]", dialect: str | None = None, *, key: bool = False) -> str:
    """Infer a dialect-aware SQL column type string from observed non-null Python values.

    Args:
        values: Observed non-null values for one column.
        dialect: Optional target dialect name.
        key: Whether the column participates in a primary key, which bounds text types
            on engines that cannot index unbounded text.

    Returns:
        SQL type string resolved through the shared category mapping.
    """
    category = _infer_value_category(values)
    if key and category == "text":
        category = "key_text"
    if category == "decimal":
        precision, scale = _decimal_values_precision(values)
        return sql_type_for_category("decimal", dialect, precision=precision, scale=scale)
    return sql_type_for_category(category, dialect)


def _infer_value_category(values: "Sequence[Any]") -> str:
    """Map observed non-null Python values to a neutral SQL type category."""
    if not values:
        return "text"
    if all(isinstance(val, bool) for val in values):
        return "boolean"
    if all(isinstance(val, (int, float, Decimal)) and not isinstance(val, bool) for val in values):
        if any(isinstance(val, Decimal) for val in values):
            return "decimal"
        if any(isinstance(val, float) for val in values):
            return "double"
        return "bigint"
    if all(isinstance(val, UUID) or _is_uuid_string(val) for val in values):
        return "uuid"
    if all(isinstance(val, datetime) or _is_iso_datetime_string(val) for val in values):
        return "timestamptz" if any(_is_timezone_aware(val) for val in values) else "timestamp"
    if all((isinstance(val, date) and not isinstance(val, datetime)) or _is_iso_date_string(val) for val in values):
        return "date"
    if all(
        isinstance(val, (date, datetime)) or _is_iso_date_string(val) or _is_iso_datetime_string(val) for val in values
    ):
        return "timestamptz" if any(_is_timezone_aware(val) for val in values) else "timestamp"
    if all(isinstance(val, time) for val in values):
        return "time"
    if all(isinstance(val, timedelta) for val in values):
        return "interval"
    if all(isinstance(val, (bytes, bytearray, memoryview)) for val in values):
        return "blob"
    if all(isinstance(val, (Mapping, list, tuple)) for val in values):
        return "json"
    return "text"


def _is_timezone_aware(value: Any) -> bool:
    """Return whether a datetime or ISO datetime string carries a UTC offset."""
    if isinstance(value, datetime):
        return value.tzinfo is not None and value.utcoffset() is not None
    if isinstance(value, str) and _is_iso_datetime_string(value):
        return bool(_ISO_OFFSET_SUFFIX_RE.search(value.strip()))
    return False


def _decimal_values_precision(values: "Sequence[Any]") -> tuple[int, int]:
    """Return a decimal precision and scale wide enough for every observed numeric value."""
    integer_digits = 1
    scale = _DEFAULT_INFERRED_DECIMAL_SCALE
    for val in values:
        if isinstance(val, Decimal) and val.is_finite():
            integer_digits = max(integer_digits, val.adjusted() + 1)
            scale = max(scale, -int(val.as_tuple().exponent))
        elif isinstance(val, int):
            integer_digits = max(integer_digits, len(str(abs(val))))
    return max(_DEFAULT_INFERRED_DECIMAL_PRECISION, integer_digits + scale), scale


def _normalize_ddl_identifier(node: exp.Expr, normalizer: Dialect) -> exp.Expr:
    """Case-normalize and quote one DDL identifier unless it carries an explicit quote marker."""
    if not isinstance(node, exp.Identifier):
        return node
    if not node.meta.get("sqlspec_explicit_table_quote"):
        normalizer.normalize_identifier(node)
    node.set("quoted", True)
    return node


def _dialect_foreign_key_actions(
    constraint: "ConstraintDefinition", dialect: "DialectType | None"
) -> tuple[str | None, str | None]:
    """Return the referential actions to render, rejecting actions the dialect cannot express.

    Oracle accepts only ``ON DELETE CASCADE`` and ``ON DELETE SET NULL``; DuckDB accepts no
    referential actions. ``NO ACTION`` and ``RESTRICT`` are the default behaviour there and
    are omitted.

    Raises:
        SQLBuilderError: When an action has no equivalent on the target dialect.
    """
    on_delete = constraint.on_delete
    on_update = constraint.on_update
    norm_dialect = _normalize_dialect(dialect) if dialect else None
    implicit_actions = {FOREIGN_KEY_ACTION_NO_ACTION, FOREIGN_KEY_ACTION_RESTRICT}
    if norm_dialect in {"duckdb", "bigquery"}:
        label_dialect = "BigQuery" if norm_dialect == "bigquery" else "DuckDB"
        for action_type, action in (("ON DELETE", on_delete), ("ON UPDATE", on_update)):
            if action and action not in implicit_actions:
                msg = f"{label_dialect} does not support {action_type} {action} on foreign keys"
                raise SQLBuilderError(msg)
        return None, None
    if norm_dialect != "oracle":
        return on_delete, on_update
    if on_update and on_update not in implicit_actions:
        msg = f"Oracle does not support ON UPDATE {on_update} on foreign keys"
        raise SQLBuilderError(msg)
    if on_delete and on_delete not in implicit_actions and on_delete not in _ORACLE_ON_DELETE_ACTIONS:
        msg = f"Oracle does not support ON DELETE {on_delete} on foreign keys"
        raise SQLBuilderError(msg)
    return (on_delete if on_delete in _ORACLE_ON_DELETE_ACTIONS else None), None
