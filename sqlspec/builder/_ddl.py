"""DDL statement builders.

Provides builders for DDL operations including CREATE, DROP, ALTER,
TRUNCATE, and other schema manipulation statements.
"""

import contextlib
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast
from uuid import UUID

import sqlglot
from mypy_extensions import mypyc_attr, trait
from sqlglot import exp
from sqlglot.errors import ParseError

from sqlspec.builder._base import QueryBuilder
from sqlspec.builder._parsing_utils import (
    _is_simple_identifier,
    _mark_explicit_table_quotes,
    _normalize_dialect,
    parse_order_expression,
)
from sqlspec.core import SQL
from sqlspec.data_dictionary._registry import get_dialect_config
from sqlspec.data_dictionary._types import (
    DDLResult,
    DialectConfig,
    MetadataFidelity,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
)
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.arrow_helpers import sql_type_from_arrow_type
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

    identity_constraint = _build_identity_column_constraint(col)
    if identity_constraint is not None:
        constraints.append(identity_constraint)

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
        persisted: bool = True,
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
        persisted: bool = True,
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
    ) -> "CreateTable":
        """Build a CreateTable builder from structured data_dictionary metadata rows."""
        builder = cls(table_name, dialect=dialect)
        if schema:
            builder.in_schema(schema)
        if if_not_exists:
            builder.if_not_exists()

        norm_columns = [_normalize_metadata_row(col) for col in columns]
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
            if not uq_cols or tuple(uq_cols) == tuple(pk_cols) or tuple(uq_cols) in seen_unique_cols:
                continue
            seen_unique_cols.add(tuple(uq_cols))
            unique_specs.append((uq_name, uq_cols))

        for row in norm_columns:
            col_name = _first_non_empty_str(row, "column_name", "name")
            if not col_name:
                continue
            dtype = _resolve_metadata_column_type(row, dialect=dialect)
            not_null = _resolve_metadata_not_null(row)
            computed_expr, persisted = _resolve_metadata_computed(row)
            identity_val = None if computed_expr is not None else _resolve_metadata_identity(row)
            auto_increment = (
                False
                if (computed_expr is not None or identity_val is not None)
                else _resolve_metadata_auto_increment(row)
            )
            raw_default = (
                None
                if (computed_expr is not None or identity_val is not None or auto_increment)
                else (
                    _first_present(row, "column_default", "default", "default_value")
                    or default_constraint_map.get(col_name)
                )
            )
            default_val = _parse_metadata_default(raw_default, dialect=dialect)
            collate_val = _first_non_empty_str(row, "collate", "collation", "collation_name")
            check_val = _first_non_empty_str(row, "check")
            is_col_pk = use_inline_pk and col_name == pk_cols[0]
            is_col_unique = (
                _truthy(_first_present(row, "is_unique", "unique"))
                and col_name not in pk_cols
                and (col_name,) not in seen_unique_cols
            )
            builder.column(
                name=col_name,
                dtype=dtype,
                default=default_val,
                not_null=not_null,
                primary_key=is_col_pk,
                unique=is_col_unique,
                auto_increment=auto_increment,
                check=check_val,
                collate=collate_val,
                identity=identity_val,
                generated_expression=computed_expr,
                persisted=persisted,
            )

        if pk_cols and not use_inline_pk:
            builder.primary_key_constraint(pk_cols, name=pk_name)

        for uq_name, uq_cols in unique_specs:
            builder.unique_constraint(uq_cols, name=uq_name)

        seen_checks: set[str] = set()
        for c_row in norm_constraints:
            if _normalize_constraint_type(c_row) != CONSTRAINT_TYPE_CHECK:
                continue
            chk_name = _first_non_empty_str(c_row, "constraint_name", "name")
            raw_cond = _first_non_empty_str(c_row, "check_clause", "condition", "definition", "expression")
            cond = _strip_check_wrapper(raw_cond) if raw_cond else None
            if not cond:
                continue
            dedup_key = chk_name or cond
            if dedup_key in seen_checks:
                continue
            seen_checks.add(dedup_key)
            builder.check_constraint(cond, name=chk_name)

        for fk_spec in _extract_metadata_foreign_keys(norm_constraints, norm_fks, table_schema=schema):
            builder.foreign_key_constraint(
                columns=fk_spec["columns"],
                references_table=fk_spec["ref_table"],
                references_columns=fk_spec["ref_columns"],
                name=fk_spec["name"],
                on_delete=fk_spec["on_delete"],
                on_update=fk_spec["on_update"],
                deferrable=fk_spec["deferrable"],
                initially_deferred=fk_spec["initially_deferred"],
            )

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

        for field in fields:
            col_name = str(field.name)
            dtype = sql_type_from_arrow_type(field.type, dialect=dialect_str)
            is_pk_member = col_name in pk_cols
            not_null = is_pk_member or not bool(getattr(field, "nullable", True))
            builder.column(name=col_name, dtype=dtype, not_null=not_null, primary_key=use_inline_pk and is_pk_member)

        if len(pk_cols) > 1:
            builder.primary_key_constraint(pk_cols)

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
            dtype = _infer_sql_type_from_values(values, dialect=dialect_str)
            builder.column(name=col_name, dtype=dtype, not_null=not_null, primary_key=use_inline_pk and is_pk_member)

        if len(pk_cols) > 1:
            builder.primary_key_constraint(pk_cols)

        return builder

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

    __slots__ = (
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

    @classmethod
    def from_metadata(
        cls,
        index: "Mapping[str, Any] | Any",
        *,
        table_name: "str | None" = None,
        schema: "str | None" = None,
        dialect: "DialectType" = None,
    ) -> "CreateIndex | None":
        """Build a CreateIndex builder from a structured index metadata row or IndexDetails object."""
        row = _normalize_metadata_row(index)
        if _truthy(_first_present(row, "is_primary", "is_primary_key", "primary_key")):
            return None
        if _truthy(row.get("is_unique_constraint")):
            return None

        index_name = _first_non_empty_str(row, "index_name", "name")
        if not index_name or index_name.lower().startswith("sqlite_autoindex_"):
            return None

        resolved_table = table_name or _first_non_empty_str(row, "table_name", "table")
        resolved_schema = (
            schema if schema is not None else _first_non_empty_str(row, "schema_name", "schema", "table_schema")
        )
        cols = _split_metadata_columns(_first_present(row, "columns", "column_names", "column_name"))
        if not resolved_table or not cols:
            return None

        builder = cls(index_name, dialect=dialect)
        if resolved_schema:
            builder.in_schema(resolved_schema)
        builder.on(resolved_table, *cols)

        if _truthy(_first_present(row, "is_unique", "unique")):
            builder.unique()

        inc_cols = _split_metadata_columns(_first_present(row, "included_columns", "include", "include_columns"))
        if inc_cols:
            builder.include(*inc_cols)

        has_filter_val = row.get("has_filter")
        if has_filter_val is None or _truthy(has_filter_val):
            where_clause = _first_non_empty_str(row, "where", "filter_definition", "predicate")
            if where_clause:
                builder.where(where_clause)

        using_method = _first_non_empty_str(row, "using", "index_type", "access_method")
        if using_method:
            using_upper = using_method.strip().upper().replace("_", " ")
            norm_dialect = _normalize_dialect(dialect) if dialect else None
            if using_upper not in _MSSQL_STORAGE_INDEX_TYPES and not (
                using_upper == "BTREE" and norm_dialect in {"tsql", "mssql", "sqlite"}
            ):
                builder.using(using_method.strip())

        return builder

    def _create_base_expression(self) -> exp.Expr:
        """Build the CREATE INDEX expression for this builder.

        Columns are turned into raw expressions (not ``Ordered``) unless explicit ordering
        is specified, string ``where`` clauses become expressions, and the final ``exp.Index``
        is wrapped in an ``exp.Create`` with the configured flags.
        """
        self._require(self._index_name and self._table_name, "Index name and table name must be set for CREATE INDEX.")

        effective_dialect = self._expression_dialect or self.dialect_name
        cols: list[exp.Expr] = []
        for col in self._columns:
            if isinstance(col, str):
                stripped = col.strip()
                if _ORDER_DIRECTION_SUFFIX_RE.search(stripped):
                    cols.append(parse_order_expression(stripped))
                elif _is_simple_identifier(stripped) or (
                    stripped[:1] in {'"', "`", "["} and "(" not in stripped and " " not in stripped[1:-1]
                ):
                    cols.append(exp.Column(this=_parse_ddl_identifier(stripped, dialect=effective_dialect)))
                else:
                    parsed_col: exp.Expr | None = exp.maybe_parse(stripped, dialect=effective_dialect)
                    cols.append(
                        parsed_col
                        if parsed_col is not None
                        else exp.Column(this=_parse_ddl_identifier(stripped, dialect=effective_dialect))
                    )
            else:
                cols.append(col.copy())

        include_cols: list[exp.Expr] = []
        for inc_col in self._include_columns:
            if isinstance(inc_col, str):
                include_cols.append(_parse_ddl_identifier(inc_col, dialect=effective_dialect))
            else:
                include_cols.append(inc_col.copy())

        where_expr = None
        if self._where:
            where_expr = exp.condition(self._where) if isinstance(self._where, str) else self._where

        index_params = exp.IndexParameters(columns=cols) if cols else None

        if include_cols:
            if index_params is None:
                index_params = exp.IndexParameters()
            index_params.set("include", include_cols)

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
            table=_parse_ddl_table(cast("str", self._table_name), schema=self._schema, dialect=effective_dialect),
            params=index_params,
        )

        return exp.Create(kind="INDEX", this=index_expr, unique=self._unique, exists=self._if_not_exists)

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
        """Build the SQLGlot Create expression for this sequence."""
        self._require(self._sequence_name, "Sequence name must be set for CREATE SEQUENCE.")
        effective_dialect = self._expression_dialect or self.dialect_name
        table_expr = _parse_ddl_table(self._sequence_name, schema=self._schema, dialect=effective_dialect)

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
        """Build the SQLGlot Drop expression for this sequence."""
        self._require(self._sequence_name, "Sequence name must be set for DROP SEQUENCE.")
        effective_dialect = self._expression_dialect or self.dialect_name
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
    object_type: str = "table",
    warnings: tuple[str, ...] = (),
) -> DDLResult:
    """Compile structured catalog metadata into a generated DDLResult for a table."""
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

    table_builder = CreateTable.from_metadata(
        table_name, columns, constraints=constraints, foreign_keys=foreign_keys, schema=schema, dialect=dialect
    )
    statements: list[str] = [table_builder.build(dialect=dialect).sql.rstrip(";").strip()]

    for index_row in _group_metadata_index_rows(indexes or ()):
        idx_builder = CreateIndex.from_metadata(index_row, table_name=table_name, schema=schema, dialect=dialect)
        if idx_builder is not None:
            statements.append(idx_builder.build(dialect=dialect).sql.rstrip(";").strip())

    if table_comment:
        tbl_comment = CommentOn(dialect=dialect).on_table(table_name).is_(table_comment)
        if schema:
            tbl_comment.in_schema(schema)
        statements.append(tbl_comment.build(dialect=dialect).sql.rstrip(";").strip())

    merged_column_comments: dict[str, str] = {}
    for col in columns:
        row = _normalize_metadata_row(col)
        col_name = _first_non_empty_str(row, "column_name", "name")
        col_comment = _first_non_empty_str(row, "comment", "column_comment", "description")
        if col_name and col_comment:
            merged_column_comments[col_name] = col_comment
    if column_comments:
        merged_column_comments.update({
            col_name: col_comment for col_name, col_comment in column_comments.items() if col_name and col_comment
        })

    for col_name, col_comment in merged_column_comments.items():
        col_comment_builder = CommentOn(dialect=dialect).on_column(table_name, col_name).is_(col_comment)
        if schema:
            col_comment_builder.in_schema(schema)
        statements.append(col_comment_builder.build(dialect=dialect).sql.rstrip(";").strip())

    ddl_text = ";\n\n".join(stmt for stmt in statements if stmt) + ";"
    return DDLResult(
        identity=identity,
        status=MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.GENERATED,
        source=MetadataSource.GENERATED,
        ddl=ddl_text,
        warnings=warnings,
    )


def _normalize_metadata_row(item: Any) -> dict[str, Any]:
    """Normalize a metadata mapping, ObjectMetadata, or ForeignKeyMetadata into a dict."""
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

    slots = getattr(type(item), "__slots__", None)
    if slots and isinstance(slots, (tuple, list)):
        slot_dict: dict[str, Any] = {}
        for key in slots:
            if isinstance(key, str) and hasattr(item, key):
                slot_dict[key] = getattr(item, key)
        if slot_dict:
            return slot_dict

    return {
        key: getattr(item, key) for key in dir(item) if not key.startswith("_") and not callable(getattr(item, key))
    }


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


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y"}
    return bool(value)


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
                is_mssql_bytes = _truthy(row.get("from_mssql_byte_length")) or (
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
        eff_precision = precision if precision is not None else (max_length if scale is not None else None)
        if eff_precision is not None and scale is not None:
            return f"{raw_type}({_safe_int(eff_precision)},{_safe_int(scale)})"
        if eff_precision is not None:
            return f"{raw_type}({_safe_int(eff_precision)})"

    if normalized in _TEMPORAL_SCALE_TYPES and scale is not None:
        return f"{raw_type}({_safe_int(scale)})"

    return raw_type


def _resolve_metadata_not_null(row: Mapping[str, Any]) -> bool:
    if row.get("not_null") is not None:
        return _truthy(row["not_null"])
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


def _resolve_metadata_computed(row: Mapping[str, Any]) -> tuple[str | exp.Expr | None, bool]:
    persisted_val = _first_present(row, "is_persisted_computed", "is_stored", "persisted")
    if persisted_val is not None:
        persisted = _truthy(persisted_val)
    elif (
        str(row.get("generated_kind") or "").strip().lower() in {"v", "virtual"}
        or "virtual" in str(row.get("extra") or "").lower()
        or ("is_persisted_computed" in row and row.get("is_persisted_computed") is None)
    ):
        persisted = False
    else:
        persisted = True

    expr_val = _first_present(row, "computed_definition", "generation_expression", "generated_expression")
    if expr_val is not None and str(expr_val).strip():
        return (expr_val if isinstance(expr_val, exp.Expr) else str(expr_val).strip(), persisted)

    gen_val = row.get("generated")
    if isinstance(gen_val, str) and gen_val.strip() and gen_val.strip().upper() not in _IDENTITY_KEYWORDS:
        return (gen_val.strip(), persisted)

    is_gen = (
        _truthy(row.get("is_computed"))
        or _truthy(row.get("is_generated"))
        or str(row.get("generated_kind") or "").strip().lower() in {"s", "v", "stored", "virtual"}
        or str(row.get("extra") or "").strip().lower() == "generated"
    )
    if is_gen:
        fallback_expr = _first_non_empty_str(row, "column_default", "default")
        if fallback_expr:
            return (fallback_expr, persisted)

    return (None, persisted)


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

    if _truthy(row.get("is_identity")):
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
    if _truthy(row.get("auto_increment")):
        return True
    extra = str(row.get("extra") or "").lower()
    return "auto_increment" in extra or "autoincrement" in extra


def _parse_metadata_default(value: Any, dialect: "DialectType | None" = None) -> Any:
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

    norm_dialect = _normalize_dialect(dialect) if dialect else None
    parsed: exp.Expr | None = exp.maybe_parse(stripped, dialect=norm_dialect)
    if parsed is None:
        return stripped
    while isinstance(parsed, exp.Paren):
        parsed = parsed.this
    if isinstance(parsed, exp.Column) and not parsed.table:
        return stripped
    return parsed


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
        elif _truthy(is_pk_val) or str(row.get("column_key") or "").strip().upper() == "PRI":
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
    return cleaned if cleaned in VALID_FOREIGN_KEY_ACTIONS else None


def _extract_metadata_foreign_keys(
    constraints: Sequence[Mapping[str, Any]],
    foreign_keys: Sequence[Mapping[str, Any]],
    *,
    table_schema: str | None = None,
) -> list[dict[str, Any]]:
    raw_items: list[Mapping[str, Any]] = [
        c_row for c_row in constraints if _normalize_constraint_type(c_row) == CONSTRAINT_TYPE_FOREIGN_KEY
    ]
    raw_items.extend(foreign_keys)

    merged: list[dict[str, Any]] = []
    for row in raw_items:
        name = _first_non_empty_str(row, "constraint_name", "name")
        cols = _split_metadata_columns(_first_present(row, "columns", "column_names", "column_name"))
        ref_table = _first_non_empty_str(row, "referenced_table", "references_table")
        ref_cols = _split_metadata_columns(
            _first_present(row, "referenced_columns", "references_columns", "referenced_column")
        )
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
            continue

        ref_schema = _first_non_empty_str(row, "referenced_schema", "references_schema")
        if ref_schema and table_schema and ref_schema != table_schema and "." not in ref_table:
            ref_table = f"{ref_schema}.{ref_table}"

        on_delete = _normalize_fk_action(_first_non_empty_str(row, "on_delete", "delete_rule"))
        on_update = _normalize_fk_action(_first_non_empty_str(row, "on_update", "update_rule"))
        if defn:
            if not on_delete:
                del_match = _FK_ON_DELETE_RE.search(defn)
                if del_match:
                    on_delete = _normalize_fk_action(del_match.group(1))
            if not on_update:
                upd_match = _FK_ON_UPDATE_RE.search(defn)
                if upd_match:
                    on_update = _normalize_fk_action(upd_match.group(1))

        deferrable = _truthy(_first_present(row, "deferrable", "is_deferrable"))
        initially_deferred = _truthy(row.get("initially_deferred"))

        matched_existing: dict[str, Any] | None = None
        for existing in merged:
            if name and existing["name"] == name:
                matched_existing = existing
                break
            if (
                tuple(existing["columns"]) == tuple(cols)
                and existing["ref_table"] == ref_table
                and tuple(existing["ref_columns"]) == tuple(ref_cols)
            ):
                matched_existing = existing
                break

        if matched_existing is not None:
            if len(cols) == 1 and len(ref_cols) == 1 and cols[0] not in matched_existing["columns"]:
                matched_existing["columns"].append(cols[0])
                matched_existing["ref_columns"].append(ref_cols[0])
            if not matched_existing["name"] and name:
                matched_existing["name"] = name
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
            "name": name,
            "columns": list(cols),
            "ref_table": ref_table,
            "ref_columns": list(ref_cols),
            "on_delete": on_delete,
            "on_update": on_update,
            "deferrable": deferrable,
            "initially_deferred": initially_deferred,
        })

    return [item for item in merged if len(item["columns"]) == len(item["ref_columns"])]


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


def _build_identity_column_constraint(col: "ColumnDefinition") -> exp.ColumnConstraint | None:
    """Build an identity ColumnConstraint for a ColumnDefinition when configured."""
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
    """Build a computed ColumnConstraint for a ColumnDefinition when configured."""
    computed_source: str | exp.Expr | None = col.generated_expression
    if (
        computed_source is None
        and isinstance(col.generated, str)
        and col.generated.strip().upper() not in _IDENTITY_KEYWORDS
    ):
        computed_source = col.generated

    if computed_source is None:
        return None

    if isinstance(computed_source, exp.Expr):
        parsed_computed: exp.Expr = computed_source.copy()
    else:
        norm_dialect = _normalize_dialect(dialect) if dialect else None
        parsed_computed = sqlglot.parse_one(str(computed_source), read=norm_dialect)
    if isinstance(parsed_computed, exp.Paren):
        parsed_computed = parsed_computed.unnest()
    return exp.ColumnConstraint(kind=exp.ComputedColumnConstraint(this=parsed_computed, persisted=col.persisted))


def _build_collate_column_constraint(collate: str, dialect: "DialectType | None" = None) -> exp.ColumnConstraint:
    """Build a COLLATE ColumnConstraint, preserving explicit case on case-folding dialects."""
    collate_ident = _parse_ddl_identifier(collate, dialect=dialect)
    if any(ch.isupper() for ch in collate) and (
        _normalize_dialect(dialect) in {"postgres", "postgresql", "cockroach", "cockroachdb", "duckdb"}
    ):
        collate_ident.set("quoted", True)
        collate_ident.meta["_explicit_quoted"] = True
    return exp.ColumnConstraint(kind=exp.CollateColumnConstraint(this=collate_ident))


def _resolve_ddl_dialect_config(dialect: str | None) -> "DialectConfig | None":
    if not dialect:
        return None
    with contextlib.suppress(ValueError):
        return get_dialect_config(dialect)
    return None


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


def _infer_sql_type_from_values(values: "Sequence[Any]", dialect: str | None = None) -> str:
    """Infer a dialect-aware SQL column type string from observed non-null Python values."""
    config = _resolve_ddl_dialect_config(dialect)
    if not values:
        return config.get_optimal_type("text") if config is not None else "TEXT"

    if all(isinstance(val, bool) for val in values):
        return config.get_optimal_type("boolean") if config is not None else "BOOLEAN"

    if all(isinstance(val, (int, float, Decimal)) and not isinstance(val, bool) for val in values):
        if any(isinstance(val, Decimal) for val in values):
            return "DECIMAL(38, 9)"
        if any(isinstance(val, float) for val in values):
            if config is not None and config.name in {"duckdb", "mysql", "mariadb"}:
                return "DOUBLE"
            if config is not None and config.name in {"bigquery", "spanner"}:
                return config.get_optimal_type("float")
            return "DOUBLE PRECISION"
        return "BIGINT"

    if all(isinstance(val, UUID) or _is_uuid_string(val) for val in values):
        return config.get_optimal_type("uuid") if config is not None else "UUID"

    if all(isinstance(val, datetime) or _is_iso_datetime_string(val) for val in values):
        return config.get_optimal_type("timestamp") if config is not None else "TIMESTAMP WITH TIME ZONE"

    if all((isinstance(val, date) and not isinstance(val, datetime)) or _is_iso_date_string(val) for val in values):
        return "DATE"

    if all(
        isinstance(val, (date, datetime)) or _is_iso_date_string(val) or _is_iso_datetime_string(val) for val in values
    ):
        return config.get_optimal_type("timestamp") if config is not None else "TIMESTAMP WITH TIME ZONE"

    if all(isinstance(val, time) for val in values):
        return "TIME"

    if all(isinstance(val, timedelta) for val in values):
        return "INTERVAL"

    if all(isinstance(val, (bytes, bytearray, memoryview)) for val in values):
        return config.get_optimal_type("blob") if config is not None else "BLOB"

    if all(isinstance(val, (Mapping, list, tuple)) for val in values):
        return config.get_optimal_type("json") if config is not None else "JSON"

    return config.get_optimal_type("text") if config is not None else "TEXT"
