"""Additive schema ensure and diff helpers."""

import logging
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final

from sqlglot import Dialect, TokenType, exp, parse, parse_one
from sqlglot.errors import ParseError, TokenError

from sqlspec.builder._ddl import AlterTable, ColumnDefinition, CreateTable, _parse_ddl_identifier, _parse_ddl_table
from sqlspec.exceptions import MigrationError
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlglot.dialects.dialect import DialectType
    from sqlglot.tokenizer_core import Token

__all__ = (
    "DropStatement",
    "SchemaEnsureResult",
    "SchemaTarget",
    "ensure_schema_async",
    "ensure_schema_sync",
    "metadata_has_index",
    "run_drop_statements_async",
    "run_drop_statements_sync",
)

logger = get_logger("sqlspec.migrations.schema")

_SAFE_ADD_COLUMN_CONSTRAINTS: Final[tuple[type[exp.Expr], ...]] = (
    exp.NotNullColumnConstraint,
    exp.DefaultColumnConstraint,
    exp.PrimaryKeyColumnConstraint,
    exp.UniqueColumnConstraint,
    exp.Reference,
)
_INLINE_CONSTRAINT_CLASSES: Final[tuple[type[exp.Expr], ...]] = (
    exp.PrimaryKeyColumnConstraint,
    exp.UniqueColumnConstraint,
    exp.Reference,
    exp.CheckColumnConstraint,
)
_UNSAFE_INLINE_CONSTRAINT_CLASSES: Final[tuple[type[exp.Expr], ...]] = (
    exp.CheckColumnConstraint,
    exp.ComputedColumnConstraint,
    exp.GeneratedAsIdentityColumnConstraint,
    exp.AutoIncrementColumnConstraint,
    exp.CollateColumnConstraint,
)
_ADD_COLUMN_REMEDY: Final[str] = (
    'Add the column with an application migration, or set migration_mode="versioned" for this extension.'
)
_ALIGN_COLUMN_REMEDY: Final[str] = (
    'Align the column with an application migration, or set migration_mode="versioned" for this extension.'
)

_TYPE_FAMILIES: Final[dict[str, str]] = {
    "tinyint": "numeric",
    "byteint": "numeric",
    "smallint": "numeric",
    "mediumint": "numeric",
    "int": "numeric",
    "int2": "numeric",
    "int4": "numeric",
    "int8": "numeric",
    "int64": "numeric",
    "integer": "numeric",
    "bigint": "numeric",
    "decimal": "numeric",
    "numeric": "numeric",
    "number": "numeric",
    "real": "numeric",
    "float": "numeric",
    "float4": "numeric",
    "float8": "numeric",
    "float32": "numeric",
    "float64": "numeric",
    "double": "numeric",
    "double precision": "numeric",
    "bignumeric": "numeric",
    "decfloat": "numeric",
    "money": "numeric",
    "smallmoney": "numeric",
    "serial": "numeric",
    "bigserial": "numeric",
    "oid": "numeric",
    "boolean": "boolean",
    "bool": "boolean",
    "bit": "bit",
    "char": "text",
    "character": "text",
    "nchar": "text",
    "bpchar": "text",
    "varchar": "text",
    "character varying": "text",
    "nvarchar": "text",
    "varchar2": "text",
    "nvarchar2": "text",
    "text": "text",
    "ntext": "text",
    "mediumtext": "text",
    "longtext": "text",
    "tinytext": "text",
    "clob": "text",
    "nclob": "text",
    "dbclob": "text",
    "string": "text",
    "long varchar": "text",
    "long nvarchar": "text",
    "name": "text",
    "citext": "text",
    "binary": "binary",
    "byte": "binary",
    "varbyte": "binary",
    "varbinary": "binary",
    "blob": "binary",
    "tinyblob": "binary",
    "mediumblob": "binary",
    "longblob": "binary",
    "bytea": "binary",
    "bytes": "binary",
    "image": "binary",
    "long binary": "binary",
    "raw": "binary",
    "long raw": "binary",
    "json": "json",
    "jsonb": "json",
    "date": "temporal",
    "time": "temporal",
    "timetz": "temporal",
    "datetime": "temporal",
    "datetime2": "temporal",
    "smalldatetime": "temporal",
    "datetimeoffset": "temporal",
    "timestamp": "temporal",
    "timestamptz": "temporal",
    "timestamp with time zone": "temporal",
    "timestamp without time zone": "temporal",
    "timestamp with local time zone": "temporal",
    "timestamp_tz": "temporal",
    "timestamp_ltz": "temporal",
    "timestamp_ntz": "temporal",
    "bigdatetime": "temporal",
    "bigtime": "temporal",
    "interval": "temporal",
    "uuid": "uuid",
    "uniqueidentifier": "uuid",
    "vector": "vector",
    "halfvec": "vector",
    "sparsevec": "vector",
    "array": "vector",
    "struct": "struct",
    "variant": "struct",
    "object": "struct",
    "tokenlist": "struct",
    "tsvector": "struct",
}


class DropStatement:
    """One DROP statement run by an extension downgrade.

    Dialects without ``DROP ... IF EXISTS`` supply a catalog probe; the statement then runs only
    when the probe returns a row, so a retried downgrade skips objects that are already gone.

    Args:
        statement: DROP statement to execute.
        exists_sql: Optional catalog query returning a row when the object exists.
        exists_parameters: Parameters bound to ``exists_sql``.
    """

    __slots__ = ("exists_parameters", "exists_sql", "statement")

    def __init__(
        self, statement: str, *, exists_sql: str | None = None, exists_parameters: "tuple[Any, ...]" = ()
    ) -> None:
        self.statement = statement
        self.exists_sql = exists_sql
        self.exists_parameters = exists_parameters

    def __repr__(self) -> str:
        return f"DropStatement({self.statement!r}, exists_sql={self.exists_sql!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DropStatement):
            return NotImplemented
        return (self.statement, self.exists_sql, self.exists_parameters) == (
            other.statement,
            other.exists_sql,
            other.exists_parameters,
        )

    def __hash__(self) -> int:
        return hash((self.statement, self.exists_sql, self.exists_parameters))


def run_drop_statements_sync(driver: Any, drops: "Sequence[DropStatement]") -> None:
    """Execute downgrade drops in order, skipping probed objects that no longer exist.

    Args:
        driver: Synchronous migration driver.
        drops: DROP statements with optional existence probes.
    """
    for drop in drops:
        if drop.exists_sql is not None and driver.select_one_or_none(drop.exists_sql, drop.exists_parameters) is None:
            continue
        driver.execute_script(drop.statement)


async def run_drop_statements_async(driver: Any, drops: "Sequence[DropStatement]") -> None:
    """Execute downgrade drops in order on an async driver, skipping probed objects that no longer exist.

    Args:
        driver: Asynchronous migration driver.
        drops: DROP statements with optional existence probes.
    """
    for drop in drops:
        if (
            drop.exists_sql is not None
            and await driver.select_one_or_none(drop.exists_sql, drop.exists_parameters) is None
        ):
            continue
        await driver.execute_script(drop.statement)


class SchemaTarget:
    """Describe one table's target schema.

    Args:
        table_name: Unqualified table name used for introspection and DDL.
        create_table: Builder containing the target columns and create DDL.
        schema: Optional schema containing the table.
        create_statement: Optional raw DDL or CreateTable executed on creation.
        constraint_columns: Extra case-folded column names governed by constraints.
        unsafe_add_columns: Extra case-folded column names unsafe for additive ALTER TABLE.
    """

    __slots__ = ("constraint_columns", "create_statement", "create_table", "schema", "table_name", "unsafe_add_columns")

    def __init__(
        self,
        table_name: str,
        create_table: CreateTable,
        schema: str | None = None,
        create_statement: "str | CreateTable | None" = None,
        *,
        constraint_columns: frozenset[str] | None = None,
        unsafe_add_columns: frozenset[str] | None = None,
    ) -> None:
        parsed_table = _parse_ddl_table(table_name, schema=schema, dialect=create_table.dialect)
        self.table_name = parsed_table.name or table_name
        db_arg = parsed_table.args.get("db")
        self.schema = db_arg.name if isinstance(db_arg, exp.Identifier) and db_arg.name else schema
        if (
            schema
            and not create_table._schema
            and not _parse_ddl_table(create_table._table_name, dialect=create_table.dialect).args.get("db")
        ):
            create_table.in_schema(schema)
        self.create_table = create_table
        self.create_statement = create_statement or create_table
        derived_constraints, derived_unsafe = _derive_builder_column_sets(create_table)
        extra_constraints = (
            frozenset(name.casefold() for name in constraint_columns) if constraint_columns else frozenset()
        )
        extra_unsafe = frozenset(name.casefold() for name in unsafe_add_columns) if unsafe_add_columns else frozenset()
        self.constraint_columns: frozenset[str] = derived_constraints.union(extra_constraints)
        self.unsafe_add_columns: frozenset[str] = derived_unsafe.union(extra_unsafe)

    @property
    def identity(self) -> str:
        """Return the schema-qualified identity used in results."""
        if self.schema:
            return f"{self.schema}.{self.table_name}"
        return self.table_name

    @classmethod
    def from_ddl(
        cls, table_name: str, create_statement: str, *, schema: str | None = None, dialect: Any = None
    ) -> "SchemaTarget":
        """Build a target descriptor from an adapter's canonical CREATE TABLE DDL.

        Args:
            table_name: Unqualified table name used for introspection.
            create_statement: Canonical adapter DDL, including any companion statements.
            schema: Optional schema containing the table.
            dialect: SQLGlot dialect used to parse column definitions.

        Returns:
            Target descriptor that executes the original DDL while deriving
            additive column statements and refusal metadata from the parsed definition.

        Raises:
            ValueError: If no CREATE TABLE definition can be parsed.
        """
        create_expression = _find_create_table_expression(create_statement, table_name, dialect)
        target = CreateTable(table_name, dialect=dialect)
        if schema:
            target.in_schema(schema)

        schema_expr = create_expression.this
        top_expressions = list(schema_expr.expressions) if isinstance(schema_expr, exp.Schema) else []
        derived_constraints: set[str] = set()
        derived_unsafe: set[str] = set()

        for item in top_expressions:
            if not isinstance(item, exp.ColumnDef):
                continue
            kind = item.args.get("kind")
            if not isinstance(kind, exp.DataType):
                continue
            folded_name = item.name.casefold()
            constraints = [
                constraint.args.get("kind")
                for constraint in item.args.get("constraints", [])
                if constraint.args.get("kind") is not None
            ]
            default = next(
                (
                    constraint.this.sql(dialect=dialect)
                    for constraint in constraints
                    if isinstance(constraint, exp.DefaultColumnConstraint) and constraint.this is not None
                ),
                None,
            )
            not_null = any(
                isinstance(constraint, exp.NotNullColumnConstraint) and not constraint.args.get("allow_null")
                for constraint in constraints
            )
            primary_key = any(isinstance(constraint, exp.PrimaryKeyColumnConstraint) for constraint in constraints)
            unique = any(isinstance(constraint, exp.UniqueColumnConstraint) for constraint in constraints)

            for constraint in constraints:
                if isinstance(constraint, _INLINE_CONSTRAINT_CLASSES):
                    derived_constraints.add(folded_name)
                if isinstance(constraint, _UNSAFE_INLINE_CONSTRAINT_CLASSES) or not isinstance(
                    constraint, _SAFE_ADD_COLUMN_CONSTRAINTS
                ):
                    derived_unsafe.add(folded_name)

            target.column(
                item.name,
                kind.sql(dialect=dialect),
                default=default,
                not_null=not_null,
                primary_key=primary_key,
                unique=unique,
            )

        if not target.columns:
            msg = f"CREATE TABLE DDL for {table_name!r} has no parseable columns"
            raise ValueError(msg)

        defined_cols = {column.name.casefold() for column in target.columns}
        table_constraint_nodes: list[exp.Expr] = [
            item for item in top_expressions if not isinstance(item, exp.ColumnDef)
        ]
        props = create_expression.args.get("properties")
        if isinstance(props, exp.Properties):
            table_constraint_nodes.extend(props.expressions)
        derived_constraints.update(_collect_table_constraint_columns(table_constraint_nodes, defined_cols))

        return cls(
            table_name,
            target,
            schema,
            create_statement,
            constraint_columns=frozenset(derived_constraints),
            unsafe_add_columns=frozenset(derived_unsafe),
        )


class SchemaEnsureResult:
    """Summarize changes made by an ensure operation."""

    __slots__ = ("added_columns", "created_tables", "deferred_tables", "migrations_run")

    def __init__(self) -> None:
        self.created_tables: list[str] = []
        self.added_columns: dict[str, list[str]] = {}
        self.deferred_tables: list[str] = []
        self.migrations_run = False


class _TargetPlan:
    """Internal planned schema action produced during preflight introspection."""

    __slots__ = ("action", "existing_tables", "statements", "target")

    def __init__(
        self, action: str, target: SchemaTarget, existing_tables: set[str], statements: "list[tuple[str, AlterTable]]"
    ) -> None:
        self.action = action
        self.target = target
        self.existing_tables = existing_tables
        self.statements = statements


def metadata_has_index(
    metadata_rows: Sequence[Any], table_name: str, index_name: str, *, schema_name: str | None = None
) -> bool:
    """Check whether catalog index metadata contains a matching index.

    Args:
        metadata_rows: Rows returned by ``data_dictionary.get_indexes``.
        table_name: Unqualified table name to match.
        index_name: Index name to match.
        schema_name: Optional schema name to match when reported on rows.

    Returns:
        ``True`` if a matching index exists, ``False`` otherwise.
    """
    folded_index = index_name.casefold()
    folded_table = table_name.casefold()
    folded_schema = schema_name.casefold() if schema_name is not None else None
    for row in metadata_rows:
        row_index = _metadata_value(row, ("index_name", "name"))
        if row_index is None or str(row_index).casefold() != folded_index:
            continue
        row_table = _metadata_value(row, ("table_name", "table"))
        if row_table is not None and str(row_table).casefold() != folded_table:
            continue
        if folded_schema is not None:
            row_schema = _metadata_value(row, ("schema_name", "table_schema", "schema"))
            if row_schema is not None and str(row_schema).casefold() != folded_schema:
                continue
        return True
    return False


def ensure_schema_sync(
    driver: Any,
    targets: Sequence[SchemaTarget],
    *,
    manage_schema: bool = False,
    create_schema: bool = True,
    run_migrations: bool = False,
    migration_runner: "Callable[[Any], None] | None" = None,
    assume_existing: bool = False,
    commit: bool = True,
    strict: bool = False,
) -> SchemaEnsureResult:
    """Apply additive target-schema changes with a synchronous driver.

    Explicit migrations remain independent of automatic schema currency. When
    requested, ``migration_runner`` runs before schema introspection and owns
    its migration-ledger behavior. Automatic changes never write ledger rows.

    Args:
        driver: Synchronous driver used for introspection and DDL.
        targets: Target table descriptors.
        manage_schema: Enable automatic table and column management.
        create_schema: Create target tables that are absent.
        run_migrations: Run the explicit migration callback first.
        migration_runner: Explicit migration callback.
        assume_existing: Skip table discovery when the caller already ensured the tables.
        commit: Commit schema changes on the driver when changes occur.
        strict: Preflight-refuse unsafe missing columns and incompatible existing-column drift.

    Returns:
        Summary of created tables, added columns, and deferred targets.

    Raises:
        ValueError: If explicit migrations are enabled without a runner.
        MigrationError: If ``strict=True`` detects non-reconcilable schema drift.
    """
    result = SchemaEnsureResult()
    if run_migrations:
        if migration_runner is None:
            msg = "migration_runner is required when run_migrations=True"
            raise ValueError(msg)
        migration_runner(driver)
        result.migrations_run = True
    if not manage_schema:
        return result

    tables_by_schema: dict[str | None, set[str]] = {}
    plans: list[_TargetPlan] = []
    for target in targets:
        if assume_existing:
            existing_tables = {target.table_name.casefold()}
        else:
            cached_tables = tables_by_schema.get(target.schema)
            if cached_tables is None:
                table_data = (
                    driver.data_dictionary.get_tables(driver, schema=target.schema)
                    if target.schema
                    else driver.data_dictionary.get_tables(driver)
                )
                cached_tables = _metadata_names(table_data, "table_name")
                tables_by_schema[target.schema] = cached_tables
            existing_tables = cached_tables

        if target.table_name.casefold() not in existing_tables:
            if create_schema:
                plans.append(_TargetPlan("create", target, existing_tables, []))
            elif strict:
                msg = (
                    f"Cannot reconcile missing table {target.identity!r} when create_schema=False. "
                    'Create the table with an application migration, or set migration_mode="versioned" '
                    "for this extension."
                )
                raise MigrationError(msg)
            else:
                plans.append(_TargetPlan("defer", target, existing_tables, []))
            continue

        columns_data = (
            driver.data_dictionary.get_columns(driver, target.table_name, schema=target.schema)
            if target.schema
            else driver.data_dictionary.get_columns(driver, target.table_name)
        )
        existing_names = _metadata_names(columns_data, "column_name")
        if strict:
            _preflight_strict_target(target, existing_names, columns_data)
        statements, deferred = _add_column_statements(target, existing_names, strict=strict)
        if deferred:
            plans.append(_TargetPlan("defer", target, existing_tables, []))
        else:
            plans.append(_TargetPlan("alter", target, existing_tables, statements))

    changed = False
    for plan in plans:
        if plan.action == "defer":
            result.deferred_tables.append(plan.target.identity)
        elif plan.action == "create":
            if plan.target.table_name.casefold() not in plan.existing_tables:
                _execute_create_sync(driver, plan.target)
                result.created_tables.append(plan.target.identity)
                plan.existing_tables.add(plan.target.table_name.casefold())
                changed = True
        elif plan.statements:
            added: list[str] = []
            for column_name, statement in plan.statements:
                driver.execute(statement)
                added.append(column_name)
            result.added_columns[plan.target.identity] = added
            changed = True

    if changed and commit:
        _commit_schema_sync(driver)
    return result


async def ensure_schema_async(
    driver: Any,
    targets: Sequence[SchemaTarget],
    *,
    manage_schema: bool = False,
    create_schema: bool = True,
    run_migrations: bool = False,
    migration_runner: "Callable[[Any], Awaitable[None]] | None" = None,
    assume_existing: bool = False,
    commit: bool = True,
    strict: bool = False,
) -> SchemaEnsureResult:
    """Apply additive target-schema changes with an asynchronous driver.

    Args:
        driver: Asynchronous driver used for introspection and DDL.
        targets: Target table descriptors.
        manage_schema: Enable automatic table and column management.
        create_schema: Create target tables that are absent.
        run_migrations: Run the explicit migration callback first.
        migration_runner: Explicit asynchronous migration callback.
        assume_existing: Skip table discovery when the caller already ensured the tables.
        commit: Commit schema changes on the driver when changes occur.
        strict: Preflight-refuse unsafe missing columns and incompatible existing-column drift.

    Returns:
        Summary of created tables, added columns, and deferred targets.

    Raises:
        ValueError: If explicit migrations are enabled without a runner.
        MigrationError: If ``strict=True`` detects non-reconcilable schema drift.
    """
    result = SchemaEnsureResult()
    if run_migrations:
        if migration_runner is None:
            msg = "migration_runner is required when run_migrations=True"
            raise ValueError(msg)
        await migration_runner(driver)
        result.migrations_run = True
    if not manage_schema:
        return result

    tables_by_schema: dict[str | None, set[str]] = {}
    plans: list[_TargetPlan] = []
    for target in targets:
        if assume_existing:
            existing_tables = {target.table_name.casefold()}
        else:
            cached_tables = tables_by_schema.get(target.schema)
            if cached_tables is None:
                table_data = (
                    await driver.data_dictionary.get_tables(driver, schema=target.schema)
                    if target.schema
                    else await driver.data_dictionary.get_tables(driver)
                )
                cached_tables = _metadata_names(table_data, "table_name")
                tables_by_schema[target.schema] = cached_tables
            existing_tables = cached_tables

        if target.table_name.casefold() not in existing_tables:
            if create_schema:
                plans.append(_TargetPlan("create", target, existing_tables, []))
            elif strict:
                msg = (
                    f"Cannot reconcile missing table {target.identity!r} when create_schema=False. "
                    'Create the table with an application migration, or set migration_mode="versioned" '
                    "for this extension."
                )
                raise MigrationError(msg)
            else:
                plans.append(_TargetPlan("defer", target, existing_tables, []))
            continue

        columns_data = (
            await driver.data_dictionary.get_columns(driver, target.table_name, schema=target.schema)
            if target.schema
            else await driver.data_dictionary.get_columns(driver, target.table_name)
        )
        existing_names = _metadata_names(columns_data, "column_name")
        if strict:
            _preflight_strict_target(target, existing_names, columns_data)
        statements, deferred = _add_column_statements(target, existing_names, strict=strict)
        if deferred:
            plans.append(_TargetPlan("defer", target, existing_tables, []))
        else:
            plans.append(_TargetPlan("alter", target, existing_tables, statements))

    changed = False
    for plan in plans:
        if plan.action == "defer":
            result.deferred_tables.append(plan.target.identity)
        elif plan.action == "create":
            if plan.target.table_name.casefold() not in plan.existing_tables:
                await _execute_create_async(driver, plan.target)
                result.created_tables.append(plan.target.identity)
                plan.existing_tables.add(plan.target.table_name.casefold())
                changed = True
        elif plan.statements:
            added: list[str] = []
            for column_name, statement in plan.statements:
                await driver.execute(statement)
                added.append(column_name)
            result.added_columns[plan.target.identity] = added
            changed = True

    if changed and commit:
        await _commit_schema_async(driver)
    return result


def _commit_schema_sync(driver: Any) -> None:
    """Commit schema changes on a sync driver unless autocommit is enabled."""
    driver_features = getattr(driver, "driver_features", None)
    if isinstance(driver_features, Mapping) and driver_features.get("autocommit", False):
        return
    try:
        driver.commit()
    except Exception as exc:
        exc_str = str(exc).lower()
        if "autocommit" not in exc_str and "cannot commit" not in exc_str:
            raise


async def _commit_schema_async(driver: Any) -> None:
    """Commit schema changes on an async driver unless autocommit is enabled."""
    driver_features = getattr(driver, "driver_features", None)
    if isinstance(driver_features, Mapping) and driver_features.get("autocommit", False):
        return
    try:
        await driver.commit()
    except Exception as exc:
        exc_str = str(exc).lower()
        if "autocommit" not in exc_str and "cannot commit" not in exc_str:
            raise


def _execute_create_sync(driver: Any, target: SchemaTarget) -> None:
    """Execute canonical table DDL through the appropriate synchronous path."""
    if isinstance(target.create_statement, str):
        driver.execute_script(target.create_statement)
        return
    driver.execute(target.create_statement)


async def _execute_create_async(driver: Any, target: SchemaTarget) -> None:
    """Execute canonical table DDL through the appropriate asynchronous path."""
    if isinstance(target.create_statement, str):
        await driver.execute_script(target.create_statement)
        return
    await driver.execute(target.create_statement)


def _add_column_statements(
    target: SchemaTarget, existing_columns: set[str], *, strict: bool = False
) -> "tuple[list[tuple[str, AlterTable]], bool]":
    """Build additive statements and identify likely rename-only drift."""
    target_columns = {column.name.casefold(): column for column in target.create_table.columns}
    missing_columns = set(target_columns).difference(existing_columns)
    extra_columns = existing_columns.difference(target_columns)
    if not strict and missing_columns and extra_columns and len(target_columns) == len(existing_columns):
        return [], True

    target_table = _parse_ddl_table(
        target.create_table._table_name, schema=target.create_table._schema, dialect=target.create_table.dialect
    )
    if not target_table.args.get("db") and target.schema:
        target_table.set("db", _parse_ddl_identifier(target.schema, dialect=target.create_table.dialect))
    alter_table_name = target_table.sql(dialect=target.create_table.dialect, copy=False)

    statements: list[tuple[str, AlterTable]] = []
    for column_name in sorted(missing_columns):
        column = target_columns[column_name]
        statement = AlterTable(alter_table_name, dialect=target.create_table.dialect)
        statement.add_column(
            name=column.name,
            dtype=column.dtype,
            default=column.default,
            not_null=column.not_null,
            unique=column.unique,
            comment=column.comment,
        )
        statements.append((column_name, statement))
    return statements, False


def _derive_builder_column_sets(create_table: CreateTable) -> "tuple[frozenset[str], frozenset[str]]":
    """Extract case-folded constraint and unsafe-add column sets from a CreateTable builder."""
    defined_cols = {col.name.casefold() for col in create_table.columns}
    derived_constraints: set[str] = set()
    derived_unsafe: set[str] = set()

    for col in create_table.columns:
        folded = col.name.casefold()
        if col.primary_key or col.unique:
            derived_constraints.add(folded)
        if col.check is not None:
            derived_constraints.add(folded)
            derived_unsafe.add(folded)
        if (
            col.generated
            or col.generated_expression is not None
            or col.identity
            or col.auto_increment
            or col.collate is not None
        ):
            derived_unsafe.add(folded)

    for constraint in getattr(create_table, "_constraints", ()):
        ctype = constraint.constraint_type
        if ctype in {"PRIMARY KEY", "UNIQUE", "FOREIGN KEY"}:
            derived_constraints.update(name.casefold() for name in constraint.columns)
        elif ctype == "CHECK":
            derived_constraints.update(name.casefold() for name in constraint.columns)
            check_expr = constraint.condition_expr
            if check_expr is None and constraint.condition:
                check_expr = _parse_optional_expression(constraint.condition, create_table.dialect)
            if check_expr is not None:
                derived_constraints.update(_collect_expression_column_names(check_expr, defined_cols))

    return frozenset(derived_constraints), frozenset(derived_unsafe)


def _parse_optional_expression(sql_fragment: str, dialect: Any) -> "exp.Expr | None":
    """Parse a SQL expression fragment, returning None on parse failure."""
    try:
        return parse_one(sql_fragment, read=dialect)
    except (ParseError, TokenError):
        return None


def _collect_expression_column_names(expression: exp.Expr, defined_cols: set[str]) -> set[str]:
    """Collect defined column names referenced inside a CHECK expression."""
    matched: set[str] = set()
    for col_node in expression.find_all(exp.Column):
        folded = col_node.name.casefold()
        if folded in defined_cols:
            matched.add(folded)
    for ident_node in expression.find_all(exp.Identifier):
        if isinstance(ident_node.parent, (exp.Table, exp.Func)):
            continue
        folded = ident_node.name.casefold()
        if folded in defined_cols:
            matched.add(folded)
    return matched


def _extract_key_node_column(node: exp.Expr) -> "str | None":
    """Extract a bare column identifier from an index/key expression node."""
    inner = node.this if isinstance(node, exp.Ordered) else node
    if isinstance(inner, (exp.Column, exp.Identifier)) and inner.name:
        return inner.name.casefold()
    return None


def _collect_table_constraint_columns(constraint_nodes: Sequence[exp.Expr], defined_cols: set[str]) -> set[str]:
    """Extract local constrained column names from table-level constraint AST nodes."""
    constrained: set[str] = set()
    expanded: list[exp.Expr] = []
    for raw_node in constraint_nodes:
        if isinstance(raw_node, exp.Constraint):
            expanded.extend(raw_node.expressions)
        else:
            expanded.append(raw_node)

    for node in expanded:
        if isinstance(node, exp.PrimaryKey):
            for item in node.expressions:
                col_name = _extract_key_node_column(item)
                if col_name is not None:
                    constrained.add(col_name)
        elif isinstance(node, exp.UniqueColumnConstraint):
            candidates: list[exp.Expr] = list(node.expressions)
            if isinstance(node.this, exp.Schema):
                candidates.extend(node.this.expressions)
            elif isinstance(node.this, exp.Expr):
                candidates.append(node.this)
            for item in candidates:
                col_name = _extract_key_node_column(item)
                if col_name is not None:
                    constrained.add(col_name)
        elif isinstance(node, exp.ForeignKey):
            for item in node.expressions:
                col_name = _extract_key_node_column(item)
                if col_name is not None:
                    constrained.add(col_name)
        elif isinstance(node, (exp.Check, exp.CheckColumnConstraint)) and isinstance(node.this, exp.Expr):
            constrained.update(_collect_expression_column_names(node.this, defined_cols))

    return constrained


def _preflight_strict_target(target: SchemaTarget, existing_names: set[str], columns_data: Sequence[Any]) -> None:
    """Validate both missing and existing target columns under strict mode."""
    missing_columns: list[ColumnDefinition] = []
    existing_columns: list[ColumnDefinition] = []
    for column in target.create_table.columns:
        if column.name.casefold() in existing_names:
            existing_columns.append(column)
        else:
            missing_columns.append(column)
    _validate_strict_missing_columns(target, missing_columns)
    _validate_strict_existing_columns(target, existing_columns, columns_data)


def _validate_strict_missing_columns(target: SchemaTarget, missing_columns: Sequence[ColumnDefinition]) -> None:
    """Refuse missing columns that cannot be added safely via additive ALTER TABLE."""
    for column in missing_columns:
        folded = column.name.casefold()
        if folded in target.constraint_columns:
            msg = (
                f"Cannot add constrained column {column.name!r} on table {target.table_name!r} "
                f"via additive reconciliation. {_ADD_COLUMN_REMEDY}"
            )
            raise MigrationError(msg)
        if folded in target.unsafe_add_columns:
            msg = (
                f"Cannot add generated or non-representable column {column.name!r} on table {target.table_name!r} "
                f"via additive reconciliation. {_ADD_COLUMN_REMEDY}"
            )
            raise MigrationError(msg)
        if column.not_null and column.default is None:
            msg = (
                f"Cannot add NOT NULL column {column.name!r} without DEFAULT on table {target.table_name!r} "
                f"via additive reconciliation. {_ADD_COLUMN_REMEDY}"
            )
            raise MigrationError(msg)


def _validate_strict_existing_columns(
    target: SchemaTarget, existing_columns: Sequence[ColumnDefinition], metadata_rows: Sequence[Any]
) -> None:
    """Refuse existing columns with nullability narrowing or cross-family type drift."""
    catalog_by_name: dict[str, Any] = {}
    for row in metadata_rows:
        raw_name = _metadata_value(row, ("column_name", "name"))
        if raw_name is not None:
            catalog_by_name[str(raw_name).casefold()] = row

    for column in existing_columns:
        row = catalog_by_name.get(column.name.casefold())
        if row is None:
            continue
        if column.not_null and _catalog_column_is_nullable(row) is True:
            msg = (
                f"Cannot narrow nullable column {column.name!r} to NOT NULL on table {target.table_name!r}. "
                f"{_ALIGN_COLUMN_REMEDY}"
            )
            raise MigrationError(msg)

        raw_catalog_type = _metadata_value(row, ("data_type", "column_type", "type"))
        target_token = _type_token(column.dtype)
        catalog_token = _type_token(raw_catalog_type)
        if target_token is None or catalog_token is None:
            continue
        target_family = _TYPE_FAMILIES.get(target_token)
        catalog_family = _TYPE_FAMILIES.get(catalog_token)
        if target_family is None or catalog_family is None:
            log_with_context(
                logger,
                logging.DEBUG,
                "migration.schema.type_unclassified",
                table_name=target.table_name,
                column_name=column.name,
                target_token=target_token,
                catalog_token=catalog_token,
            )
            continue
        if not _type_families_compatible(target_family, catalog_family):
            msg = (
                f"Incompatible column type on table {target.table_name!r} column {column.name!r}: "
                f"expected {column.dtype!r}, observed {raw_catalog_type!r}. {_ALIGN_COLUMN_REMEDY}"
            )
            raise MigrationError(msg)


def _catalog_column_is_nullable(row: Any) -> "bool | None":
    """Return catalog nullability when reported, or None when omitted."""
    nullable_val = _metadata_value(row, ("is_nullable", "nullable"))
    if nullable_val is not None:
        if isinstance(nullable_val, bool):
            return nullable_val
        if isinstance(nullable_val, int):
            return nullable_val == 1
        norm = str(nullable_val).strip().upper()
        if norm in {"YES", "Y", "TRUE", "1"}:
            return True
        if norm in {"NO", "N", "FALSE", "0"}:
            return False
        return None

    notnull_val = _metadata_value(row, ("notnull",))
    if notnull_val is not None:
        if isinstance(notnull_val, bool):
            return not notnull_val
        if isinstance(notnull_val, int):
            return notnull_val == 0
        norm_nn = str(notnull_val).strip().upper()
        if norm_nn in {"0", "FALSE", "NO", "N"}:
            return True
        if norm_nn in {"1", "TRUE", "YES", "Y"}:
            return False
    return None


def _type_token(value: Any) -> "str | None":
    """Normalize a DDL or catalog type string into a canonical family lookup token."""
    if value is None:
        return None
    normalized = " ".join(str(value).strip().lower().split())
    if not normalized:
        return None
    if normalized.startswith("array<"):
        return "array"
    if normalized.endswith(")"):
        depth = 0
        open_idx = -1
        for idx in range(len(normalized) - 1, -1, -1):
            char = normalized[idx]
            if char == ")":
                depth += 1
            elif char == "(":
                depth -= 1
                if depth == 0:
                    open_idx = idx
                    break
        if open_idx > 0:
            normalized = " ".join(normalized[:open_idx].split())
    return normalized or None


def _type_families_compatible(target_family: str, catalog_family: str) -> bool:
    """Return whether target and catalog type families are compatible under strict mode."""
    if target_family == catalog_family:
        return True
    if target_family == "json" and catalog_family in {"text", "binary"}:
        return True
    if catalog_family == "json" and target_family in {"text", "binary"}:
        return True
    if target_family == "boolean" and catalog_family in {"numeric", "bit"}:
        return True
    if target_family == "bit" and catalog_family in {"boolean", "numeric"}:
        return True
    return target_family == "uuid" and catalog_family in {"text", "binary"}


def _metadata_value(row: Any, keys: tuple[str, ...]) -> Any:
    """Read a metadata field from a mapping or attribute row case-insensitively."""
    if isinstance(row, Mapping):
        for key in keys:
            for candidate in (key, key.upper(), key.lower()):
                if candidate in row and row[candidate] is not None:
                    return row[candidate]
        folded_targets = {key.casefold() for key in keys}
        for raw_key, value in row.items():
            if value is not None and str(raw_key).casefold() in folded_targets:
                return value
        return None

    for key in keys:
        for candidate in (key, key.upper(), key.lower()):
            value = getattr(row, candidate, None)
            if value is not None:
                return value
    return None


def _metadata_names(metadata_rows: Sequence[Any], key: str) -> set[str]:
    """Extract case-folded names from mapping or attribute metadata rows."""
    names: set[str] = set()
    for row in metadata_rows:
        value = _metadata_value(row, (key,))
        if value is not None:
            names.add(str(value).casefold())
    return names


def _find_create_table_expression(create_statement: str, table_name: str, dialect: Any) -> exp.Create:
    """Return the matching CREATE TABLE expression from canonical adapter DDL."""
    candidates = _parse_create_expressions(create_statement, dialect)
    bare_name = table_name.rsplit(".", 1)[-1].strip('"`[]').casefold()
    for candidate in candidates:
        table = candidate.find(exp.Table)
        if table is not None and table.name.strip('"`[]').casefold() == bare_name:
            return candidate
    if candidates:
        return candidates[0]
    msg = f"Unable to parse CREATE TABLE DDL for {table_name!r}"
    raise ValueError(msg)


def _parse_table_creates(sql: str, dialect: Any) -> list[exp.Create]:
    """Parse SQL and return top-level CREATE TABLE expressions."""
    try:
        expressions = parse(sql, read=dialect)
    except (ParseError, TokenError):
        return []
    return [
        expression
        for expression in expressions
        if isinstance(expression, exp.Create) and str(expression.args.get("kind", "")).upper() == "TABLE"
    ]


def _parse_create_expressions(create_statement: str, dialect: Any) -> list[exp.Create]:
    """Parse direct or procedural-wrapper CREATE TABLE DDL."""
    creates = _parse_table_creates(create_statement, dialect)
    if creates:
        return creates

    stripped = _strip_generated_storage(create_statement, dialect)
    if stripped != create_statement:
        creates = _parse_table_creates(stripped, dialect)
        if creates:
            return creates

    extracted = _extract_create_table_statement(create_statement, dialect)
    if extracted is None:
        return []
    creates = _parse_table_creates(extracted, dialect)
    if creates:
        return creates

    stripped_extracted = _strip_generated_storage(extracted, dialect)
    if stripped_extracted != extracted:
        return _parse_table_creates(stripped_extracted, dialect)
    return []


def _remove_spans(sql: str, spans: "Sequence[tuple[int, int]]") -> str:
    """Return SQL string with character spans replaced by single spaces."""
    if not spans:
        return sql
    parts: list[str] = []
    cursor = 0
    for start, end in spans:
        if start > cursor:
            parts.append(sql[cursor:start])
        parts.append(" ")
        cursor = max(cursor, end)
    if cursor < len(sql):
        parts.append(sql[cursor:])
    return "".join(parts)


def _strip_generated_storage(sql: str, dialect: "DialectType") -> str:
    """Strip trailing VIRTUAL or STORED storage keywords from generated column definitions."""
    tokens = _tokenize_ddl_prefix(sql, dialect)
    spans: list[tuple[int, int]] = []
    total = len(tokens)
    in_header = False
    body_depth = 0
    idx = 0
    while idx < total:
        tok = tokens[idx]
        if body_depth == 0:
            if tok.token_type == TokenType.CREATE:
                in_header = True
            elif in_header and tok.token_type == TokenType.L_PAREN:
                in_header = False
                body_depth = 1
            elif in_header and tok.token_type == TokenType.SEMICOLON:
                in_header = False
            idx += 1
            continue

        if (
            body_depth == 1
            and tok.text.upper() == "AS"
            and idx + 1 < total
            and tokens[idx + 1].token_type == TokenType.L_PAREN
        ):
            expr_depth = 0
            close_idx = -1
            for scan_idx in range(idx + 1, total):
                scan_tok = tokens[scan_idx]
                if scan_tok.token_type == TokenType.L_PAREN:
                    expr_depth += 1
                elif scan_tok.token_type == TokenType.R_PAREN:
                    expr_depth -= 1
                    if expr_depth == 0:
                        close_idx = scan_idx
                        break
            if close_idx > 0:
                next_idx = close_idx + 1
                if (
                    next_idx < total
                    and tokens[next_idx].token_type != TokenType.STRING
                    and tokens[next_idx].text.upper() in {"VIRTUAL", "STORED"}
                ):
                    spans.append((tokens[next_idx].start, tokens[next_idx].end + 1))
                    idx = next_idx + 1
                    continue
                idx = close_idx + 1
                continue

        if tok.token_type == TokenType.L_PAREN:
            body_depth += 1
        elif tok.token_type == TokenType.R_PAREN:
            body_depth -= 1
        idx += 1

    return _remove_spans(sql, spans)


def _extract_create_table_statement(create_statement: str, dialect: "DialectType") -> "str | None":
    """Extract the first balanced CREATE TABLE statement from a wrapper."""
    upper_statement = create_statement.upper()
    start = upper_statement.find("CREATE TABLE")
    if start < 0:
        return None
    prefix = create_statement[:start].rstrip()
    if prefix.endswith("'"):
        quote_pos = create_statement.rfind("'", 0, start)
        string_tokens = _tokenize_ddl_prefix(create_statement[quote_pos:], dialect)
        if not string_tokens or string_tokens[0].token_type != TokenType.STRING:
            return None
        sql = string_tokens[0].text
    else:
        sql = create_statement[start:]
    tokens = _tokenize_ddl_prefix(sql, dialect)
    depth = 0
    for idx, token in enumerate(tokens):
        if token.token_type == TokenType.L_PAREN:
            depth += 1
        elif token.token_type == TokenType.R_PAREN and depth > 0:
            depth -= 1
            if depth == 0:
                end_offset = token.end + 1
                pk_open_idx = _trailing_primary_key_paren_index(tokens, idx + 1)
                if pk_open_idx is None:
                    return sql[:end_offset]
                pk_depth = 0
                order_spans: list[tuple[int, int]] = []
                for pk_idx in range(pk_open_idx, len(tokens)):
                    pk_tok = tokens[pk_idx]
                    if pk_tok.token_type == TokenType.L_PAREN:
                        pk_depth += 1
                    elif pk_tok.token_type == TokenType.R_PAREN and pk_depth > 0:
                        pk_depth -= 1
                        if pk_depth == 0:
                            extracted_pk = sql[: pk_tok.end + 1]
                            return _remove_spans(extracted_pk, order_spans)
                    elif pk_depth == 1 and pk_tok.token_type in {TokenType.ASC, TokenType.DESC}:
                        order_spans.append((pk_tok.start, pk_tok.end + 1))
                return None
    return None


def _trailing_primary_key_paren_index(tokens: "Sequence[Token]", next_idx: int) -> "int | None":
    """Return index of opening '(' when a trailing PRIMARY KEY (...) clause follows immediately."""
    total = len(tokens)
    if (
        next_idx + 1 < total
        and tokens[next_idx].token_type == TokenType.PRIMARY_KEY
        and tokens[next_idx + 1].token_type == TokenType.L_PAREN
    ):
        return next_idx + 1
    if (
        next_idx + 2 < total
        and tokens[next_idx].text.upper() == "PRIMARY"
        and tokens[next_idx + 1].text.upper() == "KEY"
        and tokens[next_idx + 2].token_type == TokenType.L_PAREN
    ):
        return next_idx + 2
    return None


def _tokenize_ddl_prefix(sql: str, dialect: "DialectType") -> "list[Token]":
    """Retain complete tokens before an unsupported procedural wrapper suffix.

    Only a completed string literal or balanced table definition is consumed by
    the caller. An unfinished token in the DDL cannot complete either boundary.
    """
    tokenizer = Dialect.get_or_raise(dialect).tokenizer()
    try:
        return tokenizer.tokenize(sql)
    except TokenError:
        return tokenizer.tokens
