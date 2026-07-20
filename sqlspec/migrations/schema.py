"""Additive schema ensure and diff helpers."""

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from sqlglot import exp, parse

from sqlspec.builder import AlterTable, CreateTable, sql

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

__all__ = ("SchemaEnsureResult", "SchemaTarget", "ensure_schema_async", "ensure_schema_sync")


class SchemaTarget:
    """Describe one table's target schema.

    Args:
        table_name: Unqualified table name used for introspection and DDL.
        create_table: Builder containing the target columns and create DDL.
        schema: Optional schema containing the table.
    """

    __slots__ = ("create_statement", "create_table", "schema", "table_name")

    def __init__(
        self,
        table_name: str,
        create_table: CreateTable,
        schema: str | None = None,
        create_statement: "str | CreateTable | None" = None,
    ) -> None:
        self.table_name = table_name
        self.create_table = create_table
        self.schema = schema
        self.create_statement = create_statement or create_table

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
            additive column statements from the parsed table definition.

        Raises:
            ValueError: If no CREATE TABLE definition can be parsed.
        """
        create_expression = _find_create_table_expression(create_statement, table_name, dialect)
        target = sql.create_table(table_name, dialect=dialect)
        if schema:
            target.in_schema(schema)
        for column in create_expression.find_all(exp.ColumnDef):
            kind = column.args.get("kind")
            if not isinstance(kind, exp.DataType):
                continue
            constraints = [constraint.args.get("kind") for constraint in column.args.get("constraints", [])]
            default = next(
                (
                    constraint.this.sql(dialect=dialect)
                    for constraint in constraints
                    if isinstance(constraint, exp.DefaultColumnConstraint) and constraint.this is not None
                ),
                None,
            )
            target.column(
                column.name,
                kind.sql(dialect=dialect),
                default=default,
                not_null=any(isinstance(constraint, exp.NotNullColumnConstraint) for constraint in constraints),
                primary_key=any(isinstance(constraint, exp.PrimaryKeyColumnConstraint) for constraint in constraints),
                unique=any(isinstance(constraint, exp.UniqueColumnConstraint) for constraint in constraints),
            )
        if not target.columns:
            msg = f"CREATE TABLE DDL for {table_name!r} has no parseable columns"
            raise ValueError(msg)
        return cls(table_name, target, schema, create_statement)


class SchemaEnsureResult:
    """Summarize changes made by an ensure operation."""

    __slots__ = ("added_columns", "created_tables", "deferred_tables", "migrations_run")

    def __init__(self) -> None:
        self.created_tables: list[str] = []
        self.added_columns: dict[str, list[str]] = {}
        self.deferred_tables: list[str] = []
        self.migrations_run = False


def ensure_schema_sync(
    driver: Any,
    targets: Sequence[SchemaTarget],
    *,
    manage_schema: bool = False,
    create_schema: bool = True,
    run_migrations: bool = False,
    migration_runner: "Callable[[Any], None] | None" = None,
    assume_existing: bool = False,
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

    Returns:
        Summary of created tables, added columns, and deferred targets.

    Raises:
        ValueError: If explicit migrations are enabled without a runner.
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
    changed = False
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
                _execute_create_sync(driver, target)
                result.created_tables.append(target.identity)
                existing_tables.add(target.table_name.casefold())
                changed = True
            else:
                result.deferred_tables.append(target.identity)
            continue

        columns_data = (
            driver.data_dictionary.get_columns(driver, target.table_name, schema=target.schema)
            if target.schema
            else driver.data_dictionary.get_columns(driver, target.table_name)
        )
        statements, deferred = _add_column_statements(target, _metadata_names(columns_data, "column_name"))
        if deferred:
            result.deferred_tables.append(target.identity)
            continue
        if not statements:
            continue
        added: list[str] = []
        for column_name, statement in statements:
            driver.execute(statement)
            added.append(column_name)
        result.added_columns[target.identity] = added
        changed = True

    if changed:
        driver.commit()
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

    Returns:
        Summary of created tables, added columns, and deferred targets.

    Raises:
        ValueError: If explicit migrations are enabled without a runner.
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
    changed = False
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
                await _execute_create_async(driver, target)
                result.created_tables.append(target.identity)
                existing_tables.add(target.table_name.casefold())
                changed = True
            else:
                result.deferred_tables.append(target.identity)
            continue

        columns_data = (
            await driver.data_dictionary.get_columns(driver, target.table_name, schema=target.schema)
            if target.schema
            else await driver.data_dictionary.get_columns(driver, target.table_name)
        )
        statements, deferred = _add_column_statements(target, _metadata_names(columns_data, "column_name"))
        if deferred:
            result.deferred_tables.append(target.identity)
            continue
        if not statements:
            continue
        added: list[str] = []
        for column_name, statement in statements:
            await driver.execute(statement)
            added.append(column_name)
        result.added_columns[target.identity] = added
        changed = True

    if changed:
        await driver.commit()
    return result


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
    target: SchemaTarget, existing_columns: set[str]
) -> "tuple[list[tuple[str, AlterTable]], bool]":
    """Build additive statements and identify likely rename-only drift."""
    target_columns = {column.name.casefold(): column for column in target.create_table.columns}
    missing_columns = set(target_columns).difference(existing_columns)
    extra_columns = existing_columns.difference(target_columns)
    if missing_columns and extra_columns and len(target_columns) == len(existing_columns):
        return [], True

    statements: list[tuple[str, AlterTable]] = []
    for column_name in sorted(missing_columns):
        column = target_columns[column_name]
        statement = sql.alter_table(target.table_name, dialect=target.create_table.dialect)
        if target.schema:
            statement.in_schema(target.schema)
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


def _metadata_names(metadata_rows: Sequence[Any], key: str) -> set[str]:
    """Extract case-folded names from mapping or attribute metadata rows."""
    names: set[str] = set()
    upper_key = key.upper()
    for row in metadata_rows:
        value: Any
        if isinstance(row, Mapping):
            value = row.get(key)
            if value is None:
                value = row.get(upper_key)
        else:
            value = getattr(row, key, None)
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


def _parse_create_expressions(create_statement: str, dialect: Any) -> list[exp.Create]:
    """Parse direct or procedural-wrapper CREATE TABLE DDL."""
    try:
        expressions = parse(create_statement, read=dialect)
    except Exception:
        expressions = []
    creates = [
        expression
        for expression in expressions
        if isinstance(expression, exp.Create) and str(expression.args.get("kind", "")).upper() == "TABLE"
    ]
    if creates:
        return creates
    extracted = _extract_create_table_statement(create_statement)
    if extracted is None:
        return []
    try:
        expression = parse(extracted, read=dialect)[0]
    except Exception:
        return []
    return [expression] if isinstance(expression, exp.Create) else []


def _extract_create_table_statement(create_statement: str) -> "str | None":
    """Extract the first balanced CREATE TABLE statement from a wrapper."""
    upper_statement = create_statement.upper()
    start = upper_statement.find("CREATE TABLE")
    if start < 0:
        return None
    opening = create_statement.find("(", start)
    if opening < 0:
        return None
    depth = 0
    for index in range(opening, len(create_statement)):
        character = create_statement[index]
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                return create_statement[start : index + 1].replace("''", "'")
    return None
