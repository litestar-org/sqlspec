"""Provision memory storage for existing mssql-python ADK installations.

Migration 0001 may already be applied without the newly supported memory store.
The table and index guards also make this safe after a fresh 0001 installation.
"""

from typing import TYPE_CHECKING

from sqlspec.adapters.mssql_python import MssqlPythonConfig
from sqlspec.adapters.mssql_python.adk.store import (
    MssqlPythonADKMemoryStore,
    _create_index_sql,
    _escape_sql_literal,
    _table_ref,
)
from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.adk._config_utils import _adk_memory_migration_enabled

if TYPE_CHECKING:
    from sqlspec.migrations.context import MigrationContext

__all__ = ("down", "up")


async def up(context: "MigrationContext | None" = None) -> list[str]:
    """Create missing mssql-python memory storage and its lookup indexes."""
    if context is None or context.config is None:
        msg = "Migration context must have a config to determine store class"
        raise SQLSpecError(msg)
    config = context.config
    if not isinstance(config, MssqlPythonConfig) or not _adk_memory_migration_enabled(config):
        return []
    store = MssqlPythonADKMemoryStore(config)
    statements = [store._memory_table_ddl()]  # pyright: ignore[reportPrivateUsage]
    for index_name, table, columns in store._memory_index_specs():  # pyright: ignore[reportPrivateUsage]
        statements.append(
            "IF NOT EXISTS (SELECT 1 FROM sys.indexes "
            f"WHERE name = N'{_escape_sql_literal(index_name)}' "
            f"AND object_id = OBJECT_ID(N'{_escape_sql_literal(_table_ref(table))}')) "
            f"BEGIN {_create_index_sql(table, index_name, columns)}; END;"
        )
    return statements


async def down(context: "MigrationContext | None" = None) -> list[str]:
    """Preserve memory data, which may have been created before this migration.

    Migration 0001 owns removal during a full ADK teardown. This additive repair
    cannot distinguish an existing table from one it provisioned.
    """
    return []
