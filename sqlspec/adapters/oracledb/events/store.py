"""Oracle event queue stores."""

from typing import TYPE_CHECKING, Generic, TypeVar

from sqlspec.extensions.events._store import BaseEventQueueStore

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig

ConfigT = TypeVar("ConfigT", bound="OracleSyncConfig | OracleAsyncConfig")

__all__ = (
    "OracleAsyncEventQueueStore",
    "OracleSyncEventQueueStore",
)


class _OracleEventQueueStore(BaseEventQueueStore[ConfigT], Generic[ConfigT]):
    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        return "CLOB", "CLOB", "TIMESTAMP"

    def _table_clause(self) -> str:
        if not self.settings.get("in_memory"):
            return ""
        return "INMEMORY PRIORITY HIGH"

    def _index_name(self) -> str:
        base_name = f"idx_{self.table_name.replace('.', '_')}_channel_status"
        return base_name[:30]

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        sqlcode = "-955" if object_type == "table" else "-1418"
        escaped = statement.replace("'", "''")
        return (
            "BEGIN\n"
            "    EXECUTE IMMEDIATE '{ddl}';\n"
            "EXCEPTION\n"
            "    WHEN OTHERS THEN\n"
            "        IF SQLCODE != {code} THEN\n"
            "            RAISE;\n"
            "        END IF;\n"
            "END;"
        ).format(ddl=escaped, code=sqlcode)

    def _wrap_drop_statement(self, statement: str) -> str:
        escaped = statement.replace("'", "''")
        return (
            "BEGIN\n"
            "    EXECUTE IMMEDIATE '{ddl}';\n"
            "EXCEPTION\n"
            "    WHEN OTHERS THEN\n"
            "        IF SQLCODE != -942 THEN\n"
            "            RAISE;\n"
            "        END IF;\n"
            "END;"
        ).format(ddl=escaped)


class OracleSyncEventQueueStore(_OracleEventQueueStore["OracleSyncConfig"]):
    __slots__ = ()


class OracleAsyncEventQueueStore(_OracleEventQueueStore["OracleAsyncConfig"]):
    __slots__ = ()

