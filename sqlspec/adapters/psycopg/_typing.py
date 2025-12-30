from typing import TYPE_CHECKING, Any, Protocol

from psycopg.rows import DictRow as PsycopgDictRow

if TYPE_CHECKING:
    from typing import TypeAlias

    from psycopg import AsyncConnection, Connection

    from sqlspec.builder import QueryBuilder
    from sqlspec.core import SQL, Statement, StatementConfig

    PsycopgSyncConnection: TypeAlias = Connection[PsycopgDictRow]
    PsycopgAsyncConnection: TypeAlias = AsyncConnection[PsycopgDictRow]
else:
    from psycopg import AsyncConnection, Connection

    PsycopgSyncConnection = Connection
    PsycopgAsyncConnection = AsyncConnection


class PsycopgPipelineDriver(Protocol):
    """Protocol for psycopg pipeline driver methods used in stack execution."""

    statement_config: "StatementConfig"

    def prepare_statement(
        self,
        statement: "SQL | Statement | QueryBuilder",
        parameters: Any,
        *,
        statement_config: "StatementConfig | None" = None,
        kwargs: "dict[str, Any] | None" = None,
    ) -> "SQL": ...

    def _get_compiled_sql(self, statement: "SQL", statement_config: "StatementConfig") -> "tuple[str, Any]": ...


__all__ = ("PsycopgAsyncConnection", "PsycopgDictRow", "PsycopgPipelineDriver", "PsycopgSyncConnection")
