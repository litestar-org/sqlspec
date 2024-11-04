from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, TypeVar

from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlspec.sql.queries import Statements

__all__ = ["SQLService"]

DBAPIConnectionT = TypeVar("DBAPIConnectionT", bound="Any")
QueryManagerT = TypeVar("QueryManagerT", bound="QueryManager")


class SQLService:
    """Holds queries, query metadata, and connection/connection builder"""

    queries: Statements
    connection: Any


class QueryManager:
    """Stores the queries for a version of the collection."""

    queries: Statements
    connection: Any

    def __init__(self, connection: Any, queries: Statements) -> None:
        self.connection = connection
        self.queries = queries

    def available_queries(self, prefix: str | None = None) -> list[str]:
        """Get available queries optionally filtered to queries starting with prefix."""
        if prefix is None:
            return sorted(
                [q for q in self.queries.available_statements if not q.endswith("cursor")],
            )
        return sorted(
            [q for q in self.queries.available_statements if q.startswith(prefix) and not q.endswith("cursor")],
        )

    @classmethod
    @contextlib.asynccontextmanager
    async def from_connection(
        cls: type[QueryManagerT],
        queries: Statements,
        connection: Any,
    ) -> AsyncIterator[QueryManagerT]:
        """Context manager that returns instance of query manager object.

        Returns:
            The service object instance.
        """
        yield cls(connection=connection, queries=queries)

    async def select(self, method: str, **binds: Any) -> list[dict[str, Any]]:
        data = await self.fn(method)(conn=self.connection, **binds)
        return [dict(row) for row in data]

    async def select_one(self, method: str, **binds: Any) -> dict[str, Any]:
        data = await self.fn(method)(conn=self.connection, **binds)
        return dict(data)

    async def select_one_value(self, method: str, **binds: Any) -> Any:
        return await self.fn(method)(conn=self.connection, **binds)

    async def insert_update_delete(self, method: str, **binds: Any) -> None:
        return await self.fn(method)(conn=self.connection, **binds)

    async def insert_update_delete_many(self, method: str, **binds: Any) -> Any | None:
        return await self.fn(method)(conn=self.connection, **binds)

    async def insert_returning(self, method: str, **binds: Any) -> Any | None:
        return await self.fn(method)(conn=self.connection, **binds)

    async def execute(self, method: str, **binds: Any) -> Any:
        return await self.fn(method)(conn=self.connection, **binds)

    def fn(self, method: str) -> Any:
        try:
            return getattr(self.queries, method)
        except AttributeError as exc:
            msg = "%s was not found"
            raise SQLSpecError(msg, method) from exc
