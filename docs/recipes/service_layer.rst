=====================
Service Layer Pattern
=====================

A common pattern in production sqlspec applications is a base service class that wraps
a driver and provides convenience methods for pagination, error handling, and transactions.

Base Service
============

.. code-block:: python

   from __future__ import annotations

   from dataclasses import dataclass
   from typing import Any, TypeVar

   from sqlspec.base import SQLSpec
   from sqlspec.typing import ModelT


   SchemaT = TypeVar("SchemaT")


   @dataclass
   class SQLSpecService:
       """Base service with common database operations."""

       driver: Any  # AsyncDriverAdapterBase or SyncDriverAdapterBase

       async def get_one(
           self, statement: str, params: dict | None = None, *, schema_type: type[SchemaT]
       ) -> SchemaT:
           """Fetch a single row or raise."""
           return await self.driver.select_one(statement, params, schema_type=schema_type)

       async def get_one_or_none(
           self, statement: str, params: dict | None = None, *, schema_type: type[SchemaT]
       ) -> SchemaT | None:
           """Fetch a single row or return None."""
           return await self.driver.select_one_or_none(statement, params, schema_type=schema_type)

       async def paginate(
           self, statement: str, params: dict | None = None, *, schema_type: type[SchemaT],
           limit: int = 20, offset: int = 0,
       ) -> tuple[list[SchemaT], int]:
           """Paginate results using select_with_total."""
           from sqlspec.core.statement import SQL
           sql = SQL(statement).paginate(limit=limit, offset=offset)
           return await self.driver.select_with_total(
               str(sql), params, schema_type=schema_type,
           )

Domain Services
===============

Inherit from the base to build domain-specific services:

.. code-block:: python

   from pydantic import BaseModel


   class User(BaseModel):
       id: str
       email: str
       name: str


   class UserService(SQLSpecService):
       async def get_by_id(self, user_id: str) -> User:
           return await self.get_one(
               "SELECT id, email, name FROM users WHERE id = :id",
               {"id": user_id},
               schema_type=User,
           )

       async def list_users(self, limit: int = 20, offset: int = 0) -> tuple[list[User], int]:
           return await self.paginate(
               "SELECT id, email, name FROM users ORDER BY name",
               schema_type=User,
               limit=limit,
               offset=offset,
           )

       async def create(self, email: str, name: str) -> User:
           return await self.get_one(
               "INSERT INTO users (email, name) VALUES (:email, :name) RETURNING id, email, name",
               {"email": email, "name": name},
               schema_type=User,
           )

.. seealso::

   - :doc:`/usage/drivers_and_querying` for the driver methods used here
   - :doc:`/usage/filtering` for filter-based pagination
