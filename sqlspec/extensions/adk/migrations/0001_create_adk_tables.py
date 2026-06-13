"""No-op migration: superseded by 0002_reset_adk_tables.

This file used to create the legacy ADK ``sessions`` / ``events`` tables. The
ADK 2.0 clean break replaces that schema in 0002. 0001 is retained as a no-op
so installs that already applied it keep their tracking-table row; fresh
installs run it as a no-op and proceed to 0002.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlspec.migrations.context import MigrationContext

__all__ = ("down", "up")


async def up(context: "MigrationContext | None" = None) -> "list[str]":
    return []


async def down(context: "MigrationContext | None" = None) -> "list[str]":
    return []
