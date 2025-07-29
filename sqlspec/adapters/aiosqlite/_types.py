# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from typing import TYPE_CHECKING

import aiosqlite

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    AiosqliteConnection: TypeAlias = aiosqlite.Connection
else:
    AiosqliteConnection = aiosqlite.Connection

__all__ = ("AiosqliteConnection",)
