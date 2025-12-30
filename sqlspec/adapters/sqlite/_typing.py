import sqlite3
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import TypeAlias

    SqliteConnection: TypeAlias = sqlite3.Connection
else:
    SqliteConnection = sqlite3.Connection

__all__ = ("SqliteConnection",)
