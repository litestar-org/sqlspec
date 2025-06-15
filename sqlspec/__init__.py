"""SQLSpec: Safe and elegant SQL query building for Python."""

from sqlspec import adapters, base, driver, exceptions, extensions, loader, statement, typing, utils
from sqlspec.__metadata__ import __version__
from sqlspec._sql import SQLFactory
from sqlspec.base import SQLSpec
from sqlspec.exceptions import SQLFileNotFoundError, SQLFileParseError
from sqlspec.loader import SQLFile, SQLFileLoader

sql = SQLFactory()

__all__ = (
    "SQLFile",
    "SQLFileLoader",
    "SQLFileNotFoundError",
    "SQLFileParseError",
    "SQLSpec",
    "__version__",
    "adapters",
    "base",
    "driver",
    "exceptions",
    "extensions",
    "loader",
    "sql",
    "statement",
    "typing",
    "utils",
)
