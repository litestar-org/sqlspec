"""MariaDB dialect configuration."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec.data_dictionary.dialects.mysql.config import MARIADB_CONFIG

if TYPE_CHECKING:
    from sqlspec.data_dictionary.dialects.mariadb.dictionary import (
        MariaDBAsyncDataDictionary,
        MariaDBSyncDataDictionary,
    )

__all__ = ("MARIADB_CONFIG", "MariaDBAsyncDataDictionary", "MariaDBSyncDataDictionary")

_EXPORTS: dict[str, tuple[str, str]] = {
    "MariaDBAsyncDataDictionary": ("sqlspec.data_dictionary.dialects.mariadb.dictionary", "MariaDBAsyncDataDictionary"),
    "MariaDBSyncDataDictionary": ("sqlspec.data_dictionary.dialects.mariadb.dictionary", "MariaDBSyncDataDictionary"),
}


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_path, attr_name = _EXPORTS[name]
    module = import_module(module_path)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS))
