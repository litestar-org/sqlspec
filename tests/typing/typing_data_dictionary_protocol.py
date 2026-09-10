"""Static analyzer contract for the exported data dictionary protocols.

Every implementation in the repository declares ``dialect`` as a class
attribute, so the protocols must declare it the same way for the assignments
below to type check.
"""

from sqlspec.adapters.aiosqlite.data_dictionary import AiosqliteDataDictionary
from sqlspec.adapters.sqlite.data_dictionary import SqliteDataDictionary
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase
from sqlspec.protocols import AsyncDataDictionaryProtocol, SyncDataDictionaryProtocol


def accepts_sync_dictionary(value: SyncDataDictionaryProtocol) -> str:
    return value.dialect


def accepts_async_dictionary(value: AsyncDataDictionaryProtocol) -> str:
    return value.dialect


def sync_base_satisfies_protocol(value: SyncDataDictionaryBase) -> str:
    return accepts_sync_dictionary(value)


def async_base_satisfies_protocol(value: AsyncDataDictionaryBase) -> str:
    return accepts_async_dictionary(value)


def sync_adapter_satisfies_protocol(value: SqliteDataDictionary) -> str:
    return accepts_sync_dictionary(value)


def async_adapter_satisfies_protocol(value: AiosqliteDataDictionary) -> str:
    return accepts_async_dictionary(value)
