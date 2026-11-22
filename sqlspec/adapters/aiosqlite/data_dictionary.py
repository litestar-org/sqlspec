"""SQLite-specific data dictionary for metadata queries via aiosqlite."""

from typing import TYPE_CHECKING, ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.adapters.aiosqlite._typing import aiosqlite_sqlite_module as sqlite3
from sqlspec.data_dictionary.dialects.sqlite import SqliteAsyncDataDictionary

if TYPE_CHECKING:
    from sqlspec.data_dictionary import VersionInfo
    from sqlspec.driver import AsyncDriverAdapterBase

__all__ = ("AiosqliteDataDictionary",)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class AiosqliteDataDictionary(SqliteAsyncDataDictionary):
    """SQLite-specific async data dictionary."""

    dialect: ClassVar[str] = "sqlite"
    _shared_version_info: ClassVar["VersionInfo | None"] = None

    async def get_version(self, driver: "AsyncDriverAdapterBase") -> "VersionInfo | None":
        """Get SQLite database version information.

        Checks the shared in-process cache and local ``sqlite3.sqlite_version``
        before falling back to querying the database driver.

        Args:
            driver: Async database driver instance.

        Returns:
            SQLite version information or None if detection fails.
        """
        if AiosqliteDataDictionary._shared_version_info is not None:
            return AiosqliteDataDictionary._shared_version_info

        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        version_value = getattr(sqlite3, "sqlite_version", None)
        if not version_value:
            version_value = await driver.select_value_or_none(self.get_query("version", "current"))
        if not version_value:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        version_info = self.parse_version_with_pattern(self.get_dialect_config().version_pattern, str(version_value))
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        AiosqliteDataDictionary._shared_version_info = version_info
        return version_info
