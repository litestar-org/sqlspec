"""Snowflake table-backed event queue schema."""

from typing import ClassVar

from sqlspec.adapters.snowflake.config import SnowflakeConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("SnowflakeEventQueueStore",)


class SnowflakeEventQueueStore(BaseEventQueueStore[SnowflakeConfig]):
    """Store queue payloads in VARIANT without secondary indexes."""

    __slots__ = ()
    dialect: ClassVar[str] = "snowflake"

    def _column_types(self) -> tuple[str, str, str]:
        return "VARIANT", "VARIANT", "TIMESTAMP_TZ"

    def _timestamp_default(self) -> str:
        return "CURRENT_TIMESTAMP()"

    def _index_ddl(self) -> str | None:
        return None

    def drop_statements(self) -> list[str]:
        """Return the queue table drop statement."""
        return [f"DROP TABLE IF EXISTS {self.table_name}"]
