"""IBM Db2 Events configuration."""

from sqlspec.config import EventsConfig

__all__ = ("Db2EventsConfig",)


class Db2EventsConfig(EventsConfig):
    """Db2 events settings for queue storage."""
