"""IBM Db2 Litestar configuration."""

from sqlspec.config import LitestarConfig

__all__ = ("Db2LitestarConfig",)


class Db2LitestarConfig(LitestarConfig):
    """Db2-specific Litestar settings.

    Use inside ``extension_config["litestar"]`` with Db2 session store.
    """
