"""The public Litestar config accepts plugin options and boolean session setup."""

from sqlspec.config import ExtensionConfigs
from sqlspec.extensions.litestar import LitestarConfig


def litestar_extension_config() -> ExtensionConfigs:
    config: LitestarConfig = {
        "session_table": True,
        "auto_trace_headers": True,
        "commit_mode": "autocommit",
        "connection_key": "connection",
        "correlation_header": "X-Request-ID",
        "correlation_headers": ["X-Correlation-ID"],
        "disable_di": False,
        "enable_correlation_middleware": True,
        "enable_sqlcommenter_middleware": True,
        "extra_commit_statuses": {201},
        "extra_rollback_statuses": {409},
        "migrations_path": "migrations",
        "pool_key": "pool",
        "session_key": "session",
    }
    return {"litestar": config}
