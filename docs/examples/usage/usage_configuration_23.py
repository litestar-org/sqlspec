"""Test configuration example: Environment-based configuration."""

__all__ = ("test_extension_config",)


def test_extension_config() -> None:
    import os

    from sqlspec.adapters.asyncpg import AsyncpgConfig

    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
    config = AsyncpgConfig(
        pool_config={"dsn": dsn},
        extension_config={
            "litestar": {
                "connection_key": "db_connection",
                "session_key": "db_session",
                "pool_key": "db_pool",
                "commit_mode": "autocommit_include_redirect",
                "extra_commit_statuses": {201},
                "extra_rollback_statuses": {422},
                "enable_correlation_middleware": True,
                "correlation_header": "x-request-id",
                "correlation_headers": ["traceparent", "x-correlation-id"],
                "auto_trace_headers": False,
                "disable_di": False,
            }
        },
    )

    assert config.extension_config["litestar"]["commit_mode"] == "autocommit_include_redirect"
    assert config.extension_config["litestar"]["extra_commit_statuses"] == {201}
    assert config.extension_config["litestar"]["correlation_header"] == "x-request-id"
