"""Integration tests for Litestar extension logging format."""

import logging

from litestar import Litestar, get

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.litestar import SQLSpecPlugin


@get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}


def _find_log_record(
    records: "list[logging.LogRecord]", message: str, logger_name: str, *, stage: str | None = None
) -> "logging.LogRecord":
    for record in records:
        if record.name != logger_name:
            continue
        if record.getMessage() == message:
            if stage is None:
                return record
            extra_fields = record.__dict__.get("extra_fields")
            if isinstance(extra_fields, dict) and extra_fields.get("stage") == stage:
                return record
    msg = f"Expected log message '{message}' from '{logger_name}' not found"
    raise AssertionError(msg)


def test_litestar_extension_logging_format(caplog) -> None:
    caplog.set_level(logging.DEBUG, logger="sqlspec.extensions.litestar")

    spec = SQLSpec()
    spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    Litestar(route_handlers=[health_check], plugins=[SQLSpecPlugin(sqlspec=spec)])

    record = _find_log_record(caplog.records, "extension.init", "sqlspec.extensions.litestar", stage="configured")
    extra_fields = record.__dict__.get("extra_fields")
    assert isinstance(extra_fields, dict)
    assert extra_fields.get("framework") == "litestar"
    assert extra_fields.get("stage") == "configured"
    assert extra_fields.get("config_count") == 1
