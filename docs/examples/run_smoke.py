"""Execute SQLite, AioSQLite, and DuckDB demos to guard against drift."""

import asyncio
from collections.abc import Callable
from importlib import import_module
from typing import Any

__all__ = ("main",)


RUNNERS: "tuple[str, ...]" = (
    "docs.examples.frameworks.litestar.aiosqlite_app:main",
    "docs.examples.frameworks.litestar.duckdb_app:main",
    "docs.examples.frameworks.litestar.sqlite_app:main",
    "docs.examples.frameworks.fastapi.aiosqlite_app:main",
    "docs.examples.frameworks.fastapi.sqlite_app:main",
    "docs.examples.frameworks.starlette.aiosqlite_app:main",
    "docs.examples.frameworks.flask.sqlite_app:main",
    "docs.examples.patterns.builder.select_and_insert:main",
    "docs.examples.patterns.migrations.runner_basic:main",
    "docs.examples.patterns.multi_tenant.router:main",
    "docs.examples.patterns.configs.multi_adapter_registry:main",
    "docs.examples.loaders.sql_files:main",
)


def _callable_from_path(path: str) -> "Callable[[], Any]":
    module_name, func_name = path.split(":", 1)
    module = import_module(module_name)
    return getattr(module, func_name)  # type: ignore[no-any-return]


def _invoke(func: "Callable[[], Any]") -> None:
    result = func()
    if asyncio.iscoroutine(result):
        asyncio.run(result)


def main() -> None:
    """Invoke the curated set of smoke-tested examples."""
    for target in RUNNERS:
        func = _callable_from_path(target)
        _invoke(func)


if __name__ == "__main__":
    main()
