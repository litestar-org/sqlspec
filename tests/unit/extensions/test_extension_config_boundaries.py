"""Runtime boundaries for shared and adapter-specific extension settings."""

import re
from pathlib import Path

import pytest

import sqlspec.extensions
from sqlspec.adapters.sqlite.adk.store import SqliteADKMemoryStore, SqliteADKStore
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore
from sqlspec.adapters.sqlite.litestar.store import SqliteStore
from sqlspec.config import FastAPIConfig, FlaskConfig, LitestarConfig, SanicConfig, StarletteConfig
from sqlspec.exceptions import ImproperConfigurationError


@pytest.mark.parametrize("extension", ["litestar", "events", "adk"])
def test_extension_store_rejects_unused_migration_switch(extension: str) -> None:
    with pytest.raises(ImproperConfigurationError, match="run_migrations"):
        config = SqliteConfig(extension_config={extension: {"run_migrations": True}})
        if extension == "litestar":
            SqliteStore(config)
        elif extension == "events":
            SqliteEventQueueStore(config)
        else:
            SqliteADKStore(config)


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("vector_index_type", "hnsw"),
        ("vector_dimensions", 256),
        ("enable_bm25", True),
        ("scann_num_leaves", 32),
        ("scann_quantizer", "SQ8"),
    ],
)
def test_sqlite_adk_rejects_postgres_memory_tuning(option: str, value: object) -> None:
    with pytest.raises(ImproperConfigurationError, match=option):
        config = SqliteConfig(extension_config={"adk": {option: value}})
        SqliteADKMemoryStore(config)


_FRAMEWORK_CONFIG_READS = (
    ("flask", "flask_config", FlaskConfig),
    ("starlette", "framework_config", StarletteConfig),
    ("fastapi", "framework_config", FastAPIConfig),
    ("sanic", "framework_config", SanicConfig),
    ("litestar", "litestar_config", LitestarConfig),
)


@pytest.mark.parametrize(("framework", "variable", "typed_dict"), _FRAMEWORK_CONFIG_READS)
def test_framework_extension_reads_only_declared_config_keys(framework: str, variable: str, typed_dict: type) -> None:
    """Every key an extension reads from its framework config is declared on that framework's TypedDict."""
    package = Path(sqlspec.extensions.__file__).parent / framework
    source = "".join(path.read_text() for path in sorted(package.rglob("*.py")))
    read_keys = set(re.findall(rf"\b{variable}\.get\(\"(\w+)\"", source))
    declared = set(typed_dict.__required_keys__) | set(typed_dict.__optional_keys__)

    assert read_keys, f"no {variable}.get(...) reads found for {framework}"
    assert read_keys <= declared, f"{framework} reads undeclared keys: {sorted(read_keys - declared)}"
