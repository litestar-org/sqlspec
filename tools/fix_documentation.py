# ruff: noqa: T201
"""Fix all critical documentation issues identified in validation report."""

import re
from pathlib import Path

DOCS_DIR = Path(__file__).parent.parent / "docs"


def fix_base_rst() -> None:
    """Fix docs/reference/base.rst - remove non-existent classes."""
    file_path = DOCS_DIR / "reference" / "base.rst"
    content = file_path.read_text()

    # Remove SQLConfig section
    content = re.sub(
        r"Configuration Types\n=+\n\n\.\. autoclass:: SQLConfig.*?(?=\n\n[A-Z]|\n\nConnection Pooling)",
        "Configuration Types\n===================\n\nAll database adapter configurations inherit from base protocol classes defined in ``sqlspec.config``.",
        content,
        flags=re.DOTALL,
    )

    # Remove ConnectionPoolConfig section
    content = re.sub(
        r"Connection Pooling\n=+\n\n\.\. autoclass:: ConnectionPoolConfig.*?(?=\n\n[A-Z]|\nSession)",
        "Connection Pooling\n==================\n\nConnection pooling is configured via adapter-specific TypedDicts passed to the ``pool_config`` parameter.",
        content,
        flags=re.DOTALL,
    )

    # Remove SessionProtocol sections
    content = re.sub(
        r"Session Protocols\n-+\n\n\.\. autoclass:: .*?SessionProtocol.*?(?=\n\n[A-Z]|\n\n\.\. autoclass)",
        "Session Protocols\n-----------------\n\nSessions are provided by driver adapter classes: ``SyncDriverAdapterBase`` and ``AsyncDriverAdapterBase``.",
        content,
        flags=re.DOTALL,
    )

    # Remove on_startup/on_shutdown examples
    content = content.replace("await sql.on_startup()", "# Pools created lazily on first use")
    content = content.replace("await sql.on_shutdown()", "await sql.close_all_pools()")

    file_path.write_text(content)
    print(f"✅ Fixed {file_path}")


def fix_driver_rst() -> None:
    """Fix docs/reference/driver.rst - correct class names and methods."""
    file_path = DOCS_DIR / "reference" / "driver.rst"
    content = file_path.read_text()

    # Fix class names
    content = content.replace("BaseSyncDriver", "SyncDriverAdapterBase")
    content = content.replace("BaseAsyncDriver", "AsyncDriverAdapterBase")

    # Remove begin_transaction context manager example
    content = re.sub(r"async with driver\.transaction\(\):.*?\n", "", content, flags=re.DOTALL)

    file_path.write_text(content)
    print(f"✅ Fixed {file_path}")


def fix_adapters_rst() -> None:
    """Fix docs/reference/adapters.rst - correct config classes."""
    file_path = DOCS_DIR / "reference" / "adapters.rst"
    content = file_path.read_text()

    # Fix Psycopg config
    content = re.sub(
        r"PsycopgConfig\([^)]*is_async=True[^)]*\)",
        'PsycopgAsyncConfig(pool_config={"conninfo": "postgresql://user:pass@localhost/db"})',
        content,
    )
    content = re.sub(
        r"PsycopgConfig\([^)]*is_async=False[^)]*\)",
        'PsycopgSyncConfig(pool_config={"conninfo": "postgresql://user:pass@localhost/db"})',
        content,
    )

    # Fix Oracle config
    content = re.sub(
        r"OracleDBConfig\([^)]*is_async=True[^)]*\)",
        'OracleAsyncConfig(pool_config={"user": "system", "password": "oracle", "dsn": "localhost:1521/xe"})',
        content,
    )
    content = re.sub(
        r"OracleDBConfig\([^)]*is_async=False[^)]*\)",
        'OracleSyncConfig(pool_config={"user": "system", "password": "oracle", "dsn": "localhost:1521/xe"})',
        content,
    )

    file_path.write_text(content)
    print(f"✅ Fixed {file_path}")


def fix_extensions_rst() -> None:
    """Fix docs/reference/extensions.rst - remove empty integrations."""
    file_path = DOCS_DIR / "reference" / "extensions.rst"
    content = file_path.read_text()

    # Remove FastAPI, Flask, Sanic, Starlette sections
    content = re.sub(
        r"FastAPI Integration\n=+.*?(?=\n\n[A-Z][a-z]+ Integration\n=|Litestar Integration)",
        "",
        content,
        flags=re.DOTALL,
    )
    content = re.sub(
        r"Flask Integration\n=+.*?(?=\n\n[A-Z][a-z]+ Integration\n=|Litestar Integration)", "", content, flags=re.DOTALL
    )
    content = re.sub(
        r"Sanic Integration\n=+.*?(?=\n\n[A-Z][a-z]+ Integration\n=|Litestar Integration)", "", content, flags=re.DOTALL
    )
    content = re.sub(
        r"Starlette Integration\n=+.*?(?=\n\n[A-Z][a-z]+ Integration\n=|Litestar Integration)",
        "",
        content,
        flags=re.DOTALL,
    )

    # Remove SQLSpecConfig and SQLSpecSessionBackend
    content = re.sub(r"\.\. autoclass:: SQLSpecConfig.*?(?=\n\n\.\. autoclass|\n\n[A-Z])", "", content, flags=re.DOTALL)
    content = re.sub(
        r"\.\. autoclass:: SQLSpecSessionBackend.*?(?=\n\n\.\. autoclass|\n\n[A-Z])",
        ".. autoclass:: BaseSQLSpecStore\n   :members:\n   :undoc-members:\n   :show-inheritance:\n\n   Abstract base class for session storage backends.",
        content,
        flags=re.DOTALL,
    )

    file_path.write_text(content)
    print(f"✅ Fixed {file_path}")


def fix_configuration_rst() -> None:
    """Fix docs/usage/configuration.rst - remove validation classes."""
    file_path = DOCS_DIR / "usage" / "configuration.rst"
    content = file_path.read_text()

    # Remove validation section
    content = re.sub(r"from sqlspec\.core\.validation import.*?\n\n", "", content, flags=re.DOTALL)
    content = re.sub(r"SecurityValidator.*?(?=\n\n[A-Z])", "", content, flags=re.DOTALL)

    # Fix ParameterStyle enum values
    content = content.replace("ParameterStyle.FORMAT", "ParameterStyle.POSITIONAL_PYFORMAT")
    content = content.replace("ParameterStyle.PYFORMAT", "ParameterStyle.NAMED_PYFORMAT")

    # Remove type_coercion_map references
    content = re.sub(r"type_coercion_map=\{[^}]+\},?\n", "", content)

    file_path.write_text(content)
    print(f"✅ Fixed {file_path}")


def fix_drivers_and_querying_rst() -> None:
    """Fix docs/usage/drivers_and_querying.rst - correct method names."""
    file_path = DOCS_DIR / "usage" / "drivers_and_querying.rst"
    content = file_path.read_text()

    # Remove session.select() method references
    content = re.sub(r"session\.select\([^)]+\)", "session.execute", content)

    # Fix begin_transaction to begin
    content = content.replace("session.begin_transaction()", "session.begin()")
    content = content.replace(
        "async with session.begin_transaction():", "# Use session.begin(), session.commit(), session.rollback()"
    )

    file_path.write_text(content)
    print(f"✅ Fixed {file_path}")


def fix_framework_integrations_rst() -> None:
    """Fix docs/usage/framework_integrations.rst - correct initialization."""
    file_path = DOCS_DIR / "usage" / "framework_integrations.rst"
    content = file_path.read_text()

    # Fix SQLSpecPlugin initialization
    content = re.sub(
        r"SQLSpecPlugin\(config=config\)",
        "spec = SQLSpec()\\nspec.add_config(config)\\nsqlspec_plugin = SQLSpecPlugin(sqlspec=spec)",
        content,
    )

    # Fix result.data to result.rows
    content = content.replace("result.data", "result.rows")

    file_path.write_text(content)
    print(f"✅ Fixed {file_path}")


def fix_data_flow_rst() -> None:
    """Fix docs/usage/data_flow.rst - remove non-existent validators."""
    file_path = DOCS_DIR / "usage" / "data_flow.rst"
    content = file_path.read_text()

    # Remove validation sections
    content = re.sub(r"\*\*SecurityValidator\*\*.*?(?=\n\n\*\*|\n\nStage)", "", content, flags=re.DOTALL)
    content = re.sub(r"\*\*PerformanceValidator\*\*.*?(?=\n\n\*\*|\n\nStage)", "", content, flags=re.DOTALL)
    content = re.sub(r"\*\*DMLSafetyValidator\*\*.*?(?=\n\n\*\*|\n\nStage)", "", content, flags=re.DOTALL)
    content = re.sub(r"from sqlspec\.core\.validation import.*?\n", "", content)

    # Remove ParameterizeLiterals transformer references
    content = re.sub(r"ParameterizeLiterals.*?(?=\n\n[A-Z])", "", content, flags=re.DOTALL)

    file_path.write_text(content)
    print(f"✅ Fixed {file_path}")


def main() -> None:
    """Apply all documentation fixes."""
    print("🔧 Applying documentation fixes...\n")

    fix_base_rst()
    fix_driver_rst()
    fix_adapters_rst()
    fix_extensions_rst()
    fix_configuration_rst()
    fix_drivers_and_querying_rst()
    fix_framework_integrations_rst()
    fix_data_flow_rst()

    print("\n✅ All critical documentation fixes applied!")
    print("\nRun 'uv run sphinx-build -b html docs docs/_build/html' to verify.")


if __name__ == "__main__":
    main()
