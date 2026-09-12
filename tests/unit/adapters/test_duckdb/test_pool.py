"""Unit tests for DuckDB connection pool helpers."""

import logging
from typing import Any
from uuid import uuid4

import pytest

from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool, _secret_sql, _validate_sql_identifier

pytest.importorskip("duckdb", reason="DuckDB adapter requires duckdb package")


class _FakeDuckDBConnection:
    def __init__(self, verification_row: tuple[Any, ...] | None = None) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.installed_extensions: list[tuple[str, dict[str, Any]]] = []
        self.loaded_extensions: list[str] = []
        self.verification_row = verification_row

    def install_extension(self, extension: str, **kwargs: Any) -> None:
        self.installed_extensions.append((extension, kwargs))

    def load_extension(self, extension: str) -> None:
        self.loaded_extensions.append(extension)

    def execute(self, sql: str, parameters: Any = None) -> "_FakeDuckDBConnection":
        self.executed.append((sql, parameters))
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.verification_row

    def list_filesystems(self) -> list[str]:
        return []


@pytest.mark.parametrize("identifier", ["my_openai_secret", "openai", "S3", "s3", "r2", "secret_1"])
def test_validate_sql_identifier_accepts_safe_identifiers(identifier: str) -> None:
    _validate_sql_identifier(identifier, "secret_name")


@pytest.mark.parametrize("identifier", ["evil; DROP TABLE secrets--", "bad name", "S3); DROP TABLE--", "1bad", ""])
def test_validate_sql_identifier_rejects_unsafe_identifiers(identifier: str) -> None:
    with pytest.raises(ValueError, match="secret_name"):
        _validate_sql_identifier(identifier, "secret_name")


@pytest.mark.parametrize(
    ("persistent", "replace", "prefix"),
    [
        (False, False, "CREATE SECRET IF NOT EXISTS s1 ("),
        (True, False, "CREATE PERSISTENT SECRET IF NOT EXISTS s1 ("),
        (False, True, "CREATE OR REPLACE SECRET s1 ("),
        (True, True, "CREATE OR REPLACE PERSISTENT SECRET s1 ("),
    ],
)
def test_secret_sql_statement_prefix(persistent: bool, replace: bool, prefix: str) -> None:
    secret = {
        "name": "s1",
        "secret_type": "gcs",
        "value": {"key_id": "k"},
        "persistent": persistent,
        "replace": replace,
    }

    assert _secret_sql(secret, "s1", "gcs").startswith(prefix)


def test_secret_sql_creates_only_missing_secret_by_default() -> None:
    sql = _secret_sql({"name": "s1", "secret_type": "gcs", "value": {"key_id": "k"}}, "s1", "gcs")

    assert sql.startswith("CREATE SECRET IF NOT EXISTS s1 (")


def test_create_connection_raises_for_malicious_secret_name() -> None:
    pool = DuckDBConnectionPool(
        connection_config={"database": ":memory:"},
        secrets=[
            {
                "name": "evil; DROP TABLE secrets--",
                "secret_type": "s3",
                "required": True,
                "value": {"key_id": "abc", "secret": "xyz"},
            }
        ],
    )
    with pytest.raises(ValueError, match="secret_name"):
        pool._create_connection()


def test_create_connection_raises_for_malicious_secret_type() -> None:
    pool = DuckDBConnectionPool(
        connection_config={"database": ":memory:"},
        secrets=[
            {
                "name": "safe_secret",
                "secret_type": "S3); DROP TABLE secrets--",
                "required": True,
                "value": {"key_id": "abc", "secret": "xyz"},
            }
        ],
    )
    with pytest.raises(ValueError, match="secret_type"):
        pool._create_connection()


def test_create_connection_passes_nested_config_without_rewrapping(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_connect_kwargs: dict[str, Any] = {}

    def fake_connect(**kwargs: Any) -> _FakeDuckDBConnection:
        captured_connect_kwargs.update(kwargs)
        return _FakeDuckDBConnection()

    monkeypatch.setattr("sqlspec.adapters.duckdb.pool.duckdb.connect", fake_connect)
    pool = DuckDBConnectionPool({
        "database": ":memory:",
        "read_only": False,
        "config": {"threads": 1, "parquet_metadata_cache": True, "progress_bar_time": 250},
    })

    pool._create_connection()

    assert captured_connect_kwargs == {
        "database": ":memory:",
        "read_only": False,
        "config": {"threads": 1, "parquet_metadata_cache": True, "progress_bar_time": 250},
    }


def test_create_connection_passes_repository_url_to_extension_install(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _FakeDuckDBConnection()

    def fake_connect(**_: Any) -> _FakeDuckDBConnection:
        return connection

    monkeypatch.setattr("sqlspec.adapters.duckdb.pool.duckdb.connect", fake_connect)
    pool = DuckDBConnectionPool(
        {"database": ":memory:"},
        extensions=[
            {"name": "spatial", "force_install": True, "repository_url": "https://extensions.example.test/duckdb"}
        ],
    )

    pool._create_connection()

    assert connection.installed_extensions == [
        ("spatial", {"force_install": True, "repository_url": "https://extensions.example.test/duckdb"})
    ]
    assert connection.loaded_extensions == ["spatial"]


def test_create_connection_name_only_extension_is_load_only(monkeypatch: pytest.MonkeyPatch) -> None:
    class FailingInstallConnection(_FakeDuckDBConnection):
        def install_extension(self, extension: str, **kwargs: Any) -> None:
            msg = "install must not be called for a name-only extension"
            raise RuntimeError(msg)

    connection = FailingInstallConnection()
    monkeypatch.setattr("sqlspec.adapters.duckdb.pool.duckdb.connect", lambda **_: connection)
    pool = DuckDBConnectionPool({"database": ":memory:"}, extensions=[{"name": "missing_extension"}])

    pool._create_connection()

    assert connection.loaded_extensions == ["missing_extension"]


def test_create_connection_creates_persistent_scoped_secret(tmp_path) -> None:
    secret_name = f"s3_secret_{uuid4().hex}"
    pool = DuckDBConnectionPool(
        {"database": ":memory:", "secret_directory": str(tmp_path)},
        secrets=[
            {
                "name": secret_name,
                "secret_type": "s3",
                "provider": "config",
                "persistent": True,
                "scope": "s3://sqlspec-test-bucket/",
                "value": {"key_id": "abc", "secret": "xyz", "region": "us-east-1"},
            }
        ],
    )

    connection = pool._create_connection()
    try:
        row = connection.execute(
            "SELECT name, type, scope, persistent FROM duckdb_secrets() WHERE name = ?", (secret_name,)
        ).fetchone()
    finally:
        connection.close()

    assert row == (secret_name, "s3", ["s3://sqlspec-test-bucket/"], True)


def test_create_connection_raises_when_secret_verification_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_connect(**_: Any) -> _FakeDuckDBConnection:
        return _FakeDuckDBConnection(verification_row=None)

    monkeypatch.setattr("sqlspec.adapters.duckdb.pool.duckdb.connect", fake_connect)
    pool = DuckDBConnectionPool(
        {"database": ":memory:"},
        secrets=[
            {
                "name": "missing_secret",
                "secret_type": "s3",
                "required": True,
                "value": {"key_id": "abc", "secret": "xyz"},
            }
        ],
    )

    with pytest.raises(RuntimeError, match="DuckDB secret 'missing_secret' was not visible"):
        pool._create_connection()


def _pool_with_fake_secret_row(
    monkeypatch: pytest.MonkeyPatch, verification_row: "tuple[Any, ...]", required: bool
) -> DuckDBConnectionPool:
    monkeypatch.setattr(
        "sqlspec.adapters.duckdb.pool.duckdb.connect",
        lambda **_: _FakeDuckDBConnection(verification_row=verification_row),
    )
    return DuckDBConnectionPool(
        {"database": ":memory:"},
        secrets=[{"name": "reports", "secret_type": "s3", "required": required, "value": {"key_id": "abc"}}],
    )


def test_secret_with_different_existing_type_raises_when_required(monkeypatch: pytest.MonkeyPatch) -> None:
    pool = _pool_with_fake_secret_row(monkeypatch, ("http", []), required=True)

    with pytest.raises(RuntimeError, match=r"'reports' exists as a temporary secret of type 'http'.*type 's3'"):
        pool._create_connection()


def test_secret_with_different_existing_type_is_skipped_when_optional(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    pool = _pool_with_fake_secret_row(monkeypatch, ("http", []), required=False)

    with caplog.at_level(logging.WARNING):
        connection = pool._create_connection()
    pool._thread_local.connection = connection

    assert pool._storage_settings(connection)["_duckdb_storage_secrets"] == ()
    assert any("'reports' exists as a temporary secret of type 'http'" in record.message for record in caplog.records)


def test_secret_with_matching_existing_type_is_recorded(monkeypatch: pytest.MonkeyPatch) -> None:
    pool = _pool_with_fake_secret_row(monkeypatch, ("s3", []), required=False)

    connection = pool._create_connection()
    pool._thread_local.connection = connection

    secrets = pool._storage_settings(connection)["_duckdb_storage_secrets"]
    assert [secret["name"] for secret in secrets] == ["reports"]


class _FailingSecretConnection(_FakeDuckDBConnection):
    def execute(self, sql: str, parameters: Any = None) -> "_FailingSecretConnection":
        if sql.startswith("CREATE"):
            msg = "secret creation failed"
            raise RuntimeError(msg)
        return self


def test_secret_creation_failure_is_best_effort_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sqlspec.adapters.duckdb.pool.duckdb.connect", lambda **_: _FailingSecretConnection())
    pool = DuckDBConnectionPool(
        {"database": ":memory:"},
        secrets=[{"name": "s3_secret", "secret_type": "s3", "value": {"key_id": "abc", "secret": "xyz"}}],
    )

    pool._create_connection()


def test_secret_creation_failure_raises_when_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sqlspec.adapters.duckdb.pool.duckdb.connect", lambda **_: _FailingSecretConnection())
    pool = DuckDBConnectionPool(
        {"database": ":memory:"},
        secrets=[
            {"name": "s3_secret", "secret_type": "s3", "required": True, "value": {"key_id": "abc", "secret": "xyz"}}
        ],
    )

    with pytest.raises(RuntimeError, match="secret creation failed"):
        pool._create_connection()


pytest.importorskip("duckdb", reason="DuckDB adapter requires duckdb package")


def test_pool_memory_leak_pool_has_no_connection_times_attribute() -> None:
    pool = DuckDBConnectionPool({"database": ":memory:"})
    assert not hasattr(pool, "_connection_times")


def test_pool_memory_leak_pool_has_no_created_connections_attribute() -> None:
    pool = DuckDBConnectionPool({"database": ":memory:"})
    assert not hasattr(pool, "_created_connections")


def test_pool_memory_leak_pool_slots_do_not_contain_removed_attrs() -> None:
    slots = DuckDBConnectionPool.__slots__
    assert "_connection_times" not in slots
    assert "_created_connections" not in slots


def test_pool_memory_leak_pool_creates_connection_after_attribute_removal() -> None:
    pool = DuckDBConnectionPool({"database": ":memory:"})
    try:
        conn = pool.acquire()
        row = conn.execute("SELECT 42").fetchone()
    finally:
        pool.close()
    assert row == (42,)
