"""Unit tests for Oracle zero-RTT session trace attribute piggybacking and UTF-8 truncation."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.adapters.oracledb import build_dbms_cloud_copy_data_sql as exported_build_dbms_cloud_copy_data_sql
from sqlspec.adapters.oracledb.core import (
    ORACLE_ACTION_MAX_BYTES,
    ORACLE_CLIENT_IDENTIFIER_MAX_BYTES,
    ORACLE_ECID_MAX_BYTES,
    ORACLE_MODULE_MAX_BYTES,
    apply_driver_features,
    apply_oracle_session_trace_attributes,
    build_dbms_cloud_copy_data_sql,
    build_dbms_cloud_format_json,
    default_statement_config,
    resolve_dbms_cloud_source_uri,
    truncate_utf8_bytes,
)
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
from sqlspec.core.sqlcommenter import SQLCommenterContext
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError
from sqlspec.storage.pipeline import ResolvedStorageTarget
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.serializers import from_json


def test_truncate_utf8_bytes_preserves_multibyte_codepoints() -> None:
    """truncate_utf8_bytes slices cleanly on multi-byte UTF-8 codepoint boundaries."""
    multibyte = "é" * 30
    truncated = truncate_utf8_bytes(multibyte, ORACLE_MODULE_MAX_BYTES)
    assert truncated == "é" * 24
    assert len(truncated.encode("utf-8")) == 48

    emoji_text = "🚀" * 20
    truncated_emoji = truncate_utf8_bytes(emoji_text, 15)
    assert truncated_emoji == "🚀" * 3
    assert len(truncated_emoji.encode("utf-8")) == 12


def test_apply_oracle_session_trace_attributes_ecid_fallback() -> None:
    """apply_oracle_session_trace_attributes falls back to conn.ecid when conn.econtext_id is absent."""

    class _StrictConn:
        __slots__ = ("action", "client_identifier", "ecid", "module")

        def __init__(self) -> None:
            self.module: str | None = None
            self.action: str | None = None
            self.client_identifier: str | None = None
            self.ecid: str | None = None

    conn = _StrictConn()
    apply_oracle_session_trace_attributes(
        conn, module="m" * 100, action="a" * 100, client_identifier="c" * 100, econtext_id="e" * 100
    )
    assert conn.module == "m" * ORACLE_MODULE_MAX_BYTES
    assert conn.action == "a" * ORACLE_ACTION_MAX_BYTES
    assert conn.client_identifier == "c" * ORACLE_CLIENT_IDENTIFIER_MAX_BYTES
    assert conn.ecid == "e" * ORACLE_ECID_MAX_BYTES


@pytest.mark.anyio
async def test_oracle_sync_and_async_driver_piggyback_trace_attributes_on_slow_and_fast_paths() -> None:
    """OracleSyncDriver and OracleAsyncDriver piggyback trace attributes on slow and cached fast paths without SQL comments."""
    sync_executed: list[tuple[str, dict[str, str | None]]] = []
    sync_conn: Any = SimpleNamespace(
        module=None, action=None, client_identifier=None, econtext_id=None, transaction_in_progress=False
    )

    class _SyncFakeCursor:
        def __init__(self) -> None:
            self.description = [("VAL", "NUMBER")]
            self.rowcount = 1
            self.arraysize = 100
            self.prefetchrows = 2

        def execute(self, sql: str, _params: Any = None, **_kwargs: Any) -> None:
            sync_executed.append((
                sql,
                {
                    "module": sync_conn.module,
                    "action": sync_conn.action,
                    "client_identifier": sync_conn.client_identifier,
                    "econtext_id": sync_conn.econtext_id,
                },
            ))

        def fetchall(self) -> list[tuple[int]]:
            return [(1,)]

        def close(self) -> None:
            return None

    sync_conn.cursor = _SyncFakeCursor
    base_config = default_statement_config.replace(
        enable_caching=True,
        enable_sqlcommenter=True,
        sqlcommenter_enable_context=True,
        sqlcommenter_enable_traceparent=True,
    )
    stmt_config, features = apply_driver_features(base_config, {"enable_session_tracing": True})
    sync_driver = OracleSyncDriver(
        connection=cast("Any", sync_conn), statement_config=stmt_config, driver_features=features
    )
    assert sync_driver._stmt_cache_enabled is True

    with (
        patch(
            "sqlspec.adapters.oracledb.core.get_trace_context",
            return_value=("0af7651916cd43dd8448eb211c80319c", "1111111111111111"),
        ),
        SQLCommenterContext.scope({"framework": "litestar", "route": "/orders/1"}),
        CorrelationContext.context("corr-sync-1"),
    ):
        res1 = sync_driver.execute("SELECT 1 FROM DUAL")

    with (
        patch(
            "sqlspec.adapters.oracledb.core.get_trace_context",
            return_value=("0af7651916cd43dd8448eb211c80319c", "2222222222222222"),
        ),
        SQLCommenterContext.scope({"framework": "litestar", "route": "/orders/2"}),
        CorrelationContext.context("corr-sync-2"),
    ):
        res2 = sync_driver.execute("SELECT 1 FROM DUAL")

    assert res1.data is not None
    assert res2.data is not None
    assert len(sync_executed) == 2
    assert "/*" not in sync_executed[0][0]
    assert "/*" not in sync_executed[1][0]
    assert sync_executed[0][1]["module"] == "litestar"
    assert sync_executed[0][1]["action"] == "/orders/1"
    assert sync_executed[0][1]["client_identifier"] == "corr-sync-1"
    assert sync_executed[0][1]["econtext_id"] == "00-0af7651916cd43dd8448eb211c80319c-1111111111111111-01"
    assert sync_executed[1][1]["module"] == "litestar"
    assert sync_executed[1][1]["action"] == "/orders/2"
    assert sync_executed[1][1]["client_identifier"] == "corr-sync-2"
    assert sync_executed[1][1]["econtext_id"] == "00-0af7651916cd43dd8448eb211c80319c-2222222222222222-01"

    async_executed: list[tuple[str, dict[str, str | None]]] = []
    async_conn: Any = SimpleNamespace(
        module=None, action=None, client_identifier=None, econtext_id=None, transaction_in_progress=False
    )

    class _AsyncFakeCursor:
        def __init__(self) -> None:
            self.description = [("VAL", "NUMBER")]
            self.rowcount = 1
            self.arraysize = 100
            self.prefetchrows = 2

        async def execute(self, sql: str, _params: Any = None, **_kwargs: Any) -> None:
            async_executed.append((
                sql,
                {
                    "module": async_conn.module,
                    "action": async_conn.action,
                    "client_identifier": async_conn.client_identifier,
                    "econtext_id": async_conn.econtext_id,
                },
            ))

        async def fetchall(self) -> list[tuple[int]]:
            return [(1,)]

        def close(self) -> None:
            return None

    async_conn.cursor = _AsyncFakeCursor
    async_driver = OracleAsyncDriver(
        connection=cast("Any", async_conn), statement_config=stmt_config, driver_features=features
    )
    assert async_driver._stmt_cache_enabled is True

    with (
        patch(
            "sqlspec.adapters.oracledb.core.get_trace_context",
            return_value=("0af7651916cd43dd8448eb211c80319c", "aaaaaaaaaaaaaaaa"),
        ),
        SQLCommenterContext.scope({"framework": "litestar", "route": "/async/1"}),
        CorrelationContext.context("corr-async-1"),
    ):
        ares1 = await async_driver.execute("SELECT 1 FROM DUAL")

    with (
        patch(
            "sqlspec.adapters.oracledb.core.get_trace_context",
            return_value=("0af7651916cd43dd8448eb211c80319c", "bbbbbbbbbbbbbbbb"),
        ),
        SQLCommenterContext.scope({"framework": "litestar", "route": "/async/2"}),
        CorrelationContext.context("corr-async-2"),
    ):
        ares2 = await async_driver.execute("SELECT 1 FROM DUAL")

    assert ares1.data is not None
    assert ares2.data is not None
    assert len(async_executed) == 2
    assert "/*" not in async_executed[0][0]
    assert "/*" not in async_executed[1][0]
    assert async_executed[0][1]["module"] == "litestar"
    assert async_executed[0][1]["action"] == "/async/1"
    assert async_executed[0][1]["client_identifier"] == "corr-async-1"
    assert async_executed[0][1]["econtext_id"] == "00-0af7651916cd43dd8448eb211c80319c-aaaaaaaaaaaaaaaa-01"
    assert async_executed[1][1]["module"] == "litestar"
    assert async_executed[1][1]["action"] == "/async/2"
    assert async_executed[1][1]["client_identifier"] == "corr-async-2"
    assert async_executed[1][1]["econtext_id"] == "00-0af7651916cd43dd8448eb211c80319c-bbbbbbbbbbbbbbbb-01"


def test_build_dbms_cloud_copy_data_sql_qualified_unqualified_and_format_options() -> None:
    """build_dbms_cloud_copy_data_sql emits parameterized DBMS_CLOUD.COPY_DATA blocks and validates formats."""
    assert exported_build_dbms_cloud_copy_data_sql is build_dbms_cloud_copy_data_sql

    plsql, binds = build_dbms_cloud_copy_data_sql(
        "MYSCHEMA.SALES",
        credential_name="OCI$RESOURCE_PRINCIPAL",
        file_uri="s3://bucket/sales.parquet",
        file_format="parquet",
    )
    assert "DBMS_CLOUD.COPY_DATA(" in plsql
    assert "schema_name => :schema_name" in plsql
    assert binds["table_name"] == "SALES"
    assert binds["schema_name"] == "MYSCHEMA"
    assert binds["credential_name"] == "OCI$RESOURCE_PRINCIPAL"
    assert binds["file_uri_list"] == "s3://bucket/sales.parquet"
    assert from_json(binds["format_json"]) == {"type": "parquet"}

    quoted_plsql, quoted_binds = build_dbms_cloud_copy_data_sql(
        '"my_schema"."sales.2026"',
        credential_name="AWS_CRED",
        file_uri="https://objectstorage.us-ashburn-1.oraclecloud.com/n/ns/b/bkt/o/sales.csv",
        file_format="csv",
        format_options={"delimiter": "|"},
    )
    assert "schema_name => :schema_name" in quoted_plsql
    assert quoted_binds["schema_name"] == '"my_schema"'
    assert quoted_binds["table_name"] == '"sales.2026"'
    assert from_json(quoted_binds["format_json"]) == {"type": "csv", "skipheaders": 1, "delimiter": "|"}

    upper_quoted_plsql, upper_quoted_binds = build_dbms_cloud_copy_data_sql(
        '"MYSCHEMA"."SALES"',
        credential_name="AWS_CRED",
        file_uri="https://s3.amazonaws.com/bucket/sales.parquet",
        file_format="parquet",
    )
    assert "schema_name => :schema_name" in upper_quoted_plsql
    assert upper_quoted_binds["schema_name"] == "MYSCHEMA"
    assert upper_quoted_binds["table_name"] == "SALES"

    with pytest.raises(SQLSpecError, match="Invalid Oracle table identifier"):
        build_dbms_cloud_copy_data_sql(
            "CAT.SCHEMA.SALES",
            credential_name="CRED",
            file_uri="https://s3.amazonaws.com/b/f.parquet",
            file_format="parquet",
        )

    unqual_plsql, unqual_binds = build_dbms_cloud_copy_data_sql(
        "SALES",
        credential_name="CRED",
        file_uri="https://bucket.storage.googleapis.com/sales.ndjson",
        file_format="ndjson",
    )
    assert "schema_name => :schema_name" not in unqual_plsql
    assert "schema_name" not in unqual_binds
    assert unqual_binds["table_name"] == "SALES"
    assert from_json(unqual_binds["format_json"]) == {"type": "json"}

    default_schema_plsql, default_schema_binds = build_dbms_cloud_copy_data_sql(
        "SALES",
        credential_name="CRED",
        file_uri="https://objectstorage.us-ashburn-1.oraclecloud.com/n/ns/b/bucket/o/sales.json",
        file_format="json",
        default_schema='"AppOwner"',
    )
    assert "schema_name => :schema_name" in default_schema_plsql
    assert default_schema_binds["schema_name"] == '"AppOwner"'

    assert from_json(build_dbms_cloud_format_json("jsonl")) == {"type": "json"}

    with pytest.raises(ImproperConfigurationError, match="Unsupported DBMS_CLOUD\\.COPY_DATA format"):
        build_dbms_cloud_copy_data_sql(
            "SALES", credential_name="CRED", file_uri="https://s3.amazonaws.com/b/f.arrow", file_format="arrow"
        )

    with pytest.raises(SQLSpecError, match="Table name must not be empty"):
        build_dbms_cloud_copy_data_sql(
            "   ", credential_name="CRED", file_uri="https://s3.amazonaws.com/b/f.parquet", file_format="parquet"
        )

    with pytest.raises(ImproperConfigurationError, match="credential_name"):
        build_dbms_cloud_copy_data_sql(
            "SALES", credential_name="  ", file_uri="https://s3.amazonaws.com/b/f.parquet", file_format="parquet"
        )

    assert (
        resolve_dbms_cloud_source_uri("s3://bucket/sales.parquet", file_format="parquet")
        == "https://bucket.s3.amazonaws.com/sales.parquet"
    )
    assert (
        resolve_dbms_cloud_source_uri("s3://my.dotted.bucket/sales.parquet", file_format="parquet")
        == "https://s3.amazonaws.com/my.dotted.bucket/sales.parquet"
    )
    assert (
        resolve_dbms_cloud_source_uri("gs://bucket/sales.csv", file_format="csv")
        == "https://bucket.storage.googleapis.com/sales.csv"
    )
    assert (
        resolve_dbms_cloud_source_uri("gcs://my.dotted.bucket/sales.csv", file_format="csv")
        == "https://storage.googleapis.com/my.dotted.bucket/sales.csv"
    )
    assert (
        resolve_dbms_cloud_source_uri("wasbs://cont@acct.blob.core.windows.net/sales.parquet", file_format="parquet")
        == "https://acct.blob.core.windows.net/cont/sales.parquet"
    )
    assert (
        resolve_dbms_cloud_source_uri("abfss://cont@acct.dfs.core.windows.net/sales.parquet", file_format="parquet")
        == "https://acct.blob.core.windows.net/cont/sales.parquet"
    )
    assert (
        resolve_dbms_cloud_source_uri(
            "https://objectstorage.us-ashburn-1.oraclecloud.com/n/ns/b/bucket/o/sales.json", file_format="json"
        )
        == "https://objectstorage.us-ashburn-1.oraclecloud.com/n/ns/b/bucket/o/sales.json"
    )
    assert resolve_dbms_cloud_source_uri("oci://bucket@ns/sales.json", file_format="json") is None
    assert resolve_dbms_cloud_source_uri("az://container/sales.jsonl", file_format="jsonl") is None
    assert resolve_dbms_cloud_source_uri(Path("/tmp/sales.parquet"), file_format="parquet") is None
    assert resolve_dbms_cloud_source_uri("/tmp/sales.parquet", file_format="parquet") is None
    assert resolve_dbms_cloud_source_uri("file:///tmp/sales.parquet", file_format="parquet") is None
    assert resolve_dbms_cloud_source_uri("s3://bucket/sales.arrow", file_format="arrow-ipc") is None


@pytest.mark.anyio
async def test_oracle_sync_and_async_load_from_storage_uses_dbms_cloud_copy_data() -> None:
    """Sync and async drivers execute DBMS_CLOUD.COPY_DATA for remote object-store URIs with credentials."""
    sync_calls: list[tuple[str, Any]] = []
    sync_closed = 0

    class _SyncRecordingCursor:
        def __init__(self) -> None:
            self.rowcount = 42

        def execute(self, sql: str, params: Any = None) -> None:
            sync_calls.append((sql, params))

        def close(self) -> None:
            nonlocal sync_closed
            sync_closed += 1

    sync_conn: Any = SimpleNamespace(cursor=_SyncRecordingCursor, transaction_in_progress=False)
    stmt_config, features = apply_driver_features(default_statement_config, {})
    sync_driver = OracleSyncDriver(
        connection=cast("Any", sync_conn), statement_config=stmt_config, driver_features=features
    )

    job = sync_driver.load_from_storage(
        "MYSCHEMA.SALES",
        "s3://bucket/sales.parquet",
        file_format="parquet",
        credential_name="OCI$RESOURCE_PRINCIPAL",
        overwrite=True,
        partitioner={"kind": "single"},
    )
    assert job.status == "completed"
    assert job.telemetry["backend"] == "dbms_cloud"
    assert job.telemetry["destination"] == "MYSCHEMA.SALES"
    assert job.telemetry["format"] == "parquet"
    assert job.telemetry["extra"]["partitioner"] == {"kind": "single"}
    assert len(sync_calls) == 2
    assert sync_calls[0][0].startswith("TRUNCATE TABLE")
    assert "DBMS_CLOUD.COPY_DATA(" in sync_calls[1][0]
    assert sync_calls[1][1]["table_name"] == "SALES"
    assert sync_calls[1][1]["schema_name"] == "MYSCHEMA"
    assert sync_calls[1][1]["credential_name"] == "OCI$RESOURCE_PRINCIPAL"
    assert sync_calls[1][1]["file_uri_list"] == "https://bucket.s3.amazonaws.com/sales.parquet"
    assert from_json(sync_calls[1][1]["format_json"]) == {"type": "parquet"}
    assert sync_closed >= 1

    sync_calls.clear()
    fake_pipeline = MagicMock()
    fake_pipeline.resolve_destination.return_value = ResolvedStorageTarget(
        uri="gs://lake-bucket/orders.csv", protocol="gcs"
    )
    with patch.object(OracleSyncDriver, "_storage_pipeline", return_value=fake_pipeline):
        alias_job = sync_driver.load_from_storage(
            "ORDERS", "alias://lake/orders.csv", file_format="csv", credential_name="GCP_CRED", overwrite=False
        )
    assert alias_job.telemetry["backend"] == "dbms_cloud"
    assert len(sync_calls) == 1
    assert sync_calls[0][1]["file_uri_list"] == "https://lake-bucket.storage.googleapis.com/orders.csv"

    async_calls: list[tuple[str, Any]] = []
    async_closed = 0

    class _AsyncRecordingCursor:
        def __init__(self) -> None:
            self.rowcount = 10

        async def execute(self, sql: str, params: Any = None) -> None:
            async_calls.append((sql, params))

        def close(self) -> None:
            nonlocal async_closed
            async_closed += 1

    async_conn: Any = SimpleNamespace(cursor=_AsyncRecordingCursor, transaction_in_progress=False)
    async_stmt_config, async_features = apply_driver_features(
        default_statement_config, {"dbms_cloud_credential": "DEF_CLOUD_CRED"}
    )
    async_driver = OracleAsyncDriver(
        connection=cast("Any", async_conn), statement_config=async_stmt_config, driver_features=async_features
    )

    async_job = await async_driver.load_from_storage(
        "EVENTS",
        "https://objectstorage.us-ashburn-1.oraclecloud.com/n/ns/b/bucket/o/events.jsonl",
        file_format="jsonl",
        overwrite=True,
        format_options={"unpackcolumns": True},
    )
    assert async_job.status == "completed"
    assert async_job.telemetry["backend"] == "dbms_cloud"
    assert len(async_calls) == 2
    assert async_calls[0][0].startswith("TRUNCATE TABLE")
    assert "DBMS_CLOUD.COPY_DATA(" in async_calls[1][0]
    assert "schema_name" not in async_calls[1][1]
    assert async_calls[1][1]["credential_name"] == "DEF_CLOUD_CRED"
    assert (
        async_calls[1][1]["file_uri_list"]
        == "https://objectstorage.us-ashburn-1.oraclecloud.com/n/ns/b/bucket/o/events.jsonl"
    )
    assert from_json(async_calls[1][1]["format_json"]) == {"type": "json", "unpackcolumns": True}
    assert async_closed >= 1


@pytest.mark.anyio
async def test_oracle_load_from_storage_falls_back_to_arrow_when_local_or_no_credential(tmp_path: Path) -> None:
    """load_from_storage falls back to client-side Arrow ingestion when local, uncredentialed, or Arrow format."""
    local_csv = tmp_path / "sales.csv"
    local_csv.write_text("id,amount\n1,99\n2,150\n", encoding="utf-8")

    sync_executed: list[tuple[str, Any]] = []

    class _SyncInsertCursor:
        def __init__(self) -> None:
            self.rowcount = 2

        def execute(self, sql: str, params: Any = None) -> None:
            sync_executed.append((sql, params))

        def executemany(self, sql: str, params: Any) -> None:
            sync_executed.append((sql, list(params)))

        def close(self) -> None:
            return None

    sync_conn: Any = SimpleNamespace(cursor=_SyncInsertCursor, transaction_in_progress=False)
    stmt_config, features = apply_driver_features(
        default_statement_config,
        {"enable_direct_path_load": False, "storage_capabilities": {"arrow_import_enabled": True}},
    )
    sync_driver = OracleSyncDriver(
        connection=cast("Any", sync_conn), statement_config=stmt_config, driver_features=features
    )

    local_job = sync_driver.load_from_storage(
        "SALES", local_csv, file_format="csv", credential_name="OCI$RESOURCE_PRINCIPAL"
    )
    assert local_job.status == "completed"
    assert local_job.telemetry.get("backend") != "dbms_cloud"
    assert local_job.telemetry["rows_processed"] == 2
    assert len(sync_executed) == 1
    assert "DBMS_CLOUD.COPY_DATA" not in sync_executed[0][0]
    assert "INSERT INTO" in sync_executed[0][0]

    sentinel_table = object()
    sentinel_job = MagicMock()
    with (
        patch.object(
            OracleSyncDriver, "_read_storage_arrow", return_value=(sentinel_table, {"bytes_processed": 128})
        ) as sync_read,
        patch.object(OracleSyncDriver, "load_from_arrow", return_value=sentinel_job) as sync_load,
    ):
        res_no_cred = sync_driver.load_from_storage("SALES", "s3://bucket/sales.parquet", file_format="parquet")
        res_arrow_fmt = sync_driver.load_from_storage(
            "SALES", "s3://bucket/sales.arrow", file_format="arrow-ipc", credential_name="OCI$RESOURCE_PRINCIPAL"
        )
        res_regionless_oci = sync_driver.load_from_storage(
            "SALES", "oci://bucket@ns/sales.parquet", file_format="parquet", credential_name="OCI$RESOURCE_PRINCIPAL"
        )

    assert res_no_cred is sentinel_job
    assert res_arrow_fmt is sentinel_job
    assert res_regionless_oci is sentinel_job
    assert sync_read.call_count == 3
    assert sync_load.call_count == 3

    async_executed: list[tuple[str, Any]] = []

    class _AsyncInsertCursor:
        def __init__(self) -> None:
            self.rowcount = 2

        async def execute(self, sql: str, params: Any = None) -> None:
            async_executed.append((sql, params))

        async def executemany(self, sql: str, params: Any) -> None:
            async_executed.append((sql, list(params)))

        def close(self) -> None:
            return None

    async_conn: Any = SimpleNamespace(cursor=_AsyncInsertCursor, transaction_in_progress=False)
    async_stmt_config, async_features = apply_driver_features(
        default_statement_config,
        {
            "dbms_cloud_credential": "DEF_CLOUD_CRED",
            "enable_direct_path_load": False,
            "storage_capabilities": {"arrow_import_enabled": True},
        },
    )
    async_driver = OracleAsyncDriver(
        connection=cast("Any", async_conn), statement_config=async_stmt_config, driver_features=async_features
    )

    async_local_job = await async_driver.load_from_storage("SALES", f"file://{local_csv}", file_format="csv")
    assert async_local_job.status == "completed"
    assert async_local_job.telemetry.get("backend") != "dbms_cloud"
    assert async_local_job.telemetry["rows_processed"] == 2
    assert len(async_executed) == 1
    assert "DBMS_CLOUD.COPY_DATA" not in async_executed[0][0]
    assert "INSERT INTO" in async_executed[0][0]
