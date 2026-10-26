"""Test fixtures and configuration for ADBC integration tests."""

import contextlib
import functools
import gzip
import http.server
import threading
import urllib.error
import urllib.request
from collections.abc import Callable, Generator
from importlib.util import find_spec
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, cast

import pytest
from pytest_databases.docker.snowflake import SNOWFLAKE_HTTP_PORT, SnowflakeService, check_snowflake_health
from pytest_databases.helpers import get_xdist_worker_num

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from pytest_databases import ContainerService
    from pytest_databases.docker.db2 import Db2Service
    from pytest_databases.docker.gizmosql import GizmoSQLService
    from pytest_databases.docker.oracle import OracleService
    from pytest_databases.types import ServiceContainer, XdistIsolationLevel

__all__ = (
    "adbc_db2_config",
    "adbc_db2_session",
    "adbc_duckdb_config",
    "adbc_duckdb_driver",
    "adbc_duckdb_session",
    "adbc_gizmosql_config",
    "adbc_gizmosql_connection_config",
    "adbc_gizmosql_session",
    "adbc_gizmosql_sqlite_config",
    "adbc_gizmosql_sqlite_connection_config",
    "adbc_gizmosql_sqlite_session",
    "adbc_oracle_config",
    "adbc_oracle_session",
    "adbc_postgresql_config",
    "adbc_postgresql_session",
    "adbc_snowflake_config",
    "adbc_snowflake_session",
    "adbc_sqlite_config",
    "adbc_sqlite_session",
    "snowflake_service",
    "xfail_if_driver_missing",
)

F = TypeVar("F", bound=Callable[..., Any])


def xfail_if_driver_missing(func: F) -> F:
    """Decorator to xfail a test if the ADBC driver shared object is missing."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if (
                "cannot open shared object file" in str(e)
                or "No module named" in str(e)
                or "Failed to import connect function" in str(e)
                or "Could not configure connection" in str(e)
            ):
                pytest.xfail(f"ADBC driver not available: {e}")
            raise

    return cast("F", wrapper)


@pytest.fixture(scope="session")
def adbc_gizmosql_connection_config(gizmosql_service: "GizmoSQLService") -> dict[str, Any]:
    """Shared connection configuration for DuckDB-backed GizmoSQL tests."""
    return _gizmosql_connection_config(gizmosql_service, backend="duckdb")


@pytest.fixture(scope="session")
def adbc_gizmosql_config(gizmosql_service: "GizmoSQLService") -> Generator[AdbcConfig, None, None]:
    """Provide an ADBC config targeting DuckDB-backed GizmoSQL."""
    config = _build_gizmosql_config(gizmosql_service, backend="duckdb")
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_gizmosql_session(adbc_gizmosql_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Create a DuckDB-backed GizmoSQL ADBC session."""
    with adbc_gizmosql_config.provide_session() as session:
        _prepare_gizmosql_test_table(session)
        yield session


@pytest.fixture(scope="session")
def adbc_gizmosql_sqlite_connection_config(gizmosql_sqlite_service: "GizmoSQLService") -> dict[str, Any]:
    """Shared connection configuration for SQLite-backed GizmoSQL tests."""
    return _gizmosql_connection_config(gizmosql_sqlite_service, backend="sqlite")


@pytest.fixture(scope="session")
def adbc_gizmosql_sqlite_config(gizmosql_sqlite_service: "GizmoSQLService") -> Generator[AdbcConfig, None, None]:
    """Provide an ADBC config targeting SQLite-backed GizmoSQL."""
    config = _build_gizmosql_config(gizmosql_sqlite_service, backend="sqlite")
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_gizmosql_sqlite_session(adbc_gizmosql_sqlite_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Create a SQLite-backed GizmoSQL ADBC session."""
    with adbc_gizmosql_sqlite_config.provide_session() as session:
        _prepare_gizmosql_test_table(session)
        yield session


@pytest.fixture(scope="session")
def adbc_postgresql_config(adbc_postgres_connection_config: dict[str, str]) -> Generator[AdbcConfig, None, None]:
    """ADBC config using the PostgreSQL driver implementation."""
    connection_config = dict(adbc_postgres_connection_config)
    connection_config["driver_name"] = "adbc_driver_postgresql"
    config = AdbcConfig(connection_config=connection_config)
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_postgresql_session(adbc_postgresql_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC PostgreSQL session with test table handling."""
    with adbc_postgresql_config.provide_session() as session:
        session.execute_script(
            """
                CREATE TABLE IF NOT EXISTS test_table_adbc (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        )
        session.execute("DELETE FROM test_table_adbc")
        yield session
        try:
            session.execute_script("DROP TABLE IF EXISTS test_table_adbc")
        except Exception:
            try:
                session.execute("ROLLBACK")
                session.execute_script("DROP TABLE IF EXISTS test_table_adbc")
            except Exception:
                pass


@pytest.fixture(scope="function")
def adbc_sqlite_config() -> Generator[AdbcConfig, None, None]:
    """ADBC configuration for SQLite tests."""
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "adbc_driver_sqlite"})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_sqlite_session(adbc_sqlite_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Yield a SQLite-backed ADBC session."""
    with adbc_sqlite_config.provide_session() as session:
        session.execute_script(
            """
                CREATE TABLE IF NOT EXISTS test_table_adbc (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
        )
        session.execute("DELETE FROM test_table_adbc")
        yield session


@pytest.fixture(scope="function")
def adbc_duckdb_config() -> Generator[AdbcConfig, None, None]:
    """ADBC configuration for DuckDB tests."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_duckdb_session(adbc_duckdb_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Yield a DuckDB-backed ADBC session if the driver is available."""
    try:
        with adbc_duckdb_config.provide_session() as session:
            session.execute_script(
                """
                    CREATE TABLE IF NOT EXISTS test_table_adbc (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        value INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
            )
            session.execute("DELETE FROM test_table_adbc")
            yield session
    except Exception as exc:
        if (
            "cannot open shared object file" in str(exc)
            or "No module named" in str(exc)
            or "Failed to import connect function" in str(exc)
            or "Could not configure connection" in str(exc)
        ):
            pytest.skip("DuckDB ADBC driver unavailable")
        raise


@pytest.fixture(scope="function")
def adbc_duckdb_driver(adbc_duckdb_session: AdbcDriver) -> Generator[AdbcDriver, None, None]:
    """Alias fixture to emphasize driver usage in tests."""
    yield adbc_duckdb_session


@pytest.fixture(scope="session")
def adbc_oracle_config(oracle_23ai_service: "OracleService") -> Generator[AdbcConfig, None, None]:
    """Provide an ADBC configuration connected to Oracle 23ai via pure-Go adbc-driver-oracle."""
    pytest.importorskip("adbc_driver_oracle")
    config = AdbcConfig(
        connection_config={
            "driver_name": "oracle",
            "uri": f"{oracle_23ai_service.host}:{oracle_23ai_service.port}/{oracle_23ai_service.service_name}",
            "username": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
        }
    )
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_oracle_session(adbc_oracle_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Yield an Oracle ADBC session with a clean scratch table."""
    with adbc_oracle_config.provide_session() as session:
        _drop_table_quietly(session, "DROP TABLE test_table_adbc_oracle PURGE")
        session.execute(
            """
            CREATE TABLE test_table_adbc_oracle (
                id NUMBER(10) PRIMARY KEY,
                name VARCHAR2(100) NOT NULL,
                amount NUMBER(10) NOT NULL
            )
            """
        )
        session.commit()
        try:
            yield session
        finally:
            _drop_table_quietly(session, "DROP TABLE test_table_adbc_oracle PURGE")


@pytest.fixture(scope="session")
def adbc_db2_config(db2_service: "Db2Service") -> Generator[AdbcConfig, None, None]:
    """Provide an ADBC configuration connected to IBM Db2 via pure-Go adbc-driver-db2."""
    pytest.importorskip("adbc_driver_db2")
    config = AdbcConfig(connection_config={"driver_name": "db2", "uri": db2_service.dsn})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_db2_session(adbc_db2_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Yield an IBM Db2 ADBC session with a clean scratch table."""
    with adbc_db2_config.provide_session() as session:
        _drop_table_quietly(session, "DROP TABLE test_table_adbc_db2")
        session.execute(
            """
            CREATE TABLE test_table_adbc_db2 (
                id INTEGER NOT NULL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                amount INTEGER NOT NULL
            )
            """
        )
        session.commit()
        try:
            yield session
        finally:
            _drop_table_quietly(session, "DROP TABLE test_table_adbc_db2")


@pytest.fixture(scope="session")
def snowflake_service(
    container_service: "ContainerService",
    snowflake_image: str,
    snowflake_user: str,
    snowflake_password: str,
    snowflake_account: str,
    snowflake_database: str,
    snowflake_schema: str,
    snowflake_warehouse: str,
    snowflake_role: str,
    xdist_snowflake_isolation_level: "XdistIsolationLevel",
) -> Generator[SnowflakeService, None, None]:
    """Start Embucket with a Snowflake REST wire-compatibility proxy for gosnowflake ADBC."""
    worker_num = get_xdist_worker_num()
    container_name = (
        f"snowflake_{worker_num}"
        if worker_num is not None and xdist_snowflake_isolation_level == "server"
        else "snowflake"
    )
    transient = worker_num is not None and xdist_snowflake_isolation_level == "server"
    with (
        container_service.run(
            image=snowflake_image,
            name=container_name,
            container_port=SNOWFLAKE_HTTP_PORT,
            env={"EMBUCKET_PORT": str(SNOWFLAKE_HTTP_PORT), "NO_COLOR": "true"},
            check=_is_embucket_ready,
            timeout=90,
            pause=0.5,
            transient=transient,
        ) as service,
        _provide_embucket_snowflake_proxy(service.host, service.port) as proxy_port,
    ):
        yield SnowflakeService(
            container=service.container,
            host="127.0.0.1",
            port=proxy_port,
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            database=snowflake_database,
            schema=snowflake_schema,
            warehouse=snowflake_warehouse,
            role=snowflake_role,
        )


@pytest.fixture(scope="session")
def adbc_snowflake_config(snowflake_service: SnowflakeService) -> Generator[AdbcConfig, None, None]:
    """Provide an ADBC configuration connected to Embucket Snowflake emulator via adbc-driver-snowflake."""
    pytest.importorskip("adbc_driver_snowflake")
    config = AdbcConfig(
        connection_config={
            "driver_name": "snowflake",
            "db_kwargs": {
                "username": snowflake_service.user,
                "password": snowflake_service.password,
                "adbc.snowflake.sql.account": snowflake_service.account,
                "adbc.snowflake.sql.db": snowflake_service.database,
                "adbc.snowflake.sql.schema": snowflake_service.schema,
                "adbc.snowflake.sql.warehouse": snowflake_service.warehouse,
                "adbc.snowflake.sql.role": snowflake_service.role,
                "adbc.snowflake.sql.uri.protocol": "http",
                "adbc.snowflake.sql.uri.host": snowflake_service.host,
                "adbc.snowflake.sql.uri.port": str(snowflake_service.port),
            },
        }
    )
    try:
        with config.provide_session() as bootstrap_session:
            bootstrap_session.execute(
                "CREATE EXTERNAL VOLUME IF NOT EXISTS embucket_mem_vol "
                "STORAGE_LOCATIONS = ((NAME = 'mem' STORAGE_PROVIDER = 'MEMORY'))"
            )
            bootstrap_session.execute(
                f"CREATE DATABASE IF NOT EXISTS {snowflake_service.database} EXTERNAL_VOLUME = 'embucket_mem_vol'"
            )
            bootstrap_session.execute(
                f"CREATE SCHEMA IF NOT EXISTS {snowflake_service.database}.{snowflake_service.schema}"
            )
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_snowflake_session(adbc_snowflake_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Yield a Snowflake ADBC session with a clean scratch table."""
    with adbc_snowflake_config.provide_session() as session:
        _drop_table_quietly(session, "DROP TABLE IF EXISTS test_table_adbc_snowflake")
        session.execute(
            """
            CREATE TABLE test_table_adbc_snowflake (
                id INTEGER,
                name VARCHAR,
                amount INTEGER
            )
            """
        )
        try:
            yield session
        finally:
            _drop_table_quietly(session, "DROP TABLE IF EXISTS test_table_adbc_snowflake")


class _EmbucketSnowflakeProxyHandler(http.server.BaseHTTPRequestHandler):
    """Translate Snowflake REST v1 qmark bindings and rowset cell types for Embucket."""

    upstream_host: ClassVar[str] = "127.0.0.1"
    upstream_port: ClassVar[int] = SNOWFLAKE_HTTP_PORT

    def log_message(self, format: str, *args: object) -> None:
        """Silence per-request HTTP proxy stderr logging."""
        _ = (format, args)

    def do_GET(self) -> None:
        """Forward HTTP GET requests to Embucket."""
        self._forward("GET")

    def do_POST(self) -> None:
        """Forward HTTP POST requests to Embucket with Snowflake REST normalization."""
        self._forward("POST")

    def do_DELETE(self) -> None:
        """Forward HTTP DELETE requests to Embucket."""
        self._forward("DELETE")

    def _forward(self, method: str) -> None:
        """Forward an HTTP request to Embucket and normalize query-request payloads."""
        content_length = int(self.headers.get("Content-Length", "0"))
        request_body = self.rfile.read(content_length) if content_length > 0 else b""
        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in {"host", "accept-encoding", "content-length", "transfer-encoding", "connection"}
        }
        if self.headers.get("Content-Encoding", "").lower() == "gzip" and request_body:
            request_body = gzip.decompress(request_body)
            headers.pop("Content-Encoding", None)
            headers.pop("content-encoding", None)

        if method == "POST" and "/queries/v1/query-request" in self.path and request_body:
            request_body = self._rewrite_query_request(request_body)

        upstream_url = f"http://{self.upstream_host}:{self.upstream_port}{self.path}"
        upstream_request = urllib.request.Request(
            upstream_url, data=request_body if method == "POST" else None, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(upstream_request, timeout=30) as response:
                status = response.status
                response_body = response.read()
                response_headers = list(response.headers.items())
        except urllib.error.HTTPError as exc:
            status = exc.code
            response_body = exc.read()
            response_headers = list(exc.headers.items())

        if response_body:
            response_body = self._normalize_response(response_body)

        self.send_response(status)
        for header_name, header_value in response_headers:
            if header_name.lower() not in {"content-length", "transfer-encoding", "content-encoding", "connection"}:
                self.send_header(header_name, header_value)
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        if response_body:
            self.wfile.write(response_body)

    @classmethod
    def _rewrite_query_request(cls, body: bytes) -> bytes:
        """Substitute Snowflake REST bindings into sqlText for Embucket's DataFusion planner."""
        try:
            payload = from_json(body)
        except Exception:
            return body
        if not isinstance(payload, dict):
            return body
        sql_text = payload.get("sqlText")
        bindings = payload.get("bindings")
        if not isinstance(sql_text, str) or not isinstance(bindings, dict) or not bindings:
            return body
        payload["sqlText"] = cls._substitute_qmark_bindings(sql_text, bindings)
        payload.pop("bindings", None)
        return to_json(payload, as_bytes=True)

    @classmethod
    def _substitute_qmark_bindings(cls, sql_text: str, bindings: dict[str, Any]) -> str:
        """Replace unquoted qmark placeholders with typed Snowflake REST binding literals."""
        output: list[str] = []
        index = 0
        length = len(sql_text)
        binding_position = 1
        in_single_quote = False
        in_double_quote = False

        while index < length:
            char = sql_text[index]
            if char == "'" and not in_double_quote:
                output.append(char)
                if in_single_quote and index + 1 < length and sql_text[index + 1] == "'":
                    output.append("'")
                    index += 2
                    continue
                in_single_quote = not in_single_quote
                index += 1
                continue
            if char == '"' and not in_single_quote:
                output.append(char)
                if in_double_quote and index + 1 < length and sql_text[index + 1] == '"':
                    output.append('"')
                    index += 2
                    continue
                in_double_quote = not in_double_quote
                index += 1
                continue
            if char == "?" and not in_single_quote and not in_double_quote:
                binding_entry = bindings.get(str(binding_position))
                binding_position += 1
                output.append(cls._format_binding_literal(binding_entry))
                index += 1
                continue
            output.append(char)
            index += 1

        return "".join(output)

    @staticmethod
    def _format_binding_literal(binding_entry: Any) -> str:
        """Format a single Snowflake REST binding entry into a SQL literal."""
        if not isinstance(binding_entry, dict):
            return "NULL"
        raw_value = binding_entry.get("value")
        if raw_value is None:
            return "NULL"
        binding_type = str(binding_entry.get("type", "TEXT")).upper()
        text_value = str(raw_value)
        if binding_type in {"FIXED", "REAL"}:
            return text_value
        if binding_type == "BOOLEAN":
            return "TRUE" if text_value.lower() in {"true", "1"} else "FALSE"
        escaped = text_value.replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def _normalize_response(body: bytes) -> bytes:
        """Coerce non-null Embucket JSON rowset cells to strings matching Snowflake REST v1."""
        try:
            payload = from_json(body)
        except Exception:
            return body
        if not isinstance(payload, dict):
            return body
        data = payload.get("data")
        if not isinstance(data, dict):
            return body
        rowset = data.get("rowset")
        if not isinstance(rowset, list):
            return body
        data["rowset"] = [
            [None if cell is None else str(cell) for cell in row] if isinstance(row, list) else row for row in rowset
        ]
        return to_json(payload, as_bytes=True)


def _is_embucket_ready(service: "ServiceContainer") -> bool:
    """Return True when the Embucket HTTP health endpoint responds with HTTP 200."""
    return check_snowflake_health(service.host, service.port)


@contextlib.contextmanager
def _provide_embucket_snowflake_proxy(upstream_host: str, upstream_port: int) -> Generator[int, None, None]:
    """Run a background HTTP forwarder normalizing Snowflake REST payloads for Embucket."""

    class _BoundProxyHandler(_EmbucketSnowflakeProxyHandler):
        pass

    _BoundProxyHandler.upstream_host = upstream_host
    _BoundProxyHandler.upstream_port = upstream_port
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _BoundProxyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield int(server.server_address[1])
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _drop_table_quietly(session: AdbcDriver, drop_sql: str) -> None:
    """Best-effort table teardown with transaction recovery."""
    with contextlib.suppress(Exception):
        session.rollback()
    try:
        session.execute(drop_sql)
        with contextlib.suppress(Exception):
            session.commit()
    except Exception:
        with contextlib.suppress(Exception):
            session.rollback()


def _require_gizmosql_driver() -> None:
    """Skip when neither GizmoSQL nor FlightSQL ADBC driver package is installed."""
    if find_spec("adbc_driver_gizmosql") is None and find_spec("adbc_driver_flightsql") is None:
        pytest.skip("adbc_driver_gizmosql or adbc_driver_flightsql required")


def _gizmosql_connection_config(service: "GizmoSQLService", *, backend: str) -> dict[str, Any]:
    """Build SQLSpec GizmoSQL ADBC connection parameters from a GizmoSQLService."""
    _require_gizmosql_driver()
    return {
        "driver_name": "gizmosql",
        "uri": service.uri,
        "username": service.username,
        "password": service.password,
        "tls_skip_verify": True,
        "gizmosql_backend": backend,
    }


def _build_gizmosql_config(service: "GizmoSQLService", *, backend: str = "duckdb") -> AdbcConfig:
    """Create an AdbcConfig bound to an upstream GizmoSQLService."""
    return AdbcConfig(connection_config=_gizmosql_connection_config(service, backend=backend))


def _prepare_gizmosql_test_table(session: AdbcDriver) -> None:
    """Reset the shared GizmoSQL ADBC scratch table before a test runs."""
    try:
        session.execute("DROP TABLE IF EXISTS test_table_adbc")
    except Exception:
        pass
    session.execute(
        """
            CREATE TABLE test_table_adbc (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                value INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
    )
    session.execute("DELETE FROM test_table_adbc")
    session.commit()
