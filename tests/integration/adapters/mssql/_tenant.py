"""Real SQL Server tenant acceptance in an isolated SDK-pooling process."""

import json
import subprocess
import sys
from typing import Any

_NATIVE_TENANT_PROBE = r"""
import json
import sys
from importlib import import_module
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContext, TenantContextPayload

adapter, params, autocommit = json.load(sys.stdin)
prefix = "MssqlPython" if adapter == "mssql_python" else "Pymssql"
package = import_module("sqlspec.adapters." + adapter)
Config = getattr(package, prefix + "Config")
Strategy = getattr(package, prefix + "SessionContextStrategy")
params["autocommit"] = autocommit
if adapter == "mssql_python":
    params["pool_enabled"] = False
if adapter == "mssql_python" and autocommit:
    early_tenant = Config(connection_config=params, driver_features={"tenant_strategy": True})
    early_tenant.provide_pool()
    early_tenant.close_pool()
admin = Config(connection_config=params)
strategy = Strategy(variable_keys={"a": "area", "z": "zone"})

class CursorRecorder:
    def __init__(self, cursor, calls):
        self.cursor, self.calls = cursor, calls
    @property
    def description(self):
        return self.cursor.description
    def execute(self, sql, params):
        self.calls.append((sql, params))
        return self.cursor.execute(sql, params)
    def fetchone(self):
        return self.cursor.fetchone()
    def nextset(self):
        return self.cursor.nextset()
    def close(self):
        self.cursor.close()

class ConnectionRecorder:
    def __init__(self, connection):
        self.connection, self.calls = connection, []
    def cursor(self, **kwargs):
        return CursorRecorder(self.connection.cursor(**kwargs), self.calls)
    def commit(self):
        raise AssertionError("strategy committed user work")
    def rollback(self):
        raise AssertionError("strategy rolled back user work")
    def __setattr__(self, key, value):
        if key == "autocommit":
            raise AssertionError("strategy changed autocommit")
        object.__setattr__(self, key, value)

def scalar(connection, sql):
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        return cursor.fetchone()[0]
    finally:
        cursor.close()

table = "sqlspec_tenant_" + adapter
function = table + "_filter"
policy = table + "_policy"
login = table + "_user"
setup = [
    "DROP SECURITY POLICY IF EXISTS dbo." + policy,
    "DROP TABLE IF EXISTS dbo." + table,
    "DROP FUNCTION IF EXISTS dbo." + function,
    "CREATE TABLE dbo." + table + " (tenant_id NVARCHAR(256) NOT NULL, value INT NOT NULL)",
    "CREATE FUNCTION dbo." + function + "(@tenant_id NVARCHAR(256)) RETURNS TABLE WITH SCHEMABINDING AS RETURN SELECT 1 AS allowed WHERE @tenant_id = CAST(CAST(SESSION_CONTEXT(N'tenant_id') AS VARCHAR(MAX)) AS NVARCHAR(MAX))",
    "CREATE SECURITY POLICY dbo." + policy + " ADD FILTER PREDICATE dbo." + function + "(tenant_id) ON dbo." + table + ", ADD BLOCK PREDICATE dbo." + function + "(tenant_id) ON dbo." + table + " AFTER INSERT WITH (STATE = ON)",
    "IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name=N'" + login + "') CREATE LOGIN [" + login + "] WITH PASSWORD='SqlspecTenant123!', CHECK_POLICY=OFF",
    "IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name=N'" + login + "') CREATE USER [" + login + "] FOR LOGIN [" + login + "]",
    "GRANT SELECT, INSERT ON dbo." + table + " TO [" + login + "]",
]
try:
    with admin.provide_connection() as connection:
        cursor = connection.cursor()
        try:
            for sql in setup:
                cursor.execute(sql)
            connection.commit()
        finally:
            cursor.close()
    with admin.provide_connection() as connection:
        scalar(connection, "SELECT 1")
        before = scalar(connection, "SELECT @@TRANCOUNT")
        cursor = connection.cursor()
        try:
            cursor.execute("DECLARE @baseline VARCHAR(8000)=REPLICATE(CAST('x' AS VARCHAR(8000)),8000); EXEC sys.sp_set_session_context @key=N'tenant_id', @value=@baseline, @read_only=0")
        finally:
            cursor.close()
        recorder = ConnectionRecorder(connection)
        payload = TenantContextPayload("a", variables={"z": "1", "a": "2"})
        state = strategy.checkout(recorder, payload)
        assert scalar(connection, "SELECT @@TRANCOUNT") == before
        assert state.baseline["tenant_id"] == "x" * 8000, (len(state.baseline["tenant_id"] or ""), repr(state.baseline["tenant_id"])[:100])
        assert len(recorder.calls) == 1 and len(recorder.calls[0][1]) == 12
        assert scalar(connection, "SELECT CAST(CAST(SESSION_CONTEXT(N'tenant_id') AS VARCHAR(MAX)) AS NVARCHAR(MAX))") == "a"
        cursor = connection.cursor()
        cursor.execute("INSERT INTO dbo." + table + " VALUES (N'a', 99)")
        cursor.close()
        strategy.checkin(recorder, state)
        assert len(recorder.calls) == 2 and len(recorder.calls[1][1]) == 9
        assert scalar(connection, "SELECT @@TRANCOUNT") == before
        if not autocommit:
            connection.rollback()
        assert scalar(connection, "SELECT CAST(CAST(SESSION_CONTEXT(N'tenant_id') AS VARCHAR(MAX)) AS NVARCHAR(MAX))") == state.baseline["tenant_id"]
        state = strategy.checkout(recorder, payload)
        connection.rollback()
        assert scalar(connection, "SELECT CAST(CAST(SESSION_CONTEXT(N'tenant_id') AS VARCHAR(MAX)) AS NVARCHAR(MAX))") == "a"
        if not autocommit:
            assert scalar(connection, "SELECT COUNT(*) FROM dbo." + table) == 0
        strategy.checkin(recorder, state)
        state = strategy.checkout(recorder, TenantContextPayload("b"))
        strategy.checkin(recorder, state)
    user_params = dict(params)
    if adapter == "mssql_python":
        user_params.pop("connection_string", None)
        user_params.update(uid=login, pwd="SqlspecTenant123!")
        user_params.pop("user", None)
        user_params.pop("password", None)
    else:
        user_params.update(user=login, password="SqlspecTenant123!")
    tenant = Config(connection_config=user_params, driver_features={"tenant_strategy": "session_context"})
    try:
        try:
            with tenant.provide_session():
                raise AssertionError("missing tenant accepted")
        except TenantContextError:
            pass
        for identity in ("a", "b"):
            with TenantContext.scope(identity), tenant.provide_session() as session:
                session.execute("INSERT INTO dbo." + table + " VALUES (?, ?)", (identity, 1))
                session.commit()
                assert {row["tenant_id"] for row in session.select("SELECT tenant_id FROM dbo." + table)} == {identity}
        with TenantContext.scope("a"), tenant.provide_session() as session:
            try:
                session.execute("INSERT INTO dbo." + table + " VALUES (?, ?)", ("b", 2))
                raise AssertionError("cross-tenant write accepted")
            except SQLSpecError as exc:
                assert "block predicate" in str(exc).lower()
        with TenantContext.scope("a"), tenant.provide_connection() as connection:
            assert scalar(connection, "SELECT CAST(CAST(SESSION_CONTEXT(N'tenant_id') AS VARCHAR(MAX)) AS NVARCHAR(MAX))") == "a"
        saved = None
        try:
            with TenantContext.scope("a"), tenant.provide_connection() as connection:
                saved = connection
                cursor = connection.cursor()
                try:
                    cursor.execute("EXEC sys.sp_set_session_context @key=N'tenant_id', @value=N'a', @read_only=1")
                finally:
                    cursor.close()
            raise AssertionError("unverified tenant cleanup accepted")
        except TenantCleanupError:
            pass
        try:
            scalar(saved, "SELECT 1")
        except Exception:
            pass
        else:
            raise AssertionError("failed cleanup connection remains reusable")
        if adapter == "mssql_python":
            from mssql_python.pooling import PoolingManager
            assert PoolingManager.is_initialized() and not PoolingManager.is_enabled()
            try:
                Config(connection_config={**params, "pool_enabled": True}, driver_features={"tenant_strategy": True})
                raise AssertionError("pooled tenant accepted")
            except ImproperConfigurationError:
                pass
    finally:
        tenant.close_pool()
finally:
    with admin.provide_connection() as connection:
        cursor = connection.cursor()
        try:
            for sql in setup[:3]:
                cursor.execute(sql)
            connection.commit()
        finally:
            cursor.close()
    admin.close_pool()
print("tenant native acceptance passed")
"""


def assert_native_tenant_acceptance(adapter: str, connection_config: dict[str, Any], autocommit: bool) -> None:
    """Exercise real strategies, leases, policies and SDK mode in a fresh process."""
    result = subprocess.run(
        [sys.executable, "-c", _NATIVE_TENANT_PROBE],
        input=json.dumps((adapter, connection_config, autocommit)),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "tenant native acceptance passed" in result.stdout
