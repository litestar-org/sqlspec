"""Tests for OracleDB configuration."""

from __future__ import annotations

import ssl
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from oracledb import AuthMode, Connection, Purity
from oracledb.pool import ConnectionPool

from sqlspec.adapters.oracledb.config._common import OracleGenericPoolConfig
from sqlspec.base import AsyncDatabaseConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator


@pytest.fixture
def mock_oracle_pool() -> Generator[MagicMock, None, None]:
    """Create a mock Oracle pool."""
    with patch("oracledb.create_pool") as mock_create_pool:
        pool = MagicMock(spec=ConnectionPool)
        # Set up async context manager for acquire
        connection = MagicMock(spec=Connection)
        async_cm = MagicMock()
        async_cm.__aenter__ = AsyncMock(return_value=connection)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        pool.acquire.return_value = async_cm
        mock_create_pool.return_value = pool
        yield pool


@pytest.fixture
def mock_oracle_connection() -> Generator[MagicMock, None, None]:
    """Create a mock Oracle connection."""
    connection = MagicMock(spec=Connection)
    async_cm = MagicMock()
    async_cm.__aenter__ = AsyncMock(return_value=connection)
    async_cm.__aexit__ = AsyncMock(return_value=None)
    yield connection


class TestOraclePoolConfig:
    """Test OracleGenericPoolConfig class."""

    def test_default_values(self) -> None:
        """Test default values for OracleGenericPoolConfig."""
        config = OracleGenericPoolConfig[Connection, ConnectionPool]()
        assert config.conn_class is Empty
        assert config.dsn is Empty
        assert config.pool is Empty
        assert config.params is Empty
        assert config.user is Empty
        assert config.proxy_user is Empty
        assert config.password is Empty
        assert config.newpassword is Empty
        assert config.wallet_password is Empty
        assert config.access_token is Empty
        assert config.host is Empty
        assert config.port is Empty
        assert config.protocol is Empty
        assert config.https_proxy is Empty
        assert config.https_proxy_port is Empty
        assert config.service_name is Empty
        assert config.sid is Empty
        assert config.server_type is Empty
        assert config.cclass is Empty
        assert config.purity is Empty
        assert config.expire_time is Empty
        assert config.retry_count is Empty
        assert config.retry_delay is Empty
        assert config.tcp_connect_timeout is Empty
        assert config.ssl_server_dn_match is Empty
        assert config.ssl_server_cert_dn is Empty
        assert config.wallet_location is Empty
        assert config.events is Empty
        assert config.externalauth is Empty
        assert config.mode is Empty
        assert config.disable_oob is Empty
        assert config.stmtcachesize is Empty
        assert config.edition is Empty
        assert config.tag is Empty
        assert config.matchanytag is Empty
        assert config.config_dir is Empty
        assert config.appcontext is Empty
        assert config.shardingkey is Empty
        assert config.supershardingkey is Empty
        assert config.debug_jdwp is Empty
        assert config.connection_id_prefix is Empty
        assert config.ssl_context is Empty
        assert config.sdu is Empty
        assert config.pool_boundary is Empty
        assert config.use_tcp_fast_open is Empty
        assert config.ssl_version is Empty
        assert config.handle is Empty

    def test_with_all_values(self) -> None:
        """Test OracleGenericPoolConfig with all values set."""
        config = OracleGenericPoolConfig[Connection, ConnectionPool](
            conn_class=Connection,
            dsn="localhost/orclpdb1",
            pool=MagicMock(spec=ConnectionPool),
            user="scott",
            proxy_user="proxy_scott",
            password="tiger",
            newpassword="new_tiger",
            wallet_password="wallet123",
            access_token="token123",
            host="localhost",
            port=1521,
            protocol="TCP",
            https_proxy="proxy.example.com",
            https_proxy_port=8080,
            service_name="orclpdb1",
            sid="ORCL",
            server_type="dedicated",
            cclass="MYCLASS",
            purity=Purity.NEW,
            expire_time=60,
            retry_count=3,
            retry_delay=1,
            tcp_connect_timeout=5.0,
            ssl_server_dn_match=True,
            ssl_server_cert_dn="CN=example.com",
            wallet_location="/path/to/wallet",
            events=True,
            externalauth=False,
            mode=AuthMode.SYSDBA,
            disable_oob=False,
            stmtcachesize=100,
            edition="ORA$BASE",
            tag="app1",
            matchanytag=True,
            config_dir="/path/to/config",
            appcontext=["context1", "context2"],
            shardingkey=["shard1"],
            supershardingkey=["super1"],
            debug_jdwp="debug",
            connection_id_prefix="APP",
            ssl_context=ssl.create_default_context(),
            sdu=8192,
            pool_boundary="statement",
            use_tcp_fast_open=True,
            ssl_version=ssl.TLSVersion.TLSv1_2,
            handle=12345,
        )

        assert config.conn_class == Connection
        assert config.dsn == "localhost/orclpdb1"
        assert isinstance(config.pool, MagicMock)
        assert config.user == "scott"
        assert config.proxy_user == "proxy_scott"
        assert config.password == "tiger"
        assert config.newpassword == "new_tiger"
        assert config.wallet_password == "wallet123"
        assert config.access_token == "token123"
        assert config.host == "localhost"
        assert config.port == 1521
        assert config.protocol == "TCP"
        assert config.https_proxy == "proxy.example.com"
        assert config.https_proxy_port == 8080
        assert config.service_name == "orclpdb1"
        assert config.sid == "ORCL"
        assert config.server_type == "dedicated"
        assert config.cclass == "MYCLASS"
        assert config.purity == Purity.NEW
        assert config.expire_time == 60
        assert config.retry_count == 3
        assert config.retry_delay == 1
        assert config.tcp_connect_timeout == 5.0
        assert config.ssl_server_dn_match is True
        assert config.ssl_server_cert_dn == "CN=example.com"
        assert config.wallet_location == "/path/to/wallet"
        assert config.events is True
        assert config.externalauth is False
        assert config.mode == AuthMode.SYSDBA
        assert config.disable_oob is False
        assert config.stmtcachesize == 100
        assert config.edition == "ORA$BASE"
        assert config.tag == "app1"
        assert config.matchanytag is True
        assert config.config_dir == "/path/to/config"
        assert config.appcontext == ["context1", "context2"]
        assert config.shardingkey == ["shard1"]
        assert config.supershardingkey == ["super1"]
        assert config.debug_jdwp == "debug"
        assert config.connection_id_prefix == "APP"
        assert isinstance(config.ssl_context, ssl.SSLContext)
        assert config.sdu == 8192
        assert config.pool_boundary == "statement"
        assert config.use_tcp_fast_open is True
        assert config.ssl_version == ssl.TLSVersion.TLSv1_2
        assert config.handle == 12345


class MockOracleDatabaseConfig(AsyncDatabaseConfig[Connection, ConnectionPool]):
    """Mock OracleDatabaseConfig for testing."""

    def __init__(
        self,
        pool_config: OracleGenericPoolConfig[Connection, ConnectionPool] | None = None,
        pool_instance: ConnectionPool | None = None,
    ) -> None:
        """Initialize the mock config."""
        self.pool_config = pool_config
        self.pool_instance = pool_instance

    async def create_connection(self, *args: Any, **kwargs: Any) -> Connection:
        """Mock create_connection method."""
        return MagicMock(spec=Connection)

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Mock connection_config_dict property."""
        return {}

    async def create_pool(self) -> ConnectionPool:
        """Mock create_pool method."""
        if self.pool_instance is not None:
            return self.pool_instance

        if self.pool_config is None:
            msg = "One of 'pool_config' or 'pool_instance' must be provided"
            raise ImproperConfigurationError(msg)

        # Create a mock pool with an async context manager for acquire
        pool = MagicMock(spec=ConnectionPool)
        connection = MagicMock(spec=Connection)
        async_cm = MagicMock()
        async_cm.__aenter__ = AsyncMock(return_value=connection)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        pool.acquire.return_value = async_cm
        return pool

    @property
    def pool_config_dict(self) -> dict[str, Any]:
        """Mock pool_config_dict property."""
        if self.pool_config:
            return {
                "user": self.pool_config.user,
                "password": self.pool_config.password,
                "dsn": self.pool_config.dsn,
            }
        msg = "'pool_config' methods can not be used when a 'pool_instance' is provided."
        raise ImproperConfigurationError(msg)

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[Connection, None]:
        """Mock provide_connection method."""
        pool = await self.create_pool()
        async with pool.acquire() as connection:  # type: ignore[attr-defined]
            yield connection

    async def provide_pool(self, *args: Any, **kwargs: Any) -> ConnectionPool:
        """Mock provide_pool method."""
        return await self.create_pool()


class TestOracleDatabaseConfig:
    """Test OracleGenericDatabaseConfig class."""

    def test_default_values(self) -> None:
        """Test default values for OracleGenericDatabaseConfig."""
        config = MockOracleDatabaseConfig()
        assert config.pool_config is None
        assert config.pool_instance is None

    def test_pool_config_dict_with_pool_config(self) -> None:
        """Test pool_config_dict with pool configuration."""
        pool_config = OracleGenericPoolConfig[Connection, ConnectionPool](
            user="scott",
            password="tiger",
            dsn="localhost/orclpdb1",
        )
        config = MockOracleDatabaseConfig(pool_config=pool_config)
        config_dict = config.pool_config_dict
        assert config_dict == {
            "user": "scott",
            "password": "tiger",
            "dsn": "localhost/orclpdb1",
        }

    def test_pool_config_dict_with_pool_instance(self) -> None:
        """Test pool_config_dict raises error with pool instance."""
        config = MockOracleDatabaseConfig(pool_instance=MagicMock(spec=ConnectionPool))
        with pytest.raises(ImproperConfigurationError, match="'pool_config' methods can not be used"):
            config.pool_config_dict

    @pytest.mark.asyncio
    async def test_create_pool_with_pool_config(self, mock_oracle_pool: MagicMock) -> None:
        """Test create_pool with pool configuration."""
        pool_config = OracleGenericPoolConfig[Connection, ConnectionPool](
            user="scott",
            password="tiger",
            dsn="localhost/orclpdb1",
        )
        config = MockOracleDatabaseConfig(pool_config=pool_config)
        pool = await config.create_pool()
        assert isinstance(pool, MagicMock)

    @pytest.mark.asyncio
    async def test_create_pool_with_existing_pool(self) -> None:
        """Test create_pool with existing pool instance."""
        existing_pool = MagicMock(spec=ConnectionPool)
        config = MockOracleDatabaseConfig(pool_instance=existing_pool)
        pool = await config.create_pool()
        assert pool is existing_pool

    @pytest.mark.asyncio
    async def test_create_pool_without_config_or_instance(self) -> None:
        """Test create_pool raises error without pool config or instance."""
        config = MockOracleDatabaseConfig()
        with pytest.raises(
            ImproperConfigurationError, match="One of 'pool_config' or 'pool_instance' must be provided"
        ):
            await config.create_pool()

    @pytest.mark.asyncio
    async def test_provide_connection(self, mock_oracle_pool: MagicMock, mock_oracle_connection: MagicMock) -> None:
        """Test provide_connection context manager."""
        # Create a new async context manager mock
        async_cm = MagicMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_oracle_connection)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        mock_oracle_pool.acquire.return_value = async_cm

        config = MockOracleDatabaseConfig(pool_instance=mock_oracle_pool)

        async with config.provide_connection() as conn:
            assert conn is mock_oracle_connection
