from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

from oracledb import ConnectionPool

from sqlspec.base import GenericPoolConfig
from sqlspec.typing import Empty

if TYPE_CHECKING:
    import ssl
    from collections.abc import Callable
    from typing import Any

    from oracledb import AuthMode, ConnectParams, Purity
    from oracledb.connection import AsyncConnection, Connection
    from oracledb.pool import AsyncConnectionPool, ConnectionPool

    from sqlspec.typing import EmptyType

__all__ = ("OracleGenericPoolConfig",)


T = TypeVar("T")

ConnectionT = TypeVar("ConnectionT", bound="Connection | AsyncConnection")
PoolT = TypeVar("PoolT", bound="ConnectionPool | AsyncConnectionPool")


@dataclass
class OracleGenericPoolConfig(Generic[ConnectionT, PoolT], GenericPoolConfig):
    """Configuration for Oracle database connection pools.

    This class provides configuration options for both synchronous and asynchronous Oracle
    database connection pools. It supports all standard Oracle connection parameters and pool-specific
    settings.([1](https://python-oracledb.readthedocs.io/en/latest/api_manual/module.html))
    """

    conn_class: type[ConnectionT] | EmptyType = Empty
    """The connection class to use (Connection or AsyncConnection)"""
    dsn: str | EmptyType = Empty
    """Connection string for the database   """
    pool: PoolT | EmptyType = Empty
    """Existing pool instance to use"""
    params: ConnectParams | EmptyType = Empty
    """Connection parameters object"""
    user: str | EmptyType = Empty
    """Username for database authentication"""
    proxy_user: str | EmptyType = Empty
    """Name of the proxy user to connect through"""
    password: str | EmptyType = Empty
    """Password for database authentication"""
    newpassword: str | EmptyType = Empty
    """New password for password change operations"""
    wallet_password: str | EmptyType = Empty
    """Password for accessing Oracle Wallet"""
    access_token: str | tuple[str, ...] | Callable[[], str] | EmptyType = Empty
    """Token for token-based authentication"""
    host: str | EmptyType = Empty
    """Database server hostname"""
    port: int | EmptyType = Empty
    """Database server port number"""
    protocol: str | EmptyType = Empty
    """Network protocol (TCP or TCPS)"""
    https_proxy: str | EmptyType = Empty
    """HTTPS proxy server address"""
    https_proxy_port: int | EmptyType = Empty
    """HTTPS proxy server port"""
    service_name: str | EmptyType = Empty
    """Oracle service name"""
    sid: str | EmptyType = Empty
    """Oracle System ID (SID)"""
    server_type: str | EmptyType = Empty
    """Server type (dedicated, shared, pooled, or drcp)"""
    cclass: str | EmptyType = Empty
    """Connection class for database resident connection pooling"""
    purity: Purity | EmptyType = Empty
    """Session purity (NEW, SELF, or DEFAULT)"""
    expire_time: int | EmptyType = Empty
    """Time in minutes after which idle connections are closed"""
    retry_count: int | EmptyType = Empty
    """Number of attempts to connect"""
    retry_delay: int | EmptyType = Empty
    """Time in seconds between connection attempts"""
    tcp_connect_timeout: float | EmptyType = Empty
    """Timeout for establishing TCP connections"""
    ssl_server_dn_match: bool | EmptyType = Empty
    """If True, verify server certificate DN"""
    ssl_server_cert_dn: str | EmptyType = Empty
    """Expected server certificate DN"""
    wallet_location: str | EmptyType = Empty
    """Location of Oracle Wallet"""
    events: bool | EmptyType = Empty
    """If True, enables Oracle events for FAN and RLB"""
    externalauth: bool | EmptyType = Empty
    """If True, uses external authentication"""
    mode: AuthMode | EmptyType = Empty
    """Session mode (SYSDBA, SYSOPER, etc.)"""
    disable_oob: bool | EmptyType = Empty
    """If True, disables Oracle out-of-band breaks"""
    stmtcachesize: int | EmptyType = Empty
    """Size of the statement cache"""
    edition: str | EmptyType = Empty
    """Edition name for edition-based redefinition"""
    tag: str | EmptyType = Empty
    """Connection pool tag"""
    matchanytag: bool | EmptyType = Empty
    """If True, allows connections with different tags"""
    config_dir: str | EmptyType = Empty
    """Directory containing Oracle configuration files"""
    appcontext: list[str] | EmptyType = Empty
    """Application context list"""
    shardingkey: list[str] | EmptyType = Empty
    """Sharding key list"""
    supershardingkey: list[str] | EmptyType = Empty
    """Super sharding key list"""
    debug_jdwp: str | EmptyType = Empty
    """JDWP debugging string"""
    connection_id_prefix: str | EmptyType = Empty
    """Prefix for connection identifiers"""
    ssl_context: Any | EmptyType = Empty
    """SSL context for TCPS connections"""
    sdu: int | EmptyType = Empty
    """Session data unit size"""
    pool_boundary: str | EmptyType = Empty
    """Connection pool boundary (statement or transaction)"""
    use_tcp_fast_open: bool | EmptyType = Empty
    """If True, enables TCP Fast Open"""
    ssl_version: ssl.TLSVersion | EmptyType = Empty
    """SSL/TLS protocol version"""
    handle: int | EmptyType = Empty
    """Oracle service context handle"""
