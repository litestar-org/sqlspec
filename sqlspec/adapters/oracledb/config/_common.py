from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar, Union

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

ConnectionT = TypeVar("ConnectionT", bound="Union[Connection, AsyncConnection]")
PoolT = TypeVar("PoolT", bound="Union[ConnectionPool, AsyncConnectionPool]")


@dataclass
class OracleGenericPoolConfig(Generic[ConnectionT, PoolT], GenericPoolConfig):
    """Configuration for Oracle database connection pools.

    This class provides configuration options for both synchronous and asynchronous Oracle
    database connection pools. It supports all standard Oracle connection parameters and pool-specific
    settings.([1](https://python-oracledb.readthedocs.io/en/latest/api_manual/module.html))
    """

    conn_class: "Union[type[ConnectionT], EmptyType]" = Empty
    """The connection class to use (Connection or AsyncConnection)"""
    dsn: "Union[str, EmptyType]" = Empty
    """Connection string for the database   """
    pool: "Union[PoolT, EmptyType]" = Empty
    """Existing pool instance to use"""
    params: "Union[ConnectParams, EmptyType]" = Empty
    """Connection parameters object"""
    user: "Union[str, EmptyType]" = Empty
    """Username for database authentication"""
    proxy_user: "Union[str, EmptyType]" = Empty
    """Name of the proxy user to connect through"""
    password: "Union[str, EmptyType]" = Empty
    """Password for database authentication"""
    newpassword: "Union[str, EmptyType]" = Empty
    """New password for password change operations"""
    wallet_password: "Union[str, EmptyType]" = Empty
    """Password for accessing Oracle Wallet"""
    access_token: "Union[str, tuple[str, ...], Callable[[], str], EmptyType]" = Empty
    """Token for token-based authentication"""
    host: "Union[str, EmptyType]" = Empty
    """Database server hostname"""
    port: "Union[int, EmptyType]" = Empty
    """Database server port number"""
    protocol: "Union[str, EmptyType]" = Empty
    """Network protocol (TCP or TCPS)"""
    https_proxy: "Union[str, EmptyType]" = Empty
    """HTTPS proxy server address"""
    https_proxy_port: "Union[int, EmptyType]" = Empty
    """HTTPS proxy server port"""
    service_name: "Union[str, EmptyType]" = Empty
    """Oracle service name"""
    sid: "Union[str, EmptyType]" = Empty
    """Oracle System ID (SID)"""
    server_type: "Union[str, EmptyType]" = Empty
    """Server type (dedicated, shared, pooled, or drcp)"""
    cclass: "Union[str, EmptyType]" = Empty
    """Connection class for database resident connection pooling"""
    purity: "Union[Purity, EmptyType]" = Empty
    """Session purity (NEW, SELF, or DEFAULT)"""
    expire_time: "Union[int, EmptyType]" = Empty
    """Time in minutes after which idle connections are closed"""
    retry_count: "Union[int, EmptyType]" = Empty
    """Number of attempts to connect"""
    retry_delay: "Union[int, EmptyType]" = Empty
    """Time in seconds between connection attempts"""
    tcp_connect_timeout: "Union[float, EmptyType]" = Empty
    """Timeout for establishing TCP connections"""
    ssl_server_dn_match: "Union[bool, EmptyType]" = Empty
    """If True, verify server certificate DN"""
    ssl_server_cert_dn: "Union[str, EmptyType]" = Empty
    """Expected server certificate DN"""
    wallet_location: "Union[str, EmptyType]" = Empty
    """Location of Oracle Wallet"""
    events: "Union[bool, EmptyType]" = Empty
    """If True, enables Oracle events for FAN and RLB"""
    externalauth: "Union[bool, EmptyType]" = Empty
    """If True, uses external authentication"""
    mode: "Union[AuthMode, EmptyType]" = Empty
    """Session mode (SYSDBA, SYSOPER, etc.)"""
    disable_oob: "Union[bool, EmptyType]" = Empty
    """If True, disables Oracle out-of-band breaks"""
    stmtcachesize: "Union[int, EmptyType]" = Empty
    """Size of the statement cache"""
    edition: "Union[str, EmptyType]" = Empty
    """Edition name for edition-based redefinition"""
    tag: "Union[str, EmptyType]" = Empty
    """Connection pool tag"""
    matchanytag: "Union[bool, EmptyType]" = Empty
    """If True, allows connections with different tags"""
    config_dir: "Union[str, EmptyType]" = Empty
    """Directory containing Oracle configuration files"""
    appcontext: "Union[list[str], EmptyType]" = Empty
    """Application context list"""
    shardingkey: "Union[list[str], EmptyType]" = Empty
    """Sharding key list"""
    supershardingkey: "Union[list[str], EmptyType]" = Empty
    """Super sharding key list"""
    debug_jdwp: "Union[str, EmptyType]" = Empty
    """JDWP debugging string"""
    connection_id_prefix: "Union[str, EmptyType]" = Empty
    """Prefix for connection identifiers"""
    ssl_context: "Union[Any, EmptyType]" = Empty
    """SSL context for TCPS connections"""
    sdu: "Union[int, EmptyType]" = Empty
    """Session data unit size"""
    pool_boundary: "Union[str, EmptyType]" = Empty
    """Connection pool boundary (statement or transaction)"""
    use_tcp_fast_open: "Union[bool, EmptyType]" = Empty
    """If True, enables TCP Fast Open"""
    ssl_version: "Union[ssl.TLSVersion, EmptyType]" = Empty
    """SSL/TLS protocol version"""
    handle: "Union[int, EmptyType]" = Empty
    """Oracle service context handle"""
