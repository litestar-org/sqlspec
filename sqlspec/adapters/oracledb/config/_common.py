from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

from oracledb import ConnectionPool

from sqlspec.types.configs import GenericDatabaseConfig, GenericPoolConfig
from sqlspec.types.empty import Empty

if TYPE_CHECKING:
    import ssl
    from collections.abc import Callable
    from typing import Any

    from oracledb import AuthMode, ConnectParams, Purity
    from oracledb.connection import AsyncConnection, Connection
    from oracledb.pool import AsyncConnectionPool, ConnectionPool

    from sqlspec.types.empty import EmptyType


T = TypeVar("T")

"""Path to the Alembic templates."""
ConnectionT = TypeVar("ConnectionT", bound="Connection | AsyncConnection")
PoolT = TypeVar("PoolT", bound="ConnectionPool | AsyncConnectionPool")


@dataclass
class GenericOraclePoolConfig(Generic[PoolT, ConnectionT], GenericPoolConfig):
    conn_class: type[ConnectionT] | EmptyType = Empty
    dsn: str | EmptyType = Empty
    pool: PoolT | EmptyType = Empty
    params: ConnectParams | EmptyType = Empty
    user: str | EmptyType = Empty
    proxy_user: str | EmptyType = Empty
    password: str | EmptyType = Empty
    newpassword: str | EmptyType = Empty
    wallet_password: str | EmptyType = Empty
    access_token: str | tuple | Callable | EmptyType = Empty
    host: str | EmptyType = Empty
    port: int | EmptyType = Empty
    protocol: str | EmptyType = Empty
    https_proxy: str | EmptyType = Empty
    https_proxy_port: int | EmptyType = Empty
    service_name: str | EmptyType = Empty
    sid: str | EmptyType = Empty
    server_type: str | EmptyType = Empty
    cclass: str | EmptyType = Empty
    purity: Purity | EmptyType = Empty
    expire_time: int | EmptyType = Empty
    retry_count: int | EmptyType = Empty
    retry_delay: int | EmptyType = Empty
    tcp_connect_timeout: float | EmptyType = Empty
    ssl_server_dn_match: bool | EmptyType = Empty
    ssl_server_cert_dn: str | EmptyType = Empty
    wallet_location: str | EmptyType = Empty
    events: bool | EmptyType = Empty
    externalauth: bool | EmptyType = Empty
    mode: AuthMode | EmptyType = Empty
    disable_oob: bool | EmptyType = Empty
    stmtcachesize: int | EmptyType = Empty
    edition: str | EmptyType = Empty
    tag: str | EmptyType = Empty
    matchanytag: bool | EmptyType = Empty
    config_dir: str | EmptyType = Empty
    appcontext: list | EmptyType = Empty
    shardingkey: list | EmptyType = Empty
    supershardingkey: list | EmptyType = Empty
    debug_jdwp: str | EmptyType = Empty
    connection_id_prefix: str | EmptyType = Empty
    ssl_context: Any | EmptyType = Empty
    sdu: int | EmptyType = Empty
    pool_boundary: str | EmptyType = Empty
    use_tcp_fast_open: bool | EmptyType = Empty
    ssl_version: ssl.TLSVersion | EmptyType = Empty
    handle: int | EmptyType = Empty


@dataclass
class GenericOracleDatabaseConfig(Generic[PoolT, ConnectionT], GenericDatabaseConfig):
    """Oracle database Configuration."""
