"""PyMySQL adapter configuration tests."""

import ssl
from collections.abc import Mapping
from typing import get_args, get_origin, get_type_hints

from typing_extensions import NotRequired

from sqlspec.adapters.pymysql.config import PyMysqlConfig, PyMysqlConnectionParams


def _unwrap_not_required(annotation: object) -> object:
    if get_origin(annotation) is NotRequired:
        return get_args(annotation)[0]
    return annotation


def test_connection_params_type_current_pymysql_options() -> None:
    """Connection params should cover current PyMySQL connection kwargs."""
    expected_keys = {
        "host",
        "user",
        "password",
        "database",
        "port",
        "unix_socket",
        "charset",
        "collation",
        "sql_mode",
        "read_default_file",
        "read_default_group",
        "conv",
        "use_unicode",
        "client_flag",
        "cursorclass",
        "init_command",
        "connect_timeout",
        "read_timeout",
        "write_timeout",
        "autocommit",
        "local_infile",
        "max_allowed_packet",
        "defer_connect",
        "auth_plugin_map",
        "bind_address",
        "binary_prefix",
        "program_name",
        "server_public_key",
        "ssl",
        "ssl_ca",
        "ssl_cert",
        "ssl_disabled",
        "ssl_key",
        "ssl_key_password",
        "ssl_verify_cert",
        "ssl_verify_identity",
        "extra",
    }

    assert expected_keys <= set(PyMysqlConnectionParams.__annotations__)


def test_ssl_connection_param_accepts_ssl_context_and_mapping() -> None:
    """The ssl parameter should accept both SSLContext and mapping-style config."""
    ssl_annotation = _unwrap_not_required(get_type_hints(PyMysqlConnectionParams, include_extras=True)["ssl"])
    ssl_args = set(get_args(ssl_annotation))

    assert ssl.SSLContext in ssl_args
    assert any(get_origin(arg) in {dict, Mapping} or arg in {dict, Mapping} for arg in ssl_args)


def test_create_pool_defaults_local_infile_off() -> None:
    """LOAD DATA LOCAL INFILE must be disabled unless explicitly opted in."""
    config = PyMysqlConfig(connection_config={})
    pool = config._create_pool()

    assert pool._connection_parameters["local_infile"] is False


def test_create_pool_preserves_local_infile_opt_in() -> None:
    """Explicit local infile opt-in should still pass through to PyMySQL."""
    config = PyMysqlConfig(connection_config={"local_infile": True})
    pool = config._create_pool()

    assert pool._connection_parameters["local_infile"] is True


def test_create_pool_preserves_ssl_context_and_flat_tls_options() -> None:
    """SSLContext and flat TLS kwargs should pass through to the driver config."""
    context = ssl.create_default_context()
    config = PyMysqlConfig(
        connection_config={
            "ssl": context,
            "ssl_ca": "/certs/ca.pem",
            "ssl_cert": "/certs/client.pem",
            "ssl_key": "/certs/client-key.pem",
            "ssl_key_password": "secret",
            "ssl_verify_cert": True,
            "ssl_verify_identity": True,
            "ssl_disabled": False,
        }
    )
    pool = config._create_pool()

    assert pool._connection_parameters["ssl"] is context
    assert pool._connection_parameters["ssl_ca"] == "/certs/ca.pem"
    assert pool._connection_parameters["ssl_cert"] == "/certs/client.pem"
    assert pool._connection_parameters["ssl_key"] == "/certs/client-key.pem"
    assert pool._connection_parameters["ssl_key_password"] == "secret"
    assert pool._connection_parameters["ssl_verify_cert"] is True
    assert pool._connection_parameters["ssl_verify_identity"] is True
    assert pool._connection_parameters["ssl_disabled"] is False
