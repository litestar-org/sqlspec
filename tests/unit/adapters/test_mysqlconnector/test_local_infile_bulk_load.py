"""mysqlconnector LOAD DATA LOCAL INFILE bulk-load helpers and config gate."""

import pytest

from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig, MysqlConnectorSyncConfig
from sqlspec.adapters.mysqlconnector.core import build_load_data_statement, encode_records_for_local_infile
from sqlspec.exceptions import ImproperConfigurationError


def test_encode_records_handles_none_bool_and_escapes() -> None:
    records = [("hi", None, True, "x\ty"), ("a\\b", 42, False, "line\nbreak")]
    payload = encode_records_for_local_infile(records)
    assert payload == b"hi\t\\N\t1\tx\\ty\na\\\\b\t42\t0\tline\\nbreak\n"


def test_build_load_data_statement_binds_the_filename() -> None:
    statement = build_load_data_statement("orders", ["id", "name"])
    assert statement == (
        "LOAD DATA LOCAL INFILE %s INTO TABLE `orders` "
        "CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\' "
        "LINES TERMINATED BY '\\n' (`id`, `name`)"
    )


def test_sync_config_gate_raises_when_allow_local_infile_disabled() -> None:
    with pytest.raises(ImproperConfigurationError):
        MysqlConnectorSyncConfig(driver_features={"enable_local_infile_bulk_load": True})


def test_sync_config_gate_allows_when_allow_local_infile_enabled() -> None:
    config = MysqlConnectorSyncConfig(connection_config={"allow_local_infile": True})
    assert config.driver_features["enable_local_infile_bulk_load"] is True


def test_async_config_gate_raises_when_allow_local_infile_disabled() -> None:
    with pytest.raises(ImproperConfigurationError):
        MysqlConnectorAsyncConfig(driver_features={"enable_local_infile_bulk_load": True})


def test_async_config_gate_allows_when_allow_local_infile_enabled() -> None:
    config = MysqlConnectorAsyncConfig(connection_config={"allow_local_infile": True})
    assert config.driver_features["enable_local_infile_bulk_load"] is True


def test_build_load_data_statement_never_embeds_a_path() -> None:
    """The statement carries a placeholder, so no path text can be escaped or injected."""
    statement = build_load_data_statement("orders", ["id"])

    assert "LOAD DATA LOCAL INFILE %s INTO TABLE" in statement
