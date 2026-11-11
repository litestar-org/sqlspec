"""Behavioral coverage for Teradata Unicode catalog reflection."""

import pytest

from sqlspec.data_dictionary import DataDictionaryLoader, VersionInfo, get_dialect_config


def test_teradata_config_and_versions() -> None:
    from sqlspec.data_dictionary.dialects.teradata import TeradataVersionInfo, resolve_teradata_feature_flag

    config = get_dialect_config("vantage")
    assert config.name == "teradata"
    assert config.get_optimal_type("varchar", length=255) == "VARCHAR(255)"
    assert config.get_optimal_type("varchar") == "CLOB"
    version = TeradataVersionInfo.from_version_string("20.00.18.29")
    assert str(version) == "20.00.18.29"
    assert resolve_teradata_feature_flag("supports_json", VersionInfo(14, 10, 0)) is False


@pytest.mark.parametrize(
    "domain,name",
    [
        ("schemas", "by_schema"),
        ("schemas", "current"),
        ("version", "current"),
        ("objects", "by_schema"),
        ("tables", "by_schema"),
        ("tables", "all_by_schema"),
        ("tables", "names_by_schema"),
        ("columns", "by_schema"),
        ("columns", "by_table"),
        ("views", "by_schema"),
        ("indexes", "by_schema"),
        ("indexes", "by_table"),
        ("constraints", "by_schema"),
        ("constraints", "by_table"),
        ("foreign_keys", "by_schema"),
        ("foreign_keys", "by_table"),
    ],
)
def test_teradata_queries_load(domain: str, name: str) -> None:
    query = DataDictionaryLoader().get_domain_query("vantage", domain, name)
    assert query.dialect == "teradata"
    assert isinstance(query.query_text, str)
    assert query.query_text.startswith("LOCKING ROW FOR ACCESS")


def test_teradata_catalog_reflection() -> None:
    import re
    import sqlite3

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("ATTACH DATABASE ':memory:' AS DBC")
    db.executescript("""
CREATE TABLE DBC.DBCInfoV (InfoKey TEXT, InfoData TEXT);
INSERT INTO DBC.DBCInfoV VALUES ('VERSION', '20.00.18.29   ');
CREATE TABLE DBC.DatabasesV (DatabaseName TEXT, DBKind TEXT, OwnerName TEXT, CommentString TEXT);
INSERT INTO DBC.DatabasesV VALUES ('ANALYTICS', 'D', 'owner', NULL), ('DBC', 'D', 'owner', NULL);
CREATE TABLE DBC.TablesV (DatabaseName TEXT, TableName TEXT, TableKind TEXT, CreateTimeStamp TEXT, CommentString TEXT, RequestTxtOverflow TEXT, RequestText TEXT);
INSERT INTO DBC.TablesV VALUES ('ANALYTICS', 'customers', 'T', NULL, NULL, NULL, NULL), ('ANALYTICS', 'orders', 'O', NULL, NULL, NULL, NULL), ('ANALYTICS', 'v_orders', 'V', NULL, NULL, 'R', NULL);
CREATE TABLE DBC.ColumnsV (DatabaseName TEXT, TableName TEXT, ColumnName TEXT, ColumnType TEXT, ColumnUDTName TEXT, Nullable TEXT, DefaultValue TEXT, ColumnId INTEGER, CharType INTEGER, ColumnLength INTEGER, DecimalTotalDigits INTEGER, DecimalFractionalDigits INTEGER, IdColType TEXT, CommentString TEXT, ColumnConstraint TEXT);
INSERT INTO DBC.ColumnsV VALUES ('ANALYTICS', 'orders', 'id', 'I ', NULL, 'N', NULL, 1025, NULL, 4, NULL, NULL, 'GA', NULL, NULL), ('ANALYTICS', 'orders', 'name', 'CV', NULL, 'Y', NULL, 1026, 2, 200, NULL, NULL, NULL, NULL, 'CHECK (name <> '''')');
CREATE TABLE DBC.IndicesV (DatabaseName TEXT, TableName TEXT, ColumnName TEXT, IndexType TEXT, UniqueFlag TEXT, IndexNumber INTEGER, IndexName TEXT, ColumnPosition INTEGER);
INSERT INTO DBC.IndicesV VALUES ('ANALYTICS', 'orders', 'id', 'K', 'Y', 1, NULL, 1), ('ANALYTICS', 'orders', 'name', 'P', 'N', 2, NULL, 1);
CREATE TABLE DBC.All_RI_ChildrenV (ChildDB TEXT, ChildTable TEXT, ParentDB TEXT, ParentTable TEXT, IndexID INTEGER, IndexName TEXT, InconsistencyFlag TEXT, ChildKeyColumn TEXT, ParentKeyColumn TEXT);
INSERT INTO DBC.All_RI_ChildrenV VALUES ('ANALYTICS', 'orders', 'ANALYTICS', 'customers', 3, NULL, 'N', 'id', 'id');
CREATE TABLE DBC.TableTextV (DatabaseName TEXT, TableName TEXT, TableKind TEXT, RequestText TEXT, LineNo INTEGER);
INSERT INTO DBC.TableTextV VALUES ('ANALYTICS', 'v_orders', 'V', 'SELECT id FROM orders', 2), ('ANALYTICS', 'v_orders', 'V', 'CREATE VIEW v_orders AS ', 1);
CREATE TABLE DBC.Table_LevelConstraintsV (DatabaseName TEXT, TableName TEXT, ConstraintName TEXT, ConstraintText TEXT);
INSERT INTO DBC.Table_LevelConstraintsV VALUES ('ANALYTICS', 'orders', 'ck_id', 'CHECK (id > 0)');
""")
    loader = DataDictionaryLoader()

    def rows(domain: str, name: str = "by_schema") -> list[dict[str, object]]:
        query = loader.get_domain_query_text("teradata", domain, name)
        assert isinstance(query, str)
        text = re.sub(r"^\s*LOCKING\s+ROW\s+FOR\s+ACCESS\s+", "", query, flags=re.IGNORECASE)
        text = re.sub(r"\bDATABASE\b", "'ANALYTICS'", text)
        return [
            dict(row)
            for row in db.execute(text, {"schema_name": "ANALYTICS", "table_name": "orders", "object_name": None})
        ]

    try:
        assert rows("version", "current") == [{"version_string": "20.00.18.29"}]
        assert [r["schema_name"] for r in rows("schemas")] == ["ANALYTICS"]
        assert rows("schemas", "current") == [{"schema_name": "ANALYTICS"}]
        assert [r["object_type"] for r in rows("objects")] == ["TABLE", "TABLE", "VIEW"]
        assert rows("tables")[0]["dependency_level"] == 1
        assert rows("tables", "all_by_schema")[0]["table_kind"] == "O"
        assert [r["table_name"] for r in rows("tables", "names_by_schema")] == ["customers", "orders"]
        for name in ("by_schema", "by_table"):
            columns = rows("columns", name)
            assert [r["data_type"] for r in columns] == ["INTEGER", "VARCHAR"]
            assert [r["ordinal_position"] for r in columns] == [1, 2]
            assert columns[0]["identity_generation"] == "ALWAYS"
            assert columns[1]["max_length"] == 100
            assert columns[1]["is_primary"] == 0
            indexes = rows("indexes", name)
            assert [r["index_name"] for r in indexes] == ["PK_orders", "PI_orders_2"]
            assert indexes[1]["is_unique"] == 0
            assert rows("foreign_keys", name)[0]["constraint_name"] == "FK_orders_3"
            assert {r["constraint_type"] for r in rows("constraints", name)} == {"CHECK", "PRIMARY KEY", "FOREIGN KEY"}
        assert "".join(str(r["definition"]) for r in rows("views")) == "CREATE VIEW v_orders AS SELECT id FROM orders"
    finally:
        db.close()


@pytest.mark.parametrize(
    "value,expected",
    [
        ({"InfoData": " 17.20.03.14 "}, "17.20.03.14"),
        (("16.20.53.01",), "16.20.53.01"),
        ({}, None),
        ([], None),
        ("   ", None),
        (None, None),
    ],
)
def test_teradata_version_extraction(value: object, expected: str | None) -> None:
    from sqlspec.data_dictionary.dialects.teradata import extract_teradata_version_value

    assert extract_teradata_version_value(value) == expected


def test_teradata_feature_gating_and_capabilities() -> None:
    from sqlspec.data_dictionary import MetadataRisk, MetadataSupport
    from sqlspec.data_dictionary.dialects.teradata import (
        build_teradata_metadata_capability_profile,
        resolve_teradata_feature_flag,
    )

    assert resolve_teradata_feature_flag("supports_partitioning", None) is False
    assert resolve_teradata_feature_flag("supports_json", None) is True
    assert resolve_teradata_feature_flag("supports_json", VersionInfo(15, 10, 0)) is True
    assert resolve_teradata_feature_flag("supports_returning", VersionInfo(20, 0, 0)) is False
    query = DataDictionaryLoader().get_domain_query(
        "vantage", "columns", "by_schema", version=VersionInfo(14, 10, 0), required_features=("supports_cte",)
    )
    assert query.is_supported is False
    assert MetadataRisk.VERSION_GATED in query.capability.risks
    profile = build_teradata_metadata_capability_profile("TeradataDataDictionary", ("tables", "policies"))
    assert profile.get("tables").support == MetadataSupport.SUPPORTED
    assert profile.get("policies").support == MetadataSupport.UNSUPPORTED
