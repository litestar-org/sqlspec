"""Unit tests for SQL splitter helpers."""

import inspect
from typing import Any

import pytest

import sqlspec.core.splitter as splitter_module
from sqlspec.core.splitter import GenericDialectConfig, StatementSplitter, Token, TokenType, split_sql_script

_SPLITTER_COMPILED = (splitter_module.__file__ or "").endswith((".so", ".pyd"))

PUBLIC_DIALECT_CONFIGS = (
    splitter_module.OracleDialectConfig,
    splitter_module.TSQLDialectConfig,
    splitter_module.PostgreSQLDialectConfig,
    splitter_module.GenericDialectConfig,
    splitter_module.MySQLDialectConfig,
    splitter_module.SQLiteDialectConfig,
    splitter_module.DuckDBDialectConfig,
    splitter_module.BigQueryDialectConfig,
)

DIALECT_DEFAULTS: dict[type[Any], tuple[str, set[str], set[str], set[str], set[str], set[str]]] = {
    splitter_module.OracleDialectConfig: ("oracle", {"BEGIN", "DECLARE", "CASE"}, {"END"}, {";"}, set(), {"/"}),
    splitter_module.TSQLDialectConfig: ("tsql", {"BEGIN", "TRY"}, {"END", "CATCH"}, {";"}, {"GO"}, set()),
    splitter_module.PostgreSQLDialectConfig: ("postgresql", {"DECLARE", "CASE", "DO"}, {"END"}, {";"}, set(), set()),
    splitter_module.GenericDialectConfig: ("generic", {"BEGIN", "DECLARE", "CASE"}, {"END"}, {";"}, set(), set()),
    splitter_module.MySQLDialectConfig: ("mysql", {"BEGIN", "DECLARE", "CASE"}, {"END"}, {";"}, set(), {"\\g", "\\G"}),
    splitter_module.SQLiteDialectConfig: ("sqlite", {"BEGIN", "CASE"}, {"END"}, {";"}, set(), set()),
    splitter_module.DuckDBDialectConfig: ("duckdb", {"BEGIN", "CASE"}, {"END"}, {";"}, set(), set()),
    splitter_module.BigQueryDialectConfig: ("bigquery", {"BEGIN", "CASE"}, {"END"}, {";"}, set(), set()),
}


def test_join_string_fragments_helper_removed() -> None:
    """The one-line join helper should not remain in the splitter module."""

    assert not hasattr(splitter_module, "_join_string_fragments")


def test_dialect_class_map_contains_expected_aliases() -> None:
    """split_sql_script should instantiate only the selected dialect config."""

    assert splitter_module._DIALECT_CLASS_MAP == {
        "generic": splitter_module.GenericDialectConfig,
        "oracle": splitter_module.OracleDialectConfig,
        "tsql": splitter_module.TSQLDialectConfig,
        "mssql": splitter_module.TSQLDialectConfig,
        "sqlserver": splitter_module.TSQLDialectConfig,
        "postgresql": splitter_module.PostgreSQLDialectConfig,
        "postgres": splitter_module.PostgreSQLDialectConfig,
        "paradedb": splitter_module.PostgreSQLDialectConfig,
        "pgvector": splitter_module.PostgreSQLDialectConfig,
        "mysql": splitter_module.MySQLDialectConfig,
        "sqlite": splitter_module.SQLiteDialectConfig,
        "duckdb": splitter_module.DuckDBDialectConfig,
        "bigquery": splitter_module.BigQueryDialectConfig,
    }


def test_split_sql_script_preserves_statement_output() -> None:
    """Statement splitting should preserve existing semicolon handling."""
    assert split_sql_script("SELECT 1; SELECT 2;", strip_trailing_terminator=True) == ["SELECT 1", "SELECT 2"]


def test_tokenize_returns_materialized_token_list() -> None:
    """_tokenize keeps its test-frozen name but returns a list for mypyc hot paths."""
    splitter = StatementSplitter(GenericDialectConfig())

    tokens = splitter._tokenize("SELECT 1;")

    assert isinstance(tokens, list)
    assert [token.value for token in tokens] == ["S", "E", "L", "E", "C", "T", " ", "1", ";"]


@pytest.mark.skipif(_SPLITTER_COMPILED, reason="compiled dialect classes do not expose Python source metadata")
def test_dialect_configs_share_eager_base_without_lazy_property_boilerplate() -> None:
    """Public dialect classes should keep names while sharing the private eager base config."""

    eager_base = getattr(splitter_module, "_EagerDialectConfig")
    for config_class in PUBLIC_DIALECT_CONFIGS:
        assert issubclass(config_class, eager_base)
        source = inspect.getsource(config_class)
        assert "if self._name is None" not in source
        assert "if self._block_starters is None" not in source
        assert "if self._block_enders is None" not in source
        assert "if self._statement_terminators is None" not in source


@pytest.mark.parametrize("config_class", PUBLIC_DIALECT_CONFIGS)
def test_eager_dialect_defaults_preserve_public_values(config_class: "type[splitter_module.DialectConfig]") -> None:
    expected_name, block_starters, block_enders, statement_terminators, batch_separators, special_terminators = (
        DIALECT_DEFAULTS[config_class]
    )
    config = config_class()

    assert config.name == expected_name
    assert config.block_starters == block_starters
    assert config.block_enders == block_enders
    assert config.statement_terminators == statement_terminators
    assert config.batch_separators == batch_separators
    assert set(config.special_terminators) == special_terminators
    assert config.max_nesting_depth == 256
    assert config.block_starters is config.block_starters

    config.block_starters.add("__LOCAL__")
    assert "__LOCAL__" not in config_class().block_starters


@pytest.mark.skipif(_SPLITTER_COMPILED, reason="patch.object cannot intercept mypyc-compiled methods")
def test_split_sql_script_reuses_splitter_for_dialect_and_strip_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """split_sql_script should avoid rebuilding stateless splitter instances for the same dialect and strip flag."""
    splitter_module.clear_splitter_caches()
    created: list[tuple[str, bool]] = []
    original_init = StatementSplitter.__init__

    def counting_init(
        self: StatementSplitter, dialect: splitter_module.DialectConfig, strip_trailing_semicolon: bool = False
    ) -> None:
        created.append((dialect.name, strip_trailing_semicolon))
        original_init(self, dialect, strip_trailing_semicolon)

    monkeypatch.setattr(StatementSplitter, "__init__", counting_init)

    assert split_sql_script("SELECT 1;", dialect="sqlite", strip_trailing_terminator=True) == ["SELECT 1"]
    assert split_sql_script("SELECT 2;", dialect="sqlite", strip_trailing_terminator=True) == ["SELECT 2"]
    assert split_sql_script("SELECT 3;", dialect="sqlite", strip_trailing_terminator=False) == ["SELECT 3;"]

    assert created == [("sqlite", True), ("sqlite", False)]


def test_contains_executable_content_accepts_pre_tokenized_statement(monkeypatch: pytest.MonkeyPatch) -> None:
    splitter = StatementSplitter(GenericDialectConfig())
    tokens = [
        Token(TokenType.COMMENT_LINE, "-- note", 1, 1, 0),
        Token(TokenType.WHITESPACE, "\n", 1, 8, 7),
        Token(TokenType.OTHER, "SELECT", 2, 1, 8),
    ]

    def fail_tokenize(_self: StatementSplitter, _statement: str) -> list[Token]:
        msg = "_contains_executable_content should not re-tokenize"
        raise AssertionError(msg)

    monkeypatch.setattr(StatementSplitter, "_tokenize", fail_tokenize)

    assert splitter._contains_executable_content(tokens) is True


def test_split_cache_keys_on_script_text_not_hash(monkeypatch: pytest.MonkeyPatch) -> None:
    """Splitter result cache must key on script text, not hash(sql); a hash collision must not return the wrong split."""
    splitter = StatementSplitter(GenericDialectConfig())
    splitter._result_cache.clear()
    monkeypatch.setattr(splitter_module, "hash", lambda _sql: 0, raising=False)

    result_a = splitter.split("SELECT 1")
    result_b = splitter.split("SELECT 2")

    assert result_a == ["SELECT 1"]
    assert result_b == ["SELECT 2"]


def test_contains_executable_content_rejects_comment_only_tokens() -> None:
    splitter = StatementSplitter(GenericDialectConfig())
    tokens = [Token(TokenType.COMMENT_LINE, "-- note", 1, 1, 0), Token(TokenType.WHITESPACE, "\n", 1, 8, 7)]

    assert splitter._contains_executable_content(tokens) is False


def test_split_sql_script_drops_whitespace_and_comment_only_scripts() -> None:
    assert split_sql_script(" \n\t ", strip_trailing_terminator=True) == []
    assert split_sql_script("-- note\n/* block */", strip_trailing_terminator=True) == []


def test_split_sql_script_preserves_comment_before_statement() -> None:
    script = "-- note\nSELECT 1;"

    assert split_sql_script(script, strip_trailing_terminator=True) == ["-- note\nSELECT 1"]


def test_split_sql_script_preserves_block_tracking_after_token_append_consolidation() -> None:
    script = "BEGIN\nSELECT 1;\nSELECT 2;\nEND;\nSELECT 3;"

    assert split_sql_script(script, strip_trailing_terminator=True) == ["BEGIN\nSELECT 1;\nSELECT 2;\nEND;", "SELECT 3"]


def test_tsql_go_separates_batches() -> None:
    """T-SQL GO batch separators should split scripts into executable batches."""
    script = "CREATE TABLE t1 (id INT);\nGO\nINSERT INTO t1 VALUES (1);\nGO\nSELECT * FROM t1;"

    statements = split_sql_script(script, dialect="tsql", strip_trailing_terminator=True)

    assert len(statements) == 3
    assert statements[0].startswith("CREATE TABLE")
    assert statements[1].startswith("INSERT")
    assert statements[2].startswith("SELECT")


def test_tsql_go_is_case_insensitive() -> None:
    """T-SQL GO batch matching should not depend on casing."""
    script = "SELECT 1;\ngo\nSELECT 2;\nGO\nSELECT 3;"

    statements = split_sql_script(script, dialect="tsql", strip_trailing_terminator=True)

    assert statements == ["SELECT 1", "SELECT 2", "SELECT 3"]


def test_tsql_begin_try_block_not_split_on_inner_semicolons() -> None:
    """T-SQL TRY/CATCH blocks should remain one statement despite inner semicolons."""
    script = (
        "BEGIN TRY\n"
        "  INSERT INTO t1 VALUES (1);\n"
        "  INSERT INTO t1 VALUES (2);\n"
        "END TRY\n"
        "BEGIN CATCH\n"
        "  THROW;\n"
        "END CATCH;"
    )

    statements = split_sql_script(script, dialect="tsql", strip_trailing_terminator=True)

    assert len(statements) == 1


def test_tsql_semicolon_terminator_within_batch() -> None:
    """T-SQL semicolons should split statements inside the current batch."""
    script = "SELECT 1;\nSELECT 2;\nGO\nSELECT 3;"

    statements = split_sql_script(script, dialect="tsql", strip_trailing_terminator=True)

    assert statements == ["SELECT 1", "SELECT 2", "SELECT 3"]


def test_mssql_alias_dispatches_to_tsql_splitter() -> None:
    """The mssql alias should use the same splitter behavior as tsql."""
    script = "SELECT 1;\nGO\nSELECT 2;"

    assert split_sql_script(script, dialect="mssql") == split_sql_script(script, dialect="tsql")


def test_sqlserver_alias_dispatches_to_tsql_splitter() -> None:
    """The sqlserver alias should use the same splitter behavior as tsql."""
    script = "SELECT 1;\nGO\nSELECT 2;"

    assert split_sql_script(script, dialect="sqlserver") == split_sql_script(script, dialect="tsql")
