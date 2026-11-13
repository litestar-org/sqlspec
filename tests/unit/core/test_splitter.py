"""Unit tests for SQL splitter helpers."""

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
    splitter_module.SnowflakeDialectConfig,
)

DIALECT_DEFAULTS: dict[type[Any], tuple[str, set[str], set[str], set[str], set[str], set[str]]] = {
    splitter_module.OracleDialectConfig: ("oracle", {"BEGIN", "DECLARE", "CASE"}, {"END"}, {";"}, set(), {"/"}),
    splitter_module.TSQLDialectConfig: ("tsql", {"BEGIN", "TRY"}, {"END", "CATCH"}, {";"}, {"GO"}, set()),
    splitter_module.PostgreSQLDialectConfig: ("postgresql", {"CASE", "BEGIN ATOMIC"}, {"END"}, {";"}, set(), set()),
    splitter_module.GenericDialectConfig: ("generic", {"BEGIN", "DECLARE", "CASE"}, {"END"}, {";"}, set(), set()),
    splitter_module.MySQLDialectConfig: ("mysql", {"BEGIN", "DECLARE", "CASE"}, {"END"}, {";"}, set(), {"\\g", "\\G"}),
    splitter_module.SQLiteDialectConfig: ("sqlite", {"BEGIN", "CASE"}, {"END"}, {";"}, set(), set()),
    splitter_module.DuckDBDialectConfig: ("duckdb", {"BEGIN", "CASE"}, {"END"}, {";"}, set(), set()),
    splitter_module.BigQueryDialectConfig: ("bigquery", {"BEGIN", "CASE"}, {"END"}, {";"}, set(), set()),
    splitter_module.SnowflakeDialectConfig: ("snowflake", {"BEGIN", "DECLARE", "CASE"}, {"END"}, {";"}, set(), set()),
}


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
        "pg_textsearch": splitter_module.PostgreSQLDialectConfig,
        "pgtextsearch": splitter_module.PostgreSQLDialectConfig,
        "mysql": splitter_module.MySQLDialectConfig,
        "mariadb": splitter_module.MySQLDialectConfig,
        "cockroach": splitter_module.PostgreSQLDialectConfig,
        "cockroachdb": splitter_module.PostgreSQLDialectConfig,
        "sybase": splitter_module.TSQLDialectConfig,
        "ase": splitter_module.TSQLDialectConfig,
        "sqlanywhere": splitter_module.TSQLDialectConfig,
        "teradata": splitter_module.GenericDialectConfig,
        "vantage": splitter_module.GenericDialectConfig,
        "sqlite": splitter_module.SQLiteDialectConfig,
        "duckdb": splitter_module.DuckDBDialectConfig,
        "bigquery": splitter_module.BigQueryDialectConfig,
        "spanner": splitter_module.BigQueryDialectConfig,
        "spangres": splitter_module.PostgreSQLDialectConfig,
        "db2": splitter_module.Db2DialectConfig,
        "snowflake": splitter_module.SnowflakeDialectConfig,
    }


@pytest.mark.parametrize("dialect", ["mysql", "bigquery", "spanner"])
def test_split_sql_script_keeps_terminators_inside_backtick_identifiers(dialect: str) -> None:
    """Backtick-quoted identifiers, including doubled-backtick escapes, never end a statement."""
    script = "SELECT `a;b`, `c``;d` FROM t; SELECT 2"

    assert split_sql_script(script, dialect=dialect, strip_trailing_terminator=True) == [
        "SELECT `a;b`, `c``;d` FROM t",
        "SELECT 2",
    ]


def test_split_sql_script_spanner_keeps_quoted_terminators() -> None:
    """Spanner scripts split on statement terminators outside backtick and triple-quoted literals."""
    script = 'CREATE TABLE `a;b` (id INT64) PRIMARY KEY (id);\nINSERT INTO `a;b` (id) VALUES (1);\nSELECT """x;y""";'

    assert split_sql_script(script, dialect="spanner", strip_trailing_terminator=True) == [
        "CREATE TABLE `a;b` (id INT64) PRIMARY KEY (id)",
        "INSERT INTO `a;b` (id) VALUES (1)",
        'SELECT """x;y"""',
    ]


def test_split_sql_script_preserves_statement_output() -> None:
    """Statement splitting should preserve existing semicolon handling."""
    assert split_sql_script("SELECT 1; SELECT 2;", strip_trailing_terminator=True) == ["SELECT 1", "SELECT 2"]


def test_tokenize_returns_materialized_token_list() -> None:
    """_tokenize keeps its test-frozen name but returns a list for mypyc hot paths."""
    splitter = StatementSplitter(GenericDialectConfig())

    tokens = splitter._tokenize("SELECT 1;")

    assert isinstance(tokens, list)
    assert [token.value for token in tokens] == ["SELECT", " ", "1", ";"]


@pytest.mark.skipif(_SPLITTER_COMPILED, reason="compiled dialect classes do not expose Python source metadata")
def test_dialect_configs_share_eager_base_without_lazy_property_boilerplate() -> None:
    """Public dialect classes should keep names while sharing the private eager base config."""

    eager_base = getattr(splitter_module, "_EagerDialectConfig")
    for config_class in PUBLIC_DIALECT_CONFIGS:
        assert issubclass(config_class, eager_base)


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


def test_db2_script_splits_declare_global_temporary_table() -> None:
    script = (
        "DECLARE GLOBAL TEMPORARY TABLE session.t (a INT) ON COMMIT PRESERVE ROWS NOT LOGGED; "
        "INSERT INTO session.t VALUES (1); SELECT a FROM session.t"
    )
    assert split_sql_script(script, dialect="db2", strip_trailing_terminator=True) == [
        "DECLARE GLOBAL TEMPORARY TABLE session.t (a INT) ON COMMIT PRESERVE ROWS NOT LOGGED",
        "INSERT INTO session.t VALUES (1)",
        "SELECT a FROM session.t",
    ]


def test_db2_script_keeps_begin_atomic_block() -> None:
    script = (
        "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW BEGIN ATOMIC UPDATE u SET c = c + 1; END; "
        "SELECT 1 FROM SYSIBM.SYSDUMMY1"
    )
    statements = split_sql_script(script, dialect="db2", strip_trailing_terminator=True)
    assert len(statements) == 2
    assert statements[0].endswith("BEGIN ATOMIC UPDATE u SET c = c + 1; END;")
    assert statements[1] == "SELECT 1 FROM SYSIBM.SYSDUMMY1"


def test_oracle_create_or_replace_procedure_strips_slash_keep_end_semicolon() -> None:
    script = "CREATE OR REPLACE PROCEDURE test_proc AS\nBEGIN\n    NULL;\nEND;\n/\nSELECT 1 FROM DUAL;"
    statements = split_sql_script(script, dialect="oracle", strip_trailing_terminator=True)
    assert len(statements) == 2
    assert statements[0].endswith("END;")
    assert statements[1] == "SELECT 1 FROM DUAL"


def test_snowflake_splitter_preserves_cloud_uris_and_escaped_literals() -> None:
    script = (
        "COPY INTO t FROM 's3://bucket/p;1' // ignored ;\nFILE_FORMAT=(TYPE=CSV);\n"
        "COPY INTO t2 FROM 'gcs://bucket/p;2';\n"
        "COPY INTO t3 FROM 'azure://host/c;3';\n"
        "SELECT 'https://host/a;b', 'it''s;ok', 'esc\\';ok', \"a\"\";b\";"
    )
    assert split_sql_script(script, dialect="snowflake", strip_trailing_terminator=True) == [
        "COPY INTO t FROM 's3://bucket/p;1' // ignored ;\nFILE_FORMAT=(TYPE=CSV)",
        "COPY INTO t2 FROM 'gcs://bucket/p;2'",
        "COPY INTO t3 FROM 'azure://host/c;3'",
        "SELECT 'https://host/a;b', 'it''s;ok', 'esc\\';ok', \"a\"\";b\"",
    ]


def test_snowflake_splitter_retains_dollar_quoted_procedure_body() -> None:
    procedure = "CREATE PROCEDURE p() RETURNS INT LANGUAGE SQL AS $$\nBEGIN\n// inner ;\nRETURN 1;\nEND;\n$$"
    assert split_sql_script(procedure + "; CALL p();", dialect="snowflake", strip_trailing_terminator=True) == [
        procedure,
        "CALL p()",
    ]


@pytest.mark.parametrize("begin", ["BEGIN", "BEGIN TRANSACTION", "BEGIN WORK", "BEGIN NAME tx1"])
def test_snowflake_splitter_distinguishes_transaction_begin(begin: str) -> None:
    assert split_sql_script(
        begin + "; INSERT INTO t VALUES (1); COMMIT;", dialect="snowflake", strip_trailing_terminator=True
    ) == [begin, "INSERT INTO t VALUES (1)", "COMMIT"]


@pytest.mark.parametrize("control", ["IF", "FOR", "WHILE", "LOOP", "REPEAT"])
def test_snowflake_splitter_keeps_scripting_control_enders_inside_block(control: str) -> None:
    block = f"DECLARE x INT DEFAULT 1; BEGIN {control} x INSERT INTO t VALUES (1); END {control}; RETURN x; END;"
    assert split_sql_script(block + " SELECT 3;", dialect="snowflake", strip_trailing_terminator=True) == [
        block,
        "SELECT 3",
    ]


def test_snowflake_splitter_tracks_nested_blocks_and_case_expressions() -> None:
    block = "DECLARE x INT DEFAULT 1; BEGIN DECLARE y INT DEFAULT 2; BEGIN SELECT CASE WHEN y = 2 THEN 'x;y' ELSE 'z' END; END; CASE x WHEN 1 THEN SELECT 1; END CASE; END;"
    assert split_sql_script(block + " SELECT 3;", dialect="snowflake", strip_trailing_terminator=True) == [
        block,
        "SELECT 3",
    ]


def test_snowflake_splitter_drops_comments_and_recognizes_mixed_case_dialect(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("WARNING", logger="sqlspec.core.splitter"):
        assert split_sql_script("// first ;\n-- second ;\n/* block ; */", dialect="Snowflake") == []
        assert split_sql_script("SELECT 1; SELECT 2;", dialect="Snowflake", strip_trailing_terminator=True) == [
            "SELECT 1",
            "SELECT 2",
        ]
    assert caplog.records == []


@pytest.mark.parametrize("separator", [" /* boundary */ ", " -- boundary\n ", " // boundary\n ", "/**/"])
@pytest.mark.parametrize("transaction", ["TRANSACTION", "WORK", "NAME tx1"])
def test_snowflake_splitter_transaction_keywords_allow_comments(separator: str, transaction: str) -> None:
    begin = "BEGIN" + separator + transaction
    assert split_sql_script(begin + "; SELECT 1; COMMIT;", dialect="snowflake", strip_trailing_terminator=True) == [
        begin,
        "SELECT 1",
        "COMMIT",
    ]


@pytest.mark.parametrize("separator", [" /* boundary */ ", " -- boundary\n ", " // boundary\n ", "/**/"])
@pytest.mark.parametrize(
    "control,opener,condition",
    [
        ("IF", "IF (1 = 1) THEN", ""),
        ("FOR", "FOR i IN 1 TO 1 DO", ""),
        ("WHILE", "WHILE (1 = 1) DO", ""),
        ("LOOP", "LOOP", ""),
        ("REPEAT", "REPEAT", " UNTIL (1 = 1)"),
        ("CASE", "CASE WHEN 1 = 1 THEN", ""),
    ],
)
def test_snowflake_splitter_control_enders_allow_comments(
    separator: str, control: str, opener: str, condition: str
) -> None:
    block = "BEGIN " + opener + " SELECT 1;" + condition + " END" + separator + control + "; SELECT 2; END;"
    assert split_sql_script(block + " SELECT 3;", dialect="snowflake", strip_trailing_terminator=True) == [
        block,
        "SELECT 3",
    ]


@pytest.mark.parametrize(
    ("dialect", "script", "expected"),
    [
        ("postgres", "BEGIN; INSERT INTO t VALUES (1); COMMIT;", ["BEGIN;", "INSERT INTO t VALUES (1);", "COMMIT;"]),
        (
            "postgres",
            "CREATE FUNCTION f(x int) RETURNS int LANGUAGE sql BEGIN  ATOMIC SELECT x + 1; END; SELECT 2;",
            ["CREATE FUNCTION f(x int) RETURNS int LANGUAGE sql BEGIN  ATOMIC SELECT x + 1; END;", "SELECT 2;"],
        ),
        ("postgres", "SELECT E'a\\';b'; SELECT 2;", ["SELECT E'a\\';b';", "SELECT 2;"]),
        ("postgres", "DO $$ BEGIN PERFORM 1; END $$; SELECT 2;", ["DO $$ BEGIN PERFORM 1; END $$;", "SELECT 2;"]),
        ("postgres", "DECLARE c CURSOR FOR SELECT 1; FETCH c;", ["DECLARE c CURSOR FOR SELECT 1;", "FETCH c;"]),
        ("cockroachdb", "SELECT $x$ a;b $x$; SELECT 2;", ["SELECT $x$ a;b $x$;", "SELECT 2;"]),
        ("postgres", "SELECT a[b[1]]; SELECT c[2];", ["SELECT a[b[1]];", "SELECT c[2];"]),
        ("mysql", "SELECT 'a\\';b'; SELECT 2;", ["SELECT 'a\\';b';", "SELECT 2;"]),
        ("mariadb", "SELECT 1; # it's a note ; here\nSELECT 2;", ["SELECT 1;", "# it's a note ; here\nSELECT 2;"]),
        ("mysql", "BEGIN WORK; UPDATE t SET a = 1; COMMIT;", ["BEGIN WORK;", "UPDATE t SET a = 1;", "COMMIT;"]),
        ("tsql", "SELECT [a]];b] FROM t; SELECT 2;", ["SELECT [a]];b] FROM t;", "SELECT 2;"]),
        ("tsql", "BEGIN TRAN; UPDATE t SET a = 1; COMMIT;", ["BEGIN TRAN;", "UPDATE t SET a = 1;", "COMMIT;"]),
        (
            "duckdb",
            "CREATE MACRO m() AS $$ SELECT 1; $$; SELECT 2;",
            ["CREATE MACRO m() AS $$ SELECT 1; $$;", "SELECT 2;"],
        ),
        ("sqlite", "BEGIN; INSERT INTO t VALUES (1); COMMIT;", ["BEGIN;", "INSERT INTO t VALUES (1);", "COMMIT;"]),
        (
            "sqlite",
            "BEGIN IMMEDIATE; CREATE TRIGGER tr AFTER INSERT ON t BEGIN UPDATE t SET a = 1; END; COMMIT;",
            ["BEGIN IMMEDIATE;", "CREATE TRIGGER tr AFTER INSERT ON t BEGIN UPDATE t SET a = 1; END;", "COMMIT;"],
        ),
        (
            "oracle",
            "SELECT q'[a; it's b]', nq'{c;d}', Q'!e;f!' FROM dual; SELECT 2 FROM dual;",
            ["SELECT q'[a; it's b]', nq'{c;d}', Q'!e;f!' FROM dual;", "SELECT 2 FROM dual;"],
        ),
        (
            "oracle",
            "BEGIN\n IF x THEN y := 1; END IF;\n FOR i IN 1..2 LOOP NULL; END LOOP;\nEND;\n/\nSELECT 1 FROM dual;",
            ["BEGIN\n IF x THEN y := 1; END IF;\n FOR i IN 1..2 LOOP NULL; END LOOP;\nEND;\n/", "SELECT 1 FROM dual;"],
        ),
        (None, "SELECT 1; /* open ; SELECT 2;", ["SELECT 1;"]),
    ],
)
def test_split_sql_script_dialect_lexing(dialect: "str | None", script: str, expected: "list[str]") -> None:
    """Dialect comments, string escapes, quoted identifiers, and blocks keep statements intact."""
    splitter_module.clear_splitter_caches()

    assert split_sql_script(script, dialect=dialect) == expected


@pytest.mark.parametrize(
    ("dialect", "script"),
    [
        (None, "SELECT 1; " + "/* a " * 8000),
        ("postgres", "SELECT 1; " + "".join(f"$t{index}$ x " for index in range(5000))),
    ],
)
def test_split_sql_script_unterminated_spans_scan_once(dialect: "str | None", script: str) -> None:
    """Unterminated comments and dollar-quoted literals extend to the end of the script."""
    splitter_module.clear_splitter_caches()

    assert len(split_sql_script(script, dialect=dialect)) <= 2


def test_split_caches_only_scripts_within_size_bound() -> None:
    """Scripts above the size bound are split without entering the result cache."""
    splitter_module.clear_splitter_caches()
    splitter = StatementSplitter(GenericDialectConfig())
    small = "SELECT 1; SELECT 2;"
    large = "SELECT 1;\n" * (splitter_module.MAX_CACHEABLE_SCRIPT_BYTES // 10 + 1)

    assert splitter.split(small) is splitter.split(small)
    first = splitter.split(large)
    second = splitter.split(large)
    assert first == second
    assert first is not second
