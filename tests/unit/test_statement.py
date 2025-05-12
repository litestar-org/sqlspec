# ruff: noqa: ERA001
# --- Test Case Groups ---

BASIC_PARAMETER_CASES = [
    ("Colon named", "SELECT * FROM users WHERE id = :id", [("var_colon_named", "id")]),
    ("Colon numeric", "SELECT * FROM users WHERE id = :12", [("var_colon_numeric", "12")]),
    ("Question mark", "SELECT * FROM users WHERE id = ?", [("var_qmark", "?")]),
    ("Dollar named", "SELECT * FROM products WHERE name = $name", [("var_dollar", "name")]),
    ("Dollar numeric", "SELECT * FROM products WHERE id = $12", [("var_numeric", "12")]),
    ("At named", "SELECT * FROM employees WHERE email = @email", [("var_at", "email")]),
    ("Pyformat named", "INSERT INTO logs (message) VALUES (%(msg)s)", [("var_pyformat", "msg")]),
    ("Format type", "SELECT name FROM users WHERE status = %s", [("var_format_type", "s")]),
]

COMMENTS_AND_STRINGS_CASES: list[tuple[str, str, list[tuple[str, str]]]] = [
    ("Inside single quotes", "SELECT * FROM users WHERE notes = 'param: :value, other: ?'", []),
    ("Inside double quotes", 'SELECT * FROM users WHERE description = "param: :value, other: ?"', []),
    ("Single quotes with escaped quote", "SELECT 'It''s value: :not_param' FROM test", []),
    ("Double quotes with escaped quote", 'SELECT "It""s value: :not_param" FROM test', []),
    ("Inside single-line comment", "SELECT * FROM users; -- id = :id, name = $name, status = ?", []),
    ("Inside multi-line comment", "SELECT * FROM users; /* id = :id, name = $name, status = ? */", []),
    (
        "Multi-line comment with params",
        "/* \n :param1 \n ? \n $param2 \n @param3 \n %(param4)s \n %d \n $5 \n */ SELECT 1",
        [],
    ),
]

MIXED_AND_MULTIPLE_CASES = [
    (
        "Mixed parameters",
        "SELECT * FROM orders WHERE id = :order_id AND customer_id = @customer AND product_id = $prod AND user_id = ? AND tracking_code = %(track)s AND status = %s AND region_id = $1",
        [
            ("var_colon_named", "order_id"),
            ("var_at", "customer"),
            ("var_dollar", "prod"),
            ("var_qmark", "?"),
            ("var_pyformat", "track"),
            ("var_format_type", "s"),
            ("var_numeric", "1"),
        ],
    ),
    ("Multiple colon named", "SELECT :value1, :value2", [("var_colon_named", "value1"), ("var_colon_named", "value2")]),
    ("Multiple question mark", "SELECT ?, ?", [("var_qmark", "?"), ("var_qmark", "?")]),
    (
        "Multiple dollar (numeric and named)",
        "SELECT $1, $2, $name_val",
        [("var_numeric", "1"), ("var_numeric", "2"), ("var_dollar", "name_val")],
    ),
    (
        "Multiple percent (format and pyformat)",
        "SELECT %s, %(name_val)s, %d",
        [("var_format_type", "s"), ("var_pyformat", "name_val"), ("var_format_type", "d")],
    ),
]

EDGE_CASES = [
    (
        "Complex with comment and quotes",
        "SELECT data->>'key' as val, :param1 FROM test WHERE id = $1; -- :ignored_param 'text' /* :ignored2 */",
        [("var_colon_named", "param1"), ("var_numeric", "1")],
    ),
    (
        "Param after escaped quote",
        "SELECT * FROM test WHERE name = 'it''s a test :not_a_param' AND value = :param_actual",
        [("var_colon_named", "param_actual")],
    ),
    (
        "Param after single line comment",
        "SELECT * FROM test WHERE name = 'foo' -- :param_in_comment \n AND id = :actual_param",
        [("var_colon_named", "actual_param")],
    ),
    (
        "Param after multi-line comment",
        "SELECT * FROM test /* \n multiline comment with :param \n */ WHERE id = :actual_param2",
        [("var_colon_named", "actual_param2")],
    ),
    (
        "All ignored, one real param at end",
        "SELECT 'abc :np1', \"def :np2\", -- :np3 \n /* :np4 */ :real_param",
        [("var_colon_named", "real_param")],
    ),
]

NAMING_VARIATIONS = [
    (
        "Colon named with numbers",
        "SELECT 1 from table where value = :value_1_numeric",
        [("var_colon_named", "value_1_numeric")],
    ),
    ("Colon numeric only", "SELECT 1 from table where value = :123", [("var_colon_numeric", "123")]),
    (
        "Dollar named with numbers",
        "SELECT 1 from table where value = $value_1_numeric",
        [("var_dollar", "value_1_numeric")],
    ),
    ("Dollar numeric only", "SELECT 1 from table where value = $123", [("var_numeric", "123")]),
    ("At named with numbers", "SELECT 1 from table where value = @value_1_numeric", [("var_at", "value_1_numeric")]),
    (
        "Pyformat named with numbers",
        "SELECT 1 from table where value = %(value_1_pyformat)s",
        [("var_pyformat", "value_1_pyformat")],
    ),
    ("Format type (d)", "SELECT 1 from table where value = %d", [("var_format_type", "d")]),
]

LOOKAROUND_SYNTAX_CASES = [
    ("SQL cast ::text", "SELECT foo FROM bar WHERE baz = mycol::text", [("var_colon_named", "text")]),
    ("SQL cast ::numeric", "SELECT foo FROM bar WHERE baz = mycol::12", [("var_colon_numeric", "12")]),
    (
        "Double percent format type %s%s",
        "SELECT foo FROM bar WHERE baz = %s%s",
        [("var_format_type", "s"), ("var_format_type", "s")],
    ),
    (
        "Double pyformat %(n)s%(a)s",
        "SELECT foo FROM bar WHERE baz = %(name)s%(another)s",
        [("var_pyformat", "name"), ("var_pyformat", "another")],
    ),
]

PERCENT_STYLE_EDGE_CASES = [
    ("Single %s", "SELECT %s", [("var_format_type", "s")]),
    ("Double %%s (escaped %)", "SELECT %%s", []),
    ("Triple %%%s (literal % + param %s)", "SELECT %%%s", [("var_format_type", "s")]),
    ("Quadruple %%%%s (two literal %%)", "SELECT %%%%s", []),
    ("Single %(name)s", "SELECT %(name)s", [("var_pyformat", "name")]),
    ("Double %%(name)s", "SELECT %%(name)s", []),
    ("Triple %%%(name)s", "SELECT %%%(name)s", [("var_pyformat", "name")]),
]

DOLLAR_AT_COLON_EDGE_CASES = [
    ("Single $name", "SELECT $name", [("var_dollar", "name")]),
    ("Double $$name (not a var)", "SELECT $$name", []),
    ("Triple $$$name (literal $ + var)", "SELECT $$$name", [("var_dollar", "name")]),
    ("Single $1", "SELECT $1", [("var_numeric", "1")]),
    ("Double $$1 (not a var)", "SELECT $$1", []),
    ("Triple $$$1 (literal $ + var)", "SELECT $$$1", [("var_numeric", "1")]),
    ("Single @name", "SELECT @name", [("var_at", "name")]),
    ("Double @@name (not a var)", "SELECT @@name", []),
    ("Triple @@@name (literal @ + var)", "SELECT @@@name", [("var_at", "name")]),
    ("word:name (not a var)", "SELECT word:name FROM t", []),
    ("_val:name (not a var)", "SELECT _val:name FROM t", []),
    ("val_val:name (not a var)", "SELECT val_val:name FROM t", []),
    ("word:1 (not a var)", "SELECT word:1 FROM t", []),
    ("::name (handled by cast test)", "SELECT foo::name", [("var_colon_named", "name")]),
]

POSTGRES_JSON_OP_CASES = [
    ("Postgres JSON op ??", "SELECT * FROM test WHERE json_col ?? 'key'", [("var_qmark", "?"), ("var_qmark", "?")]),
    (
        "Postgres JSON op ?? with param",
        "SELECT id FROM test WHERE json_col ?? 'key' AND id = ?",
        [("var_qmark", "?"), ("var_qmark", "?"), ("var_qmark", "?")],
    ),
    (
        "Postgres JSON op ?|",
        "SELECT data FROM test WHERE tags ?| array['tag1'] AND id = ?",
        [("var_qmark", "?"), ("var_qmark", "?")],
    ),
    (
        "Postgres JSON op ?&",
        "SELECT data FROM test WHERE tags ?& array['tag1'] AND id = ?",
        [("var_qmark", "?"), ("var_qmark", "?")],
    ),
]


# # --- Helper ---
# def _transform_regex_params_for_test(sql: str, params_info: Optional[list[RegexParamInfo]]) -> list[tuple[str, str]]:
#     """
#     Transforms the RegexParamInfo list from SQLStatement into the format
#     expected by the test cases: List[Tuple[param_type_group_name, param_value]].
#     """
#     if not params_info:
#         return []
#     output = []
#     for p_info in params_info:
#         style = p_info.style
#         name = p_info.name
#         val: str = ""
#         var_group: str = ""
#         if style == "colon":
#             var_group = "var_colon_named"
#             val = name or ""
#         elif style == "colon_numeric":
#             var_group = "var_colon_numeric"
#             val = sql[p_info.start_pos + 1 : p_info.end_pos]
#         elif style == "qmark":
#             var_group = "var_qmark"
#             val = sql[p_info.start_pos : p_info.end_pos]
#         elif style == "dollar":
#             var_group = "var_dollar"
#             val = name or ""
#         elif style == "numeric":
#             var_group = "var_numeric"
#             val = sql[p_info.start_pos + 1 : p_info.end_pos]
#         elif style == "at":
#             var_group = "var_at"
#             val = name or ""
#         elif style == "pyformat":
#             var_group = "var_pyformat"
#             val = name or ""
#         elif style == "format":
#             var_group = "var_format_type"
#             val = sql[p_info.start_pos + 1 : p_info.end_pos]
#         else:
#             raise ValueError(f"Unknown RegexParamInfo style: {style}")
#         output.append((var_group, val))
#     return output


# # --- Test Functions ---


# @pytest.mark.parametrize(("description", "sql", "expected_params"), BASIC_PARAMETER_CASES)
# def test_basic_parameter_types(description: str, sql: str, expected_params: list[tuple[str, str]]) -> None:
#     stmt = SQLStatement(sql=sql)
#     discovered_info = stmt._regex_discovered_params
#     actual = _transform_regex_params_for_test(sql, discovered_info)
#     assert actual == expected_params, f"{description}\nSQL: {sql}\nExpected: {expected_params}\nActual: {actual}"


# @pytest.mark.parametrize(("description", "sql", "expected_params"), COMMENTS_AND_STRINGS_CASES)
# def test_parameters_ignored_in_comments_and_strings(
#     description: str, sql: str, expected_params: list[tuple[str, str]]
# ) -> None:
#     stmt = SQLStatement(sql=sql)
#     discovered_info = stmt._regex_discovered_params
#     actual = _transform_regex_params_for_test(sql, discovered_info)
#     assert actual == expected_params, f"{description}\nSQL: {sql}\nExpected: {expected_params}\nActual: {actual}"


# @pytest.mark.parametrize(("description", "sql", "expected_params"), MIXED_AND_MULTIPLE_CASES)
# def test_mixed_and_multiple_parameters(description: str, sql: str, expected_params: list[tuple[str, str]]) -> None:
#     stmt = SQLStatement(sql=sql)
#     discovered_info = stmt._regex_discovered_params
#     actual = _transform_regex_params_for_test(sql, discovered_info)
#     assert actual == expected_params, f"{description}\nSQL: {sql}\nExpected: {expected_params}\nActual: {actual}"


# @pytest.mark.parametrize(("description", "sql", "expected_params"), EDGE_CASES)
# def test_edge_cases(description: str, sql: str, expected_params: list[tuple[str, str]]) -> None:
#     stmt = SQLStatement(sql=sql)
#     discovered_info = stmt._regex_discovered_params
#     actual = _transform_regex_params_for_test(sql, discovered_info)
#     assert actual == expected_params, f"{description}\nSQL: {sql}\nExpected: {expected_params}\nActual: {actual}"


# @pytest.mark.parametrize(("description", "sql", "expected_params"), NAMING_VARIATIONS)
# def test_parameter_naming_variations(description: str, sql: str, expected_params: list[tuple[str, str]]) -> None:
#     stmt = SQLStatement(sql=sql)
#     discovered_info = stmt._regex_discovered_params
#     actual = _transform_regex_params_for_test(sql, discovered_info)
#     assert actual == expected_params, f"{description}\nSQL: {sql}\nExpected: {expected_params}\nActual: {actual}"


# @pytest.mark.parametrize(("description", "sql", "expected_params"), LOOKAROUND_SYNTAX_CASES)
# def test_lookaround_and_syntax_interaction(description: str, sql: str, expected_params: list[tuple[str, str]]) -> None:
#     stmt = SQLStatement(sql=sql)
#     discovered_info = stmt._regex_discovered_params
#     actual = _transform_regex_params_for_test(sql, discovered_info)
#     assert actual == expected_params, f"{description}\nSQL: {sql}\nExpected: {expected_params}\nActual: {actual}"


# @pytest.mark.parametrize(("description", "sql", "expected_params"), PERCENT_STYLE_EDGE_CASES)
# def test_percent_style_edge_cases(description: str, sql: str, expected_params: list[tuple[str, str]]) -> None:
#     stmt = SQLStatement(sql=sql)
#     discovered_info = stmt._regex_discovered_params
#     actual = _transform_regex_params_for_test(sql, discovered_info)
#     assert actual == expected_params, f"{description}\nSQL: {sql}\nExpected: {expected_params}\nActual: {actual}"


# @pytest.mark.parametrize(("description", "sql", "expected_params"), DOLLAR_AT_COLON_EDGE_CASES)
# def test_dollar_at_colon_edge_cases(description: str, sql: str, expected_params: list[tuple[str, str]]) -> None:
#     stmt = SQLStatement(sql=sql)
#     discovered_info = stmt._regex_discovered_params
#     actual = _transform_regex_params_for_test(sql, discovered_info)
#     assert actual == expected_params, f"{description}\nSQL: {sql}\nExpected: {expected_params}\nActual: {actual}"


# @pytest.mark.parametrize(("description", "sql", "expected_params"), POSTGRES_JSON_OP_CASES)
# def test_postgres_json_operator_cases(description: str, sql: str, expected_params: list[tuple[str, str]]) -> None:
#     stmt = SQLStatement(sql=sql)
#     discovered_info = stmt._regex_discovered_params
#     actual = _transform_regex_params_for_test(sql, discovered_info)
#     assert actual == expected_params, f"{description}\nSQL: {sql}\nExpected: {expected_params}\nActual: {actual}"
