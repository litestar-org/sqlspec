"""Performance and edge case tests for PARAMETER_REGEX.

Tests regex efficiency, order dependency, and edge cases across all SQL dialects.
"""

import time

from sqlspec.core.parameters import ParameterValidator


class TestParameterRegexPerformance:
    """Test parameter regex performance and correctness."""

    def test_oracle_system_views_not_detected_as_parameters(self) -> None:
        """Verify Oracle system views with $ are not detected as parameters."""
        validator = ParameterValidator()

        # Oracle system views should NOT be detected as parameters
        test_cases = [
            ("SELECT * FROM v$version", []),
            ("SELECT * FROM v$session WHERE sid = :sid", ["sid"]),
            ("SELECT * FROM v$database, v$instance", []),
            ("SELECT banner FROM v$version WHERE banner LIKE :pattern", ["pattern"]),
            ("SELECT * FROM gv$session WHERE inst_id = :inst", ["inst"]),
        ]

        for sql, expected_params in test_cases:
            params = validator.extract_parameters(sql)
            param_names = [p.name for p in params if p.name is not None]
            assert param_names == expected_params, f"Failed for: {sql}"

    def test_sql_server_global_variables_not_detected(self) -> None:
        """Verify SQL Server @@variables are not detected as parameters."""
        validator = ParameterValidator()

        test_cases = [
            ("SELECT @@VERSION", []),
            ("SELECT @@IDENTITY", []),
            ("SELECT @@ROWCOUNT, @param", ["param"]),
            ("IF @@ERROR <> 0 SELECT @value", ["value"]),
        ]

        for sql, expected_params in test_cases:
            params = validator.extract_parameters(sql)
            param_names = [p.name for p in params if p.name is not None]
            assert param_names == expected_params, f"Failed for: {sql}"

    def test_postgres_dollar_quoted_strings_not_detected(self) -> None:
        """Verify PostgreSQL dollar-quoted strings don't create false parameters."""
        validator = ParameterValidator()

        test_cases = [
            ("SELECT $$hello$$", []),
            ("SELECT $tag$world$tag$", []),
            ("SELECT $$value:123$$, :param", ["param"]),
            ("SELECT $func$SELECT $1$func$, $1", ["1"]),
        ]

        for sql, expected_params in test_cases:
            params = validator.extract_parameters(sql)
            param_names = [p.name for p in params if p.name is not None]
            assert param_names == expected_params, f"Failed for: {sql}"

    def test_xml_namespaces_not_detected(self) -> None:
        """Verify XML namespaces with colons are not detected as parameters."""
        validator = ParameterValidator()

        test_cases = [
            ("SELECT '<xml:element>data</xml:element>'", []),
            ("SELECT '<ns:tag attr=\":value\">' WHERE id = :id", ["id"]),
            ("UPDATE xml SET data = '<schema:table>' WHERE id = :id", ["id"]),
        ]

        for sql, expected_params in test_cases:
            params = validator.extract_parameters(sql)
            param_names = [p.name for p in params if p.name is not None]
            assert param_names == expected_params, f"Failed for: {sql}"

    def test_parameter_order_precedence(self) -> None:
        """Verify parameter detection order is correct."""
        validator = ParameterValidator()

        # Positional colon (:1, :2) MUST be detected before named colon (:name)
        params = validator.extract_parameters("SELECT :1, :name, :123, :user")
        assert len(params) == 4
        assert params[0].name == "1"
        assert params[1].name == "name"
        assert params[2].name == "123"
        assert params[3].name == "user"

    def test_mixed_identifiers_and_parameters(self) -> None:
        """Test SQL with identifiers that contain parameter-like characters."""
        validator = ParameterValidator()

        test_cases = [
            # Table names with special chars (not detected due to negative lookbehind)
            ("SELECT * FROM user$data WHERE id = :id", ["id"]),
            ("SELECT * FROM price@2023 WHERE amount > @amount", ["amount"]),
            ("SELECT * FROM log:entry WHERE time = :time", ["time"]),
            # Column names - NOTE: @column IS detected as parameter in SQL Server
            # This is correct behavior - if you write SELECT @var, it's a parameter
            ("SELECT user$id, :param FROM table", ["param"]),
            # @@VERSION is skipped, but @column and @param are detected (correct!)
            ("SELECT @column, @@VERSION, @param FROM t", ["column", "param"]),
        ]

        for sql, expected_params in test_cases:
            params = validator.extract_parameters(sql)
            param_names = [p.name for p in params if p.name is not None]
            assert param_names == expected_params, f"Failed for: {sql}"

    def test_all_dialect_parameter_styles(self) -> None:
        """Comprehensive test of all supported parameter styles."""
        validator = ParameterValidator()

        dialect_tests = {
            "qmark": ("SELECT * FROM t WHERE a = ? AND b = ?", [None, None]),
            "numeric": ("SELECT * FROM t WHERE a = $1 AND b = $2", ["1", "2"]),
            "named_dollar": ("SELECT * FROM t WHERE a = $foo AND b = $bar", ["foo", "bar"]),
            "named_colon": ("SELECT * FROM t WHERE a = :foo AND b = :bar", ["foo", "bar"]),
            "positional_colon": ("SELECT * FROM t WHERE a = :1 AND b = :2", ["1", "2"]),
            "named_at": ("SELECT * FROM t WHERE a = @foo AND b = @bar", ["foo", "bar"]),
            "pyformat_named": ("SELECT * FROM t WHERE a = %(foo)s AND b = %(bar)s", ["foo", "bar"]),
            "pyformat_pos": ("SELECT * FROM t WHERE a = %s AND b = %s", [None, None]),
        }

        for style, (sql, expected) in dialect_tests.items():
            params = validator.extract_parameters(sql)
            param_names = [p.name for p in params]
            assert param_names == expected, f"Failed for {style}: {sql}"

    def test_regex_performance_on_large_sql(self) -> None:
        """Benchmark regex performance on large SQL statements."""
        validator = ParameterValidator()

        # Generate large SQL with many parameters
        large_sql = "SELECT * FROM t WHERE " + " AND ".join([f"col{i} = :param{i}" for i in range(1000)])

        start = time.perf_counter()
        params = validator.extract_parameters(large_sql)
        elapsed = time.perf_counter() - start

        assert len(params) == 1000
        assert elapsed < 0.1, f"Regex took too long: {elapsed:.4f}s"  # Should be <100ms

        # Test cache hit (should be much faster)
        start = time.perf_counter()
        params_cached = validator.extract_parameters(large_sql)
        elapsed_cached = time.perf_counter() - start

        assert len(params_cached) == 1000
        assert elapsed_cached < 0.001, f"Cache lookup too slow: {elapsed_cached:.6f}s"  # Should be <1ms

    def test_no_catastrophic_backtracking(self) -> None:
        """Ensure regex doesn't have catastrophic backtracking."""
        validator = ParameterValidator()

        # Pathological cases that could cause backtracking
        pathological_cases = [
            # Many nested quotes
            "SELECT '" + ("x" * 10000) + "'",
            # Many dollar signs (but not valid parameters)
            "SELECT price" + ("$" * 1000) + "2023",
            # Many colons in strings
            "SELECT '" + ("::" * 1000) + "'",
        ]

        for sql in pathological_cases:
            start = time.perf_counter()
            validator.extract_parameters(sql)
            elapsed = time.perf_counter() - start
            assert elapsed < 0.1, f"Pathological case too slow: {elapsed:.4f}s"

    def test_edge_case_empty_and_whitespace(self) -> None:
        """Test edge cases with empty strings and whitespace."""
        validator = ParameterValidator()

        test_cases = [
            ("", []),
            ("   ", []),
            ("SELECT :param", ["param"]),
            ("SELECT   :param1  ,  :param2  ", ["param1", "param2"]),
            ("-- comment :not_param\nSELECT :param", ["param"]),
        ]

        for sql, expected in test_cases:
            params = validator.extract_parameters(sql)
            param_names = [p.name for p in params if p.name is not None]
            assert param_names == expected, f"Failed for: {sql!r}"

    def test_unicode_and_special_characters(self) -> None:
        """Test parameter detection with Unicode and special characters."""
        validator = ParameterValidator()

        test_cases = [
            ("SELECT :café FROM table", ["café"]),
            ("SELECT :用户 FROM table", ["用户"]),
            ("SELECT :Москва FROM table", ["Москва"]),
            ("SELECT :param_123 FROM table", ["param_123"]),
        ]

        for sql, expected in test_cases:
            params = validator.extract_parameters(sql)
            param_names = [p.name for p in params if p.name is not None]
            assert param_names == expected, f"Failed for: {sql}"
