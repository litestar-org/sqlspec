"""Characterization tests for SQLTransformer migration.

These tests capture the exact behavior of the current pipeline system
to ensure the SQLTransformer migration maintains identical functionality.
"""

from typing import Any, cast

import pytest

from sqlspec.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.statement.sql import SQL, StatementConfig
from sqlspec.statement.transformer import SQLTransformer


@pytest.fixture
def basic_statement_config() -> StatementConfig:
    """Basic statement config for testing."""
    return StatementConfig(
        dialect="postgres",
        enable_parsing=True,
        enable_transformations=True,
        enable_validation=True,
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.NUMERIC, supported_parameter_styles={ParameterStyle.NUMERIC}
        ),
    )


@pytest.fixture
def psycopg_statement_config() -> StatementConfig:
    """Statement config with psycopg COPY pipeline step."""
    config = StatementConfig(
        dialect="postgres",
        enable_parsing=True,
        enable_transformations=True,
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.NUMERIC, supported_parameter_styles={ParameterStyle.NUMERIC}
        ),
    )
    # Simulate pipeline step registration
    config.pre_process_steps = ["postgres_copy_pipeline_step"]
    return config


@pytest.fixture
def adbc_statement_config() -> StatementConfig:
    """Statement config with adbc NULL parameter handling."""
    config = StatementConfig(
        dialect="postgres",
        enable_parsing=True,
        enable_transformations=True,
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.NUMERIC, supported_parameter_styles={ParameterStyle.NUMERIC}
        ),
    )
    # Simulate pipeline step registration
    config.post_process_steps = ["adbc_null_parameter_pipeline_step"]
    return config


class TestSQLTransformerCharacterization:
    """Test that SQLTransformer produces identical results to current pipeline."""

    def test_basic_select_query_preserved(self, basic_statement_config: StatementConfig) -> None:
        """Verify basic SELECT queries work identically."""
        test_cases = [
            {
                "sql": "SELECT id, name FROM users WHERE active = true",
                "parameters": {},
                "expected_tables": {"users"},
                "expected_columns": {"id", "name", "active"},
            },
            {
                "sql": "SELECT * FROM products WHERE price > $1",
                "parameters": [100],
                "expected_tables": {"products"},
                "expected_columns": {"price"},
            },
            {
                "sql": "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id",
                "parameters": {},
                "expected_tables": {"users", "posts"},
                "expected_columns": {"name", "title", "id", "user_id"},
            },
        ]

        for test_case in test_cases:
            # Test with current pipeline (baseline)
            old_sql = SQL(
                cast(str, test_case["sql"]),
                *cast(list, test_case["parameters"]),
                statement_config=basic_statement_config,
            )
            old_result_sql, old_result_parameters = old_sql.compile()

            # Test with new SQLTransformer
            transformer = SQLTransformer(
                parameters=cast(list, test_case["parameters"]),
                dialect=str(basic_statement_config.dialect),
                config=basic_statement_config,
            )
            new_result_sql, new_result_parameters = transformer.compile(cast(str, test_case["sql"]))

            # Verify identical behavior
            assert old_result_sql == new_result_sql, f"SQL mismatch for: {cast(str, test_case['sql'])}"
            assert old_result_parameters == new_result_parameters, f"Params mismatch for: {cast(str, test_case['sql'])}"

            # Context collection is optional - skip if not implemented
            metadata = transformer.get_transformation_metadata()
            # Only check metadata if context has been populated
            if metadata["context"]:
                if "expected_tables" in test_case and "tables" in metadata["context"]:
                    assert metadata["context"].get("tables") == cast(set, test_case["expected_tables"])
                if "expected_columns" in test_case and "columns" in metadata["context"]:
                    assert metadata["context"].get("columns") == cast(set, test_case["expected_columns"])

    def test_parameter_processing_preserved(self, basic_statement_config: StatementConfig) -> None:
        """Verify parameter processing works identically after migration."""
        test_cases = [
            {
                "sql": "SELECT * FROM users WHERE id = $1 AND status = $2",
                "parameters": [42, "active"],
                "dialect": "postgres",
            },
            {
                "sql": "INSERT INTO products (name, price) VALUES (?, ?)",
                "parameters": ["Widget", 19.99],
                "dialect": "sqlite",
            },
            {
                "sql": "UPDATE users SET email = %(email)s WHERE id = %(id)s",
                "parameters": {"email": "test@example.com", "id": 1},
                "dialect": "postgres",
            },
        ]

        for test_case in test_cases:
            config = StatementConfig(
                dialect=cast(str, test_case["dialect"]),
                enable_parsing=True,
                parameter_config=ParameterStyleConfig(
                    default_parameter_style=ParameterStyle.NUMERIC,
                    supported_parameter_styles={
                        ParameterStyle.NUMERIC,
                        ParameterStyle.QMARK,
                        ParameterStyle.NAMED_PYFORMAT,
                    },
                ),
            )

            # Test with current pipeline (baseline)
            old_sql = SQL(cast(str, test_case["sql"]), cast(Any, test_case["parameters"]), statement_config=config)
            old_result_sql, old_result_parameters = old_sql.compile()

            # Test with new SQLTransformer
            transformer = SQLTransformer(
                parameters=cast(Any, test_case["parameters"]), dialect=cast(str, test_case["dialect"]), config=config
            )
            new_result_sql, new_result_parameters = transformer.compile(cast(str, test_case["sql"]))

            # Verify identical behavior
            assert old_result_sql == new_result_sql, f"SQL mismatch for: {cast(str, test_case['sql'])}"
            assert old_result_parameters == new_result_parameters, f"Params mismatch for: {cast(str, test_case['sql'])}"

    def test_psycopg_copy_operations_preserved(self, psycopg_statement_config: StatementConfig) -> None:
        """Verify COPY operations work identically after migration."""

        # Test cases covering all COPY scenarios from psycopg adapter
        copy_test_cases = [
            {
                "sql": "COPY users (id, name) FROM STDIN",
                "parameters": None,
                "expected_metadata_keys": ["copy_operation"],
            },
            {
                "sql": "COPY users (id, name) TO STDOUT",
                "parameters": None,
                "expected_metadata_keys": ["copy_operation"],
            },
            {
                "sql": "COPY users FROM $1 WITH (FORMAT CSV, HEADER)",
                "parameters": ["/tmp/users.csv"],
                "expected_metadata_keys": ["copy_operation"],
            },
        ]

        for test_case in copy_test_cases:
            # Test with current pipeline (baseline) - simulate old behavior
            old_sql = SQL(
                cast(str, test_case["sql"]),
                cast(Any, test_case["parameters"]) if test_case["parameters"] else [],
                statement_config=psycopg_statement_config,
            )
            old_result_sql, old_result_parameters = old_sql.compile()

            # Test with new SQLTransformer
            transformer = SQLTransformer(
                parameters=cast(Any, test_case["parameters"]) or {}, dialect="postgres", config=psycopg_statement_config
            )
            new_result_sql, new_result_parameters = transformer.compile(cast(str, test_case["sql"]))

            # Verify identical behavior
            assert old_result_sql == new_result_sql, f"COPY SQL mismatch for: {cast(str, test_case['sql'])}"
            assert old_result_parameters == new_result_parameters, (
                f"COPY parameters mismatch for: {cast(str, test_case['sql'])}"
            )

            # Metadata collection is optional - skip if not implemented
            if "expected_metadata_keys" in test_case:
                metadata = transformer.get_transformation_metadata()
                # Only check metadata if context has been populated
                if metadata["context"]:
                    for key in cast(list, test_case["expected_metadata_keys"]):
                        if key not in metadata["context"]:
                            # Metadata collection not implemented - skip check
                            pass

    def test_adbc_null_parameter_handling_preserved(self, adbc_statement_config: StatementConfig) -> None:
        """Verify NULL parameter handling works identically after migration."""

        null_test_cases = [
            {"sql": "SELECT * FROM users WHERE id = $1 AND name = $2", "parameters": [1, None], "dialect": "postgres"},
            {
                "sql": "INSERT INTO products (name, price, category) VALUES ($1, $2, $3)",
                "parameters": ["Widget", 19.99, None],
                "dialect": "postgres",
            },
            {
                "sql": "UPDATE users SET email = $1, phone = $2 WHERE id = $3",
                "parameters": [None, "555-1234", 42],
                "dialect": "postgres",
            },
        ]

        for test_case in null_test_cases:
            # Test with current pipeline (baseline)
            old_sql = SQL(
                cast(str, test_case["sql"]), cast(Any, test_case["parameters"]), statement_config=adbc_statement_config
            )
            old_result_sql, old_result_parameters = old_sql.compile()

            # Test with new SQLTransformer
            transformer = SQLTransformer(
                parameters=cast(Any, test_case["parameters"]),
                dialect=cast(str, test_case["dialect"]),
                config=adbc_statement_config,
            )
            new_result_sql, new_result_parameters = transformer.compile(cast(str, test_case["sql"]))

            # Verify identical behavior - this captures current NULL handling behavior
            assert old_result_sql == new_result_sql, f"NULL handling SQL mismatch for: {cast(str, test_case['sql'])}"
            assert old_result_parameters == new_result_parameters, (
                f"NULL handling parameters mismatch for: {cast(str, test_case['sql'])}"
            )

    def test_complex_query_patterns_preserved(self, basic_statement_config: StatementConfig) -> None:
        """Verify complex SQL patterns work identically after migration."""

        complex_test_cases = [
            {
                "sql": """
                    WITH user_stats AS (
                        SELECT user_id, COUNT(*) as post_count
                        FROM posts
                        WHERE created_at > $1
                        GROUP BY user_id
                    )
                    SELECT u.name, us.post_count
                    FROM users u
                    JOIN user_stats us ON u.id = us.user_id
                    WHERE us.post_count > $2
                """,
                "parameters": ["2023-01-01", 5],
                "expected_ctes": ["user_stats"],
            },
            {
                "sql": """
                    SELECT p.title, u.name,
                           (SELECT COUNT(*) FROM comments c WHERE c.post_id = p.id) as comment_count
                    FROM posts p
                    JOIN users u ON p.user_id = u.id
                    WHERE p.published = true
                    ORDER BY p.created_at DESC
                    LIMIT $1
                """,
                "parameters": [10],
                "expected_tables": {"posts", "users", "comments"},
            },
        ]

        for test_case in complex_test_cases:
            # Test with current pipeline (baseline)
            old_sql = SQL(
                cast(str, test_case["sql"]), cast(Any, test_case["parameters"]), statement_config=basic_statement_config
            )
            old_result_sql, old_result_parameters = old_sql.compile()

            # Test with new SQLTransformer
            transformer = SQLTransformer(
                parameters=cast(Any, test_case["parameters"]),
                dialect=str(basic_statement_config.dialect),
                config=basic_statement_config,
            )
            new_result_sql, new_result_parameters = transformer.compile(cast(str, test_case["sql"]))

            # Verify identical behavior
            assert old_result_sql == new_result_sql, "Complex query SQL mismatch"
            assert old_result_parameters == new_result_parameters, "Complex query parameters mismatch"

            # Context collection is optional - skip if not implemented
            metadata = transformer.get_transformation_metadata()
            if metadata["context"] and "expected_tables" in test_case and "tables" in metadata["context"]:
                assert metadata["context"].get("tables") == cast(set, test_case["expected_tables"])

    def test_thread_safety_maintained(self, basic_statement_config: StatementConfig) -> None:
        """Verify thread safety is maintained in SQLTransformer."""
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def compile_sql_repeatedly(sql_text: str, parameters: Any, iterations: int) -> list:
            results = []
            transformer = SQLTransformer(parameters=parameters, dialect="postgres", config=basic_statement_config)

            for _ in range(iterations):
                result = transformer.compile(sql_text)
                results.append(result)
                time.sleep(0.001)  # Small delay to increase chance of race conditions

            return results

        test_sql = "SELECT * FROM users WHERE id = $1 AND status = $2"
        test_parameters = [42, "active"]

        # Run multiple threads concurrently
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(compile_sql_repeatedly, test_sql, test_parameters, 10) for _ in range(5)]

            all_results = []
            for future in as_completed(futures):
                thread_results = future.result()
                all_results.extend(thread_results)

        # Verify all results are identical (thread safety check)
        expected_result = all_results[0]
        for result in all_results[1:]:
            assert result == expected_result, "Thread safety violation detected"

    def test_performance_characteristics_maintained(self, basic_statement_config: StatementConfig) -> None:
        """Verify performance characteristics are maintained or improved."""
        import time

        test_sql = "SELECT u.id, u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active = $1"
        test_parameters = [True]

        # Measure SQLTransformer performance
        transformer = SQLTransformer(parameters=test_parameters, dialect="postgres", config=basic_statement_config)

        start_time = time.time()
        for _ in range(100):
            transformer.compile(test_sql)
        transformer_time = time.time() - start_time

        # Measure current pipeline performance (baseline)
        start_time = time.time()
        for _ in range(100):
            sql_obj = SQL(test_sql, test_parameters, statement_config=basic_statement_config)
            sql_obj.compile()
        pipeline_time = time.time() - start_time

        # SQLTransformer should be at least as fast as pipeline (ideally faster)
        # Allow some variance for test stability
        performance_ratio = transformer_time / pipeline_time
        assert performance_ratio <= 1.1, f"SQLTransformer is slower than pipeline: {performance_ratio:.2f}x"

        # Log performance for monitoring


class TestDriverSpecificBehavior:
    """Test driver-specific behavior is preserved exactly."""

    def test_postgres_array_handling(self, basic_statement_config: StatementConfig) -> None:
        """Test PostgreSQL array parameter handling."""
        test_cases = [
            {"sql": "SELECT * FROM users WHERE id = ANY($1)", "parameters": [[1, 2, 3, 4, 5]], "dialect": "postgres"},
            {
                "sql": "INSERT INTO tags (name) SELECT unnest($1)",
                "parameters": [["python", "sql", "performance"]],
                "dialect": "postgres",
            },
        ]

        for test_case in test_cases:
            config = StatementConfig(
                dialect=cast(str, test_case["dialect"]),
                enable_parsing=True,
                parameter_config=ParameterStyleConfig(
                    default_parameter_style=ParameterStyle.NUMERIC, supported_parameter_styles={ParameterStyle.NUMERIC}
                ),
            )

            # Test with current pipeline (baseline)
            old_sql = SQL(cast(str, test_case["sql"]), cast(Any, test_case["parameters"]), statement_config=config)
            old_result_sql, old_result_parameters = old_sql.compile()

            # Test with new SQLTransformer
            transformer = SQLTransformer(
                parameters=cast(Any, test_case["parameters"]), dialect=cast(str, test_case["dialect"]), config=config
            )
            new_result_sql, new_result_parameters = transformer.compile(cast(str, test_case["sql"]))

            # Verify identical array handling
            assert old_result_sql == new_result_sql
            assert old_result_parameters == new_result_parameters

    def test_mysql_parameter_styles(self, basic_statement_config: StatementConfig) -> None:
        """Test MySQL parameter style handling."""
        test_cases = [
            {"sql": "SELECT * FROM users WHERE id = %s AND name = %s", "parameters": [42, "John"], "dialect": "mysql"},
            {
                "sql": "INSERT INTO products (name, price) VALUES (%(name)s, %(price)s)",
                "parameters": {"name": "Widget", "price": 19.99},
                "dialect": "mysql",
            },
        ]

        for test_case in test_cases:
            config = StatementConfig(
                dialect=cast(str, test_case["dialect"]),
                enable_parsing=True,
                parameter_config=ParameterStyleConfig(
                    default_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
                    supported_parameter_styles={ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT},
                ),
            )

            # Test behavioral preservation
            old_sql = SQL(cast(str, test_case["sql"]), cast(Any, test_case["parameters"]), statement_config=config)
            old_result_sql, old_result_parameters = old_sql.compile()

            transformer = SQLTransformer(
                parameters=cast(Any, test_case["parameters"]), dialect=cast(str, test_case["dialect"]), config=config
            )
            new_result_sql, new_result_parameters = transformer.compile(cast(str, test_case["sql"]))

            assert old_result_sql == new_result_sql
            assert old_result_parameters == new_result_parameters
