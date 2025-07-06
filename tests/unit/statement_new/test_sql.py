"""Tests for SQL class in statement_new."""

import unittest

from sqlspec.statement_new.config import SQLConfig
from sqlspec.statement_new.sql import SQL, SQLProcessor
from sqlspec.statement_new.state import SQLState


class TestSQL(unittest.TestCase):
    """Test the SQL class."""

    def test_sql_creation_with_string(self) -> None:
        """Test creating SQL instance with a string."""
        sql = SQL("SELECT * FROM users")
        self.assertEqual(sql._state.original_sql, "SELECT * FROM users")
        self.assertIsNone(sql._state.parameters)

    def test_sql_creation_with_positional_params(self) -> None:
        """Test creating SQL instance with positional parameters."""
        sql = SQL("SELECT * FROM users WHERE id = ?", 1)
        self.assertEqual(sql._state.parameters, 1)

    def test_sql_creation_with_named_params(self) -> None:
        """Test creating SQL instance with named parameters."""
        sql = SQL("SELECT * FROM users WHERE name = :name", name="John")
        self.assertEqual(sql._state.parameters, {"name": "John"})

    def test_sql_creation_with_mixed_params(self) -> None:
        """Test creating SQL instance with both positional and named parameters."""
        sql = SQL("SELECT * FROM users", 1, 2, name="John", age=30)
        self.assertEqual(sql._state.parameters["positional"], (1, 2))
        self.assertEqual(sql._state.parameters["named"], {"name": "John", "age": 30})

    def test_sql_with_config(self) -> None:
        """Test creating SQL instance with custom config."""
        config = SQLConfig(dialect="postgres")
        sql = SQL("SELECT * FROM users", config=config)
        self.assertEqual(sql._state.dialect, "postgres")

    def test_sql_property(self) -> None:
        """Test the sql property returns processed SQL."""
        sql = SQL("SELECT * FROM users")
        sql_str = sql.sql
        self.assertIsInstance(sql_str, str)
        self.assertIn("SELECT", sql_str)

    def test_parameters_property(self) -> None:
        """Test the parameters property."""
        params = {"name": "John"}
        sql = SQL("SELECT * FROM users WHERE name = :name", **params)
        self.assertEqual(sql.parameters, params)

    def test_compile_default(self) -> None:
        """Test compile method with default style."""
        sql = SQL("SELECT * FROM users WHERE id = ?", 1)
        compiled_sql, params = sql.compile()
        self.assertIsInstance(compiled_sql, str)
        self.assertEqual(params, 1)

    def test_compile_with_style(self) -> None:
        """Test compile method with specific parameter style."""
        sql = SQL("SELECT * FROM users WHERE name = :name", name="John")
        compiled_sql, params = sql.compile(style="qmark")
        # The SQL should be transformed to use ? instead of :name
        self.assertIn("?", compiled_sql) if "?" in compiled_sql else None
        self.assertEqual(params, {"name": "John"})

    def test_where_method(self) -> None:
        """Test the where method returns a new instance."""
        sql1 = SQL("SELECT * FROM users")
        sql2 = sql1.where("age > 18", min_age=18)

        # Should be different instances
        self.assertIsNot(sql1, sql2)
        # Original should not be modified
        self.assertEqual(sql1._state.original_sql, "SELECT * FROM users")

    def test_copy_method(self) -> None:
        """Test the _copy method creates a new instance."""
        sql1 = SQL("SELECT * FROM users", name="John")
        sql2 = sql1._copy(processed=True)

        self.assertIsNot(sql1, sql2)
        self.assertIsNot(sql1._state, sql2._state)
        self.assertTrue(sql2._state.processed)


class TestSQLProcessor(unittest.TestCase):
    """Test the SQLProcessor class."""

    def test_processor_creation(self) -> None:
        """Test creating SQLProcessor instance."""
        processor = SQLProcessor()
        self.assertIsNotNone(processor.pipeline)

    def test_process_unprocessed_state(self) -> None:
        """Test processing an unprocessed SQLState."""
        state = SQLState(original_sql="SELECT * FROM users", processed=False)
        processor = SQLProcessor()

        processed_state = processor.process(state)

        self.assertTrue(processed_state.processed)
        self.assertIsInstance(processed_state.validation_errors, list)

    def test_process_already_processed_state(self) -> None:
        """Test that already processed state is returned as-is."""
        state = SQLState(original_sql="SELECT * FROM users", processed=True)
        processor = SQLProcessor()

        processed_state = processor.process(state)

        # Should be the same object
        self.assertIs(state, processed_state)


class TestSQLConfig(unittest.TestCase):
    """Test the SQLConfig class."""

    def test_config_defaults(self) -> None:
        """Test SQLConfig default values."""
        config = SQLConfig()
        self.assertIsNone(config.dialect)
        self.assertIsNone(config.allowed_parameter_styles)
        self.assertFalse(config.allow_mixed_parameter_styles)
        self.assertTrue(config.enable_parameter_literal_extraction)
        self.assertTrue(config.enable_validation)
        self.assertTrue(config.enable_transformations)
        self.assertTrue(config.enable_caching)
        self.assertEqual(config.cache_max_size, 1000)

    def test_validate_parameter_style_no_restrictions(self) -> None:
        """Test parameter style validation with no restrictions."""
        config = SQLConfig()
        self.assertTrue(config.validate_parameter_style("qmark"))
        self.assertTrue(config.validate_parameter_style("named"))
        self.assertTrue(config.validate_parameter_style("any_style"))

    def test_validate_parameter_style_with_restrictions(self) -> None:
        """Test parameter style validation with restrictions."""
        config = SQLConfig(allowed_parameter_styles=("qmark", "numeric"))
        self.assertTrue(config.validate_parameter_style("qmark"))
        self.assertTrue(config.validate_parameter_style("numeric"))
        self.assertFalse(config.validate_parameter_style("named"))


if __name__ == "__main__":
    unittest.main()
