"""Tests for parameter handling in statement_new."""

import unittest

from sqlspec.statement_new.parameters import ParameterHandler, ParameterInfo, ParameterStyle, TypedParameter


class TestParameterHandler(unittest.TestCase):
    """Test the ParameterHandler class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.handler = ParameterHandler()

    def test_extract_parameters_named_colon(self) -> None:
        """Test extraction of named colon parameters."""
        sql = "SELECT * FROM users WHERE name = :name AND age > :age"
        params = self.handler.extract_parameters(sql)

        self.assertEqual(len(params), 2)
        self.assertEqual(params[0].style, ParameterStyle.NAMED_COLON)
        self.assertEqual(params[0].name, "name")
        self.assertEqual(params[1].style, ParameterStyle.NAMED_COLON)
        self.assertEqual(params[1].name, "age")

    def test_extract_parameters_qmark(self) -> None:
        """Test extraction of question mark parameters."""
        sql = "SELECT * FROM users WHERE name = ? AND age > ?"
        params = self.handler.extract_parameters(sql)

        self.assertEqual(len(params), 2)
        self.assertEqual(params[0].style, ParameterStyle.QMARK)
        self.assertEqual(params[1].style, ParameterStyle.QMARK)

    def test_extract_parameters_numeric(self) -> None:
        """Test extraction of numeric parameters."""
        sql = "SELECT * FROM users WHERE name = $1 AND age > $2"
        params = self.handler.extract_parameters(sql)

        self.assertEqual(len(params), 2)
        self.assertEqual(params[0].style, ParameterStyle.NUMERIC)
        self.assertEqual(params[0].name, "1")
        self.assertEqual(params[1].style, ParameterStyle.NUMERIC)
        self.assertEqual(params[1].name, "2")

    def test_extract_parameters_pyformat(self) -> None:
        """Test extraction of pyformat parameters."""
        sql = "SELECT * FROM users WHERE name = %(name)s AND age > %(age)s"
        params = self.handler.extract_parameters(sql)

        self.assertEqual(len(params), 2)
        self.assertEqual(params[0].style, ParameterStyle.NAMED_PYFORMAT)
        self.assertEqual(params[0].name, "name")
        self.assertEqual(params[1].style, ParameterStyle.NAMED_PYFORMAT)
        self.assertEqual(params[1].name, "age")

    def test_transform_sql_for_parsing(self) -> None:
        """Test transformation of SQL for SQLGlot parsing."""
        sql = "SELECT * FROM users WHERE name = %(name)s"
        params = self.handler.extract_parameters(sql)

        transformed_sql, transformation = self.handler.transform_sql_for_parsing(sql, params)

        self.assertTrue(transformation.was_transformed)
        self.assertIn(":param_0", transformed_sql)
        self.assertEqual(transformation.transformed_style, ParameterStyle.NAMED_COLON)

    def test_convert_parameters_direct_with_sqlglot(self) -> None:
        """Test parameter conversion with SQLGlot path."""
        sql = "SELECT * FROM users WHERE name = :name"

        converted = self.handler.convert_parameters_direct(
            sql, ParameterStyle.NAMED_COLON, ParameterStyle.QMARK, use_sqlglot=True
        )

        self.assertIn("?", converted.transformed_sql)
        self.assertNotIn(":name", converted.transformed_sql)

    def test_convert_parameters_direct_without_sqlglot(self) -> None:
        """Test parameter conversion without SQLGlot (regex path)."""
        sql = "SELECT * FROM users WHERE name = :name"

        converted = self.handler.convert_parameters_direct(
            sql, ParameterStyle.NAMED_COLON, ParameterStyle.QMARK, use_sqlglot=False
        )

        self.assertIn("?", converted.transformed_sql)
        self.assertNotIn(":name", converted.transformed_sql)

    def test_parameter_cache(self) -> None:
        """Test that parameter extraction is cached."""
        sql = "SELECT * FROM users WHERE id = ?"

        # First call
        params1 = self.handler.extract_parameters(sql)
        # Second call should use cache
        params2 = self.handler.extract_parameters(sql)

        # Should be the same list object from cache
        self.assertIs(params1, params2)


class TestParameterInfo(unittest.TestCase):
    """Test the ParameterInfo dataclass."""

    def test_parameter_info_creation(self) -> None:
        """Test creating ParameterInfo instances."""
        info = ParameterInfo(
            name="test", style=ParameterStyle.NAMED_COLON, position=10, ordinal=0, placeholder_text=":test"
        )

        self.assertEqual(info.name, "test")
        self.assertEqual(info.style, ParameterStyle.NAMED_COLON)
        self.assertEqual(info.position, 10)
        self.assertEqual(info.ordinal, 0)
        self.assertEqual(info.placeholder_text, ":test")


class TestTypedParameter(unittest.TestCase):
    """Test the TypedParameter dataclass."""

    def test_typed_parameter_creation(self) -> None:
        """Test creating TypedParameter instances."""
        from sqlglot import exp

        param = TypedParameter(
            value="test_value",
            sqlglot_type=exp.DataType.build("VARCHAR"),
            type_hint="string",
            semantic_name="user_name",
        )

        self.assertEqual(param.value, "test_value")
        self.assertEqual(param.type_hint, "string")
        self.assertEqual(param.semantic_name, "user_name")


if __name__ == "__main__":
    unittest.main()
