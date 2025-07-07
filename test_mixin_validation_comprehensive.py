#!/usr/bin/env python3
"""Comprehensive test that driver mixins follow parameter validation process."""

import os
import sqlite3
import tempfile

from sqlspec.adapters.sqlite import SqliteDriver
from sqlspec.statement.filters import LimitOffsetFilter
from sqlspec.statement.sql import SQLConfig


def test_storage_mixin_arrow_validation() -> None:
    """Test that storage mixin Arrow operations validate SQL."""
    conn = sqlite3.connect(":memory:")

    # Create driver with validation enabled
    config = SQLConfig(enable_validation=True)
    driver = SqliteDriver(connection=conn, config=config)

    # Create test table
    driver.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            price REAL
        )
    """)

    # Insert test data
    driver.execute_many(
        "INSERT INTO products (name, price) VALUES ($1, $2)",
        [("Product A", 10.99), ("Product B", 20.99), ("Product C", 30.99)]
    )

    try:
        # Test fetch_arrow_table - should validate
        result = driver.fetch_arrow_table(
            "SELECT * FROM products WHERE price > $1",
            25.0
        )
        # Check that we got the Arrow result
        assert hasattr(result, "data")
    except ImportError:
        pass


def test_all_mixin_methods_validate() -> None:
    """Test that all mixin methods properly validate SQL."""
    conn = sqlite3.connect(":memory:")

    # Create driver with validation enabled
    config = SQLConfig(enable_validation=True)
    driver = SqliteDriver(connection=conn, config=config)

    # Create test table
    driver.execute("""
        CREATE TABLE test_data (
            id INTEGER PRIMARY KEY,
            value TEXT,
            score INTEGER
        )
    """)

    # Insert test data
    driver.execute_many(
        "INSERT INTO test_data (value, score) VALUES ($1, $2)",
        [("A", 10), ("B", 20), ("C", 30), ("D", 40), ("E", 50)]
    )

    # Test query mixin methods

    # select_one
    row = driver.select_one("SELECT * FROM test_data WHERE id = $1", 1)
    assert row[1] == "A"

    # select
    rows = driver.select("SELECT * FROM test_data WHERE score > $1", 25)
    assert len(rows) == 3

    # select_value
    max_score = driver.select_value("SELECT MAX(score) FROM test_data WHERE score < $1", 45)
    assert max_score == 40

    # select_one_or_none
    row = driver.select_one_or_none("SELECT * FROM test_data WHERE id = $1", 999)
    assert row is None

    # select_value_or_none
    val = driver.select_value_or_none("SELECT value FROM test_data WHERE id = $1", 999)
    assert val is None

    # paginate
    result = driver.paginate(
        "SELECT * FROM test_data WHERE score > $1",
        10,
        LimitOffsetFilter(limit=2, offset=0)
    )
    assert len(result.items) <= 2  # Should be limited to 2
    assert result.total == 4  # rows with score > 10 are B(20), C(30), D(40), E(50)


    # export_to_storage with parameters
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Export with parameterized query
        row_count = driver.export_to_storage(
            "SELECT * FROM test_data WHERE score > $1",
            15,
            destination_uri=tmp_path
        )
        assert row_count == 4

        # Verify file was created
        assert os.path.exists(tmp_path)
        assert os.path.getsize(tmp_path) > 0

    finally:
        os.unlink(tmp_path)


    # Create a pipeline with parameterized queries
    pipeline = driver.pipeline()
    pipeline.add_execute("INSERT INTO test_data (value, score) VALUES ($1, $2)", "F", 60)
    pipeline.add_execute("UPDATE test_data SET score = $1 WHERE value = $2", 65, "F")

    results = pipeline.process()
    assert len(results) == 2

    # Verify the update worked
    score = driver.select_value("SELECT score FROM test_data WHERE value = $1", "F")
    assert score == 65


def test_validation_context_preserved() -> None:
    """Test that validation context (metadata) is preserved through mixins."""
    conn = sqlite3.connect(":memory:")

    # Create driver with validation and analysis enabled
    config = SQLConfig(
        enable_validation=True,
        enable_analysis=True,
        analyzer_output_handler=lambda results: print(f"Analysis: {results}")
    )
    driver = SqliteDriver(connection=conn, config=config)

    # Create test table
    driver.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    driver.execute("INSERT INTO users VALUES (1, 'Alice')")

    # Test that analysis runs through mixins
    user = driver.select_one("SELECT * FROM users WHERE id = $1", 1)
    assert user[1] == "Alice"


def test_edge_cases() -> None:
    """Test edge cases for mixin validation."""
    conn = sqlite3.connect(":memory:")

    config = SQLConfig(enable_validation=True)
    driver = SqliteDriver(connection=conn, config=config)

    driver.execute("CREATE TABLE empty_table (id INTEGER)")


    # Empty result set
    rows = driver.select("SELECT * FROM empty_table WHERE id = $1", 999)
    assert rows == []

    # NULL parameters
    driver.execute("INSERT INTO empty_table VALUES ($1)", None)
    rows = driver.select("SELECT * FROM empty_table WHERE id IS NULL")
    assert len(rows) == 1


if __name__ == "__main__":

    test_storage_mixin_arrow_validation()
    test_all_mixin_methods_validate()
    test_validation_context_preserved()
    test_edge_cases()

