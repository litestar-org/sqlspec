#!/usr/bin/env python3
"""Demonstration of the new unified storage architecture.

This example shows how the new SyncStorageMixin and AsyncStorageMixin provide
a clean, consistent API that intelligently routes between native database
capabilities and storage backends for optimal performance.
"""

import tempfile
from pathlib import Path

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.base import SQLSpec
from sqlspec.statement.sql import SQL

__all__ = ("demo_unified_storage_architecture",)


def demo_unified_storage_architecture() -> None:
    """Demonstrate the unified storage architecture."""

    print("🚀 SQLSpec Unified Storage Architecture Demo")
    print("=" * 50)

    # Create SQLSpec with unified storage (no config needed - uses intelligent backend selection)
    sqlspec = SQLSpec()
    duck = sqlspec.add_config(DuckDBConfig(database=":memory:"))

    with sqlspec.provide_session(duck) as session:
        print("\n📊 Creating sample data...")

        # Create sample data
        session.execute(
            SQL("""
            CREATE TABLE sales AS
            SELECT
                range AS id,
                'Product_' || (range % 10) AS product_name,
                (random() * 1000)::int AS amount,
                DATE '2024-01-01' + (range % 365) AS sale_date
            FROM range(1000)
        """)
        )

        print("✅ Created 1000 sales records")

        # ================================================================
        # Demonstration 1: Native Database Capabilities (Fastest)
        # ================================================================

        print("\n🎯 Demo 1: Native Database Operations (DuckDB)")
        print("-" * 45)

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            _parquet_file = tmp_path / "sales_native.parquet"

        # ================================================================
        # Demonstration 2: Storage Backend Operations
        # ================================================================

        print("\n🗄️  Demo 2: Storage Backend Integration")
        print("-" * 40)

        try:
            # Export using storage backend (automatic format detection)
            rows_exported = session.export_to_storage(
                "SELECT product_name, SUM(amount) as total_sales FROM sales GROUP BY product_name",
                destination_uri="analytics/product_summary.csv",
            )
            print(f"✅ Exported {rows_exported} rows to storage backend")

            # Import from storage backend
            session.execute(SQL("DROP TABLE IF EXISTS product_summary"))
            rows_imported = session.import_from_storage(
                source_uri="analytics/product_summary.csv", table_name="product_summary"
            )
            print(f"✅ Imported {rows_imported} rows from storage backend")

            # Verify the data
            verification = session.execute(SQL("SELECT COUNT(*) FROM product_summary"))
            print(f"✅ Verification: {verification.scalar()} rows in imported table")

        except Exception as e:
            print(f"⚠️  Storage backend operations failed: {e}")

        # ================================================================
        # Demonstration 3: Arrow Integration
        # ================================================================

        print("\n🏹 Demo 3: Arrow Integration")
        print("-" * 30)

        try:
            # Fetch data as Arrow table (zero-copy with DuckDB)
            arrow_table = session.fetch_arrow_table("SELECT * FROM sales ORDER BY amount DESC LIMIT 100")
            print(f"✅ Fetched Arrow table: {arrow_table.num_rows} rows, {arrow_table.num_columns} columns")
            print(f"   Schema: {arrow_table.schema}")

            # Ingest Arrow table back to database
            session.execute(SQL("DROP TABLE IF EXISTS top_sales"))
            rows_ingested = session.ingest_arrow_table(table=arrow_table.data, table_name="top_sales", mode="create")
            print(f"✅ Ingested {rows_ingested} rows from Arrow table")

        except Exception as e:
            print(f"⚠️  Arrow operations failed (may need pyarrow): {e}")

        # ================================================================
        # Demonstration 4: Intelligent Routing
        # ================================================================

        print("\n🧠 Demo 4: Intelligent Routing")
        print("-" * 35)

        print("The unified architecture automatically chooses:")
        print("  • Native DB operations when supported (fastest)")
        print("  • Arrow operations for efficient data transfer")
        print("  • Storage backends as fallback")
        print("  • Format auto-detection from file extensions")

        # Show what capabilities are detected
        print(f"\nDetected capabilities for {session.__class__.__name__}:")
        print(f"  • Native Parquet: {session._has_native_capability('parquet', 's3://bucket/file.parquet', 'parquet')}")
        print(f"  • Native CSV: {session._has_native_capability('export', 'file:///tmp/data.csv', 'csv')}")
        print(f"  • Native import: {session._has_native_capability('import', 'gs://bucket/data.json', 'json')}")

        # Show format detection
        test_uris = ["s3://bucket/data.parquet", "gs://bucket/export.csv", "file:///tmp/results.json", "data.unknown"]

        print("\nFormat detection:")
        for uri in test_uris:
            detected = session._detect_format(uri)
            print(f"  • {uri} → {detected}")

        print("\n🎉 Demo completed successfully!")
        print("\nKey benefits of unified architecture:")
        print("  ✅ Single mixin instead of 4+ complex mixins")
        print("  ✅ Intelligent routing for optimal performance")
        print("  ✅ Consistent API across all database drivers")
        print("  ✅ Native database capabilities when available")
        print("  ✅ Automatic fallback to storage backends")
        print("  ✅ Format auto-detection")
        print("  ✅ Type-safe Arrow integration")


if __name__ == "__main__":
    demo_unified_storage_architecture()
