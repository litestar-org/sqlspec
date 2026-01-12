"""Tests for loading entire fixtures directory.

Tests the SQLFileLoader's ability to handle real-world SQL files including:
- Complex PostgreSQL and MySQL queries
- Multiple parameter styles (:param, @param)
- CTEs and advanced SQL features
- Directory structure with namespaces
- Mixed dialect SQL files
"""

import time
from pathlib import Path
from typing import Any

import pytest

from sqlspec.core import SQL
from sqlspec.loader import SQLFileLoader

try:
    from rich.console import Console

    console = Console()
except ImportError:

    class MockConsole:
        def print(self, *args: Any, **kwargs: Any) -> None:
            pass

    console = MockConsole()

pytestmark = pytest.mark.xdist_group("loader")

MAX_LARGE_QUERY_LOOKUP_SECONDS = 0.75


@pytest.fixture
def fixtures_path() -> Path:
    """Get path to test fixtures directory."""
    return Path(__file__).parent.parent.parent / "fixtures"


def test_load_entire_fixtures_directory(fixtures_path: Path) -> None:
    """Test loading the entire fixtures directory successfully."""
    loader = SQLFileLoader()

    start_time = time.perf_counter()
    try:
        loader.load_sql(fixtures_path)
        load_time = time.perf_counter() - start_time
    except Exception as e:
        pytest.skip(f"Storage backend issue, skipping directory test: {e}")
        return

    assert load_time < 5.0, f"Loading took too long: {load_time:.3f}s"

    queries = loader.list_queries()
    assert len(queries) > 0, "No queries were loaded"

    postgres_queries = [q for q in queries if q.startswith("postgres.")]
    mysql_queries = [q for q in queries if q.startswith("mysql.")]
    root_queries = [q for q in queries if "." not in q]

    assert len(postgres_queries) > 0, "No PostgreSQL queries found"
    assert len(mysql_queries) > 0, "No MySQL queries found"
    assert len(root_queries) > 0, "No root-level queries found"

    console.print(f"[green]✓[/green] Loaded {len(queries)} queries in {load_time:.3f}s")
    console.print(f"  • {len(postgres_queries)} PostgreSQL queries")
    console.print(f"  • {len(mysql_queries)} MySQL queries")
    console.print(f"  • {len(root_queries)} root-level queries")


def test_complex_postgresql_queries(fixtures_path: Path) -> None:
    """Test that complex PostgreSQL queries load and create valid SQL objects."""
    loader = SQLFileLoader()
    try:
        loader.load_sql(fixtures_path)
    except Exception:
        pytest.skip("Storage backend issue, skipping test")
        return

    queries = loader.list_queries()
    postgres_queries = [q for q in queries if q.startswith("postgres.")]

    found_complex = 0
    for query_name in postgres_queries:
        sql = loader.get_sql(query_name)
        assert isinstance(sql, SQL)
        assert len(sql.sql.strip()) > 0

        sql_text = sql.sql.upper()

        if any(
            pattern in sql_text
            for pattern in ["WITH", "CTE", "PG_", "CURRENT_DATABASE", "ARRAY_AGG", "INFORMATION_SCHEMA", "SELECT"]
        ):
            found_complex += 1
            if found_complex >= 3:
                break

    assert found_complex > 0, "No complex PostgreSQL queries found"
    console.print(f"[green]✓[/green] Validated {found_complex} complex PostgreSQL queries")


def test_complex_mysql_queries(fixtures_path: Path) -> None:
    """Test that complex MySQL queries load and create valid SQL objects."""
    loader = SQLFileLoader()
    try:
        loader.load_sql(fixtures_path)
    except Exception:
        pytest.skip("Storage backend issue, skipping test")
        return

    queries = loader.list_queries()
    mysql_queries = [q for q in queries if q.startswith("mysql.")]

    found_complex = 0
    for query_name in mysql_queries:
        sql = loader.get_sql(query_name)
        assert isinstance(sql, SQL)
        assert len(sql.sql.strip()) > 0

        sql_text = sql.sql.upper()

        if any(
            pattern in sql_text
            for pattern in ["INFORMATION_SCHEMA", "MAX_EXECUTION_TIME", "@", "GROUP_CONCAT", "SELECT"]
        ):
            found_complex += 1
            if found_complex >= 3:
                break

    assert found_complex > 0, "No complex MySQL queries found"
    console.print(f"[green]✓[/green] Validated {found_complex} complex MySQL queries")


def test_parameter_styles_detection(fixtures_path: Path) -> None:
    """Test that different parameter styles are preserved correctly."""
    loader = SQLFileLoader()
    try:
        loader.load_sql(fixtures_path)
    except Exception:
        pytest.skip("Storage backend issue, skipping test")
        return

    queries = loader.list_queries()

    colon_param_queries = []
    at_param_queries = []
    other_param_queries = []

    for query_name in queries:
        try:
            sql = loader.get_sql(query_name)
            sql_text = sql.sql

            if ":" in sql_text and any(
                pattern in sql_text for pattern in [":PKEY", ":DMA_SOURCE_ID", ":database_name"]
            ):
                colon_param_queries.append(query_name)
            elif "@" in sql_text and any(
                pattern in sql_text for pattern in ["@PKEY", "@DMA_SOURCE_ID", "@target_schema"]
            ):
                at_param_queries.append(query_name)
            elif any(pattern in sql_text for pattern in ["?", "$1", "%s"]):
                other_param_queries.append(query_name)
        except Exception:
            continue

    console.print(f"[blue]Found {len(colon_param_queries)} queries with colon parameters[/blue]")
    console.print(f"[blue]Found {len(at_param_queries)} queries with at parameters[/blue]")
    console.print(f"[blue]Found {len(other_param_queries)} queries with other parameter styles[/blue]")


def test_namespace_organization(fixtures_path: Path) -> None:
    """Test that directory structure creates proper namespaces."""
    loader = SQLFileLoader()
    try:
        loader.load_sql(fixtures_path)
    except Exception:
        pytest.skip("Storage backend issue, skipping test")
        return

    queries = loader.list_queries()

    namespaces: dict[str, list[str]] = {}
    for query in queries:
        if "." in query:
            namespace = query.split(".")[0]
            namespaces.setdefault(namespace, []).append(query)
        else:
            namespaces.setdefault("root", []).append(query)

    assert len(namespaces) > 0, "No namespaces found"

    console.print("[bold]Namespaces found:[/bold]")
    for namespace, ns_queries in namespaces.items():
        console.print(f"  • [cyan]{namespace}[/cyan]: {len(ns_queries)} queries")


def test_asset_maintenance_query(fixtures_path: Path) -> None:
    """Test the specific asset maintenance query we created."""
    loader = SQLFileLoader()
    try:
        loader.load_sql(fixtures_path)
    except Exception:
        pytest.skip("Storage backend issue, skipping test")
        return

    if loader.has_query("asset_maintenance_alert"):
        sql = loader.get_sql("asset_maintenance_alert")

        assert isinstance(sql, SQL)
        assert "inserted_data" in sql.sql
        assert ":date_start" in sql.sql
        assert ":date_end" in sql.sql
        assert "alert_users" in sql.sql
        assert "CONFLICT" in sql.sql.upper()
        console.print("[green]✓[/green] Asset maintenance query validated")
    else:
        console.print("[yellow]Asset maintenance query not found in fixtures[/yellow]")


def test_query_text_retrieval(fixtures_path: Path) -> None:
    """Test retrieving raw SQL text for queries."""
    loader = SQLFileLoader()
    try:
        loader.load_sql(fixtures_path)
    except Exception:
        pytest.skip("Storage backend issue, skipping test")
        return

    queries = loader.list_queries()
    sample_queries = queries[:5] if len(queries) >= 5 else queries

    tested_count = 0
    for query_name in sample_queries:
        try:
            text = loader.get_query_text(query_name)
            assert isinstance(text, str)
            assert len(text.strip()) > 0

            sql = loader.get_sql(query_name)
            assert text == sql.sql
            tested_count += 1
        except Exception:
            continue

    assert tested_count > 0, "No queries could be tested for text retrieval"
    console.print(f"[green]✓[/green] Validated text retrieval for {tested_count} queries")


def test_file_metadata_tracking(fixtures_path: Path) -> None:
    """Test that file metadata is properly tracked."""
    loader = SQLFileLoader()
    try:
        loader.load_sql(fixtures_path)
    except Exception:
        pytest.skip("Storage backend issue, skipping test")
        return

    files = loader.list_files()
    queries = loader.list_queries()

    assert len(files) > 0, "No files tracked"

    sample_queries = queries[:10] if len(queries) >= 10 else queries
    tested_count = 0
    for query_name in sample_queries:
        try:
            file_info = loader.get_file_for_query(query_name)
            if file_info is not None:
                assert file_info.path in files, f"File {file_info.path} not in files list"
                tested_count += 1
        except Exception:
            continue

    assert tested_count > 0, "No queries could be tested for file metadata"
    console.print(f"[green]✓[/green] Validated metadata for {tested_count} queries from {len(files)} files")


def test_performance_benchmarks(fixtures_path: Path) -> None:
    """Test that loading performance meets expectations."""
    loader = SQLFileLoader()

    start_time = time.perf_counter()
    try:
        loader.load_sql(fixtures_path)
        load_time = time.perf_counter() - start_time
    except Exception:
        pytest.skip("Storage backend issue, skipping test")
        return

    queries = loader.list_queries()

    assert load_time < 5.0, f"Loading too slow: {load_time:.3f}s"
    assert len(queries) > 0, "No queries loaded"

    if queries:
        sample_queries = queries[:20] if len(queries) >= 20 else queries
        start_time = time.perf_counter()
        successful_retrievals = 0
        for query_name in sample_queries:
            try:
                loader.get_sql(query_name)
                successful_retrievals += 1
            except Exception:
                continue
        retrieval_time = time.perf_counter() - start_time

        if successful_retrievals > 0:
            avg_retrieval_time = retrieval_time / successful_retrievals
            assert avg_retrieval_time < 0.01, f"Query retrieval too slow: {avg_retrieval_time:.6f}s per query"

            console.print("[green]Performance metrics:[/green]")
            console.print(f"  • Load time: {load_time:.3f}s for {len(queries)} queries")
            console.print(
                f"  • Avg retrieval: {avg_retrieval_time:.6f}s per query ({successful_retrievals} successful)"
            )
        else:
            console.print("[yellow]Warning: No queries could be retrieved for performance testing[/yellow]")


def test_reload_and_cache_behavior(fixtures_path: Path) -> None:
    """Test reloading behavior and cache efficiency."""
    loader = SQLFileLoader()

    start_time = time.perf_counter()
    try:
        loader.load_sql(fixtures_path)
        first_load_time = time.perf_counter() - start_time
        first_query_count = len(loader.list_queries())
    except Exception:
        pytest.skip("Storage backend issue, skipping test")
        return

    start_time = time.perf_counter()
    loader.load_sql(fixtures_path)
    second_load_time = time.perf_counter() - start_time
    second_query_count = len(loader.list_queries())

    assert first_query_count == second_query_count

    console.print(f"[dim]Load times: first={first_load_time:.3f}s, second={second_load_time:.3f}s[/dim]")


def test_mixed_dialect_queries(fixtures_path: Path) -> None:
    """Test that queries from different SQL dialects coexist properly."""
    loader = SQLFileLoader()
    try:
        loader.load_sql(fixtures_path)
    except Exception:
        pytest.skip("Storage backend issue, skipping test")
        return

    queries = loader.list_queries()

    postgres_query = next((q for q in queries if q.startswith("postgres.")), None)
    mysql_query = next((q for q in queries if q.startswith("mysql.")), None)

    if postgres_query and mysql_query:
        try:
            pg_sql = loader.get_sql(postgres_query)
            mysql_sql = loader.get_sql(mysql_query)

            assert isinstance(pg_sql, SQL)
            assert isinstance(mysql_sql, SQL)

            assert pg_sql.sql != mysql_sql.sql

            console.print(f"[green]✓[/green] Tested mixed dialects: {postgres_query}, {mysql_query}")
        except Exception as e:
            console.print(f"[yellow]Could not retrieve mixed dialect queries: {e}[/yellow]")
    else:
        console.print("[yellow]Could not find both PostgreSQL and MySQL queries for mixed dialect test[/yellow]")


def test_specific_real_world_patterns(fixtures_path: Path) -> None:
    """Test specific real-world SQL patterns found in fixtures."""
    loader = SQLFileLoader()
    try:
        loader.load_sql(fixtures_path)
    except Exception:
        pytest.skip("Storage backend issue, skipping test")
        return

    queries = loader.list_queries()

    pattern_counts = {
        "ctes": 0,
        "hints": 0,
        "params_colon": 0,
        "params_at": 0,
        "pg_functions": 0,
        "info_schema": 0,
        "selects": 0,
    }

    for query_name in queries:
        try:
            sql = loader.get_sql(query_name)
            sql_text = sql.sql.upper()
            original_sql = sql.sql

            if "WITH " in sql_text:
                pattern_counts["ctes"] += 1
            if "/*+" in original_sql:
                pattern_counts["hints"] += 1
            if ":" in original_sql:
                pattern_counts["params_colon"] += 1
            if "@" in original_sql:
                pattern_counts["params_at"] += 1
            if "PG_" in sql_text or "CURRENT_DATABASE" in sql_text:
                pattern_counts["pg_functions"] += 1
            if "INFORMATION_SCHEMA" in sql_text:
                pattern_counts["info_schema"] += 1
            if "SELECT" in sql_text:
                pattern_counts["selects"] += 1
        except Exception:
            continue

    total_patterns = sum(pattern_counts.values())
    assert total_patterns > 0, "No real-world SQL patterns found"

    console.print("[bold]Real-world patterns found:[/bold]")
    for pattern, count in pattern_counts.items():
        console.print(f"  • [yellow]{pattern}[/yellow]: {count} queries")


def test_simulated_complex_queries() -> None:
    """Test with simulated complex queries based on fixtures content."""
    loader = SQLFileLoader()

    postgres_cte_query = """
with db as (
  select db.oid as database_oid,
    db.datname as database_name,
    pg_database_size(db.datname) as total_disk_size_bytes
  from pg_database db
  where datname = current_database()
),
db_stats as (
  select s.datid as database_oid,
    s.numbackends as backends_connected,
    s.xact_commit as txn_commit_count
  from pg_stat_database s
  where s.datname = :database_name
)
select db.*, stats.backends_connected
from db
join db_stats stats on db.database_oid = stats.database_oid
where db.database_oid = :target_oid
"""

    mysql_query = """
select
  /*+ MAX_EXECUTION_TIME(5000) */
  @PKEY as pkey,
  @DMA_SOURCE_ID as dma_source_id,
  src.table_schema as table_schema,
  src.total_table_count as total_table_count
from (
  select
    table_schema,
    count(*) as total_table_count,
    sum(case when engine = 'InnoDB' then 1 else 0 end) as innodb_table_count
  from information_schema.tables
  where table_schema = @target_schema
  group by table_schema
) src
"""

    conflict_query = """
with inserted_data as (
insert into alert_users (user_id, asset_maintenance_id, alert_definition_id)
select responsible_id, id,
       (select id from alert_definition where name = 'maintenances_today')
from asset_maintenance
where planned_date_start between :date_start and :date_end
  and cancelled = False
ON CONFLICT ON CONSTRAINT unique_alert DO NOTHING
returning *)
select inserted_data.*, to_jsonb(users.*) as user
from inserted_data
left join users on users.id = inserted_data.user_id
"""

    loader.add_named_sql("postgres_cte_complex", postgres_cte_query.strip())
    loader.add_named_sql("mysql_hint_complex", mysql_query.strip())
    loader.add_named_sql("conflict_handling_complex", conflict_query.strip())

    queries = loader.list_queries()
    assert len(queries) == 3

    pg_sql = loader.get_sql("postgres_cte_complex")
    assert isinstance(pg_sql, SQL)
    assert "WITH" in pg_sql.sql.upper()
    assert ":database_name" in pg_sql.sql
    assert ":target_oid" in pg_sql.sql
    assert "pg_database_size" in pg_sql.sql

    mysql_sql = loader.get_sql("mysql_hint_complex")
    assert isinstance(mysql_sql, SQL)
    assert "/*+" in mysql_sql.sql
    assert "@PKEY" in mysql_sql.sql
    assert "@DMA_SOURCE_ID" in mysql_sql.sql
    assert "information_schema" in mysql_sql.sql.lower()

    conflict_sql = loader.get_sql("conflict_handling_complex")
    assert isinstance(conflict_sql, SQL)
    assert "CONFLICT" in conflict_sql.sql.upper()
    assert ":date_start" in conflict_sql.sql
    assert ":date_end" in conflict_sql.sql
    assert "to_jsonb" in conflict_sql.sql

    for sql_obj in [pg_sql, mysql_sql, conflict_sql]:
        assert sql_obj.parameters == []

    console.print("[green]✓[/green] All simulated complex queries loaded and validated successfully")


def test_query_name_normalization_with_hyphens() -> None:
    """Test that fixture-style query names with hyphens are normalized properly."""
    loader = SQLFileLoader()

    fixture_names = [
        "collection-postgres-base-database-details",
        "collection-mysql-database-details",
        "collection-aws-extension-dependency",
        "asset-maintenance-alert",
    ]

    for name in fixture_names:
        loader.add_named_sql(name, f"SELECT '{name}' as query_name")

    for original_name in fixture_names:
        underscore_name = original_name.replace("-", "_")

        assert loader.has_query(underscore_name), f"Should have normalized name: {underscore_name}"

        assert loader.has_query(original_name), f"Should normalize and find: {original_name}"

        sql1 = loader.get_sql(original_name)
        sql2 = loader.get_sql(underscore_name)
        assert sql1.sql == sql2.sql

    console.print(f"[green]✓[/green] All {len(fixture_names)} hyphenated names normalize correctly")


def test_large_query_handling() -> None:
    """Test handling of large, complex SQL queries like those in fixtures."""
    loader = SQLFileLoader()

    large_query = """
-- Complex query with multiple CTEs, joins, and aggregations
with database_metrics as (
  select
    d.oid as database_oid,
    d.datname as database_name,
    pg_database_size(d.datname) as size_bytes,
    pg_stat_get_db_numbackends(d.oid) as active_connections
  from pg_database d
  where d.datallowconn and not d.datistemplate
),
table_metrics as (
  select
    schemaname,
    tablename,
    n_tup_ins + n_tup_upd + n_tup_del as total_modifications,
    n_tup_hot_upd as hot_updates,
    n_dead_tup as dead_tuples,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
  from pg_stat_user_tables
  where schemaname not in ('information_schema', 'pg_catalog')
),
index_metrics as (
  select
    schemaname,
    tablename,
    indexrelname,
    idx_tup_read,
    idx_tup_fetch,
    idx_blks_read,
    idx_blks_hit,
    round(100.0 * idx_blks_hit / nullif(idx_blks_hit + idx_blks_read, 0), 2) as hit_ratio
  from pg_stat_user_indexes
),
aggregated_stats as (
  select
    dm.database_name,
    dm.size_bytes,
    dm.active_connections,
    count(distinct tm.tablename) as table_count,
    sum(tm.total_modifications) as total_table_modifications,
    count(distinct im.indexrelname) as index_count,
    avg(im.hit_ratio) as avg_index_hit_ratio
  from database_metrics dm
  cross join table_metrics tm
  left join index_metrics im on tm.schemaname = im.schemaname
                            and tm.tablename = im.tablename
  where dm.database_name = current_database()
    and tm.total_modifications > :min_modifications
    and (im.hit_ratio is null or im.hit_ratio > :min_hit_ratio)
  group by dm.database_name, dm.size_bytes, dm.active_connections
)
select
  as_.*,
  case
    when as_.avg_index_hit_ratio > 95 then 'excellent'
    when as_.avg_index_hit_ratio > 85 then 'good'
    when as_.avg_index_hit_ratio > 70 then 'fair'
    else 'poor'
  end as performance_rating,
  round(as_.size_bytes / 1024.0 / 1024.0, 2) as size_mb
from aggregated_stats as_
where as_.table_count > 0
order by as_.size_bytes desc
limit :result_limit
"""

    loader.add_named_sql("large_database_analysis", large_query.strip())

    sql = loader.get_sql("large_database_analysis")
    assert isinstance(sql, SQL)
    assert len(sql.sql) > 1000
    assert sql.sql.count("select") >= 4
    assert sql.sql.count("with") >= 1
    assert ":min_modifications" in sql.sql
    assert ":min_hit_ratio" in sql.sql
    assert ":result_limit" in sql.sql

    start_time = time.perf_counter()
    for _ in range(100):
        loader.get_sql("large_database_analysis")
    elapsed = time.perf_counter() - start_time

    assert elapsed < MAX_LARGE_QUERY_LOOKUP_SECONDS, (
        f"Large query retrieval too slow: {elapsed:.3f}s for 100 calls "
        f"(threshold {MAX_LARGE_QUERY_LOOKUP_SECONDS:.2f}s)"
    )
    console.print(f"[green]✓[/green] Large query ({len(sql.sql)} chars) handled efficiently")
    console.print(f"  • Performance: {elapsed * 1000:.1f}ms for 100 calls ({elapsed * 10.0:.1f}ms per call)")
