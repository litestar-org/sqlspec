"""Shared PostgreSQL extension behavior helpers for pgvector and ParadeDB."""

from typing import Any, cast

from sqlspec import sql
from sqlspec.builder import Column
from tests.integration.adapters.contracts._postgres_extension_cases import PostgresExtensionCase


def _lower_keys(row: dict[str, object]) -> dict[str, object]:
    return {key.lower(): value for key, value in row.items()}


def _table(case: PostgresExtensionCase, suffix: str) -> str:
    return f"pgext_{suffix}_{case.id.replace('-', '_')}"


def _sync_commit(driver: Any) -> None:
    if hasattr(driver, "commit"):
        driver.commit()


async def _async_commit(driver: Any) -> None:
    if hasattr(driver, "commit"):
        await driver.commit()


def _assert_extension_state(config: object, driver: object, case: PostgresExtensionCase) -> None:
    config_any = cast("Any", config)
    driver_any = cast("Any", driver)
    assert config_any._pgvector_available is True
    assert config_any._paradedb_available is (case.dialect == "paradedb")
    assert config_any.statement_config.dialect == case.dialect
    assert driver_any.statement_config.dialect == case.dialect


def assert_sync_postgres_extension_detection_contract(
    config: object, driver: object, case: PostgresExtensionCase
) -> None:
    """Assert a sync config detects pgvector/ParadeDB and exposes the dialect on the first session."""
    _assert_extension_state(config, driver, case)


async def assert_async_postgres_extension_detection_contract(
    config: object, driver: object, case: PostgresExtensionCase
) -> None:
    """Assert an async config detects pgvector/ParadeDB and exposes the dialect on the first session."""
    _assert_extension_state(config, driver, case)


def _sync_seed_pgvector_table(driver: Any, case: PostgresExtensionCase) -> str:
    table = _table(case, "vector")
    driver.execute_script(f"""
        DROP TABLE IF EXISTS {table} CASCADE;
        CREATE TABLE {table} (
            id SERIAL PRIMARY KEY,
            content TEXT NOT NULL,
            embedding vector(3)
        );
    """)
    for content, embedding in (("doc1", "[0.1, 0.2, 0.3]"), ("doc2", "[0.4, 0.5, 0.6]"), ("doc3", "[0.7, 0.8, 0.9]")):
        driver.execute(f"INSERT INTO {table} (content, embedding) VALUES ('{content}', '{embedding}'::vector)")
    _sync_commit(driver)
    return table


async def _async_seed_pgvector_table(driver: Any, case: PostgresExtensionCase) -> str:
    table = _table(case, "vector")
    await driver.execute_script(f"""
        DROP TABLE IF EXISTS {table} CASCADE;
        CREATE TABLE {table} (
            id SERIAL PRIMARY KEY,
            content TEXT NOT NULL,
            embedding vector(3)
        );
    """)
    for content, embedding in (("doc1", "[0.1, 0.2, 0.3]"), ("doc2", "[0.4, 0.5, 0.6]"), ("doc3", "[0.7, 0.8, 0.9]")):
        await driver.execute(f"INSERT INTO {table} (content, embedding) VALUES ('{content}', '{embedding}'::vector)")
    await _async_commit(driver)
    return table


def _assert_pgvector_order(rows: list[dict[str, Any]]) -> None:
    normalized = [_lower_keys(row) for row in rows]
    assert len(normalized) == 3
    assert normalized[0]["content"] == "doc1"
    assert normalized[0]["distance"] < 0.01


def _sync_inner_product_rows(driver: Any, case: PostgresExtensionCase, table: str) -> object:
    query = f"""
        SELECT content, embedding <#> '[0.1, 0.2, 0.3]'::vector AS neg_inner_product
        FROM {table}
        ORDER BY neg_inner_product
    """
    if case.inner_product_strategy == "psycopg_cursor":
        cursor = driver.connection.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchall()
        finally:
            cursor.close()
    return driver.execute(query).get_data()


async def _async_inner_product_rows(driver: Any, case: PostgresExtensionCase, table: str) -> object:
    query = f"""
        SELECT content, embedding <#> '[0.1, 0.2, 0.3]'::vector AS neg_inner_product
        FROM {table}
        ORDER BY neg_inner_product
    """
    if case.inner_product_strategy == "psqlpy_fetch":
        result = await driver.connection.fetch(query)
        return result.result()
    return (await driver.execute(query)).get_data()


def _sync_multiple_metric_rows(driver: Any, case: PostgresExtensionCase, table: str) -> object:
    query = f"""
        SELECT content,
               embedding <-> '[0.1, 0.2, 0.3]'::vector AS euclidean_dist,
               embedding <=> '[0.1, 0.2, 0.3]'::vector AS cosine_dist,
               embedding <#> '[0.1, 0.2, 0.3]'::vector AS neg_inner_product
        FROM {table}
    """
    if case.inner_product_strategy == "psycopg_cursor":
        cursor = driver.connection.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchall()
        finally:
            cursor.close()
    return driver.execute(query).get_data()


async def _async_multiple_metric_rows(driver: Any, case: PostgresExtensionCase, table: str) -> object:
    query = f"""
        SELECT content,
               embedding <-> '[0.1, 0.2, 0.3]'::vector AS euclidean_dist,
               embedding <=> '[0.1, 0.2, 0.3]'::vector AS cosine_dist,
               embedding <#> '[0.1, 0.2, 0.3]'::vector AS neg_inner_product
        FROM {table}
    """
    if case.inner_product_strategy == "psqlpy_fetch":
        result = await driver.connection.fetch(query)
        return result.result()
    return (await driver.execute(query)).get_data()


def assert_sync_pgvector_contract(driver: object, case: PostgresExtensionCase) -> None:
    """Assert sync pgvector distance operators and builder column-distance behavior."""
    sync_driver = cast("Any", driver)
    table = _sync_seed_pgvector_table(sync_driver, case)
    try:
        euclidean = sync_driver.execute(f"""
            SELECT content, embedding <-> '[0.1, 0.2, 0.3]'::vector AS distance
            FROM {table}
            ORDER BY distance
        """).get_data()
        _assert_pgvector_order(euclidean)

        cosine = sync_driver.execute(f"""
            SELECT content, embedding <=> '[0.1, 0.2, 0.3]'::vector AS distance
            FROM {table}
            ORDER BY distance
        """).get_data()
        assert _lower_keys(cosine[0])["content"] == "doc1"

        assert len(cast("Any", _sync_inner_product_rows(sync_driver, case, table))) == 3

        sync_driver.execute_script(f"""
            ALTER TABLE {table} ADD COLUMN IF NOT EXISTS embedding2 vector(3);
            UPDATE {table} SET embedding2 = '[0.1, 0.2, 0.3]'::vector;
        """)
        builder = (
            sql
            .select("content", Column("embedding").vector_distance(Column("embedding2")).alias("distance"))
            .from_(table)
            .order_by("distance")
        )
        builder_rows = [_lower_keys(row) for row in sync_driver.execute(builder).get_data()]
        assert builder_rows[0]["content"] == "doc1"
        assert builder_rows[0]["distance"] < 0.01

        limited = sync_driver.execute(f"""
            SELECT content FROM {table}
            ORDER BY embedding <-> '[0.1, 0.2, 0.3]'::vector
            LIMIT 2
        """).get_data()
        assert len(limited) == 2
        assert _lower_keys(limited[0])["content"] == "doc1"

        threshold = sync_driver.execute(f"""
            SELECT content FROM {table}
            WHERE embedding <-> '[0.1, 0.2, 0.3]'::vector < 0.3
        """).get_data()
        assert len(threshold) == 1
        assert _lower_keys(threshold[0])["content"] == "doc1"

        assert len(cast("Any", _sync_multiple_metric_rows(sync_driver, case, table))) == 3
    finally:
        sync_driver.execute_script(f"DROP TABLE IF EXISTS {table} CASCADE")


async def assert_async_pgvector_contract(driver: object, case: PostgresExtensionCase) -> None:
    """Async mirror of assert_sync_pgvector_contract."""
    async_driver = cast("Any", driver)
    table = await _async_seed_pgvector_table(async_driver, case)
    try:
        euclidean = (
            await async_driver.execute(f"""
            SELECT content, embedding <-> '[0.1, 0.2, 0.3]'::vector AS distance
            FROM {table}
            ORDER BY distance
        """)
        ).get_data()
        _assert_pgvector_order(euclidean)

        cosine = (
            await async_driver.execute(f"""
            SELECT content, embedding <=> '[0.1, 0.2, 0.3]'::vector AS distance
            FROM {table}
            ORDER BY distance
        """)
        ).get_data()
        assert _lower_keys(cosine[0])["content"] == "doc1"

        assert len(cast("Any", await _async_inner_product_rows(async_driver, case, table))) == 3

        await async_driver.execute_script(f"""
            ALTER TABLE {table} ADD COLUMN IF NOT EXISTS embedding2 vector(3);
            UPDATE {table} SET embedding2 = '[0.1, 0.2, 0.3]'::vector;
        """)
        builder = (
            sql
            .select("content", Column("embedding").vector_distance(Column("embedding2")).alias("distance"))
            .from_(table)
            .order_by("distance")
        )
        builder_rows = [_lower_keys(row) for row in (await async_driver.execute(builder)).get_data()]
        assert builder_rows[0]["content"] == "doc1"
        assert builder_rows[0]["distance"] < 0.01

        limited = (
            await async_driver.execute(f"""
            SELECT content FROM {table}
            ORDER BY embedding <-> '[0.1, 0.2, 0.3]'::vector
            LIMIT 2
        """)
        ).get_data()
        assert len(limited) == 2
        assert _lower_keys(limited[0])["content"] == "doc1"

        threshold = (
            await async_driver.execute(f"""
            SELECT content FROM {table}
            WHERE embedding <-> '[0.1, 0.2, 0.3]'::vector < 0.3
        """)
        ).get_data()
        assert len(threshold) == 1
        assert _lower_keys(threshold[0])["content"] == "doc1"

        assert len(cast("Any", await _async_multiple_metric_rows(async_driver, case, table))) == 3
    finally:
        await async_driver.execute_script(f"DROP TABLE IF EXISTS {table} CASCADE")


def _sync_seed_paradedb_table(driver: Any, case: PostgresExtensionCase) -> str:
    table = _table(case, "search")
    driver.execute_script(f"""
        DROP TABLE IF EXISTS {table} CASCADE;
        CREATE TABLE {table} (
            id SERIAL PRIMARY KEY,
            description TEXT NOT NULL,
            category TEXT,
            rating INTEGER
        );
    """)
    for description, category, rating in (
        ("comfortable running shoes for athletes", "footwear", 5),
        ("leather dress shoes formal", "footwear", 4),
        ("casual sneakers everyday wear", "footwear", 4),
        ("hiking boots waterproof", "footwear", 5),
        ("summer sandals beach", "footwear", 3),
        ("running shorts athletic", "apparel", 4),
        ("formal dress pants", "apparel", 3),
    ):
        driver.execute(
            f"INSERT INTO {table} (description, category, rating) VALUES ('{description}', '{category}', {rating})"
        )
    driver.execute_script(f"""
        CREATE INDEX {table}_idx ON {table}
        USING bm25 (id, description, category)
        WITH (key_field = 'id');
    """)
    _sync_commit(driver)
    return table


async def _async_seed_paradedb_table(driver: Any, case: PostgresExtensionCase) -> str:
    table = _table(case, "search")
    await driver.execute_script(f"""
        DROP TABLE IF EXISTS {table} CASCADE;
        CREATE TABLE {table} (
            id SERIAL PRIMARY KEY,
            description TEXT NOT NULL,
            category TEXT,
            rating INTEGER
        );
    """)
    for description, category, rating in (
        ("comfortable running shoes for athletes", "footwear", 5),
        ("leather dress shoes formal", "footwear", 4),
        ("casual sneakers everyday wear", "footwear", 4),
        ("hiking boots waterproof", "footwear", 5),
        ("summer sandals beach", "footwear", 3),
        ("running shorts athletic", "apparel", 4),
        ("formal dress pants", "apparel", 3),
    ):
        await driver.execute(
            f"INSERT INTO {table} (description, category, rating) VALUES ('{description}', '{category}', {rating})"
        )
    await driver.execute_script(f"""
        CREATE INDEX {table}_idx ON {table}
        USING bm25 (id, description, category)
        WITH (key_field = 'id');
    """)
    await _async_commit(driver)
    return table


def _assert_paradedb_search_rows(driver: Any, table: str) -> None:
    running = driver.execute(f"SELECT id, description FROM {table} WHERE description @@@ 'running'").get_data()
    assert running
    assert all("running" in _lower_keys(row)["description"].lower() for row in running)

    conjunction = driver.execute(
        f"SELECT id, description FROM {table} "
        "WHERE description @@@ pdb.match('running shoes', conjunction_mode => true)"
    ).get_data()
    assert conjunction
    for row in conjunction:
        desc = _lower_keys(row)["description"].lower()
        assert "running" in desc and "shoes" in desc

    disjunction = driver.execute(
        f"SELECT id, description FROM {table} WHERE description @@@ pdb.match('running boots')"
    ).get_data()
    descriptions = [_lower_keys(row)["description"].lower() for row in disjunction]
    assert len(descriptions) >= 2
    assert any("running" in desc for desc in descriptions)
    assert any("boots" in desc for desc in descriptions)

    phrase = driver.execute(
        f"SELECT id, description FROM {table} WHERE description @@@ pdb.phrase('running shoes')"
    ).get_data()
    assert phrase
    assert all("running shoes" in _lower_keys(row)["description"].lower() for row in phrase)
    wrong_phrase = driver.execute(
        f"SELECT id, description FROM {table} WHERE description @@@ pdb.phrase('shoes running')"
    ).get_data()
    assert wrong_phrase == []

    category = driver.execute(f"SELECT id, category FROM {table} WHERE category @@@ pdb.term('footwear')").get_data()
    assert category
    assert all(_lower_keys(row)["category"] == "footwear" for row in category)


async def _assert_paradedb_search_rows_async(driver: Any, table: str) -> None:
    running = (await driver.execute(f"SELECT id, description FROM {table} WHERE description @@@ 'running'")).get_data()
    assert running
    assert all("running" in _lower_keys(row)["description"].lower() for row in running)

    conjunction = (
        await driver.execute(
            f"SELECT id, description FROM {table} "
            "WHERE description @@@ pdb.match('running shoes', conjunction_mode => true)"
        )
    ).get_data()
    assert conjunction
    for row in conjunction:
        desc = _lower_keys(row)["description"].lower()
        assert "running" in desc and "shoes" in desc

    disjunction = (
        await driver.execute(f"SELECT id, description FROM {table} WHERE description @@@ pdb.match('running boots')")
    ).get_data()
    descriptions = [_lower_keys(row)["description"].lower() for row in disjunction]
    assert len(descriptions) >= 2
    assert any("running" in desc for desc in descriptions)
    assert any("boots" in desc for desc in descriptions)

    phrase = (
        await driver.execute(f"SELECT id, description FROM {table} WHERE description @@@ pdb.phrase('running shoes')")
    ).get_data()
    assert phrase
    assert all("running shoes" in _lower_keys(row)["description"].lower() for row in phrase)
    wrong_phrase = (
        await driver.execute(f"SELECT id, description FROM {table} WHERE description @@@ pdb.phrase('shoes running')")
    ).get_data()
    assert wrong_phrase == []

    category = (
        await driver.execute(f"SELECT id, category FROM {table} WHERE category @@@ pdb.term('footwear')")
    ).get_data()
    assert category
    assert all(_lower_keys(row)["category"] == "footwear" for row in category)


def assert_sync_paradedb_search_contract(driver: object, case: PostgresExtensionCase) -> None:
    """Assert sync ParadeDB BM25 operators and query helpers."""
    sync_driver = cast("Any", driver)
    table = _sync_seed_paradedb_table(sync_driver, case)
    try:
        _assert_paradedb_search_rows(sync_driver, table)
    finally:
        sync_driver.execute_script(f"DROP TABLE IF EXISTS {table} CASCADE")


async def assert_async_paradedb_search_contract(driver: object, case: PostgresExtensionCase) -> None:
    """Async mirror of assert_sync_paradedb_search_contract."""
    async_driver = cast("Any", driver)
    table = await _async_seed_paradedb_table(async_driver, case)
    try:
        await _assert_paradedb_search_rows_async(async_driver, table)
    finally:
        await async_driver.execute_script(f"DROP TABLE IF EXISTS {table} CASCADE")


def assert_sync_paradedb_pgvector_contract(driver: object, case: PostgresExtensionCase) -> None:
    """Assert sync ParadeDB also exposes pgvector distance operators."""
    sync_driver = cast("Any", driver)
    table = _sync_seed_pgvector_table(sync_driver, case)
    try:
        rows = sync_driver.execute(f"""
            SELECT id, embedding <=> '[0.1, 0.2, 0.3]'::vector AS distance
            FROM {table}
            ORDER BY distance
            LIMIT 2
        """).get_data()
        assert len(rows) == 2
        assert _lower_keys(rows[0])["distance"] < 0.01
    finally:
        sync_driver.execute_script(f"DROP TABLE IF EXISTS {table} CASCADE")


async def assert_async_paradedb_pgvector_contract(driver: object, case: PostgresExtensionCase) -> None:
    """Async mirror of assert_sync_paradedb_pgvector_contract."""
    async_driver = cast("Any", driver)
    table = await _async_seed_pgvector_table(async_driver, case)
    try:
        rows = (
            await async_driver.execute(f"""
            SELECT id, embedding <=> '[0.1, 0.2, 0.3]'::vector AS distance
            FROM {table}
            ORDER BY distance
            LIMIT 2
        """)
        ).get_data()
        assert len(rows) == 2
        assert _lower_keys(rows[0])["distance"] < 0.01
    finally:
        await async_driver.execute_script(f"DROP TABLE IF EXISTS {table} CASCADE")
