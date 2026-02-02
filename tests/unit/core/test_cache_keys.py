from sqlspec.core.compiler import SQLProcessor
from sqlspec.core.parameters import _structural_fingerprint
from sqlspec.core.statement import SQL, get_default_config

# pyright: reportPrivateUsage=false


def test_sql_processor_cache_key_stability() -> None:
    config = get_default_config()
    processor = SQLProcessor(config)

    sql1 = "SELECT * FROM table WHERE id = ?"
    params1 = (1,)

    # _make_cache_key expects a precomputed fingerprint, not raw params
    fp1 = _structural_fingerprint(params1)
    key1 = processor._make_cache_key(sql1, fp1)

    # Same SQL, different param value (same structure)
    params2 = (2,)
    fp2 = _structural_fingerprint(params2)
    key2 = processor._make_cache_key(sql1, fp2)

    assert key1 == key2, "Cache key should be stable for same structure"

    # Different SQL
    sql3 = "SELECT * FROM table WHERE id = ? AND active = ?"
    params3 = (1, True)
    fp3 = _structural_fingerprint(params3)
    key3 = processor._make_cache_key(sql3, fp3)

    assert key1 != key3


def test_sql_hash_stability() -> None:
    # SQL objects should hash based on content
    sql1 = SQL("SELECT 1", (1,))
    sql2 = SQL("SELECT 1", (1,))

    assert hash(sql1) == hash(sql2)
    assert sql1 == sql2

    # Different params
    sql3 = SQL("SELECT 1", (2,))

    # Hashes differ because SQL includes params in hash
    # This is correct for SQL objects equality, but Processor handles structural hashing
    assert hash(sql1) != hash(sql3)
    assert sql1 != sql3


def test_structural_fingerprint_list_vs_tuple() -> None:
    # Verify [1] and (1,) produce same structural fingerprint
    config = get_default_config()
    processor = SQLProcessor(config)

    sql = "SELECT ?"
    # _make_cache_key expects a precomputed fingerprint, not raw params
    fp_list = _structural_fingerprint([1])
    fp_tuple = _structural_fingerprint((1,))
    key_list = processor._make_cache_key(sql, fp_list)
    key_tuple = processor._make_cache_key(sql, fp_tuple)

    # They produce same fingerprint for same structure
    assert key_list == key_tuple
