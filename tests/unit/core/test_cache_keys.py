from sqlspec.core.compiler import SQLProcessor
from sqlspec.core.statement import get_default_config, SQL

# pyright: reportPrivateUsage=false

def test_sql_processor_cache_key_stability() -> None:
    config = get_default_config()
    processor = SQLProcessor(config)
    
    sql1 = "SELECT * FROM table WHERE id = ?"
    params1 = (1,)
    
    key1 = processor._make_cache_key(sql1, params1)
    
    # Same SQL, different param value (same structure)
    params2 = (2,)
    key2 = processor._make_cache_key(sql1, params2)
    
    assert key1 == key2, "Cache key should be stable for same structure"

    # Different SQL
    sql3 = "SELECT * FROM table WHERE id = ? AND active = ?"
    params3 = (1, True)
    key3 = processor._make_cache_key(sql3, params3)
    
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
    key_list = processor._make_cache_key(sql, [1])
    key_tuple = processor._make_cache_key(sql, (1,))
    
    # They usually produce same fingerprint "seq:hash(...)"
    assert key_list == key_tuple
