-- name: by_owner
-- dialect: oracle
SELECT
    sequence_owner AS schema_name,
    sequence_name,
    min_value,
    max_value,
    increment_by,
    cycle_flag,
    order_flag,
    cache_size,
    last_number,
    scale_flag,
    extend_flag,
    sharded_flag,
    session_flag,
    keep_value
FROM all_sequences
WHERE sequence_owner = COALESCE(:schema_name, USER)
  AND (:sequence_name IS NULL OR sequence_name = :sequence_name)
ORDER BY sequence_owner, sequence_name;
