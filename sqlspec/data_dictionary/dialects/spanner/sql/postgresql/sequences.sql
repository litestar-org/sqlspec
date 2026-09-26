-- name: by_schema
-- dialect: spanner
SELECT
    sequence_catalog,
    sequence_schema,
    sequence_name,
    data_type,
    start_with_counter,
    skip_range_min,
    skip_range_max
FROM information_schema.sequences
WHERE (:schema_name::text IS NULL OR sequence_schema = :schema_name)
  AND (:sequence_name::text IS NULL OR sequence_name = :sequence_name)
ORDER BY sequence_schema, sequence_name;
