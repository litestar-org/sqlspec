-- name: by_schema
-- dialect: snowflake
SELECT
    sequence_catalog AS "catalog_name",
    sequence_schema AS "schema_name",
    sequence_name AS "sequence_name",
    sequence_owner AS "owner_name",
    data_type AS "data_type",
    numeric_precision AS "numeric_precision",
    numeric_scale AS "numeric_scale",
    start_value AS "start_value",
    minimum_value AS "min_value",
    maximum_value AS "max_value",
    next_value AS "next_value",
    "INCREMENT" AS "increment_by",
    cycle_option AS "cycle",
    created AS "created",
    last_altered AS "last_altered",
    comment AS "comment"
FROM information_schema.sequences
WHERE sequence_schema = COALESCE(:schema_name, CURRENT_SCHEMA())
ORDER BY sequence_schema, sequence_name;

-- name: by_name
-- dialect: snowflake
SELECT
    sequence_catalog AS "catalog_name",
    sequence_schema AS "schema_name",
    sequence_name AS "sequence_name",
    sequence_owner AS "owner_name",
    data_type AS "data_type",
    numeric_precision AS "numeric_precision",
    numeric_scale AS "numeric_scale",
    start_value AS "start_value",
    minimum_value AS "min_value",
    maximum_value AS "max_value",
    next_value AS "next_value",
    "INCREMENT" AS "increment_by",
    cycle_option AS "cycle",
    created AS "created",
    last_altered AS "last_altered",
    comment AS "comment"
FROM information_schema.sequences
WHERE sequence_schema = COALESCE(:schema_name, CURRENT_SCHEMA())
  AND sequence_name = :sequence_name;
