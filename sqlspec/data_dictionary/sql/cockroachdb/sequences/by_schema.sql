-- name: by_schema
-- dialect: cockroachdb
SELECT
    sequence_schema::text AS schema_name,
    sequence_name::text AS sequence_name,
    data_type::text AS data_type,
    start_value::text AS start_value,
    minimum_value::text AS minimum_value,
    maximum_value::text AS maximum_value,
    increment::text AS increment,
    cycle_option::text AS cycle_option
FROM information_schema.sequences
WHERE sequence_schema = :schema_name
ORDER BY sequence_schema, sequence_name;
