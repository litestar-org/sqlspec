-- name: by_schema
-- dialect: duckdb
SELECT
    database_name,
    schema_name,
    function_name,
    function_type,
    return_type,
    parameters,
    parameter_types,
    macro_definition,
    has_side_effects,
    internal,
    comment,
    tags
FROM duckdb_functions()
WHERE schema_name = COALESCE(:schema_name, current_schema())
ORDER BY function_type, function_name;
