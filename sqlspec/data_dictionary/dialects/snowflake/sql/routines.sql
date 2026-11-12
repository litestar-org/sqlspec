-- name: by_schema
-- dialect: snowflake
WITH target_schema AS (
    SELECT COALESCE(:schema_name, CURRENT_SCHEMA()) AS schema_name
)
SELECT
    f.function_catalog AS "catalog_name",
    f.function_schema AS "schema_name",
    f.function_name AS "routine_name",
    'function' AS "routine_type",
    f.function_owner AS "owner_name",
    f.argument_signature AS "argument_signature",
    f.data_type AS "return_type",
    f.function_language AS "routine_language",
    f.function_definition AS "routine_definition",
    f.is_secure AS "is_secure",
    f.created AS "created",
    f.last_altered AS "last_altered",
    f.comment AS "comment"
FROM information_schema.functions f
JOIN target_schema ts ON f.function_schema = ts.schema_name
UNION ALL
SELECT
    p.procedure_catalog AS "catalog_name",
    p.procedure_schema AS "schema_name",
    p.procedure_name AS "routine_name",
    'procedure' AS "routine_type",
    p.procedure_owner AS "owner_name",
    p.argument_signature AS "argument_signature",
    p.data_type AS "return_type",
    p.procedure_language AS "routine_language",
    p.procedure_definition AS "routine_definition",
    p.is_secure AS "is_secure",
    p.created AS "created",
    p.last_altered AS "last_altered",
    p.comment AS "comment"
FROM information_schema.procedures p
JOIN target_schema ts ON p.procedure_schema = ts.schema_name
ORDER BY 2, 4, 3;

-- name: functions_by_schema
-- dialect: snowflake
SELECT
    function_catalog AS "catalog_name",
    function_schema AS "schema_name",
    function_name AS "routine_name",
    'function' AS "routine_type",
    function_owner AS "owner_name",
    argument_signature AS "argument_signature",
    data_type AS "return_type",
    function_language AS "routine_language",
    function_definition AS "routine_definition",
    is_secure AS "is_secure",
    created AS "created",
    last_altered AS "last_altered",
    comment AS "comment"
FROM information_schema.functions
WHERE function_schema = COALESCE(:schema_name, CURRENT_SCHEMA())
ORDER BY function_schema, function_name;

-- name: procedures_by_schema
-- dialect: snowflake
SELECT
    procedure_catalog AS "catalog_name",
    procedure_schema AS "schema_name",
    procedure_name AS "routine_name",
    'procedure' AS "routine_type",
    procedure_owner AS "owner_name",
    argument_signature AS "argument_signature",
    data_type AS "return_type",
    procedure_language AS "routine_language",
    procedure_definition AS "routine_definition",
    is_secure AS "is_secure",
    created AS "created",
    last_altered AS "last_altered",
    comment AS "comment"
FROM information_schema.procedures
WHERE procedure_schema = COALESCE(:schema_name, CURRENT_SCHEMA())
ORDER BY procedure_schema, procedure_name;
