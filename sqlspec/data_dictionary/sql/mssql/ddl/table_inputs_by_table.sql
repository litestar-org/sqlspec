-- name: table_inputs_by_table
-- dialect: mssql
/* sqlspec:mssql:ddl_table_inputs */
SELECT
    s.name AS schema_name,
    tab.name AS table_name,
    c.name AS column_name,
    typ.name AS data_type,
    c.max_length,
    c.precision AS numeric_precision,
    c.scale AS numeric_scale,
    CAST(c.is_nullable AS BIT) AS is_nullable,
    c.column_id AS ordinal_position,
    OBJECT_DEFINITION(c.default_object_id) AS column_default,
    CAST(CASE WHEN cc.object_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_computed,
    cc.definition AS computed_definition,
    CAST(CASE WHEN ic.object_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_identity,
    CONVERT(BIGINT, ic.seed_value) AS identity_seed,
    CONVERT(BIGINT, ic.increment_value) AS identity_increment,
    pk.name AS primary_key_name,
    pk_col.key_ordinal AS primary_key_ordinal
FROM sys.columns AS c
INNER JOIN sys.types AS typ ON c.user_type_id = typ.user_type_id
INNER JOIN sys.tables AS tab ON c.object_id = tab.object_id
INNER JOIN sys.schemas AS s ON tab.schema_id = s.schema_id
LEFT JOIN sys.computed_columns AS cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
LEFT JOIN sys.identity_columns AS ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
LEFT JOIN sys.index_columns AS pk_col
    ON c.object_id = pk_col.object_id
   AND c.column_id = pk_col.column_id
   AND pk_col.is_included_column = 0
LEFT JOIN sys.indexes AS pk
    ON pk_col.object_id = pk.object_id
   AND pk_col.index_id = pk.index_id
   AND pk.is_primary_key = 1
WHERE s.name = :schema_name
  AND tab.name = :table_name
ORDER BY c.column_id;
