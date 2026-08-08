-- name: index_inputs_by_table
-- dialect: mssql
/* sqlspec:mssql:ddl_index_inputs */
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    i.name AS index_name,
    i.type_desc,
    CAST(i.is_unique AS BIT) AS is_unique,
    CAST(i.has_filter AS BIT) AS has_filter,
    i.filter_definition,
    STUFF((
        SELECT ',' + c2.name
        FROM sys.index_columns AS ic2
        INNER JOIN sys.columns AS c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
        WHERE ic2.object_id = i.object_id
          AND ic2.index_id = i.index_id
          AND ic2.is_included_column = 0
        ORDER BY ic2.key_ordinal
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS columns,
    STUFF((
        SELECT ',' + c3.name
        FROM sys.index_columns AS ic3
        INNER JOIN sys.columns AS c3 ON ic3.object_id = c3.object_id AND ic3.column_id = c3.column_id
        WHERE ic3.object_id = i.object_id
          AND ic3.index_id = i.index_id
          AND ic3.is_included_column = 1
        ORDER BY ic3.index_column_id
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS included_columns
FROM sys.indexes AS i
INNER JOIN sys.tables AS t ON i.object_id = t.object_id
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
WHERE s.name = :schema_name
  AND t.name = :table_name
  AND i.name IS NOT NULL
  AND i.is_primary_key = 0
  AND i.is_unique_constraint = 0
ORDER BY i.name;

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
