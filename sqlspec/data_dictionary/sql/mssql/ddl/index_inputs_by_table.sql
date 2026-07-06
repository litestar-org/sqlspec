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
