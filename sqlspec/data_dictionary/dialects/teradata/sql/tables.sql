-- name: by_schema
-- dialect: teradata
LOCKING ROW FOR ACCESS
WITH RECURSIVE dependency_tree (schema_name, table_name, dependency_level, path) AS (
 SELECT TRIM(t.DatabaseName), TRIM(t.TableName), 0, CAST('/' || UPPER(TRIM(t.DatabaseName)) || '.' || UPPER(TRIM(t.TableName)) || '/' AS VARCHAR(32000))
 FROM DBC.TablesV t WHERE UPPER(TRIM(t.DatabaseName)) = UPPER(TRIM(COALESCE(:schema_name, DATABASE))) AND UPPER(TRIM(t.DatabaseName)) NOT IN ('DBC', 'SYSUDTLIB', 'SYSLIB', 'TD_SYSFNLIB', 'SYSSPATIAL', 'SYSBAR', 'LOCKLOGSHREDDER', 'SQLJ', 'TDSTATS', 'TD_SERVER_DB', 'CRASHDUMPS') AND TRIM(t.TableKind) IN ('T', 'O') AND NOT EXISTS (SELECT 1 FROM DBC.All_RI_ChildrenV r WHERE UPPER(TRIM(r.ChildDB)) = UPPER(TRIM(t.DatabaseName)) AND UPPER(TRIM(r.ChildTable)) = UPPER(TRIM(t.TableName)) AND UPPER(TRIM(r.ParentDB)) = UPPER(TRIM(t.DatabaseName)) AND UPPER(TRIM(r.ParentTable)) <> UPPER(TRIM(t.TableName)))
 UNION ALL
 SELECT TRIM(child.DatabaseName), TRIM(child.TableName), dt.dependency_level + 1, dt.path || UPPER(TRIM(child.DatabaseName)) || '.' || UPPER(TRIM(child.TableName)) || '/'
 FROM dependency_tree dt JOIN DBC.All_RI_ChildrenV r ON UPPER(TRIM(r.ParentDB)) = UPPER(TRIM(dt.schema_name)) AND UPPER(TRIM(r.ParentTable)) = UPPER(TRIM(dt.table_name)) JOIN DBC.TablesV child ON UPPER(TRIM(child.DatabaseName)) = UPPER(TRIM(r.ChildDB)) AND UPPER(TRIM(child.TableName)) = UPPER(TRIM(r.ChildTable))
 WHERE UPPER(TRIM(child.DatabaseName)) = UPPER(TRIM(COALESCE(:schema_name, DATABASE))) AND UPPER(TRIM(child.DatabaseName)) NOT IN ('DBC', 'SYSUDTLIB', 'SYSLIB', 'TD_SYSFNLIB', 'SYSSPATIAL', 'SYSBAR', 'LOCKLOGSHREDDER', 'SQLJ', 'TDSTATS', 'TD_SERVER_DB', 'CRASHDUMPS') AND TRIM(child.TableKind) IN ('T', 'O') AND UPPER(TRIM(r.ParentDB)) = UPPER(TRIM(r.ChildDB)) AND dt.dependency_level < 20 AND dt.path NOT LIKE '%/' || UPPER(TRIM(child.DatabaseName)) || '.' || UPPER(TRIM(child.TableName)) || '/%'
), dependency_levels AS (SELECT schema_name, table_name, MAX(dependency_level) AS dependency_level FROM dependency_tree GROUP BY schema_name, table_name)
SELECT TRIM(t.DatabaseName) AS schema_name, TRIM(t.TableName) AS table_name, 'BASE TABLE' AS table_type, TRIM(t.TableKind) AS table_kind, COALESCE(dl.dependency_level, 0) AS dependency_level, COALESCE(dl.dependency_level, 0) AS level, t.CommentString AS table_comment FROM DBC.TablesV t LEFT JOIN dependency_levels dl ON UPPER(TRIM(dl.schema_name)) = UPPER(TRIM(t.DatabaseName)) AND UPPER(TRIM(dl.table_name)) = UPPER(TRIM(t.TableName)) WHERE UPPER(TRIM(t.DatabaseName)) = UPPER(TRIM(COALESCE(:schema_name, DATABASE))) AND UPPER(TRIM(t.DatabaseName)) NOT IN ('DBC', 'SYSUDTLIB', 'SYSLIB', 'TD_SYSFNLIB', 'SYSSPATIAL', 'SYSBAR', 'LOCKLOGSHREDDER', 'SQLJ', 'TDSTATS', 'TD_SERVER_DB', 'CRASHDUMPS') AND TRIM(t.TableKind) IN ('T', 'O') AND (:table_name IS NULL OR UPPER(TRIM(t.TableName)) = UPPER(TRIM(:table_name))) ORDER BY COALESCE(dl.dependency_level, 0), TRIM(t.DatabaseName), TRIM(t.TableName);

-- name: all_by_schema
-- dialect: teradata
LOCKING ROW FOR ACCESS
SELECT TRIM(t.DatabaseName) AS schema_name, TRIM(t.TableName) AS table_name, CASE TRIM(t.TableKind) WHEN 'V' THEN 'VIEW' ELSE 'BASE TABLE' END AS table_type, TRIM(t.TableKind) AS table_kind, 0 AS dependency_level, 0 AS level, t.CommentString AS table_comment FROM DBC.TablesV t WHERE UPPER(TRIM(t.DatabaseName)) = UPPER(TRIM(COALESCE(:schema_name, DATABASE))) AND UPPER(TRIM(t.DatabaseName)) NOT IN ('DBC', 'SYSUDTLIB', 'SYSLIB', 'TD_SYSFNLIB', 'SYSSPATIAL', 'SYSBAR', 'LOCKLOGSHREDDER', 'SQLJ', 'TDSTATS', 'TD_SERVER_DB', 'CRASHDUMPS') AND TRIM(t.TableKind) IN ('T', 'O', 'V') AND (:table_name IS NULL OR UPPER(TRIM(t.TableName)) = UPPER(TRIM(:table_name))) ORDER BY TRIM(t.DatabaseName), TRIM(t.TableName);

-- name: names_by_schema
-- dialect: teradata
LOCKING ROW FOR ACCESS
SELECT TRIM(t.DatabaseName) AS schema_name, TRIM(t.TableName) AS table_name FROM DBC.TablesV t WHERE UPPER(TRIM(t.DatabaseName)) = UPPER(TRIM(COALESCE(:schema_name, DATABASE))) AND UPPER(TRIM(t.DatabaseName)) NOT IN ('DBC', 'SYSUDTLIB', 'SYSLIB', 'TD_SYSFNLIB', 'SYSSPATIAL', 'SYSBAR', 'LOCKLOGSHREDDER', 'SQLJ', 'TDSTATS', 'TD_SERVER_DB', 'CRASHDUMPS') AND TRIM(t.TableKind) IN ('T', 'O') ORDER BY TRIM(t.TableName);
