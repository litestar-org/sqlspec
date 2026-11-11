-- name: current
-- dialect: teradata
LOCKING ROW FOR ACCESS
SELECT TRIM(DATABASE) AS schema_name;

-- name: by_schema
-- dialect: teradata
LOCKING ROW FOR ACCESS
SELECT TRIM(d.DatabaseName) AS schema_name, TRIM(d.DBKind) AS schema_kind, TRIM(d.OwnerName) AS owner_name, d.CommentString AS comment FROM DBC.DatabasesV d WHERE TRIM(d.DBKind) IN ('D', 'U') AND UPPER(TRIM(d.DatabaseName)) NOT IN ('DBC', 'SYSUDTLIB', 'SYSLIB', 'TD_SYSFNLIB', 'SYSSPATIAL', 'SYSBAR', 'LOCKLOGSHREDDER', 'SQLJ', 'TDSTATS', 'TD_SERVER_DB', 'CRASHDUMPS') AND (:schema_name IS NULL OR UPPER(TRIM(d.DatabaseName)) = UPPER(TRIM(:schema_name))) ORDER BY TRIM(d.DatabaseName);
