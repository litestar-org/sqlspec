-- name: current
-- dialect: teradata
LOCKING ROW FOR ACCESS
SELECT TRIM(InfoData) AS version_string FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION';
