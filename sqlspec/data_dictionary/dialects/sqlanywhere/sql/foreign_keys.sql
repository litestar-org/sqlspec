-- name: by_schema
-- dialect: sqlanywhere
SELECT
    fu.user_name AS schema_name,
    ft.table_name AS table_name,
    fi.index_name AS constraint_name,
    fc.column_name AS column_name,
    pu.user_name AS referenced_schema,
    pt.table_name AS referenced_table,
    pc.column_name AS referenced_column
FROM SYS.SYSFKEY fk
JOIN SYS.SYSTAB ft ON fk.foreign_table_id = ft.table_id
JOIN SYS.SYSUSER fu ON ft.creator = fu.user_id
JOIN SYS.SYSIDX fi ON fk.foreign_table_id = fi.table_id AND fk.foreign_index_id = fi.index_id
JOIN SYS.SYSIDXCOL fic ON fi.table_id = fic.table_id AND fi.index_id = fic.index_id
JOIN SYS.SYSTABCOL fc ON fic.table_id = fc.table_id AND fic.column_id = fc.column_id
JOIN SYS.SYSTAB pt ON fk.primary_table_id = pt.table_id
JOIN SYS.SYSUSER pu ON pt.creator = pu.user_id
JOIN SYS.SYSTABCOL pc ON pt.table_id = pc.table_id AND fic.primary_column_id = pc.column_id
WHERE fu.user_name = COALESCE(:schema_name, CURRENT USER)
  AND (:table_name IS NULL OR ft.table_name = :table_name)
ORDER BY fu.user_name, ft.table_name, fi.index_name, fic.sequence;

-- name: by_table
-- dialect: sqlanywhere
SELECT
    fu.user_name AS schema_name,
    ft.table_name AS table_name,
    fi.index_name AS constraint_name,
    fc.column_name AS column_name,
    pu.user_name AS referenced_schema,
    pt.table_name AS referenced_table,
    pc.column_name AS referenced_column
FROM SYS.SYSFKEY fk
JOIN SYS.SYSTAB ft ON fk.foreign_table_id = ft.table_id
JOIN SYS.SYSUSER fu ON ft.creator = fu.user_id
JOIN SYS.SYSIDX fi ON fk.foreign_table_id = fi.table_id AND fk.foreign_index_id = fi.index_id
JOIN SYS.SYSIDXCOL fic ON fi.table_id = fic.table_id AND fi.index_id = fic.index_id
JOIN SYS.SYSTABCOL fc ON fic.table_id = fc.table_id AND fic.column_id = fc.column_id
JOIN SYS.SYSTAB pt ON fk.primary_table_id = pt.table_id
JOIN SYS.SYSUSER pu ON pt.creator = pu.user_id
JOIN SYS.SYSTABCOL pc ON pt.table_id = pc.table_id AND fic.primary_column_id = pc.column_id
WHERE ft.table_name = :table_name
  AND fu.user_name = COALESCE(:schema_name, CURRENT USER)
ORDER BY fi.index_name, fic.sequence;
