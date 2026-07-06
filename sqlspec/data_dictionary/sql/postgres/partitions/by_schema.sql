-- name: by_schema
-- dialect: postgres
SELECT
    parent_ns.nspname::text AS schema_name,
    parent.relname::text AS parent_table,
    child_ns.nspname::text AS partition_schema,
    child.relname::text AS partition_name,
    pg_catalog.pg_get_expr(child.relpartbound, child.oid, true)::text AS partition_bound,
    pt.partstrat::text AS partition_strategy,
    inh.inhseqno AS inherit_sequence
FROM pg_catalog.pg_inherits inh
JOIN pg_catalog.pg_class parent ON parent.oid = inh.inhparent
JOIN pg_catalog.pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
JOIN pg_catalog.pg_class child ON child.oid = inh.inhrelid
JOIN pg_catalog.pg_namespace child_ns ON child_ns.oid = child.relnamespace
LEFT JOIN pg_catalog.pg_partitioned_table pt ON pt.partrelid = parent.oid
WHERE parent_ns.nspname = :schema_name
  AND (:table_name::text IS NULL OR parent.relname = :table_name OR child.relname = :table_name)
ORDER BY parent_ns.nspname, parent.relname, inh.inhseqno, child.relname;
