/*
 Copyright 2024 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
-- name: collection-postgres-schema-objects
with all_tables as (
  select distinct c.oid as object_id,
    'TABLE' as object_category,
    case
      when c.relkind = 'r' then 'TABLE'
      when c.relkind = 'S' then 'SEQUENCE'
      when c.relkind = 'f' then 'FOREIGN_TABLE'
      when c.relkind = 'p' then 'PARTITIONED_TABLE'
      when c.relkind = 'c' then 'COMPOSITE_TYPE'
      when c.relkind = 't' then 'TOAST_TABLE'
      else 'UNCATEGORIZED_TABLE'
    end as object_type,
    ns.nspname as object_schema,
    c.relname as object_name,
    pg_get_userbyid(c.relowner) as object_owner
  from pg_class c
    join pg_catalog.pg_namespace as ns on (c.relnamespace = ns.oid)
  where ns.nspname <> all (array ['pg_catalog', 'information_schema'])
    and ns.nspname !~ '^pg_toast'
    and c.relkind = ANY (ARRAY ['r', 'p', 'S', 'f', 'c','t'])
),
all_views as (
  select distinct c.oid as object_id,
    'VIEW' as object_category,
    case
      when c.relkind = 'v' then 'VIEW'
      when c.relkind = 'm' then 'MATERIALIZED_VIEW'
      else 'UNCATEGORIZED_VIEW'
    end as object_type,
    ns.nspname as object_schema,
    c.relname as object_name,
    pg_get_userbyid(c.relowner) as object_owner
  from pg_class c
    join pg_catalog.pg_namespace as ns on (c.relnamespace = ns.oid)
  where ns.nspname <> all (array ['pg_catalog', 'information_schema'])
    and ns.nspname !~ '^pg_toast'
    and c.relkind = ANY (ARRAY [ 'v', 'm'])
),
all_indexes as (
  select distinct i.indexrelid as object_id,
    'INDEX' as object_category,
    case
      when c.relkind = 'I'
      and c.relname !~ '^pg_toast' then 'PARTITIONED_INDEX'
      when c.relkind = 'I'
      and c.relname ~ '^pg_toast' then 'TOAST_PARTITIONED_INDEX'
      when c.relkind = 'i'
      and c.relname !~ '^pg_toast' then 'INDEX'
      when c.relkind = 'i'
      and c.relname ~ '^pg_toast' then 'TOAST_INDEX'
      else 'UNCATEGORIZED_INDEX'
    end as object_type,
    sut.relname as table_name,
    sut.schemaname as object_schema,
    c.relname as object_name,
    pg_get_userbyid(c.relowner) as object_owner
  from pg_index i
    join pg_stat_user_tables sut on (i.indrelid = sut.relid)
    join pg_class c on (i.indexrelid = c.oid)
),
all_constraints as (
  select distinct con.oid as object_id,
    'CONSTRAINT' as object_category,
    case
      when con.contype = 'c' then 'CHECK_CONSTRAINT'
      when con.contype = 'f' then 'FOREIGN_KEY_CONSTRAINT'
      when con.contype = 'p' then 'PRIMARY_KEY_CONSTRAINT'
      when con.contype = 'u' then 'UNIQUE_CONSTRAINT'
      when con.contype = 't' then 'CONSTRAINT_TRIGGER'
      when con.contype = 'x' then 'EXCLUSION_CONSTRAINT'
      else 'UNCATEGORIZED_CONSTRAINT'
    end as object_type,
    ns.nspname as object_schema,
    con.conname as object_name,
    pg_get_userbyid(c.relowner) as object_owner
  from pg_constraint con
    join pg_class as c on con.conrelid = c.oid
    join pg_catalog.pg_namespace as ns on (con.connamespace = ns.oid)
  where ns.nspname <> all (array ['pg_catalog', 'information_schema'])
    and ns.nspname !~ '^pg_toast'
),
all_triggers as (
  select distinct t.tgrelid as object_id,
    'TRIGGER' as object_category,
    case
      t.tgtype::integer & 66
      when 2 then 'BEFORE'
      when 64 then 'INSTEAD_OF'
      else 'AFTER'
    end || '_' || case
      t.tgtype::integer & cast(28 as int2)
      when 16 then 'UPDATE'
      when 8 then 'DELETE'
      when 4 then 'INSERT'
      when 20 then 'INSERT_UPDATE'
      when 28 then 'INSERT_UPDATE_DELETE'
      when 24 then 'UPDATE_DELETE'
      when 12 then 'INSERT_DELETE'
    end || '_' || 'TRIGGER' as object_type,
    ns.nspname as object_schema,
    t.tgname as object_name,
    pg_get_userbyid(c.relowner) as object_owner
  from pg_trigger t
    join pg_class c on t.tgrelid = c.oid
    join pg_namespace ns on ns.oid = c.relnamespace
    /* exclude triggers generated from constraints */
  where t.tgrelid not in (
      select conrelid
      from pg_constraint
    )
),
all_procedures as (
  select distinct p.oid as object_id,
    'SOURCE_CODE' as object_category,
    ns.nspname as object_schema,
    case
      when p.prokind = 'f' then 'FUNCTION'
      when p.prokind = 'p' then 'PROCEDURE'
      when p.prokind = 'a' then 'AGGREGATE_FUNCTION'
      when p.prokind = 'w' then 'WINDOW_FUNCTION'
      else 'UNCATEGORIZED_PROCEDURE'
    end as object_type,
    p.proname as object_name,
    pg_get_userbyid(p.proowner) as object_owner
  from pg_proc p
    left join pg_namespace ns on ns.oid = p.pronamespace
  where ns.nspname <> all (array ['pg_catalog', 'information_schema'])
    and ns.nspname !~ '^pg_toast'
),
src as (
  select a.object_owner,
    a.object_category,
    a.object_type,
    a.object_schema,
    a.object_name,
    a.object_id
  from all_tables a
  union all
  select a.object_owner,
    a.object_category,
    a.object_type,
    a.object_schema,
    a.object_name,
    a.object_id
  from all_views a
  union all
  select a.object_owner,
    a.object_category,
    a.object_type,
    a.object_schema,
    a.object_name,
    a.object_id
  from all_indexes a
  union all
  select a.object_owner,
    a.object_category,
    a.object_type,
    a.object_schema,
    a.object_name,
    a.object_id
  from all_procedures a
  union all
  select a.object_owner,
    a.object_category,
    a.object_type,
    a.object_schema,
    a.object_name,
    a.object_id
  from all_constraints a
  union all
  select a.object_owner,
    a.object_category,
    a.object_type,
    a.object_schema,
    a.object_name,
    a.object_id
  from all_triggers a
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.object_owner,
  src.object_category,
  src.object_type,
  src.object_schema,
  src.object_name,
  src.object_id,
  current_database() as database_name
from src;
