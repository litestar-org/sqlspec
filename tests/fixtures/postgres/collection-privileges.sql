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
 -- name: collection-postgres-pglogical-schema-usage-privilege
with src as (
  select pg_catalog.has_schema_privilege('pglogical', 'USAGE') as has_schema_usage_privilege
  from pg_extension
  where extname = 'pglogical'
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.has_schema_usage_privilege,
  current_database() as database_name
from src;

-- name: collection-postgres-pglogical-privileges
with src as (
  select pg_catalog.has_table_privilege('"pglogical"."tables"', 'SELECT') as has_tables_select_privilege,
    pg_catalog.has_table_privilege('"pglogical"."local_node"', 'SELECT') as has_local_node_select_privilege,
    pg_catalog.has_table_privilege('"pglogical"."node"', 'SELECT') as has_node_select_privilege,
    pg_catalog.has_table_privilege('"pglogical"."node_interface"', 'SELECT') as has_node_interface_select_privilege
  from pg_extension
  where extname = 'pglogical'
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.has_tables_select_privilege,
  src.has_local_node_select_privilege,
  src.has_node_select_privilege,
  src.has_node_interface_select_privilege,
  current_database() as database_name
from src;

-- name: collection-postgres-user-schemas-without-privilege
with src as (
  select nspname
  from pg_catalog.pg_namespace
  where nspname not in (
      'information_schema',
      'pglogical',
      'pglogical_origin',
      'cron',
      'pgbouncer',
      'google_vacuum_mgmt'
    )
    and nspname not like 'pg\_%%'
    and pg_catalog.has_schema_privilege(nspname, 'USAGE') = 'f'
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.nspname as namespace_name,
  current_database() as database_name
from src;

-- name: collection-postgres-user-tables-without-privilege
with src as (
  select schemaname,
    tablename
  from pg_catalog.pg_tables
  where schemaname not in (
      'information_schema',
      'pglogical',
      'pglogical_origin'
    )
    and schemaname not like 'pg\_%%'
    and pg_catalog.has_table_privilege(
      quote_ident(schemaname) || '.' || quote_ident(tablename),
      'SELECT'
    ) = 'f'
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.schemaname as schema_name,
  src.tablename as table_name,
  current_database() as database_name
from src;

-- name: collection-postgres-user-views-without-privilege
with src as (
  select schemaname,
    viewname
  from pg_catalog.pg_views
  where schemaname not in (
      'information_schema',
      'pglogical',
      'pglogical_origin'
    )
    and schemaname not like 'pg\_%%'
    and pg_catalog.has_table_privilege(
      quote_ident(schemaname) || '.' || quote_ident(viewname),
      'SELECT'
    ) = 'f'
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.schemaname as schema_name,
  src.viewname as view_name,
  current_database() as database_name
from src;

-- name: collection-postgres-user-sequences-without-privilege
with src as (
  select n.nspname as nspname,
    relname
  from pg_catalog.pg_class c
    left join pg_catalog.pg_namespace n on n.oid = c.relnamespace
  where c.relkind = 'S'
    and n.nspname != 'pglogical'
    and n.nspname != 'pglogical_origin'
    and n.nspname not like 'pg\_%%'
    and pg_catalog.has_sequence_privilege(
      quote_ident(n.nspname) || '.' || quote_ident(relname),
      'SELECT'
    ) = 'f'
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.nspname as namespace_name,
  src.relname as rel_name,
  current_database() as database_name
from src;

-- name: collection-postgres-replication-role
with src as (
  SELECT rolname, rolreplication FROM pg_catalog.pg_roles
  WHERE rolname IN (SELECT CURRENT_USER)
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.rolname,
  src.rolreplication,
  current_database() as database_name
from src;
