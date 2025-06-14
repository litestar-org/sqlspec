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
-- name: collection-postgres-schema-details
with all_schemas as (
  select n.oid as object_id,
    n.nspname as object_schema,
    pg_get_userbyid(n.nspowner) as schema_owner,
    case
      when n.nspname !~ '^pg_'
      and (
        n.nspname <> all (ARRAY ['pg_catalog' , 'information_schema'])
      ) then false
      else true
    end as system_object
  from pg_namespace n
),
all_functions as (
  select n.nspname as object_schema,
    count(distinct p.oid) as function_count
  from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
  group by n.nspname
),
all_views as (
  select n.nspname as object_schema,
    count(distinct c.oid) as view_count
  from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
  where c.relkind = ANY (ARRAY ['v' , 'm' ])
  group by n.nspname
),
src as (
  select all_schemas.object_schema,
    all_schemas.schema_owner,
    all_schemas.system_object,
    COALESCE(count(all_tables.*), 0) as table_count,
    COALESCE(all_views.view_count, 0) as view_count,
    COALESCE(all_functions.function_count, 0) as function_count,
    sum(pg_table_size(all_tables.oid)) as table_data_size_bytes,
    sum(pg_total_relation_size(all_tables.oid)) as total_table_size_bytes
  from all_schemas
    left join pg_class all_tables on all_schemas.object_id = all_tables.relnamespace
    and (all_tables.relkind = ANY (ARRAY ['r', 'p']))
    left join all_functions on all_functions.object_schema = all_schemas.object_schema
    left join all_views on all_views.object_schema = all_schemas.object_schema
  group by all_schemas.object_schema,
    all_schemas.schema_owner,
    all_schemas.system_object,
    all_views.view_count,
    all_functions.function_count
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.object_schema,
  src.schema_owner,
  src.system_object,
  src.table_count,
  src.view_count,
  src.function_count,
  COALESCE(src.table_data_size_bytes, 0) as table_data_size_bytes,
  COALESCE(src.total_table_size_bytes, 0) as total_table_size_bytes,
  current_database() as database_name
from src;
