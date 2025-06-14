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
-- name: collection-postgres-data-types
with table_columns as (
  select n.nspname as table_schema,
    case
      c.relkind
      when 'r' then 'TABLE'
      when 'v' then 'VIEW'
      when 'm' then 'MATERIALIZED_VIEW'
      when 'S' then 'SEQUENCE'
      when 'f' then 'FOREIGN_TABLE'
      when 'p' then 'PARTITIONED_TABLE'
      when 'c' then 'COMPOSITE_TYPE'
      when 'I' then 'PARTITIONED INDEX'
      when 't' then 'TOAST_TABLE'
      else 'UNCATEGORIZED'
    end as table_type,
    c.relname as table_name,
    a.attname as column_name,
    t.typname as data_type
  from pg_attribute a
    join pg_class c on a.attrelid = c.oid
    join pg_namespace n on n.oid = c.relnamespace
    join pg_type t on a.atttypid = t.oid
  where a.attnum > 0
    and (
      n.nspname <> all (
        ARRAY ['pg_catalog', 'information_schema']
      )
      and n.nspname !~ '^pg_toast'
    )
    and (
      c.relkind = ANY (
        ARRAY ['r', 'p', 'S', 'v', 'f', 'm', 'c', 'I', 't']
      )
    )
),
src as (
  select a.table_schema,
    a.table_type,
    a.table_name,
    a.data_type,
    count(a.data_type) as data_type_count
  from table_columns a
  group by a.table_schema,
    a.table_type,
    a.table_name,
    a.data_type
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.table_schema,
  src.table_type,
  src.table_name,
  src.data_type,
  src.data_type_count
from src
