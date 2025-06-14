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
-- name: collection-postgres-calculated-metrics
with table_summary as (
  select count(distinct c.oid) as total_table_count
  from pg_class c
    join pg_catalog.pg_namespace as ns on (c.relnamespace = ns.oid)
  where ns.nspname <> all (array ['pg_catalog', 'information_schema'])
    and ns.nspname !~ '^pg_toast'
    and c.relkind = ANY (ARRAY ['r', 'p', 't'])
),
foreign_table_summary as (
  select count(distinct ft.ftrelid) total_foreign_table_count,
    count(
      distinct case
        when w.fdwname = ANY (ARRAY ['oracle_fdw', 'orafdw','postgres_fdw']) then ft.ftrelid
        else null
      end
    ) as supported_foreign_table_count,
    count(
      distinct case
        when w.fdwname != all (ARRAY ['oracle_fdw', 'orafdw','postgres_fdw']) then ft.ftrelid
        else null
      end
    ) as unsupported_foreign_table_count
  from pg_catalog.pg_foreign_table ft
    inner join pg_catalog.pg_class c on c.oid = ft.ftrelid
    inner join pg_catalog.pg_foreign_server s on s.oid = ft.ftserver
    inner join pg_catalog.pg_foreign_data_wrapper w on s.srvfdw = w.oid
),
extension_summary as (
  select count(distinct e.extname) total_extension_count,
    count(
      distinct case
        when e.extname = any (
          array ['btree_gin',
                'btree_gist',
                'chkpass',
                'citext',
                'cube',
                'hstore',
                'isn',
                'ip4r',
                'ltree',
                'lo',
                'postgresql-hll',
                'prefix',
                'postgis',
                'postgis_raster',
                'postgis_sfcgal',
                'postgis_tiger_geocoder',
                'postgis_topology',
                'address_standardizer',
                'address_standardizer_data_us',
                'plpgsql',
                'plv8',
                'amcheck',
                'auto_explain',
                'dblink',
                'decoderbufs',
                'dict_int',
                'earthdistance',
                'fuzzystrmatch',
                'intagg',
                'intarray',
                'oracle_fdw',
                'orafce',
                'pageinspect',
                'pgAudit',
                'pg_bigm',
                'pg_buffercache',
                'pg_cron',
                'pgcrypto',
                'pglogical',
                'pgfincore',
                'pg_freespacemap',
                'pg_hint_plan',
                'pgoutput',
                'pg_partman',
                'pg_prewarm',
                'pg_proctab',
                'pg_repack',
                'pgrowlocks',
                'pgstattuple',
                'pg_similarity',
                'pg_stat_statements',
                'pgtap',
                'pg_trgm',
                'pgtt',
                'pgvector',
                'pg_visibility',
                'pg_wait_sampling',
                'plproxy',
                'postgres_fdw',
                'postgresql_anonymizer',
                'rdkit',
                'refint',
                'sslinfo',
                'tablefunc',
                'tsm_system_rows',
                'tsm_system_time',
                'unaccent',
                'uuid-ossp']
        ) then e.extname
        else null
      end
    ) as supported_extension_count,
    count(
      distinct case
        when e.extname != all (
          array ['btree_gin',
                'btree_gist',
                'chkpass',
                'citext',
                'cube',
                'hstore',
                'isn',
                'ip4r',
                'ltree',
                'lo',
                'postgresql-hll',
                'prefix',
                'postgis',
                'postgis_raster',
                'postgis_sfcgal',
                'postgis_tiger_geocoder',
                'postgis_topology',
                'address_standardizer',
                'address_standardizer_data_us',
                'plpgsql',
                'plv8',
                'amcheck',
                'auto_explain',
                'dblink',
                'decoderbufs',
                'dict_int',
                'earthdistance',
                'fuzzystrmatch',
                'intagg',
                'intarray',
                'oracle_fdw',
                'orafce',
                'pageinspect',
                'pgAudit',
                'pg_bigm',
                'pg_buffercache',
                'pg_cron',
                'pgcrypto',
                'pglogical',
                'pgfincore',
                'pg_freespacemap',
                'pg_hint_plan',
                'pgoutput',
                'pg_partman',
                'pg_prewarm',
                'pg_proctab',
                'pg_repack',
                'pgrowlocks',
                'pgstattuple',
                'pg_similarity',
                'pg_stat_statements',
                'pgtap',
                'pg_trgm',
                'pgtt',
                'pgvector',
                'pg_visibility',
                'pg_wait_sampling',
                'plproxy',
                'postgres_fdw',
                'postgresql_anonymizer',
                'rdkit',
                'refint',
                'sslinfo',
                'tablefunc',
                'tsm_system_rows',
                'tsm_system_time',
                'unaccent',
                'uuid-ossp']
        ) then e.extname
        else null
      end
    ) as unsupported_extension_count
  from pg_extension e
),
calculated_metrics as (
  select 'VERSION_NUM' as metric_name,
    current_setting('server_version_num') as metric_value
  union
  select 'VERSION' as metric_name,
    current_setting('server_version') as metric_value
  union
  select 'UNSUPPORTED_EXTENSION_COUNT' as metric_name,
    cast(es.unsupported_extension_count as varchar) as metric_value
  from extension_summary es
  union
  select 'SUPPORTED_EXTENSION_COUNT' as metric_name,
    cast(es.supported_extension_count as varchar) as metric_value
  from extension_summary es
  union all
  select 'EXTENSION_COUNT' as metric_name,
    cast(es.total_extension_count as varchar) as metric_value
  from extension_summary es
  union all
  select 'FOREIGN_TABLE_COUNT' as metric_name,
    cast(fts.total_foreign_table_count as varchar) as metric_value
  from foreign_table_summary fts
  union all
  select 'UNSUPPORTED_FOREIGN_TABLE_COUNT' as metric_name,
    cast(fts.unsupported_foreign_table_count as varchar) as metric_value
  from foreign_table_summary fts
  union all
  select 'SUPPORTED_FOREIGN_TABLE_COUNT' as metric_name,
    cast(fts.supported_foreign_table_count as varchar) as metric_value
  from foreign_table_summary fts
  union all
  select 'TABLE_COUNT' as metric_name,
    cast(ts.total_table_count as varchar) as metric_value
  from table_summary ts
),
src as (
  select 'CALCULATED_METRIC' as metric_category,
    metric_name,
    metric_value
  from calculated_metrics
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.metric_category as metric_category,
  src.metric_name as metric_name,
  src.metric_value as metric_value
from src;
