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
-- name: collection-mysql-table-details
select
  /*+ MAX_EXECUTION_TIME(5000) */
  @PKEY as pkey,
  @DMA_SOURCE_ID as dma_source_id,
  @DMA_MANUAL_ID as dma_manual_id,
  table_schema as table_schema,
  table_name as table_name,
  table_engine as table_engine,
  table_rows as table_rows,
  data_length as data_length,
  index_length as index_length,
  is_compressed as is_compressed,
  is_partitioned as is_partitioned,
  partition_count as partition_count,
  index_count as index_count,
  fulltext_index_count as fulltext_index_count,
  is_encrypted as is_encrypted,
  spatial_index_count as spatial_index_count,
  has_primary_key as has_primary_key,
  row_format as row_format,
  table_type as table_type
from (
    select t.table_schema as table_schema,
      t.table_name as table_name,
      t.table_rows as table_rows,
      t.DATA_LENGTH as DATA_LENGTH,
      t.INDEX_LENGTH as INDEX_LENGTH,
      t.DATA_LENGTH + t.INDEX_LENGTH as total_length,
      t.ROW_FORMAT as row_format,
      t.TABLE_TYPE as table_type,
      t.ENGINE as table_engine,
      if(pks.table_name is not null, 1, 0) as has_primary_key,
      if(t.ROW_FORMAT = 'COMPRESSED', 1, 0) as is_compressed,
      if(pt.PARTITION_METHOD is not null, 1, 0) as is_partitioned,
      if(
        locate('ENCRYPTED', upper(t.CREATE_OPTIONS)) > 0,
        1,
        0
      ) as is_encrypted,
      COALESCE(pt.PARTITION_COUNT, 0) as partition_count,
      COALESCE(idx.index_count, 0) as index_count,
      COALESCE(idx.fulltext_index_count, 0) as fulltext_index_count,
      COALESCE(idx.spatial_index_count, 0) as spatial_index_count
    from information_schema.TABLES t
      left join (
        select TABLE_SCHEMA,
          TABLE_NAME,
          PARTITION_METHOD,
          SUBPARTITION_METHOD,
          count(1) as PARTITION_COUNT
        from information_schema.PARTITIONS
        where table_schema not in (
            'mysql',
            'information_schema',
            'performance_schema',
            'sys'
          )
        group by TABLE_SCHEMA,
          TABLE_NAME,
          PARTITION_METHOD,
          SUBPARTITION_METHOD
      ) pt on (
        t.table_schema = pt.table_schema
        and t.TABLE_NAME = pt.TABLE_NAME
      )
      left join (
        select table_schema,
          TABLE_NAME
        from information_schema.statistics
        where table_schema not in (
            'mysql',
            'information_schema',
            'performance_schema',
            'sys'
          )
        group by table_schema,
          TABLE_NAME,
          index_name
        having SUM(
            if(
              non_unique = 0
              and NULLABLE != 'YES',
              1,
              0
            )
          ) = count(*)
      ) pks on (
        t.table_schema = pks.table_schema
        and t.TABLE_NAME = pks.TABLE_NAME
      )
      left join (
        select s.table_schema,
          s.table_name,
          count(1) as index_count,
          sum(
            if(s.INDEX_TYPE = 'FULLTEXT', 1, 0)
          ) as fulltext_index_count,
          sum(if(s.INDEX_TYPE = 'SPATIAL', 1, 0)) as spatial_index_count
        from information_schema.STATISTICS s
        where s.table_schema not in (
            'mysql',
            'information_schema',
            'performance_schema',
            'sys'
          )
        group by s.table_schema,
          s.table_name
      ) idx on (
        t.table_schema = idx.table_schema
        and t.TABLE_NAME = idx.TABLE_NAME
      )
    where t.table_schema not in (
        'mysql',
        'information_schema',
        'performance_schema',
        'sys'
      )
  ) user_tables;
