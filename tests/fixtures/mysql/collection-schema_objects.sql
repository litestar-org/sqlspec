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
-- name: collection-mysql-schema-objects
select @PKEY as pkey,
  @DMA_SOURCE_ID as dma_source_id,
  @DMA_MANUAL_ID as dma_manual_id,
  src.object_catalog as object_catalog,
  src.object_schema as object_schema,
  src.object_category as object_category,
  src.object_type as object_type,
  src.object_owner_schema as object_owner_schema,
  src.object_owner as object_owner,
  src.object_name as object_name
from (
    select i.CONSTRAINT_CATALOG as object_catalog,
      i.CONSTRAINT_SCHEMA as object_schema,
      'CONSTRAINT' as object_category,
      concat(i.CONSTRAINT_TYPE, ' CONSTRAINT') as object_type,
      i.TABLE_SCHEMA as object_owner_schema,
      i.TABLE_NAME as object_owner,
      i.CONSTRAINT_NAME as object_name
    from information_schema.TABLE_CONSTRAINTS i
    where i.CONSTRAINT_SCHEMA not in (
        'mysql',
        'information_schema',
        'performance_schema',
        'sys'
      )
    union
    select i.TRIGGER_CATALOG as object_catalog,
      i.TRIGGER_SCHEMA as object_schema,
      'TRIGGER' as object_category,
      concat(
        i.ACTION_TIMING,
        ' ',
        i.EVENT_MANIPULATION,
        ' TRIGGER'
      ) as object_type,
      i.TRIGGER_SCHEMA as object_owner_schema,
      i.definer as object_owner,
      i.TRIGGER_NAME as object_name
    from information_schema.TRIGGERS i
    where i.TRIGGER_SCHEMA not in (
        'mysql',
        'information_schema',
        'performance_schema',
        'sys'
      )
    union
    select i.TABLE_CATALOG as object_catalog,
      i.TABLE_SCHEMA as object_schema,
      'VIEW' as object_category,
      i.TABLE_TYPE as object_type,
      null as object_schema_schema,
      null as object_owner,
      i.TABLE_NAME as object_name
    from information_schema.TABLES i
    where i.table_type = 'VIEW'
      and i.TABLE_SCHEMA not in (
        'mysql',
        'information_schema',
        'performance_schema',
        'sys'
      )
    union
    select i.TABLE_CATALOG as object_catalog,
      i.TABLE_SCHEMA as object_schema,
      'TABLE' as object_category,
      if(
        pt.PARTITION_METHOD is null,
        'TABLE',
        if(
          pt.SUBPARTITION_METHOD is not null,
          concat(
            'TABLE-COMPOSITE_PARTITIONED-',
            pt.PARTITION_METHOD,
            '-',
            pt.SUBPARTITION_METHOD
          ),
          concat('TABLE-PARTITIONED-', pt.PARTITION_METHOD)
        )
      ) as object_type,
      null as object_schema_schema,
      null as object_owner,
      i.TABLE_NAME as object_name
    from information_schema.TABLES i
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
        i.TABLE_NAME = pt.TABLE_NAME
        and i.TABLE_SCHEMA = pt.TABLE_SCHEMA
      )
    where i.table_type != 'VIEW'
      and i.TABLE_SCHEMA not in (
        'mysql',
        'information_schema',
        'performance_schema',
        'sys'
      )
    union
    select i.ROUTINE_CATALOG as object_catalog,
      i.ROUTINE_SCHEMA as object_schema,
      'PROCEDURE' as object_category,
      i.ROUTINE_TYPE as object_type,
      i.ROUTINE_SCHEMA as object_owner_schema,
      i.definer as object_owner,
      i.ROUTINE_NAME as object_name
    from information_schema.ROUTINES i
    where i.ROUTINE_TYPE = 'PROCEDURE'
      and i.ROUTINE_SCHEMA not in (
        'mysql',
        'information_schema',
        'performance_schema',
        'sys'
      )
    union
    select i.ROUTINE_CATALOG as object_catalog,
      i.ROUTINE_SCHEMA as object_schema,
      'FUNCTION' as object_category,
      i.ROUTINE_TYPE as object_type,
      i.ROUTINE_SCHEMA as object_owner_schema,
      i.definer as object_owner,
      i.ROUTINE_NAME as object_name
    from information_schema.ROUTINES i
    where i.ROUTINE_TYPE = 'FUNCTION'
      and i.ROUTINE_SCHEMA not in (
        'mysql',
        'information_schema',
        'performance_schema',
        'sys'
      )
    union
    select i.EVENT_CATALOG as object_catalog,
      i.EVENT_SCHEMA as object_schema,
      'EVENT' as object_category,
      i.EVENT_TYPE as object_type,
      i.EVENT_SCHEMA as object_owner_schema,
      i.definer as object_owner,
      i.EVENT_NAME as object_name
    from information_schema.EVENTS i
    where i.EVENT_SCHEMA not in (
        'mysql',
        'information_schema',
        'performance_schema',
        'sys'
      )
    union
    select i.TABLE_CATALOG as object_catalog,
      i.TABLE_SCHEMA as object_schema,
      'INDEX' as object_category,
      case
        when i.INDEX_TYPE = 'BTREE' then 'INDEX'
        when i.INDEX_TYPE = 'HASH' then 'INDEX-HASH'
        when i.INDEX_TYPE = 'FULLTEXT' then 'INDEX-FULLTEXT'
        when i.INDEX_TYPE = 'SPATIAL' then 'INDEX-SPATIAL'
        else 'INDEX-UNCATEGORIZED'
      end as object_type,
      i.TABLE_SCHEMA as object_owner_schema,
      i.TABLE_NAME as object_owner,
      i.INDEX_NAME as object_name
    from information_schema.STATISTICS i
    where i.INDEX_NAME != 'PRIMARY'
      and i.TABLE_SCHEMA not in (
        'mysql',
        'information_schema',
        'performance_schema',
        'sys'
      )
  ) src;
