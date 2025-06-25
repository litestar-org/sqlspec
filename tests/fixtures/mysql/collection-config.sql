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
-- name: collection-mysql-config
select distinct @PKEY as pkey,
  @DMA_SOURCE_ID as dma_source_id,
  @DMA_MANUAL_ID as dma_manual_id,
  src.variable_category as variable_category,
  src.variable_name as variable_name,
  src.variable_value as variable_value
from (
    select 'ALL_VARIABLES' as variable_category,
      variable_name,
      variable_value
    from (
        select variable_name,
          variable_value
        from (
            select upper(variable_name) as variable_name,
              variable_value
            from performance_schema.global_variables
            union
            select upper(variable_name),
              variable_value
            from performance_schema.session_variables
            where variable_name not in (
                select variable_name
                from performance_schema.global_variables
              )
          ) a
        where a.variable_name not in ('FT_BOOLEAN_SYNTAX')
          and a.variable_name not like '%PUBLIC_KEY'
          and a.variable_name not like '%PRIVATE_KEY'
      ) all_vars
    union
    select 'GLOBAL_STATUS' as variable_category,
      variable_name,
      variable_value
    from (
        select upper(variable_name) as variable_name,
          variable_value
        from performance_schema.global_status a
        where a.variable_name not in ('FT_BOOLEAN_SYNTAX')
          and a.variable_name not like '%PUBLIC_KEY'
          and a.variable_name not like '%PRIVATE_KEY'
      ) global_status
    union
    select 'CALCULATED_METRIC' as variable_category,
      variable_name,
      variable_value
    from (
        select 'IS_MARIADB' as variable_name,
          if(upper(gv.variable_value) like '%MARIADB%', 1, 0) as variable_value
        from performance_schema.global_variables gv
        where gv.variable_name = 'VERSION'
        union
        select 'TABLE_SIZE' as variable_name,
          total_data_size_bytes as variable_value
        from (
            select sum(data_length) as total_data_size_bytes
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
                  if(pks.table_name is not null, 1, 0) as has_primary_key
                from information_schema.TABLES t
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
                where t.table_schema not in (
                    'mysql',
                    'information_schema',
                    'performance_schema',
                    'sys'
                  )
              ) user_tables
          ) data_summary
        union
        select 'TABLE_NO_INNODB_SIZE' as variable_name,
          non_innodb_data_size_bytes as variable_value
        from (
            select sum(
                if(upper(table_engine) != 'INNODB', data_length, 0)
              ) as non_innodb_data_size_bytes
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
                  if(pks.table_name is not null, 1, 0) as has_primary_key
                from information_schema.TABLES t
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
                where t.table_schema not in (
                    'mysql',
                    'information_schema',
                    'performance_schema',
                    'sys'
                  )
              ) user_tables
          ) data_summary
        union
        select 'TABLE_INNODB_SIZE' as variable_name,
          innodb_data_size_bytes as variable_value
        from (
            select sum(
                if(upper(table_engine) = 'INNODB', data_length, 0)
              ) as innodb_data_size_bytes
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
                  if(pks.table_name is not null, 1, 0) as has_primary_key
                from information_schema.TABLES t
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
                where t.table_schema not in (
                    'mysql',
                    'information_schema',
                    'performance_schema',
                    'sys'
                  )
              ) user_tables
          ) data_summary
        union
        select 'TABLE_COUNT' as variable_name,
          total_table_count as variable_value
        from (
            select count(table_name) as total_table_count
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
                  if(pks.table_name is not null, 1, 0) as has_primary_key
                from information_schema.TABLES t
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
                where t.table_schema not in (
                    'mysql',
                    'information_schema',
                    'performance_schema',
                    'sys'
                  )
              ) user_tables
          ) data_summary
        union
        select 'TABLE_NO_INNODB_COUNT' as variable_name,
          non_innodb_table_count as variable_value
        from (
            select sum(if(upper(table_engine) != 'INNODB', 1, 0)) as non_innodb_table_count
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
                  if(pks.table_name is not null, 1, 0) as has_primary_key
                from information_schema.TABLES t
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
                where t.table_schema not in (
                    'mysql',
                    'information_schema',
                    'performance_schema',
                    'sys'
                  )
              ) user_tables
          ) data_summary
        union
        select 'TABLE_INNODB_COUNT' as variable_name,
          innodb_table_count as variable_value
        from (
            select sum(if(upper(table_engine) = 'INNODB', 1, 0)) as innodb_table_count
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
                  if(pks.table_name is not null, 1, 0) as has_primary_key
                from information_schema.TABLES t
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
                where t.table_schema not in (
                    'mysql',
                    'information_schema',
                    'performance_schema',
                    'sys'
                  )
              ) user_tables
          ) data_summary
        union
        select 'TABLE_NO_PK_COUNT' as variable_name,
          total_tables_without_primary_key as variable_value
        from (
            select sum(if(has_primary_key = 0, 1, 0)) as total_tables_without_primary_key
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
                  if(pks.table_name is not null, 1, 0) as has_primary_key
                from information_schema.TABLES t
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
                where t.table_schema not in (
                    'mysql',
                    'information_schema',
                    'performance_schema',
                    'sys'
                  )
              ) user_tables
          ) data_summary
        union
        select 'MYSQLX_PLUGIN' as variable_name,
          p.mysqlx_plugin_enabled as variable_value
        from (
            select if(agg.mysqlx_plugin > 0, 1, 0) as mysqlx_plugin_enabled
            from (
                select sum(
                    if(
                      upper(p.plugin_name) like '%MYSQLX%',
                      1,
                      0
                    )
                  ) as mysqlx_plugin
                from (
                    select p.plugin_name as plugin_name,
                      p.PLUGIN_STATUS
                    from information_schema.PLUGINS p
                  ) p
              ) agg
          ) p
        union
        select 'MEMCACHED_PLUGIN' as variable_name,
          p.memcached_plugin_enabled as variable_value
        from (
            select if(agg.memcached_plugin > 0, 1, 0) as memcached_plugin_enabled
            from (
                select sum(
                    if(
                      upper(p.plugin_name) like '%MEMCACHED%',
                      1,
                      0
                    )
                  ) as memcached_plugin
                from (
                    select p.plugin_name as plugin_name,
                      p.PLUGIN_STATUS
                    from information_schema.PLUGINS p
                  ) p
              ) agg
          ) p
        union
        select 'CLONE_PLUGIN' as variable_name,
          p.clone_plugin_enabled as variable_value
        from (
            select if(agg.clone_plugin > 0, 1, 0) as clone_plugin_enabled
            from (
                select sum(
                    if(
                      upper(p.plugin_name) like '%CLONE%',
                      1,
                      0
                    )
                  ) as clone_plugin
                from (
                    select p.plugin_name as plugin_name,
                      p.PLUGIN_STATUS
                    from information_schema.PLUGINS p
                  ) p
              ) agg
          ) p
        union
        select 'KEYRING_PLUGIN' as variable_name,
          p.keyring_plugin_enabled as variable_value
        from (
            select if(agg.keyring_plugin > 0, 1, 0) as keyring_plugin_enabled
            from (
                select sum(
                    if(
                      upper(p.plugin_name) like '%KEYRING%',
                      1,
                      0
                    )
                  ) as keyring_plugin
                from (
                    select p.plugin_name as plugin_name,
                      p.PLUGIN_STATUS
                    from information_schema.PLUGINS p
                  ) p
              ) agg
          ) p
        union
        select 'VALIDATE_PASSWORD_PLUGIN' as variable_name,
          p.validate_password_plugin_enabled as variable_value
        from (
            select if(agg.validate_password_plugin > 0, 1, 0) as validate_password_plugin_enabled
            from (
                select sum(
                    if(
                      upper(p.plugin_name) like '%VALIDATE_PASSWORD%',
                      1,
                      0
                    )
                  ) as validate_password_plugin
                from (
                    select p.plugin_name as plugin_name,
                      p.PLUGIN_STATUS
                    from information_schema.PLUGINS p
                  ) p
              ) agg
          ) p
        union
        select 'THREAD_POOL_PLUGIN' as variable_name,
          p.thread_pool_plugin_enabled as variable_value
        from (
            select if(agg.thread_pool_plugin > 0, 1, 0) as thread_pool_plugin_enabled
            from (
                select sum(
                    if(
                      upper(p.plugin_name) like '%THREAD_POOL%',
                      1,
                      0
                    )
                  ) as thread_pool_plugin
                from (
                    select p.plugin_name as plugin_name,
                      p.PLUGIN_STATUS
                    from information_schema.PLUGINS p
                  ) p
              ) agg
          ) p
        union
        select 'FIREWALL_PLUGIN' as variable_name,
          p.firewall_plugin_enabled as variable_value
        from (
            select if(agg.firewall_plugin > 0, 1, 0) as firewall_plugin_enabled
            from (
                select sum(
                    if(
                      upper(p.plugin_name) like '%FIREWALL%',
                      1,
                      0
                    )
                  ) as firewall_plugin
                from (
                    select p.plugin_name as plugin_name,
                      p.PLUGIN_STATUS
                    from information_schema.PLUGINS p
                  ) p
              ) agg
          ) p
        union
        select 'VERSION_NUM' as variable_name,
          if(
            version() rlike '^[0-9]+\.[0-9]+\.[0-9]+$' = 1,
            version(),
            concat(SUBSTRING_INDEX(VERSION(), '.', 2), '.0')
          ) as variable_value
      ) calculated_metrics
  ) src;
