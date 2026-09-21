package com.flying.orm.rdb.metadata;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.type.DatabaseTypes;

import java.util.List;

/**
 * SQL Server 的元数据查询定义。这里仅保存 INFORMATION_SCHEMA 和系统目录的方言事实，
 * 查询编排和结果组装统一由 {@link InformationSchemaFormMetadataReader} 负责。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
final class SqlServerMetadataQueries {

    private static final String OFFSET_TIME_MARKER = "[[flying-orm:v1:OFFSET_TIME]]";
    private static final String COMMENT_ESCAPE = "[[flying-orm:v1:COMMENT]]";

    private static final String BASE_COLUMNS_SQL = """
            select c.COLUMN_NAME,
                   c.TABLE_SCHEMA as RESOLUTION_SCHEMA,
                   case
                       when convert(varbinary(128), left(cast(ep.value as nvarchar(4000)),
                                                         len('${COMMENT_ESCAPE}')))
                                = convert(varbinary(128), N'${COMMENT_ESCAPE}')
                           then c.DATA_TYPE
                       when lower(c.DATA_TYPE) = 'varchar' and c.CHARACTER_MAXIMUM_LENGTH = 32
                            and convert(varbinary(128), left(cast(ep.value as nvarchar(4000)),
                                                             len('${OFFSET_TIME_MARKER}')))
                                = convert(varbinary(128), N'${OFFSET_TIME_MARKER}')
                           then 'OFFSET_TIME'
                       when lower(c.DATA_TYPE) in ('nvarchar', 'varchar')
                            and c.CHARACTER_MAXIMUM_LENGTH = -1
                           then lower(c.DATA_TYPE) + '(max)'
                       else c.DATA_TYPE
                   end as DATA_TYPE,
                   case when lower(c.DATA_TYPE) in ('nvarchar', 'varchar', 'varbinary')
                                  and c.CHARACTER_MAXIMUM_LENGTH = -1
                        then lower(c.DATA_TYPE) + '(max)'
                        else c.DATA_TYPE end as PHYSICAL_DATA_TYPE,
                   c.CHARACTER_MAXIMUM_LENGTH,
                   c.NUMERIC_PRECISION,
                   c.NUMERIC_SCALE,
                   case when c.DATA_TYPE in ('time', 'datetime2', 'datetimeoffset')
                        then c.DATETIME_PRECISION end as TEMPORAL_PRECISION,
                   case
                       when convert(varbinary(128), left(cast(ep.value as nvarchar(4000)),
                                                         len('${COMMENT_ESCAPE}')))
                                = convert(varbinary(128), N'${COMMENT_ESCAPE}')
                           then substring(cast(ep.value as nvarchar(4000)), len('${COMMENT_ESCAPE}') + 1, 4000)
                       when lower(c.DATA_TYPE) = 'varchar' and c.CHARACTER_MAXIMUM_LENGTH = 32
                            and convert(varbinary(128), left(cast(ep.value as nvarchar(4000)),
                                                             len('${OFFSET_TIME_MARKER}')))
                                = convert(varbinary(128), N'${OFFSET_TIME_MARKER}')
                           then nullif(substring(cast(ep.value as nvarchar(4000)),
                                                len('${OFFSET_TIME_MARKER}') + 1, 4000), '')
                       else cast(ep.value as nvarchar(4000))
                   end as REMARKS,
                   c.IS_NULLABLE as NULLABLE,
                   dc.definition as COLUMN_DEFAULT,
                   dc.name as DEFAULT_CONSTRAINT_NAME,
                   dc.definition as GENERATION_EXPRESSION,
                   try_convert(bigint, coalesce(idc.seed_value, generation_sequence.start_value))
                       as GENERATION_START,
                   try_convert(bigint, coalesce(idc.increment_value, generation_sequence.increment))
                       as GENERATION_INCREMENT,
                   case when idc.column_id is not null then 100
                        when generation_sequence.object_id is not null
                            then coalesce(generation_sequence.cache_size, 0)
                        else null end as GENERATION_CACHE,
                   null as COLUMN_CHARSET,
                   c.COLLATION_NAME as COLUMN_COLLATION,
                   cast(case when idc.column_id is null then 0 else 1 end as bit) as IS_IDENTITY,
                   cast(case when sc.is_computed = 0
                                  and sc.is_rowguidcol = 0
                                  and sc.is_sparse = 0
                                  and sc.is_column_set = 0
                                  and sc.is_filestream = 0
                                  and isnull(columnproperty(sc.object_id, sc.name,
                                                            'GeneratedAlwaysType'), 0) = 0
                                  and isnull(columnproperty(sc.object_id, sc.name, 'IsHidden'), 0) = 0
                                  and sc.rule_object_id = 0
                                  and c.DOMAIN_NAME is null
                                  and (sc.default_object_id = 0 or dc.object_id is not null)
                                  and (idc.column_id is null or (
                                      idc.is_not_for_replication = 0
                                      and try_convert(bigint, idc.seed_value) is not null
                                      and try_convert(bigint, idc.increment_value) is not null
                                  ))
                                  and (generation_sequence.object_id is null
                                       or (generation_sequence.is_cycling = 0
                                           and generation_sequence.is_cached = 1
                                           and try_convert(bigint, generation_sequence.start_value) is not null
                                           and try_convert(bigint, generation_sequence.increment) is not null
                                           and generation_sequence.data_type = lower(c.DATA_TYPE)
                                           and (generation_sequence.data_type not in ('decimal', 'numeric')
                                                or (generation_sequence.numeric_precision = c.NUMERIC_PRECISION
                                                    and generation_sequence.numeric_scale = c.NUMERIC_SCALE))
                                           and generation_sequence.default_bounds = 1))
                                  and not (lower(c.DATA_TYPE) = 'varchar'
                                           and c.CHARACTER_MAXIMUM_LENGTH = -1)
                             then 1 else 0 end as bit) as COLUMN_REPRESENTABLE,
                   case
                       when sc.is_computed = 1 then 'computed column'
                       when sc.is_rowguidcol = 1 then 'rowguid column'
                       when sc.is_sparse = 1 then 'sparse column'
                       when sc.is_column_set = 1 then 'column set'
                       when sc.is_filestream = 1 then 'filestream column'
                       when isnull(columnproperty(sc.object_id, sc.name, 'GeneratedAlwaysType'), 0) <> 0
                           then 'generated-always column'
                       when isnull(columnproperty(sc.object_id, sc.name, 'IsHidden'), 0) <> 0
                           then 'hidden column'
                       when sc.rule_object_id <> 0 then 'bound rule'
                       when c.DOMAIN_NAME is not null then 'alias data type'
                       when sc.default_object_id <> 0 and dc.object_id is null then 'bound default'
                       when idc.is_not_for_replication = 1 then 'identity not for replication'
                       when idc.column_id is not null
                                and (try_convert(bigint, idc.seed_value) is null
                                     or try_convert(bigint, idc.increment_value) is null)
                           then 'identity options exceed canonical range'
                       when generation_sequence.is_cycling = 1 then 'cycling sequence'
                       when generation_sequence.object_id is not null
                                and generation_sequence.is_cached = 0
                           then 'non-cached sequence'
                       when generation_sequence.object_id is not null
                                and (try_convert(bigint, generation_sequence.start_value) is null
                                     or try_convert(bigint, generation_sequence.increment) is null)
                           then 'sequence options exceed canonical range'
                       when generation_sequence.object_id is not null
                                and (generation_sequence.data_type <> lower(c.DATA_TYPE)
                                     or (generation_sequence.data_type in ('decimal', 'numeric')
                                         and (generation_sequence.numeric_precision <> c.NUMERIC_PRECISION
                                              or generation_sequence.numeric_scale <> c.NUMERIC_SCALE)))
                           then 'sequence data type differs from column'
                       when generation_sequence.object_id is not null
                                and generation_sequence.default_bounds = 0
                           then 'sequence bounds are not type defaults'
                       when lower(c.DATA_TYPE) = 'varchar' and c.CHARACTER_MAXIMUM_LENGTH = -1
                           then 'varchar(max) cannot be reconstructed exactly'
                       else null
                   end as UNSUPPORTED_COLUMN_REASON,
                   case when pk.CONSTRAINT_NAME is null then cast(0 as bit) else cast(1 as bit) end as PRIMARY_KEY
            from INFORMATION_SCHEMA.COLUMNS c
            left join (
                select pk_kcu.TABLE_SCHEMA,
                       pk_kcu.TABLE_NAME,
                       pk_kcu.COLUMN_NAME,
                       pk_kcu.CONSTRAINT_NAME
                from INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk_kcu
                join INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk_tc
                  on pk_tc.CONSTRAINT_SCHEMA = pk_kcu.CONSTRAINT_SCHEMA
                 and pk_tc.TABLE_SCHEMA = pk_kcu.TABLE_SCHEMA
                 and pk_tc.TABLE_NAME = pk_kcu.TABLE_NAME
                 and pk_tc.CONSTRAINT_NAME = pk_kcu.CONSTRAINT_NAME
                 and pk_tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ) pk
              on pk.TABLE_SCHEMA = c.TABLE_SCHEMA
             and pk.TABLE_NAME = c.TABLE_NAME
             and pk.COLUMN_NAME = c.COLUMN_NAME
            left join sys.schemas s
                   on s.name = c.TABLE_SCHEMA
            left join sys.tables t
                   on t.name = c.TABLE_NAME
                  and t.schema_id = s.schema_id
            left join sys.columns sc
                   on sc.object_id = t.object_id
                  and sc.name = c.COLUMN_NAME
            left join sys.identity_columns idc
                   on idc.object_id = sc.object_id
                  and idc.column_id = sc.column_id
            left join sys.default_constraints dc
                   on dc.object_id = sc.default_object_id
            left join (
                select generation_dependency.referencing_id,
                       referenced_sequence.object_id,
                       referenced_sequence.start_value,
                       referenced_sequence.increment,
                       referenced_sequence.cache_size,
                       referenced_sequence.is_cycling,
                       referenced_sequence.is_cached,
                       lower(type_name(referenced_sequence.system_type_id)) as data_type,
                       referenced_sequence.precision as numeric_precision,
                       referenced_sequence.scale as numeric_scale,
                       cast(case
                           when try_convert(decimal(38, 0), referenced_sequence.minimum_value) = case
                                    when type_name(referenced_sequence.system_type_id) = 'tinyint' then 0
                                    when type_name(referenced_sequence.system_type_id) = 'smallint' then -32768
                                    when type_name(referenced_sequence.system_type_id) = 'int' then -2147483648
                                    when type_name(referenced_sequence.system_type_id) = 'bigint'
                                        then convert(decimal(38, 0), '-9223372036854775808')
                                    when type_name(referenced_sequence.system_type_id) in ('decimal', 'numeric')
                                        then -convert(decimal(38, 0),
                                                      replicate('9', referenced_sequence.precision))
                                    else null
                                end
                            and try_convert(decimal(38, 0), referenced_sequence.maximum_value) = case
                                    when type_name(referenced_sequence.system_type_id) = 'tinyint' then 255
                                    when type_name(referenced_sequence.system_type_id) = 'smallint' then 32767
                                    when type_name(referenced_sequence.system_type_id) = 'int' then 2147483647
                                    when type_name(referenced_sequence.system_type_id) = 'bigint'
                                        then convert(decimal(38, 0), '9223372036854775807')
                                    when type_name(referenced_sequence.system_type_id) in ('decimal', 'numeric')
                                        then convert(decimal(38, 0),
                                                     replicate('9', referenced_sequence.precision))
                                    else null
                                end
                           then 1 else 0 end as bit) as default_bounds
                from sys.sql_expression_dependencies generation_dependency
                join sys.sequences referenced_sequence
                  on referenced_sequence.object_id = generation_dependency.referenced_id
                where generation_dependency.referenced_class = 1
            ) generation_sequence
                   on generation_sequence.referencing_id = dc.object_id
            left join sys.extended_properties ep
                   on ep.major_id = t.object_id
                  and ep.minor_id = sc.column_id
                  and ep.name = 'MS_Description'
            where c.TABLE_NAME = ?
            """.replace("${OFFSET_TIME_MARKER}", OFFSET_TIME_MARKER)
               .replace("${COMMENT_ESCAPE}", COMMENT_ESCAPE);

    private static final String BASE_INDEXES_SQL = """
            select i.name as INDEX_NAME,
                   c.name as COLUMN_NAME,
                   i.has_filter as INDEX_FILTERED,
                   i.filter_definition as INDEX_FILTER,
                   cast(case when i.is_unique = 1 then 1 else 0 end as bit) as UNIQUE_INDEX,
                   cast(case when i.type_desc = 'NONCLUSTERED'
                                  and (i.has_filter = 0 or i.is_unique = 1)
                                  and i.is_disabled = 0
                                  and i.is_hypothetical = 0
                                  and i.fill_factor = 0
                                  and i.is_padded = 0
                                  and i.ignore_dup_key = 0
                                  and i.allow_row_locks = 1
                                  and i.allow_page_locks = 1
                                  and exists (
                                      select 1
                                      from sys.filegroups index_filegroup
                                      where index_filegroup.data_space_id = i.data_space_id
                                        and index_filegroup.is_default = 1
                                  )
                                  and not exists (
                                      select 1
                                      from sys.index_columns partitioned
                                      where partitioned.object_id = i.object_id
                                        and partitioned.index_id = i.index_id
                                        and partitioned.partition_ordinal > 0
                                  )
                                  and not exists (
                                      select 1
                                      from sys.index_columns included
                                      where included.object_id = i.object_id
                                        and included.index_id = i.index_id
                                        and included.is_included_column = 1
                                  )
                             then 1 else 0 end as bit) as INDEX_REPRESENTABLE,
                   case when ic.is_descending_key = 0 then 'ASC' else 'DESC' end as INDEX_DIRECTION,
                   case
                       when i.type_desc <> 'NONCLUSTERED' then 'index type is not nonclustered rowstore'
                       when i.has_filter = 1 then 'filtered index'
                       when i.is_disabled = 1 then 'disabled index'
                       when i.is_hypothetical = 1 then 'hypothetical index'
                       when i.fill_factor <> 0 then 'non-default fill factor'
                       when i.is_padded = 1 then 'padded index'
                       when i.ignore_dup_key = 1 then 'ignore duplicate keys'
                       when i.allow_row_locks = 0 or i.allow_page_locks = 0
                           then 'non-default index locking options'
                       when not exists (
                           select 1
                           from sys.filegroups index_filegroup
                           where index_filegroup.data_space_id = i.data_space_id
                             and index_filegroup.is_default = 1
                       ) then 'non-default index filegroup'
                       when exists (
                           select 1
                           from sys.index_columns partitioned
                           where partitioned.object_id = i.object_id
                             and partitioned.index_id = i.index_id
                             and partitioned.partition_ordinal > 0
                       ) then 'partitioned index'
                       when exists (
                           select 1
                           from sys.index_columns included
                           where included.object_id = i.object_id
                             and included.index_id = i.index_id
                             and included.is_included_column = 1
                       ) then 'included column'
                       else null
                   end as UNSUPPORTED_INDEX_REASON
            from sys.indexes i
            join sys.tables t
              on t.object_id = i.object_id
            join sys.schemas s
              on s.schema_id = t.schema_id
            join sys.index_columns ic
              on ic.object_id = i.object_id
             and ic.index_id = i.index_id
            join sys.columns c
              on c.object_id = t.object_id
             and c.column_id = ic.column_id
            where t.name = ?
              and i.is_primary_key = 0
              and not exists (
                  select 1
                  from sys.key_constraints owned_constraint
                  where owned_constraint.parent_object_id = i.object_id
                    and owned_constraint.unique_index_id = i.index_id
              )
              and i.name is not null
              and ic.is_included_column = 0
            """;

    private static final String BASE_FOREIGN_KEYS_SQL = """
            select ps.name as TABLE_SCHEMA,
                   fk.name as FOREIGN_KEY_NAME,
                   pc.name as COLUMN_NAME,
                   rs.name as REFERENCED_TABLE_SCHEMA,
                   rt.name as REFERENCED_TABLE_NAME,
                   rc.name as REFERENCED_COLUMN_NAME,
                   fk.delete_referential_action_desc as ON_DELETE,
                   fk.update_referential_action_desc as ON_UPDATE,
                   cast(case when fk.is_disabled = 0
                                  and fk.is_not_trusted = 0
                                  and fk.is_not_for_replication = 0
                             then 1 else 0 end as bit) as CONSTRAINT_REPRESENTABLE
            from sys.foreign_keys fk
            join sys.foreign_key_columns fkc
              on fkc.constraint_object_id = fk.object_id
            join sys.tables pt
              on pt.object_id = fk.parent_object_id
            join sys.schemas ps
              on ps.schema_id = pt.schema_id
            join sys.columns pc
              on pc.object_id = pt.object_id
             and pc.column_id = fkc.parent_column_id
            join sys.tables rt
              on rt.object_id = fk.referenced_object_id
            join sys.schemas rs
              on rs.schema_id = rt.schema_id
            join sys.columns rc
              on rc.object_id = rt.object_id
             and rc.column_id = fkc.referenced_column_id
            where pt.name = ?
            """;

    private static final String BASE_TABLE_SQL = """
            select cast(ep.value as nvarchar(4000)) as TABLE_COMMENT,
                   cast(case when t.is_ms_shipped = 0
                                  and t.is_filetable = 0
                                  and t.large_value_types_out_of_row = 0
                                  and t.text_in_row_limit = 0
                                  and t.lock_escalation_desc = 'TABLE'
                                  and isnull(objectpropertyex(t.object_id, 'TableIsMemoryOptimized'), 0) = 0
                                  and isnull(objectpropertyex(t.object_id, 'TableTemporalType'), 0) = 0
                                  and not exists (
                                      select 1
                                      from sys.indexes partitioned_index
                                      join sys.data_spaces partition_space
                                        on partition_space.data_space_id = partitioned_index.data_space_id
                                      where partitioned_index.object_id = t.object_id
                                        and partition_space.type = 'PS'
                                  )
                                  and not exists (
                                      select 1
                                      from sys.partitions compressed_partition
                                      where compressed_partition.object_id = t.object_id
                                        and compressed_partition.data_compression <> 0
                                  )
                                  and not exists (
                                      select 1
                                      from sys.indexes table_storage
                                      left join sys.filegroups table_filegroup
                                        on table_filegroup.data_space_id = table_storage.data_space_id
                                      where table_storage.object_id = t.object_id
                                        and table_storage.index_id in (0, 1)
                                        and (table_filegroup.data_space_id is null
                                             or table_filegroup.is_default = 0)
                                  )
                             then 1 else 0 end as bit) as TABLE_REPRESENTABLE
            from sys.tables t
            join sys.schemas s on s.schema_id = t.schema_id
            left join sys.extended_properties ep
              on ep.class = 1
             and ep.major_id = t.object_id
             and ep.minor_id = 0
             and ep.name = 'MS_Description'
            where t.name = ?
            """;

    private static final String BASE_PRIMARY_KEY_SQL = keyConstraintSql("PK", "CLUSTERED");
    private static final String BASE_UNIQUE_SQL = keyConstraintSql("UQ", "NONCLUSTERED");

    private static final String BASE_CHECKS_SQL = """
            select cc.name as CONSTRAINT_NAME,
                   cc.definition as CHECK_EXPRESSION,
                   cast(case when cc.is_disabled = 0
                                  and cc.is_not_trusted = 0
                                  and cc.is_not_for_replication = 0
                             then 1 else 0 end as bit) as CHECK_REPRESENTABLE
            from sys.check_constraints cc
            join sys.tables t on t.object_id = cc.parent_object_id
            join sys.schemas s on s.schema_id = t.schema_id
            where t.name = ?
            """;

    private SqlServerMetadataQueries() {
    }

    static InformationSchemaFormMetadataReader.Queries queries() {
        return InformationSchemaFormMetadataReader.Queries.complete(
                SqlServerMetadataQueries::columnQuery,
                SqlServerMetadataQueries::indexQuery,
                SqlServerMetadataQueries::foreignKeyQuery,
                SqlServerMetadataQueries::logicalType,
                SqlServerMetadataQueries::tableQuery,
                SqlServerMetadataQueries::primaryKeyQuery,
                SqlServerMetadataQueries::uniqueConstraintQuery,
                SqlServerMetadataQueries::checkConstraintQuery,
                InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER);
    }

    private static SqlRequest columnQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            // 表名已经是一段身份；先引用再查 OBJECT_ID，避免字面点号再次被当成 schema 分隔符。
            String sql = BASE_COLUMNS_SQL + " and t.object_id = object_id(quotename(?)) order by c.ORDINAL_POSITION";
            return new SqlRequest(sql, List.of(safeTable, safeTable));
        }
        String sql = BASE_COLUMNS_SQL + " and c.TABLE_SCHEMA = ? order by c.ORDINAL_POSITION";
        return new SqlRequest(sql, List.of(safeTable, schema.trim()));
    }

    private static SqlRequest indexQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            String sql = BASE_INDEXES_SQL + " and t.object_id = object_id(quotename(?)) order by i.name, ic.key_ordinal";
            return new SqlRequest(sql, List.of(safeTable, safeTable));
        }
        String sql = BASE_INDEXES_SQL + " and s.name = ? order by i.name, ic.key_ordinal";
        return new SqlRequest(sql, List.of(safeTable, schema.trim()));
    }

    private static SqlRequest foreignKeyQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            String sql = BASE_FOREIGN_KEYS_SQL
                    + " and pt.object_id = object_id(quotename(?)) order by fk.name, fkc.constraint_column_id";
            return new SqlRequest(sql, List.of(safeTable, safeTable));
        }
        String sql = BASE_FOREIGN_KEYS_SQL + " and ps.name = ? order by fk.name, fkc.constraint_column_id";
        return new SqlRequest(sql, List.of(safeTable, schema.trim()));
    }

    private static SqlRequest tableQuery(String schema, String table) {
        return tableScopedQuery(BASE_TABLE_SQL, schema, table, "");
    }

    private static SqlRequest primaryKeyQuery(String schema, String table) {
        return tableScopedQuery(BASE_PRIMARY_KEY_SQL, schema, table,
                                " order by kc.name, ic.key_ordinal");
    }

    private static SqlRequest uniqueConstraintQuery(String schema, String table) {
        return tableScopedQuery(BASE_UNIQUE_SQL, schema, table,
                                " order by kc.name, ic.key_ordinal");
    }

    private static SqlRequest checkConstraintQuery(String schema, String table) {
        return tableScopedQuery(BASE_CHECKS_SQL, schema, table, " order by cc.name");
    }

    private static SqlRequest tableScopedQuery(
            String baseSql, String schema, String table, String orderBy) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            return new SqlRequest(baseSql + " and t.object_id = object_id(quotename(?))" + orderBy,
                                  List.of(safeTable, safeTable));
        }
        return new SqlRequest(baseSql + " and s.name = ?" + orderBy,
                              List.of(safeTable, schema.trim()));
    }

    private static String keyConstraintSql(String constraintType, String indexType) {
        String constraintIndexFlag = switch (constraintType) {
            case "PK" -> "i.is_primary_key = 1";
            case "UQ" -> "i.is_unique_constraint = 1";
            default -> throw new IllegalArgumentException("unsupported SQL Server key constraint type");
        };
        return """
                select kc.name as CONSTRAINT_NAME,
                       c.name as COLUMN_NAME,
                       cast(case when i.type_desc = '${INDEX_TYPE}'
                                      and i.is_unique = 1
                                      and ${CONSTRAINT_INDEX_FLAG}
                                      and i.is_disabled = 0
                                      and i.is_hypothetical = 0
                                      and i.has_filter = 0
                                      and i.fill_factor = 0
                                      and i.is_padded = 0
                                      and i.ignore_dup_key = 0
                                      and i.allow_row_locks = 1
                                      and i.allow_page_locks = 1
                                      and exists (
                                          select 1
                                          from sys.filegroups constraint_filegroup
                                          where constraint_filegroup.data_space_id = i.data_space_id
                                            and constraint_filegroup.is_default = 1
                                      )
                                      and ic.is_descending_key = 0
                                      and not exists (
                                          select 1
                                          from sys.index_columns extra_column
                                          where extra_column.object_id = i.object_id
                                            and extra_column.index_id = i.index_id
                                            and (extra_column.is_included_column = 1
                                                 or extra_column.partition_ordinal > 0)
                                      )
                                 then 1 else 0 end as bit) as CONSTRAINT_REPRESENTABLE
                from sys.key_constraints kc
                join sys.tables t on t.object_id = kc.parent_object_id
                join sys.schemas s on s.schema_id = t.schema_id
                join sys.indexes i
                  on i.object_id = kc.parent_object_id
                 and i.index_id = kc.unique_index_id
                join sys.index_columns ic
                  on ic.object_id = i.object_id
                 and ic.index_id = i.index_id
                 and ic.key_ordinal > 0
                join sys.columns c
                  on c.object_id = ic.object_id
                 and c.column_id = ic.column_id
                where kc.type = '${CONSTRAINT_TYPE}' and t.name = ?
                """.replace("${CONSTRAINT_TYPE}", constraintType)
                   .replace("${INDEX_TYPE}", indexType)
                   .replace("${CONSTRAINT_INDEX_FLAG}", constraintIndexFlag);
    }

    private static String logicalType(String dataType) {
        return DatabaseTypes.logicalDeclaration(dataType, "sqlserver");
    }
}
