package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.type.DatabaseTypes;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

/**
 * SQL Server 的动态表单元数据读取器，读取 INFORMATION_SCHEMA，并从扩展属性取列注释。
 * schema 条件和扩展属性关联都在本方言实现中收口，对上层仍返回统一只读元数据。
 * 具体实现由 {@link ReactiveFormMetadataReaders} 在包内选择，业务不直接依赖系统目录查询。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
final class SqlServerReactiveFormMetadataReader implements ReactiveFormMetadataReader, ReactiveMetadataExecutorSource {

    private static final String OFFSET_TIME_MARKER = "[[flying-orm:v1:OFFSET_TIME]]";
    private static final String COMMENT_ESCAPE = "[[flying-orm:v1:COMMENT]]";

    private static final String BASE_COLUMNS_SQL = """
            select c.COLUMN_NAME,
                   SCHEMA_NAME() as RESOLUTION_SCHEMA,
                   case
                       when convert(varbinary(128), left(cast(ep.value as nvarchar(4000)),
                                                         len('${COMMENT_ESCAPE}')))
                                = convert(varbinary(128), '${COMMENT_ESCAPE}')
                           then c.DATA_TYPE
                       when lower(c.DATA_TYPE) = 'varchar' and c.CHARACTER_MAXIMUM_LENGTH = 32
                            and convert(varbinary(128), left(cast(ep.value as nvarchar(4000)),
                                                             len('${OFFSET_TIME_MARKER}')))
                                = convert(varbinary(128), '${OFFSET_TIME_MARKER}')
                           then 'OFFSET_TIME'
                       when lower(c.DATA_TYPE) in ('nvarchar', 'varchar')
                            and c.CHARACTER_MAXIMUM_LENGTH = -1
                           then lower(c.DATA_TYPE) + '(max)'
                       else c.DATA_TYPE
                   end as DATA_TYPE,
                   c.CHARACTER_MAXIMUM_LENGTH,
                   c.NUMERIC_PRECISION,
                   c.NUMERIC_SCALE,
                   case when c.DATA_TYPE in ('time', 'datetime2', 'datetimeoffset')
                        then c.DATETIME_PRECISION end as TEMPORAL_PRECISION,
                   case
                       when convert(varbinary(128), left(cast(ep.value as nvarchar(4000)),
                                                         len('${COMMENT_ESCAPE}')))
                                = convert(varbinary(128), '${COMMENT_ESCAPE}')
                           then substring(cast(ep.value as nvarchar(4000)), len('${COMMENT_ESCAPE}') + 1, 4000)
                       when lower(c.DATA_TYPE) = 'varchar' and c.CHARACTER_MAXIMUM_LENGTH = 32
                            and convert(varbinary(128), left(cast(ep.value as nvarchar(4000)),
                                                             len('${OFFSET_TIME_MARKER}')))
                                = convert(varbinary(128), '${OFFSET_TIME_MARKER}')
                           then nullif(substring(cast(ep.value as nvarchar(4000)),
                                                len('${OFFSET_TIME_MARKER}') + 1, 4000), '')
                       else cast(ep.value as nvarchar(4000))
                   end as REMARKS,
                   c.IS_NULLABLE as NULLABLE,
                   dc.definition as GENERATION_EXPRESSION,
                   cast(case when idc.column_id is null then 0 else 1 end as bit) as IS_IDENTITY,
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
                   cast(case when i.is_unique = 1 then 1 else 0 end as bit) as UNIQUE_INDEX,
                   cast(case when i.type_desc = 'NONCLUSTERED'
                                  and i.has_filter = 0
                                  and i.is_disabled = 0
                                  and i.is_hypothetical = 0
                                  and ic.is_descending_key = 0
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
                   case
                       when i.type_desc <> 'NONCLUSTERED' then 'index type is not nonclustered rowstore'
                       when i.has_filter = 1 then 'filtered index'
                       when i.is_disabled = 1 then 'disabled index'
                       when i.is_hypothetical = 1 then 'hypothetical index'
                       when exists (
                           select 1
                           from sys.index_columns partitioned
                           where partitioned.object_id = i.object_id
                             and partitioned.index_id = i.index_id
                             and partitioned.partition_ordinal > 0
                       ) then 'partitioned index'
                       when ic.is_descending_key = 1 then 'descending key part'
                       else 'included column'
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
              and i.name is not null
              and ic.is_included_column = 0
            """;

    private static final String BASE_FOREIGN_KEYS_SQL = """
            select ps.name as TABLE_SCHEMA,
                   fk.name as FOREIGN_KEY_NAME,
                   pc.name as COLUMN_NAME,
                   rs.name as REFERENCED_TABLE_SCHEMA,
                   rt.name as REFERENCED_TABLE_NAME,
                   rc.name as REFERENCED_COLUMN_NAME
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

    private final InformationSchemaFormMetadataReader delegate;

    private SqlServerReactiveFormMetadataReader(ReactiveSqlExecutor executor) {
        this.delegate = new InformationSchemaFormMetadataReader(Objects.requireNonNull(executor,
                                                                                       "reactive sql executor must not be null"),
                                                                                       queries());
    }

    static SqlServerReactiveFormMetadataReader create(ReactiveSqlExecutor executor) {
        return new SqlServerReactiveFormMetadataReader(executor);
    }

    static InformationSchemaFormMetadataReader.Queries queries() {
        return new InformationSchemaFormMetadataReader.Queries(SqlServerReactiveFormMetadataReader::columnQuery,
                                                               SqlServerReactiveFormMetadataReader::indexQuery,
                                                               SqlServerReactiveFormMetadataReader::foreignKeyQuery,
                                                               SqlServerReactiveFormMetadataReader::logicalType);
    }

    @Override
    public ReactiveSqlExecutor metadataExecutor() {
        return delegate.metadataExecutor();
    }

    @Override
    public Mono<DynamicForm> readForm(String formId, String table) {
        return delegate.readForm(formId, table);
    }

    @Override
    public Mono<DynamicForm> readForm(String formId, String schema, String table) {
        return delegate.readForm(formId, schema, table);
    }

    @Override
    public Mono<TableMetadata> readTable(String table) {
        return delegate.readTable(table);
    }

    @Override
    public Mono<TableMetadata> readTable(String schema, String table) {
        return delegate.readTable(schema, table);
    }

    private static SqlRequest columnQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            String sql = BASE_COLUMNS_SQL + " and t.object_id = object_id(?) order by c.ORDINAL_POSITION";
            return new SqlRequest(sql, List.of(safeTable, safeTable));
        }
        String sql = BASE_COLUMNS_SQL + " and c.TABLE_SCHEMA = ? order by c.ORDINAL_POSITION";
        return new SqlRequest(sql, List.of(safeTable, schema.trim()));
    }

    private static SqlRequest indexQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            String sql = BASE_INDEXES_SQL + " and t.object_id = object_id(?) order by i.name, ic.key_ordinal";
            return new SqlRequest(sql, List.of(safeTable, safeTable));
        }
        String sql = BASE_INDEXES_SQL + " and s.name = ? order by i.name, ic.key_ordinal";
        return new SqlRequest(sql, List.of(safeTable, schema.trim()));
    }

    private static SqlRequest foreignKeyQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            String sql = BASE_FOREIGN_KEYS_SQL
                    + " and pt.object_id = object_id(?) order by fk.name, fkc.constraint_column_id";
            return new SqlRequest(sql, List.of(safeTable, safeTable));
        }
        String sql = BASE_FOREIGN_KEYS_SQL + " and ps.name = ? order by fk.name, fkc.constraint_column_id";
        return new SqlRequest(sql, List.of(safeTable, schema.trim()));
    }

    private static String logicalType(String dataType) {
        return DatabaseTypes.logicalDeclaration(dataType, "sqlserver");
    }
}
