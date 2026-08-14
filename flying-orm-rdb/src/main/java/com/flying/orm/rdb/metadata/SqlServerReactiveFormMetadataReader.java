package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Locale;
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

    private static final String BASE_COLUMNS_SQL = """
            select c.COLUMN_NAME,
                   c.DATA_TYPE,
                   c.CHARACTER_MAXIMUM_LENGTH,
                   c.NUMERIC_PRECISION,
                   c.NUMERIC_SCALE,
                   cast(ep.value as nvarchar(4000)) as REMARKS,
                   c.IS_NULLABLE as NULLABLE,
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
            left join sys.extended_properties ep
                   on ep.major_id = t.object_id
                  and ep.minor_id = sc.column_id
                  and ep.name = 'MS_Description'
            where c.TABLE_NAME = ?
            """;

    private static final String BASE_INDEXES_SQL = """
            select i.name as INDEX_NAME,
                   c.name as COLUMN_NAME,
                   cast(case when i.is_unique = 1 then 1 else 0 end as bit) as UNIQUE_INDEX
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
            select fk.name as FOREIGN_KEY_NAME,
                   pc.name as COLUMN_NAME,
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
        String type = InformationSchemaFormMetadataReader.requireText(dataType, "data type").toLowerCase(Locale.ROOT);
        return switch (type) {
            case "varchar", "nvarchar", "char", "nchar", "uniqueidentifier" -> "VARCHAR";
            case "text", "ntext", "xml" -> "TEXT";
            case "binary", "varbinary", "image" -> "BLOB";
            case "decimal", "numeric", "money", "smallmoney", "float", "real" -> "DECIMAL";
            case "int", "smallint", "tinyint" -> "INTEGER";
            case "bigint" -> "BIGINT";
            case "bit" -> "BOOLEAN";
            case "datetime", "datetime2", "smalldatetime", "datetimeoffset" -> "TIMESTAMP";
            case "date" -> "DATE";
            case "time" -> "TIME";
            default -> type.toUpperCase(Locale.ROOT);
        };
    }
}
