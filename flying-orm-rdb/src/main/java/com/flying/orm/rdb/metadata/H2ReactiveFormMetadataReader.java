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
 * H2 的动态表单元数据读取器。它走 R2DBC 查询 INFORMATION_SCHEMA，不碰 JDBC 元数据。
 * 主要服务内嵌开发和测试环境，返回结构仍与生产方言使用同一套 DynamicForm/TableMetadata 模型。
 * 具体实现由 {@link ReactiveFormMetadataReaders} 在包内选择，业务只依赖元数据 reader 接口。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
final class H2ReactiveFormMetadataReader implements ReactiveFormMetadataReader, ReactiveMetadataExecutorSource {

    private static final String BASE_COLUMNS_SQL = """
            select c.COLUMN_NAME,
                   c.DATA_TYPE,
                   c.CHARACTER_MAXIMUM_LENGTH,
                   c.NUMERIC_PRECISION,
                   c.NUMERIC_SCALE,
                   c.REMARKS,
                   c.IS_NULLABLE as NULLABLE,
                   case when pk.CONSTRAINT_NAME is null then false else true end as PRIMARY_KEY
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
            where c.TABLE_NAME = case
                when exists (
                    select 1
                    from INFORMATION_SCHEMA.COLUMNS exact_column
                    where exact_column.TABLE_SCHEMA = c.TABLE_SCHEMA
                      and exact_column.TABLE_NAME = ?
                ) then ? else upper(?) end
            """;

    private static final String BASE_INDEXES_SQL = """
            select i.INDEX_NAME,
                   ic.COLUMN_NAME,
                   case when ic.IS_UNIQUE then true else false end as UNIQUE_INDEX
            from INFORMATION_SCHEMA.INDEXES i
            join INFORMATION_SCHEMA.INDEX_COLUMNS ic
              on ic.TABLE_SCHEMA = i.TABLE_SCHEMA
             and ic.TABLE_NAME = i.TABLE_NAME
             and ic.INDEX_NAME = i.INDEX_NAME
            where i.TABLE_NAME = case
                when exists (
                    select 1
                    from INFORMATION_SCHEMA.COLUMNS exact_column
                    where exact_column.TABLE_SCHEMA = i.TABLE_SCHEMA
                      and exact_column.TABLE_NAME = ?
                ) then ? else upper(?) end
              and i.INDEX_NAME is not null
              and i.INDEX_TYPE_NAME <> 'PRIMARY KEY'
            """;

    private static final String BASE_FOREIGN_KEYS_SQL = """
            select fk.CONSTRAINT_NAME as FOREIGN_KEY_NAME,
                   fk.COLUMN_NAME,
                   pk.TABLE_NAME as REFERENCED_TABLE_NAME,
                   pk.COLUMN_NAME as REFERENCED_COLUMN_NAME
            from INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            join INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk
              on fk.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
             and fk.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            join INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
              on rc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
             and rc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            join INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk
              on pk.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA
             and pk.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
             and pk.ORDINAL_POSITION = fk.POSITION_IN_UNIQUE_CONSTRAINT
            where tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
              and tc.TABLE_NAME = case
                  when exists (
                      select 1
                      from INFORMATION_SCHEMA.COLUMNS exact_column
                      where exact_column.TABLE_SCHEMA = tc.TABLE_SCHEMA
                        and exact_column.TABLE_NAME = ?
                  ) then ? else upper(?) end
            """;

    private final InformationSchemaFormMetadataReader delegate;

    private H2ReactiveFormMetadataReader(ReactiveSqlExecutor executor) {
        this.delegate = new InformationSchemaFormMetadataReader(Objects.requireNonNull(executor,
                                                                                      "reactive sql executor must not be null"),
                                                                                      queries());
    }

    static H2ReactiveFormMetadataReader create(ReactiveSqlExecutor executor) {
        return new H2ReactiveFormMetadataReader(executor);
    }

    static InformationSchemaFormMetadataReader.Queries queries() {
        return new InformationSchemaFormMetadataReader.Queries(H2ReactiveFormMetadataReader::columnQuery,
                                                               H2ReactiveFormMetadataReader::indexQuery,
                                                               H2ReactiveFormMetadataReader::foreignKeyQuery,
                                                               H2ReactiveFormMetadataReader::logicalType);
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
            String sql = BASE_COLUMNS_SQL
                    + " and c.TABLE_SCHEMA = current_schema() order by c.ORDINAL_POSITION";
            return new SqlRequest(sql, tableParameters(safeTable));
        }
        String sql = BASE_COLUMNS_SQL + " and c.TABLE_SCHEMA = " + schemaExpression()
                + " order by c.ORDINAL_POSITION";
        return new SqlRequest(sql, tableAndSchemaParameters(safeTable, schema));
    }

    private static SqlRequest indexQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            String sql = BASE_INDEXES_SQL
                    + " and i.TABLE_SCHEMA = current_schema()"
                    + " order by i.INDEX_NAME, ic.ORDINAL_POSITION";
            return new SqlRequest(sql, tableParameters(safeTable));
        }
        String sql = BASE_INDEXES_SQL + " and i.TABLE_SCHEMA = " + schemaExpression()
                + " order by i.INDEX_NAME, ic.ORDINAL_POSITION";
        return new SqlRequest(sql, tableAndSchemaParameters(safeTable, schema));
    }

    private static SqlRequest foreignKeyQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            String sql = BASE_FOREIGN_KEYS_SQL
                    + " and tc.TABLE_SCHEMA = current_schema()"
                    + " order by fk.CONSTRAINT_NAME, fk.ORDINAL_POSITION";
            return new SqlRequest(sql, tableParameters(safeTable));
        }
        String sql = BASE_FOREIGN_KEYS_SQL
                + " and tc.TABLE_SCHEMA = " + schemaExpression()
                + " order by fk.CONSTRAINT_NAME, fk.ORDINAL_POSITION";
        return new SqlRequest(sql, tableAndSchemaParameters(safeTable, schema));
    }

    /** H2 未加引号名称会折叠为大写；已有精确 quoted 名称时必须优先保留精确身份。 */
    private static String schemaExpression() {
        return "case when exists (select 1 from INFORMATION_SCHEMA.SCHEMATA exact_schema "
                + "where exact_schema.SCHEMA_NAME = ?) then ? else upper(?) end";
    }

    private static List<Object> tableParameters(String table) {
        return List.of(table, table, table);
    }

    private static List<Object> tableAndSchemaParameters(String table, String schema) {
        String safeSchema = schema.trim();
        return List.of(table, table, table, safeSchema, safeSchema, safeSchema);
    }

    private static String logicalType(String dataType) {
        String type = InformationSchemaFormMetadataReader.requireText(dataType, "data type").toUpperCase(Locale.ROOT);
        return switch (type) {
            case "CHARACTER VARYING", "VARCHAR", "VARCHAR_IGNORECASE" -> "VARCHAR";
            case "CHARACTER LARGE OBJECT", "CLOB" -> "TEXT";
            case "BINARY LARGE OBJECT", "BLOB" -> "BLOB";
            case "DECIMAL", "NUMERIC" -> "DECIMAL";
            case "INT", "INTEGER" -> "INTEGER";
            case "BIGINT" -> "BIGINT";
            case "BOOLEAN" -> "BOOLEAN";
            case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP WITH TIME ZONE" -> "TIMESTAMP";
            case "DATE" -> "DATE";
            case "TIME WITH TIME ZONE" -> "OFFSET_TIME";
            case "TIME", "TIME WITHOUT TIME ZONE" -> "TIME";
            default -> type;
        };
    }
}
