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
 * MySQL 的动态表单元数据读取器，读取 information_schema 的列、主键、索引和外键信息。
 * catalog/schema 差异封装在参数化查询模板里，共享转换层只看到统一列别名。
 * 具体实现由 {@link ReactiveFormMetadataReaders} 在包内选择，避免业务绑定数据库字典 SQL。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
final class MySqlReactiveFormMetadataReader implements ReactiveFormMetadataReader, ReactiveMetadataExecutorSource {

    private static final String BASE_COLUMNS_SQL = """
            select c.COLUMN_NAME,
                   c.DATA_TYPE,
                   c.CHARACTER_MAXIMUM_LENGTH,
                   c.NUMERIC_PRECISION,
                   c.NUMERIC_SCALE,
                   c.COLUMN_COMMENT as REMARKS,
                   c.IS_NULLABLE as NULLABLE,
                   case when pk.CONSTRAINT_NAME is null then false else true end as PRIMARY_KEY
            from information_schema.COLUMNS c
            left join (
                select pk_kcu.TABLE_SCHEMA,
                       pk_kcu.TABLE_NAME,
                       pk_kcu.COLUMN_NAME,
                       pk_kcu.CONSTRAINT_NAME
                from information_schema.KEY_COLUMN_USAGE pk_kcu
                join information_schema.TABLE_CONSTRAINTS pk_tc
                  on pk_tc.CONSTRAINT_SCHEMA = pk_kcu.CONSTRAINT_SCHEMA
                 and pk_tc.TABLE_SCHEMA = pk_kcu.TABLE_SCHEMA
                 and pk_tc.TABLE_NAME = pk_kcu.TABLE_NAME
                 and pk_tc.CONSTRAINT_NAME = pk_kcu.CONSTRAINT_NAME
                 and pk_tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ) pk
              on pk.TABLE_SCHEMA = c.TABLE_SCHEMA
             and pk.TABLE_NAME = c.TABLE_NAME
             and pk.COLUMN_NAME = c.COLUMN_NAME
            where c.TABLE_NAME = ?
            """;

    private static final String BASE_INDEXES_SQL = """
            select s.INDEX_NAME,
                   s.COLUMN_NAME,
                   case when s.NON_UNIQUE = 0 then true else false end as UNIQUE_INDEX
            from information_schema.STATISTICS s
            where s.TABLE_NAME = ?
              and s.INDEX_NAME <> 'PRIMARY'
            """;

    private static final String BASE_FOREIGN_KEYS_SQL = """
            select kcu.CONSTRAINT_NAME as FOREIGN_KEY_NAME,
                   kcu.COLUMN_NAME,
                   kcu.REFERENCED_TABLE_NAME,
                   kcu.REFERENCED_COLUMN_NAME
            from information_schema.KEY_COLUMN_USAGE kcu
            where kcu.TABLE_NAME = ?
              and kcu.REFERENCED_TABLE_NAME is not null
            """;

    private final InformationSchemaFormMetadataReader delegate;

    private MySqlReactiveFormMetadataReader(ReactiveSqlExecutor executor) {
        this.delegate = new InformationSchemaFormMetadataReader(Objects.requireNonNull(executor,
                                                                                       "reactive sql executor must not be null"),
                                                                queries());
    }

    static MySqlReactiveFormMetadataReader create(ReactiveSqlExecutor executor) {
        return new MySqlReactiveFormMetadataReader(executor);
    }

    static InformationSchemaFormMetadataReader.Queries queries() {
        return new InformationSchemaFormMetadataReader.Queries(MySqlReactiveFormMetadataReader::columnQuery,
                                                               MySqlReactiveFormMetadataReader::indexQuery,
                                                               MySqlReactiveFormMetadataReader::foreignKeyQuery,
                                                               MySqlReactiveFormMetadataReader::logicalType);
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
            return new SqlRequest(BASE_COLUMNS_SQL + " and c.TABLE_SCHEMA = DATABASE() order by c.ORDINAL_POSITION",
                                  List.of(safeTable));
        }
        String sql = BASE_COLUMNS_SQL + " and c.TABLE_SCHEMA = ? order by c.ORDINAL_POSITION";
        return new SqlRequest(sql, List.of(safeTable, schema.trim()));
    }

    private static SqlRequest indexQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            return new SqlRequest(BASE_INDEXES_SQL
                                          + " and s.TABLE_SCHEMA = DATABASE() order by s.INDEX_NAME, s.SEQ_IN_INDEX",
                                  List.of(safeTable));
        }
        String sql = BASE_INDEXES_SQL + " and s.TABLE_SCHEMA = ? order by s.INDEX_NAME, s.SEQ_IN_INDEX";
        return new SqlRequest(sql, List.of(safeTable, schema.trim()));
    }

    private static SqlRequest foreignKeyQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            return new SqlRequest(BASE_FOREIGN_KEYS_SQL
                                          + " and kcu.TABLE_SCHEMA = DATABASE()"
                                          + " order by kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION",
                                  List.of(safeTable));
        }
        String sql = BASE_FOREIGN_KEYS_SQL
                + " and kcu.TABLE_SCHEMA = ? order by kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION";
        return new SqlRequest(sql, List.of(safeTable, schema.trim()));
    }

    private static String logicalType(String dataType) {
        String type = InformationSchemaFormMetadataReader.requireText(dataType, "data type").toLowerCase(Locale.ROOT);
        return switch (type) {
            case "varchar", "char" -> "VARCHAR";
            case "text", "tinytext", "mediumtext", "longtext" -> "TEXT";
            case "blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary" -> "BLOB";
            case "decimal", "numeric" -> "DECIMAL";
            case "int", "integer", "smallint", "mediumint", "tinyint" -> "INTEGER";
            case "bigint" -> "BIGINT";
            case "boolean", "bool", "bit" -> "BOOLEAN";
            case "timestamp", "datetime" -> "TIMESTAMP";
            case "date" -> "DATE";
            case "time" -> "TIME";
            default -> type.toUpperCase(Locale.ROOT);
        };
    }
}
