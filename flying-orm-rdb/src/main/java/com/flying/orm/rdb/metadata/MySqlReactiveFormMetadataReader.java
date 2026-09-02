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
 * MySQL 的动态表单元数据读取器，读取 information_schema 的列、主键、索引和外键信息。
 * catalog/schema 差异封装在参数化查询模板里，共享转换层只看到统一列别名。
 * 具体实现由 {@link ReactiveFormMetadataReaders} 在包内选择，避免业务绑定数据库字典 SQL。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
final class MySqlReactiveFormMetadataReader implements ReactiveFormMetadataReader, ReactiveMetadataExecutorSource {

    private static final String OFFSET_TIME_MARKER = "[[flying-orm:v1:OFFSET_TIME]]";
    private static final String COMMENT_ESCAPE = "[[flying-orm:v1:COMMENT]]";

    private static final String BASE_COLUMNS_SQL = """
            select c.COLUMN_NAME,
                   c.TABLE_SCHEMA as RESOLUTION_SCHEMA,
                   case
                       when binary left(c.COLUMN_COMMENT, char_length('${COMMENT_ESCAPE}'))
                                = binary '${COMMENT_ESCAPE}'
                           then c.DATA_TYPE
                       when lower(c.DATA_TYPE) = 'varchar' and c.CHARACTER_MAXIMUM_LENGTH = 32
                            and binary left(c.COLUMN_COMMENT, char_length('${OFFSET_TIME_MARKER}'))
                                = binary '${OFFSET_TIME_MARKER}'
                           then 'OFFSET_TIME'
                       when lower(c.DATA_TYPE) in ('tinyint', 'smallint', 'mediumint', 'int', 'integer', 'bigint',
                                                   'bit', 'binary', 'varbinary')
                           then c.COLUMN_TYPE
                       else c.DATA_TYPE
                   end as DATA_TYPE,
                   c.CHARACTER_MAXIMUM_LENGTH,
                   c.NUMERIC_PRECISION,
                   c.NUMERIC_SCALE,
                   c.DATETIME_PRECISION as TEMPORAL_PRECISION,
                   case
                       when binary left(c.COLUMN_COMMENT, char_length('${COMMENT_ESCAPE}'))
                                = binary '${COMMENT_ESCAPE}'
                           then substring(c.COLUMN_COMMENT, char_length('${COMMENT_ESCAPE}') + 1)
                       when lower(c.DATA_TYPE) = 'varchar' and c.CHARACTER_MAXIMUM_LENGTH = 32
                            and binary left(c.COLUMN_COMMENT, char_length('${OFFSET_TIME_MARKER}'))
                                = binary '${OFFSET_TIME_MARKER}'
                           then nullif(substring(c.COLUMN_COMMENT,
                                                char_length('${OFFSET_TIME_MARKER}') + 1), '')
                       else c.COLUMN_COMMENT
                   end as REMARKS,
                   c.IS_NULLABLE as NULLABLE,
                   c.COLUMN_DEFAULT as GENERATION_EXPRESSION,
                   case when lower(c.EXTRA) like '%auto_increment%' then true else false end as IS_IDENTITY,
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
            """.replace("${OFFSET_TIME_MARKER}", OFFSET_TIME_MARKER)
               .replace("${COMMENT_ESCAPE}", COMMENT_ESCAPE);

    private static final String BASE_INDEXES_SQL = """
            select s.INDEX_NAME,
                   s.COLUMN_NAME,
                   case when s.NON_UNIQUE = 0 then true else false end as UNIQUE_INDEX,
                   case when s.COLUMN_NAME is not null
                              and s.SUB_PART is null
                              and (s.COLLATION is null or s.COLLATION = 'A')
                              and s.INDEX_TYPE = 'BTREE'
                        then true else false end as INDEX_REPRESENTABLE,
                   case
                       when s.COLUMN_NAME is null then 'functional key part'
                       when s.SUB_PART is not null then 'prefix key part'
                       when s.COLLATION = 'D' then 'descending key part'
                       when s.INDEX_TYPE <> 'BTREE' then 'non-btree index'
                       else null
                   end as UNSUPPORTED_INDEX_REASON
            from information_schema.STATISTICS s
            where s.TABLE_NAME = ?
              and s.INDEX_NAME <> 'PRIMARY'
            """;

    private static final String BASE_FOREIGN_KEYS_SQL = """
            select kcu.TABLE_SCHEMA as TABLE_SCHEMA,
                   kcu.CONSTRAINT_NAME as FOREIGN_KEY_NAME,
                   kcu.COLUMN_NAME,
                   kcu.REFERENCED_TABLE_SCHEMA as REFERENCED_TABLE_SCHEMA,
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
        return DatabaseTypes.logicalDeclaration(dataType, "mysql");
    }
}
