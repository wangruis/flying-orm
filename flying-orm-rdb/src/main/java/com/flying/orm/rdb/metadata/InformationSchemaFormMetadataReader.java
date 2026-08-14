package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * 使用方言提供的字典 SQL 读取动态表单元数据。
 *
 * <p>这个类只编排响应式查询。SQL 模板由各方言 reader 提供，行转换由
 * {@link FormMetadataRowConverter} 统一处理，JDBC 路径可以复用同一套规则而不需要调用 Reactor。</p>
 */
final class InformationSchemaFormMetadataReader
        implements ReactiveFormMetadataReader, ReactiveMetadataExecutorSource {

    private final ReactiveSqlExecutor executor;
    private final Queries queries;

    InformationSchemaFormMetadataReader(ReactiveSqlExecutor executor,
                                        ColumnQuery columnQuery,
                                        Function<String, String> typeMapper) {
        this(executor, new Queries(columnQuery, null, null, typeMapper));
    }

    InformationSchemaFormMetadataReader(ReactiveSqlExecutor executor,
                                        ColumnQuery columnQuery,
                                        IndexQuery indexQuery,
                                        Function<String, String> typeMapper) {
        this(executor, new Queries(columnQuery, indexQuery, null, typeMapper));
    }

    InformationSchemaFormMetadataReader(ReactiveSqlExecutor executor,
                                        ColumnQuery columnQuery,
                                        IndexQuery indexQuery,
                                        ForeignKeyQuery foreignKeyQuery,
                                        Function<String, String> typeMapper) {
        this(executor, new Queries(columnQuery, indexQuery, foreignKeyQuery, typeMapper));
    }

    InformationSchemaFormMetadataReader(ReactiveSqlExecutor executor, Queries queries) {
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.queries = Objects.requireNonNull(queries, "metadata queries must not be null");
    }

    @Override
    public ReactiveSqlExecutor metadataExecutor() {
        return executor;
    }

    @Override
    public Mono<DynamicForm> readForm(String formId, String table) {
        TableName tableName = parseTable(table);
        return readForm(formId, tableName.schema(), tableName.name(), tableName.displayName());
    }

    @Override
    public Mono<DynamicForm> readForm(String formId, String schema, String table) {
        return readForm(formId, schema, table, schema + "." + table);
    }

    @Override
    public Mono<TableMetadata> readTable(String table) {
        TableName tableName = parseTable(table);
        return readTable(tableName.schema(), tableName.name(), tableName.displayName());
    }

    @Override
    public Mono<TableMetadata> readTable(String schema, String table) {
        return readTable(schema, table, schema + "." + table);
    }

    private Mono<DynamicForm> readForm(String formId, String schema, String table, String displayTable) {
        return executor.query(queries.columnQuery().create(schema, table))
                       .collectList()
                       .map(rows -> FormMetadataRowConverter.toDynamicForm(formId, displayTable, rows,
                                                                            queries.typeMapper()));
    }

    private Mono<TableMetadata> readTable(String schema, String table, String displayTable) {
        Mono<DynamicForm> form = readForm(displayTable, schema, table, displayTable);
        Mono<List<DynamicRow>> indexes = readRows(queries.indexQuery(), schema, table);
        Mono<List<DynamicRow>> foreignKeys = readRows(queries.foreignKeyQuery(), schema, table);
        return Mono.zip(form, indexes, foreignKeys)
                   .map(result -> FormMetadataRowConverter.toTableMetadata(displayTable, result.getT1(),
                                                                           result.getT2(), result.getT3()));
    }

    private Mono<List<DynamicRow>> readRows(Query query, String schema, String table) {
        if (query == null) {
            return Mono.just(List.of());
        }
        return executor.query(query.create(schema, table)).collectList();
    }

    static String requireText(String value, String fieldName) {
        return FormMetadataRowConverter.requireText(value, fieldName);
    }

    static TableName parseTable(String table) {
        return TableName.parse(table);
    }

    @FunctionalInterface
    interface Query {

        SqlRequest create(String schema, String table);
    }

    interface ColumnQuery extends Query {
    }

    interface IndexQuery extends Query {
    }

    interface ForeignKeyQuery extends Query {
    }

    record Queries(ColumnQuery columnQuery,
                   IndexQuery indexQuery,
                   ForeignKeyQuery foreignKeyQuery,
                   Function<String, String> typeMapper) {

        Queries {
            Objects.requireNonNull(columnQuery, "column query must not be null");
            Objects.requireNonNull(typeMapper, "type mapper must not be null");
        }
    }

    record TableName(String schema, String name, String displayName) {

        private static TableName parse(String table) {
            String text = requireText(table, "table");
            String[] parts = text.split("\\.");
            if (parts.length == 1) {
                return new TableName(null, parts[0], text);
            }
            if (parts.length == 2) {
                return new TableName(parts[0], parts[1], text);
            }
            throw new IllegalArgumentException("table must be table or schema.table");
        }
    }
}
