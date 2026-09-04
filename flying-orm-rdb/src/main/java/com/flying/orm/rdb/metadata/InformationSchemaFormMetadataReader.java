package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.schema.SchemaSnapshotCoverage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.EnumSet;
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
    private final SchemaSnapshotCoverage coverage;

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
        this(executor, new MetadataQueryProfile(
                queries, InformationSchemaFormMetadataReader.coverage(queries)));
    }

    InformationSchemaFormMetadataReader(ReactiveSqlExecutor executor, MetadataQueryProfile profile) {
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        MetadataQueryProfile safeProfile = Objects.requireNonNull(
                profile, "metadata query profile must not be null");
        this.queries = safeProfile.queries();
        this.coverage = safeProfile.coverage();
    }

    @Override
    public SchemaSnapshotCoverage snapshotCoverage() {
        return coverage;
    }

    static SchemaSnapshotCoverage coverage(Queries queries) {
        Queries safeQueries = Objects.requireNonNull(queries, "metadata queries must not be null");
        if (safeQueries.completeSnapshotQueries()) {
            return SchemaSnapshotCoverage.complete();
        }
        EnumSet<SchemaSnapshotCoverage.Fact> observed = EnumSet.of(
                SchemaSnapshotCoverage.Fact.TABLE_EXISTENCE,
                SchemaSnapshotCoverage.Fact.COLUMNS,
                SchemaSnapshotCoverage.Fact.PRIMARY_KEY);
        if (safeQueries.indexQuery() != null) {
            observed.add(SchemaSnapshotCoverage.Fact.INDEXES);
        }
        if (safeQueries.foreignKeyQuery() != null) {
            observed.add(SchemaSnapshotCoverage.Fact.FOREIGN_KEYS);
        }
        return SchemaSnapshotCoverage.of(observed);
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

    @Override
    public Mono<SchemaSnapshot> readSnapshot(String table) {
        TableName tableName = parseTable(table);
        return readSnapshot(tableName.schema(), tableName.name(), tableName.displayName());
    }

    @Override
    public Mono<SchemaSnapshot> readSnapshot(String schema, String table) {
        return readSnapshot(schema, table, schema + "." + table);
    }

    private Mono<DynamicForm> readForm(String formId, String schema, String table, String displayTable) {
        return executor.query(queries.columnQuery().create(schema, table))
                       .collectList()
                       .map(rows -> FormMetadataRowConverter.toDynamicForm(formId, displayTable, rows,
                                                                            queries.typeMapper()));
    }

    private Mono<TableMetadata> readTable(String schema, String table, String displayTable) {
        return readForm(displayTable, schema, table, displayTable)
                .flatMap(form -> readRows(queries.indexQuery(), schema, table)
                        .flatMap(indexes -> readRows(queries.foreignKeyQuery(), schema, table)
                                .map(foreignKeys -> FormMetadataRowConverter.toTableMetadata(
                                        displayTable, form, indexes, foreignKeys))));
    }

    private Mono<SchemaSnapshot> readSnapshot(String schema, String table, String displayTable) {
        Mono<List<DynamicRow>> columns = executor.query(queries.columnQuery().create(schema, table)).collectList();
        RelationIdentity identity = RelationIdentity.of(null, schema, table);
        if (queries.completeSnapshotQueries()) {
            return Flux.concat(
                            columns,
                            readRows(queries.tableQuery(), schema, table),
                            readRows(queries.primaryKeyQuery(), schema, table),
                            readRows(queries.uniqueConstraintQuery(), schema, table),
                            readRows(queries.indexQuery(), schema, table),
                            readRows(queries.foreignKeyQuery(), schema, table),
                            readRows(queries.checkConstraintQuery(), schema, table))
                    .collectList()
                    .map(result -> FormMetadataRowConverter.toCompleteSchemaSnapshot(
                            identity,
                            result.get(0), result.get(1), result.get(2), result.get(3),
                            result.get(4), result.get(5), result.get(6),
                            queries.typeMapper(), queries.snapshotDialect()));
        }
        return Flux.concat(
                        columns,
                        readRows(queries.indexQuery(), schema, table),
                        readRows(queries.foreignKeyQuery(), schema, table))
                .collectList()
                .map(result -> FormMetadataRowConverter.toSchemaSnapshot(
                        identity, displayTable, result.get(0), result.get(1), queries.indexQuery() != null,
                        result.get(2), queries.foreignKeyQuery() != null, queries.typeMapper()));
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

    interface TableQuery extends Query {
    }

    interface PrimaryKeyQuery extends Query {
    }

    interface UniqueConstraintQuery extends Query {
    }

    interface CheckConstraintQuery extends Query {
    }

    enum SnapshotDialect {
        LEGACY,
        POSTGRESQL,
        MYSQL,
        H2,
        ORACLE,
        SQL_SERVER
    }

    record Queries(ColumnQuery columnQuery,
                   IndexQuery indexQuery,
                   ForeignKeyQuery foreignKeyQuery,
                   Function<String, String> typeMapper,
                   TableQuery tableQuery,
                   PrimaryKeyQuery primaryKeyQuery,
                   UniqueConstraintQuery uniqueConstraintQuery,
                   CheckConstraintQuery checkConstraintQuery,
                   SnapshotDialect snapshotDialect) {

        Queries(ColumnQuery columnQuery,
                IndexQuery indexQuery,
                ForeignKeyQuery foreignKeyQuery,
                Function<String, String> typeMapper) {
            this(columnQuery, indexQuery, foreignKeyQuery, typeMapper,
                 null, null, null, null, SnapshotDialect.LEGACY);
        }

        Queries {
            Objects.requireNonNull(columnQuery, "column query must not be null");
            Objects.requireNonNull(typeMapper, "type mapper must not be null");
            Objects.requireNonNull(snapshotDialect, "snapshot dialect must not be null");
            if (snapshotDialect != SnapshotDialect.LEGACY && !hasAllSnapshotQueries(
                    tableQuery, primaryKeyQuery, uniqueConstraintQuery, indexQuery,
                    foreignKeyQuery, checkConstraintQuery)) {
                throw new IllegalArgumentException(
                        "complete schema metadata requires every structural query");
            }
        }

        static Queries complete(ColumnQuery columnQuery,
                                IndexQuery indexQuery,
                                ForeignKeyQuery foreignKeyQuery,
                                Function<String, String> typeMapper,
                                TableQuery tableQuery,
                                PrimaryKeyQuery primaryKeyQuery,
                                UniqueConstraintQuery uniqueConstraintQuery,
                                CheckConstraintQuery checkConstraintQuery,
                                SnapshotDialect snapshotDialect) {
            if (snapshotDialect == SnapshotDialect.LEGACY) {
                throw new IllegalArgumentException("complete schema metadata requires a concrete dialect");
            }
            return new Queries(columnQuery, indexQuery, foreignKeyQuery, typeMapper,
                               tableQuery, primaryKeyQuery, uniqueConstraintQuery,
                               checkConstraintQuery, snapshotDialect);
        }

        boolean completeSnapshotQueries() {
            return snapshotDialect != SnapshotDialect.LEGACY;
        }

        private static boolean hasAllSnapshotQueries(TableQuery tableQuery,
                                                     PrimaryKeyQuery primaryKeyQuery,
                                                     UniqueConstraintQuery uniqueConstraintQuery,
                                                     IndexQuery indexQuery,
                                                     ForeignKeyQuery foreignKeyQuery,
                                                     CheckConstraintQuery checkConstraintQuery) {
            return tableQuery != null
                    && primaryKeyQuery != null
                    && uniqueConstraintQuery != null
                    && indexQuery != null
                    && foreignKeyQuery != null
                    && checkConstraintQuery != null;
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
