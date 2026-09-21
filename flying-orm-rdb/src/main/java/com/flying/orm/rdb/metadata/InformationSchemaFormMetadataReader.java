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
import java.util.Locale;
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
        implements ReactiveFormMetadataReader {

    private final ReactiveSqlExecutor executor;
    private final Queries queries;
    private final SchemaSnapshotCoverage coverage;

    InformationSchemaFormMetadataReader(ReactiveSqlExecutor executor,
                                         Query columnQuery,
                                         Function<String, String> typeMapper) {
        this(executor, new Queries(columnQuery, null, null, typeMapper));
    }

    InformationSchemaFormMetadataReader(ReactiveSqlExecutor executor,
                                         Query columnQuery,
                                         Query indexQuery,
                                         Function<String, String> typeMapper) {
        this(executor, new Queries(columnQuery, indexQuery, null, typeMapper));
    }

    InformationSchemaFormMetadataReader(ReactiveSqlExecutor executor,
                                         Query columnQuery,
                                         Query indexQuery,
                                         Query foreignKeyQuery,
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

    @Override
    public Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
        RelationIdentity target = Objects.requireNonNull(
                relation, "schema relation identity must not be null");
        if (target.catalog().isPresent()) {
            return Mono.error(new UnsupportedOperationException(
                    "catalog-qualified schema snapshots are not supported by the reactive reader"));
        }
        String schema = target.schema().orElse(null);
        String displayTable = schema == null ? target.table() : schema + "." + target.table();
        return readSnapshot(schema, target.table(), displayTable, target);
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
                                .flatMap(foreignKeys -> readRows(
                                        queries.snapshotDialect() == SnapshotDialect.MYSQL
                                                ? queries.uniqueConstraintQuery() : null, schema, table)
                                        .map(uniqueConstraints -> FormMetadataRowConverter.toTableMetadata(
                                                displayTable, form, indexes, foreignKeys,
                                                uniqueConstraints, queries.snapshotDialect())))));
    }

    private Mono<SchemaSnapshot> readSnapshot(String schema, String table, String displayTable) {
        return readSnapshot(
                schema, table, displayTable, RelationIdentity.of(null, schema, table));
    }

    private Mono<SchemaSnapshot> readSnapshot(String schema,
                                              String table,
                                              String displayTable,
                                              RelationIdentity identity) {
        Mono<List<DynamicRow>> columns = executor.query(queries.columnQuery().create(schema, table)).collectList();
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
                            queries.typeMapper(), queries.snapshotTypeMapper(), queries.snapshotDialect()));
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

    enum SnapshotDialect {
        LEGACY,
        POSTGRESQL,
        MYSQL,
        H2,
        ORACLE,
        SQL_SERVER
    }

    record Queries(Query columnQuery,
                   Query indexQuery,
                   Query foreignKeyQuery,
                   Function<String, String> typeMapper,
                   Function<String, String> snapshotTypeMapper,
                   Query tableQuery,
                   Query primaryKeyQuery,
                   Query uniqueConstraintQuery,
                   Query checkConstraintQuery,
                   SnapshotDialect snapshotDialect) {

        Queries(Query columnQuery,
                Query indexQuery,
                Query foreignKeyQuery,
                Function<String, String> typeMapper) {
            this(columnQuery, indexQuery, foreignKeyQuery, typeMapper, typeMapper,
                 null, null, null, null, SnapshotDialect.LEGACY);
        }

        Queries {
            Objects.requireNonNull(columnQuery, "column query must not be null");
            Objects.requireNonNull(typeMapper, "type mapper must not be null");
            Objects.requireNonNull(snapshotTypeMapper, "snapshot type mapper must not be null");
            Objects.requireNonNull(snapshotDialect, "snapshot dialect must not be null");
            if (snapshotDialect != SnapshotDialect.LEGACY && !hasAllSnapshotQueries(
                    tableQuery, primaryKeyQuery, uniqueConstraintQuery, indexQuery,
                    foreignKeyQuery, checkConstraintQuery)) {
                throw new IllegalArgumentException(
                        "complete schema metadata requires every structural query");
            }
        }

        static Queries complete(Query columnQuery,
                                Query indexQuery,
                                Query foreignKeyQuery,
                                Function<String, String> typeMapper,
                                Query tableQuery,
                                Query primaryKeyQuery,
                                Query uniqueConstraintQuery,
                                Query checkConstraintQuery,
                                SnapshotDialect snapshotDialect) {
            return complete(columnQuery, indexQuery, foreignKeyQuery, typeMapper,
                            Queries::physicalType,
                            tableQuery, primaryKeyQuery, uniqueConstraintQuery,
                            checkConstraintQuery, snapshotDialect);
        }

        private static String physicalType(String type) {
            return requireText(type, "physical data type").replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
        }

        static Queries complete(Query columnQuery,
                                Query indexQuery,
                                Query foreignKeyQuery,
                                Function<String, String> typeMapper,
                                Function<String, String> snapshotTypeMapper,
                                Query tableQuery,
                                Query primaryKeyQuery,
                                Query uniqueConstraintQuery,
                                Query checkConstraintQuery,
                                SnapshotDialect snapshotDialect) {
            if (snapshotDialect == SnapshotDialect.LEGACY) {
                throw new IllegalArgumentException("complete schema metadata requires a concrete dialect");
            }
            return new Queries(columnQuery, indexQuery, foreignKeyQuery, typeMapper, snapshotTypeMapper,
                               tableQuery, primaryKeyQuery, uniqueConstraintQuery,
                               checkConstraintQuery, snapshotDialect);
        }

        boolean completeSnapshotQueries() {
            return snapshotDialect != SnapshotDialect.LEGACY;
        }

        private static boolean hasAllSnapshotQueries(Query tableQuery,
                                                     Query primaryKeyQuery,
                                                     Query uniqueConstraintQuery,
                                                     Query indexQuery,
                                                     Query foreignKeyQuery,
                                                     Query checkConstraintQuery) {
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
