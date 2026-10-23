package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.metadata.MetadataQueryProfile.Queries;
import com.flying.orm.rdb.metadata.MetadataQueryProfile.Query;
import com.flying.orm.rdb.metadata.MetadataQueryProfile.SnapshotDialect;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.schema.SchemaSnapshotCoverage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

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

    @Override
    public Flux<String> readTableNames(String schema) {
        return Flux.defer(() -> executor.query(queries.tableNames(schema)).map(row -> (String) row.value(0)));
    }

    @Override
    public Flux<RelationalTableDefinition> readTables(String schema) {
        return Flux.defer(() -> (queries.snapshotDialect() == SnapshotDialect.POSTGRESQL
                ? executor.query(PostgreSqlMetadataQueries.emptyTableQuery(schema))
                        .collectList()
                : Mono.just(List.<DynamicRow>of()))
                .flatMapMany(names -> Flux.concat(
                        readRows(queries.columnQuery(), schema, null),
                        readRows(queries.indexQuery(), schema, null),
                        readRows(queries.foreignKeyQuery(), schema, null),
                        readRows(queries.snapshotDialect() == SnapshotDialect.MYSQL
                                ? queries.uniqueConstraintQuery() : null, schema, null))
                .collectList()
                .flatMapIterable(rows -> FormMetadataRowConverter.toTables(schema,
                        names, rows.get(0), rows.get(1), rows.get(2), rows.get(3), queries))));
    }

    @Override
    public Mono<DynamicForm> readForm(String formId, RelationIdentity relation) {
        return Mono.defer(() -> readForm(formId, MetadataCacheKey.table(queries.requireRelation(relation))));
    }

    @Override
    public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
        return Mono.defer(() -> {
            RelationIdentity target = queries.requireRelation(relation);
            return loadTable(target.schema().orElse(null), target.table());
        });
    }

    @Override
    public Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
        return Mono.defer(() -> {
            RelationIdentity target = queries.requireRelation(relation);
            return readSnapshot(target.schema().orElse(null), target.table(), target);
        });
    }

    private Mono<DynamicForm> readForm(String formId, MetadataCacheKey key) {
        return executor.query(queries.columnQuery().create(key.schema(), key.table()))
                       .collectList()
                       .map(rows -> FormMetadataRowConverter.toDynamicForm(formId,
                               key.relation(), rows,
                                                                            queries.typeMapper()));
    }

    private Mono<RelationalTableDefinition> loadTable(String schema, String table) {
        RelationIdentity identity = RelationIdentity.of(null, schema, table);
        return readRows(queries.columnQuery(), schema, table)
                .map(rows -> FormMetadataRowConverter.tableBuilder(
                        identity, rows, queries.typeMapper()))
                .flatMap(builder -> readRows(queries.indexQuery(), schema, table)
                        .flatMap(indexes -> readRows(queries.foreignKeyQuery(), schema, table)
                                .flatMap(foreignKeys -> readRows(
                                        queries.snapshotDialect() == SnapshotDialect.MYSQL
                                                ? queries.uniqueConstraintQuery() : null, schema, table)
                                        .map(uniqueConstraints -> FormMetadataRowConverter.toTableDefinition(
                                                identity, builder, indexes, foreignKeys,
                                                uniqueConstraints, queries.snapshotDialect())))));
    }

    private Mono<SchemaSnapshot> readSnapshot(String schema,
                                              String table,
                                              RelationIdentity identity) {
        return Flux.concat(queries.snapshotQueries().stream()
                        .map(query -> readRows(query, schema, table)).toList())
                .collectList()
                .map(rows -> queries.toSnapshot(identity, rows));
    }

    private Mono<List<DynamicRow>> readRows(Query query, String schema, String table) {
        if (query == null) {
            return Mono.just(List.of());
        }
        return executor.query(query.create(schema, table)).collectList();
    }

}
