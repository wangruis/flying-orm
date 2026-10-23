package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.BoundedCacheRegion;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.schema.SchemaSnapshotCoverage;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * 原生 JDBC 动态表单元数据 reader。
 *
 * <p>它只依赖同步 SQL 执行契约，不创建 Reactor、不调用 {@code block()}，也不通过响应式 reader
 * 做同步等待。方言 SQL 仍由现有 reader 提供，字段、索引、外键的转换仍由共享转换器完成。</p>
 *
 * <p>有界缓存按显式策略启用，不检测连接或事务状态。上层需要隔离的结构视图时使用独立 reader 或禁用缓存；
 * Schema 审核始终直读当前数据库事实。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
final class DefaultJdbcFormMetadataReader extends JdbcFormMetadataReader {

    private final SyncSqlExecutor executor;
    private final MetadataQueryProfile.Queries queries;
    private final SchemaSnapshotCoverage coverage;
    private final BoundedCacheRegion<MetadataCacheKey, JdbcMetadataValue> metadata;
    private final MetadataCacheInvalidator dependentInvalidator;
    private final AtomicLong invalidationGeneration = new AtomicLong();

    /** 返回当前方言查询集真实具备的结构覆盖范围；覆盖不完整时后续规划会安全停止自动 DDL。 */
    public SchemaSnapshotCoverage snapshotCoverage() {
        return coverage;
    }

    @Override
    public List<String> readTableNames(String schema) {
        return executor.queryMapped(queries.tableNames(schema), null, row -> (String) row.value(0), 0);
    }

    @Override
    public List<RelationalTableDefinition> readTables(String schema) {
        return FormMetadataRowConverter.toTables(schema,
                queries.snapshotDialect() == MetadataQueryProfile.SnapshotDialect.POSTGRESQL
                        ? executor.query(PostgreSqlMetadataQueries.emptyTableQuery(schema)) : List.of(),
                query(queries.columnQuery(), schema, null),
                query(queries.indexQuery(), schema, null),
                query(queries.foreignKeyQuery(), schema, null),
                queries.snapshotDialect() == MetadataQueryProfile.SnapshotDialect.MYSQL
                        ? query(queries.uniqueConstraintQuery(), schema, null) : List.of(), queries);
    }

    DefaultJdbcFormMetadataReader(SyncSqlExecutor executor,
                           MetadataQueryProfile profile,
                           CacheRegionPolicy policy,
                           MetadataCacheInvalidator dependentInvalidator) {
        this.executor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        MetadataQueryProfile safeProfile = Objects.requireNonNull(
                profile, "metadata query profile must not be null");
        this.queries = safeProfile.queries();
        this.coverage = safeProfile.coverage();
        CacheRegionPolicy safePolicy = Objects.requireNonNull(policy, "metadata cache policy must not be null");
        this.metadata = BoundedCacheRegion.create(safePolicy, (ignored, value) -> value.weight());
        this.dependentInvalidator = Objects.requireNonNull(
                dependentInvalidator, "dependent metadata invalidator must not be null");
    }

    @Override
    public DynamicForm readForm(String formId, RelationIdentity relation) {
        MetadataCacheKey key = MetadataCacheKey.form(formId, queries.requireRelation(relation));
        JdbcMetadataValue value = cached(key, ignored -> new CachedForm(loadForm(key)));
        if (value instanceof CachedForm cachedForm) {
            return cachedForm.form();
        }
        throw new IllegalStateException("metadata cache returned table metadata for a form key");
    }

    private DynamicForm loadForm(MetadataCacheKey key) {
        List<DynamicRow> rows = query(queries.columnQuery(), key.schema(), key.table());
        return FormMetadataRowConverter.toDynamicForm(key.formId(),
                key.relation(), rows, queries.typeMapper());
    }

    @Override
    public RelationalTableDefinition readTableForSchema(RelationIdentity relation) {
        return loadTable(MetadataCacheKey.table(queries.requireRelation(relation)));
    }

    /** 关系型入口保留 catalog/schema/table 的原始分段，不再解析表名中的点号。 */
    public SchemaSnapshot readSnapshot(RelationIdentity relation) {
        RelationIdentity target = queries.requireRelation(relation);
        return loadSnapshot(MetadataCacheKey.table(target), target);
    }

    @Override
    public RelationalTableDefinition readTable(RelationIdentity relation) {
        MetadataCacheKey key = MetadataCacheKey.table(queries.requireRelation(relation));
        JdbcMetadataValue value = cached(key, ignored -> new CachedTable(loadTable(key)));
        if (value instanceof CachedTable cachedTable) {
            return cachedTable.table();
        }
        throw new IllegalStateException("metadata cache returned form metadata for a table key");
    }

    private RelationalTableDefinition loadTable(MetadataCacheKey key) {
        RelationalTableDefinition.Builder builder = FormMetadataRowConverter.tableBuilder(
                key.relation(),
                query(queries.columnQuery(), key.schema(), key.table()), queries.typeMapper());
        List<DynamicRow> indexes = query(queries.indexQuery(), key.schema(), key.table());
        List<DynamicRow> foreignKeys = query(queries.foreignKeyQuery(), key.schema(), key.table());
        List<DynamicRow> uniqueConstraints = queries.snapshotDialect()
                == MetadataQueryProfile.SnapshotDialect.MYSQL
                ? query(queries.uniqueConstraintQuery(), key.schema(), key.table()) : List.of();
        return FormMetadataRowConverter.toTableDefinition(
                key.relation(), builder, indexes, foreignKeys, uniqueConstraints, queries.snapshotDialect());
    }

    private SchemaSnapshot loadSnapshot(MetadataCacheKey key, RelationIdentity identity) {
        return queries.toSnapshot(identity, queries.snapshotQueries().stream()
                .map(query -> query(query, key.schema(), key.table())).toList());
    }

    @Override
    public void invalidate(RelationIdentity relation) {
        RelationIdentity target = Objects.requireNonNull(
                relation, "metadata cache relation must not be null");
        invalidationGeneration.incrementAndGet();
        metadata.invalidateIf(key -> key.matchesInvalidation(target));
        dependentInvalidator.invalidate(target);
    }

    @Override
    public void invalidateAll() {
        invalidationGeneration.incrementAndGet();
        metadata.invalidateAll();
        dependentInvalidator.invalidateAll();
    }

    private JdbcMetadataValue cached(MetadataCacheKey key,
                                      Function<MetadataCacheKey, JdbcMetadataValue> loader) {
        long generation = invalidationGeneration.get();
        JdbcMetadataValue value = metadata.get(key, loader);
        // 字典查询完成前条目尚未发布；跨失效的在途读取可返回原结果，但不能留下旧缓存。
        if (generation != invalidationGeneration.get()) {
            metadata.invalidate(key);
        }
        return value;
    }

    private List<DynamicRow> query(MetadataQueryProfile.Query query,
                                   String schema,
                                   String table) {
        if (query == null) {
            return List.of();
        }
        SqlRequest request = query.create(schema, table);
        return executor.query(request);
    }

    private sealed interface JdbcMetadataValue permits CachedForm, CachedTable {

        int weight();
    }

    private record CachedForm(DynamicForm form) implements JdbcMetadataValue {

        @Override
        public int weight() {
            return 1 + form.fields().size();
        }
    }

    private record CachedTable(RelationalTableDefinition table) implements JdbcMetadataValue {

        @Override
        public int weight() {
            return 1 + table.columns().size() + table.indexes().size() + table.foreignKeys().size();
        }
    }
}
