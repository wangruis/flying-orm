package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.BoundedCacheRegion;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.schema.SchemaSnapshotCoverage;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;
import java.util.Objects;

/**
 * 原生 JDBC 动态表单元数据 reader。
 *
 * <p>它只依赖同步 SQL 执行契约，不创建 Reactor、不调用 {@code block()}，也不通过响应式 reader
 * 做同步等待。方言 SQL 仍由现有 reader 提供，字段、索引、外键的转换仍由共享转换器完成。</p>
 *
 * <p>可选的有界缓存只服务事务外共享读取。外部事务可能看到尚未提交的 DDL，因此事务内直接读取数据库字典，
 * 既不读取也不写入进程级共享缓存。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class JdbcFormMetadataReader implements MetadataCacheInvalidator {

    private final SyncSqlExecutor executor;
    private final InformationSchemaFormMetadataReader.Queries queries;
    private final SchemaSnapshotCoverage coverage;
    private final BoundedCacheRegion<MetadataCacheKey, JdbcMetadataValue> metadata;
    private final MetadataCacheInvalidator dependentInvalidator;

    /** 返回当前方言查询集真实具备的结构覆盖范围；覆盖不完整时后续规划会安全停止自动 DDL。 */
    public SchemaSnapshotCoverage snapshotCoverage() {
        return coverage;
    }

    JdbcFormMetadataReader(SyncSqlExecutor executor,
                           InformationSchemaFormMetadataReader.Queries queries) {
        this(executor, queries, CacheRegionPolicy.disabled(), MetadataCacheInvalidator.none());
    }

    JdbcFormMetadataReader(SyncSqlExecutor executor, MetadataQueryProfile profile) {
        this(executor, profile, CacheRegionPolicy.disabled(), MetadataCacheInvalidator.none());
    }

    JdbcFormMetadataReader(SyncSqlExecutor executor,
                           InformationSchemaFormMetadataReader.Queries queries,
                           CacheRegionPolicy policy,
                           MetadataCacheInvalidator dependentInvalidator) {
        this(executor, new MetadataQueryProfile(
                queries, InformationSchemaFormMetadataReader.coverage(queries)),
                policy, dependentInvalidator);
    }

    JdbcFormMetadataReader(SyncSqlExecutor executor,
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

    /** 按 table 或 schema.table 读取动态表单。 */
    public DynamicForm readForm(String formId, String table) {
        MetadataCacheKey key = MetadataCacheKey.form(formId, null, table);
        return readForm(key, externalTransactionActive());
    }

    /** 按 schema 和物理表名读取动态表单。 */
    public DynamicForm readForm(String formId, String schema, String table) {
        MetadataCacheKey key = MetadataCacheKey.form(formId, schema, table);
        return readForm(key, externalTransactionActive());
    }

    private DynamicForm readForm(MetadataCacheKey key, boolean externalTransactionActive) {
        if (externalTransactionActive) {
            return loadForm(key);
        }
        JdbcMetadataValue value = metadata.get(key, ignored -> new CachedForm(loadForm(key)));
        if (value instanceof CachedForm cachedForm) {
            return cachedForm.form();
        }
        throw new IllegalStateException("metadata cache returned table metadata for a form key");
    }

    private DynamicForm loadForm(MetadataCacheKey key) {
        String displayTable = displayTable(key.schema(), key.table());
        List<DynamicRow> rows = query(queries.columnQuery(), key.schema(), key.table());
        return FormMetadataRowConverter.toDynamicForm(key.formId(), displayTable, rows, queries.typeMapper());
    }

    /** 按 table 或 schema.table 读取表元数据。 */
    public TableMetadata readTable(String table) {
        return readTable(MetadataCacheKey.table(null, table));
    }

    /** 按 schema 和物理表名读取表元数据。 */
    public TableMetadata readTable(String schema, String table) {
        return readTable(MetadataCacheKey.table(schema, table));
    }

    /** Schema 审核始终直读当前数据库，不命中也不写入普通元数据缓存。 */
    public SchemaSnapshot readSnapshot(String table) {
        return loadSnapshot(MetadataCacheKey.table(null, table));
    }

    /** 按明确 schema 直读当前数据库结构事实。 */
    public SchemaSnapshot readSnapshot(String schema, String table) {
        return loadSnapshot(MetadataCacheKey.table(schema, table));
    }

    private TableMetadata readTable(MetadataCacheKey key) {
        boolean externalTransactionActive = externalTransactionActive();
        if (externalTransactionActive) {
            return loadTable(key);
        }
        JdbcMetadataValue value = metadata.get(key, ignored -> new CachedTable(loadTable(key)));
        if (value instanceof CachedTable cachedTable) {
            return cachedTable.table();
        }
        throw new IllegalStateException("metadata cache returned form metadata for a table key");
    }

    private boolean externalTransactionActive() {
        return executor.currentTransaction().isPresent();
    }

    private TableMetadata loadTable(MetadataCacheKey key) {
        String displayTable = displayTable(key.schema(), key.table());
        MetadataCacheKey formKey = key.asForm(displayTable);
        DynamicForm form = loadForm(formKey);
        List<DynamicRow> indexes = query(queries.indexQuery(), key.schema(), key.table());
        List<DynamicRow> foreignKeys = query(queries.foreignKeyQuery(), key.schema(), key.table());
        return FormMetadataRowConverter.toTableMetadata(displayTable, form, indexes, foreignKeys);
    }

    private SchemaSnapshot loadSnapshot(MetadataCacheKey key) {
        String displayTable = displayTable(key.schema(), key.table());
        List<DynamicRow> columns = query(queries.columnQuery(), key.schema(), key.table());
        RelationIdentity identity = RelationIdentity.of(null, key.schema(), key.table());
        if (queries.completeSnapshotQueries()) {
            List<DynamicRow> tableRows = query(queries.tableQuery(), key.schema(), key.table());
            List<DynamicRow> primaryKey = query(queries.primaryKeyQuery(), key.schema(), key.table());
            List<DynamicRow> uniqueConstraints = query(
                    queries.uniqueConstraintQuery(), key.schema(), key.table());
            List<DynamicRow> indexes = query(queries.indexQuery(), key.schema(), key.table());
            List<DynamicRow> foreignKeys = query(queries.foreignKeyQuery(), key.schema(), key.table());
            List<DynamicRow> checks = query(queries.checkConstraintQuery(), key.schema(), key.table());
            return FormMetadataRowConverter.toCompleteSchemaSnapshot(
                    identity,
                    columns,
                    tableRows,
                    primaryKey,
                    uniqueConstraints,
                    indexes,
                    foreignKeys,
                    checks,
                    queries.typeMapper(),
                    queries.snapshotDialect());
        }
        List<DynamicRow> indexes = query(queries.indexQuery(), key.schema(), key.table());
        List<DynamicRow> foreignKeys = query(queries.foreignKeyQuery(), key.schema(), key.table());
        return FormMetadataRowConverter.toSchemaSnapshot(
                identity,
                displayTable,
                columns,
                indexes,
                queries.indexQuery() != null,
                foreignKeys,
                queries.foreignKeyQuery() != null,
                queries.typeMapper());
    }

    @Override
    public void invalidate(String table) {
        MetadataCacheKey key = MetadataCacheKey.table(null, table);
        removeMatching(key.schema(), key.table());
        dependentInvalidator.invalidate(table);
    }

    @Override
    public void invalidate(String schema, String table) {
        MetadataCacheKey key = MetadataCacheKey.table(schema, table);
        removeMatching(key.schema(), key.table());
        dependentInvalidator.invalidate(schema, table);
    }

    @Override
    public void invalidateAll() {
        metadata.invalidateAll();
        dependentInvalidator.invalidateAll();
    }

    private void removeMatching(String schema, String table) {
        metadata.invalidateIf(key -> sameTable(key, schema, table));
    }

    private static boolean sameTable(MetadataCacheKey key, String schema, String table) {
        return key.table().equals(table) && (schema == null || Objects.equals(key.schema(), schema));
    }

    private List<DynamicRow> query(InformationSchemaFormMetadataReader.Query query,
                                   String schema,
                                   String table) {
        if (query == null) {
            return List.of();
        }
        SqlRequest request = query.create(schema, table);
        return executor.query(request);
    }

    private static String displayTable(String schema, String table) {
        return schema == null ? table : schema + "." + table;
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

    private record CachedTable(TableMetadata table) implements JdbcMetadataValue {

        @Override
        public int weight() {
            return 1 + table.columns().size() + table.indexes().size() + table.foreignKeys().size();
        }
    }
}
