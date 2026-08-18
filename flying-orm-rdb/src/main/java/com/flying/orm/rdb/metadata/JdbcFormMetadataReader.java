package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.BoundedCacheRegion;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.result.DynamicRow;
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
    private final BoundedCacheRegion<MetadataCacheKey, DynamicForm> forms;
    private final BoundedCacheRegion<MetadataCacheKey, TableMetadata> tables;
    private final MetadataCacheInvalidator dependentInvalidator;

    JdbcFormMetadataReader(SyncSqlExecutor executor,
                           InformationSchemaFormMetadataReader.Queries queries) {
        this(executor, queries, CacheRegionPolicy.disabled(), MetadataCacheInvalidator.none());
    }

    JdbcFormMetadataReader(SyncSqlExecutor executor,
                           InformationSchemaFormMetadataReader.Queries queries,
                           CacheRegionPolicy policy,
                           MetadataCacheInvalidator dependentInvalidator) {
        this.executor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        this.queries = Objects.requireNonNull(queries, "metadata queries must not be null");
        CacheRegionPolicy safePolicy = Objects.requireNonNull(policy, "metadata cache policy must not be null");
        this.forms = BoundedCacheRegion.create(safePolicy, (ignored, form) -> metadataWeight(form));
        this.tables = BoundedCacheRegion.create(safePolicy, (ignored, table) -> metadataWeight(table));
        this.dependentInvalidator = Objects.requireNonNull(
                dependentInvalidator, "dependent metadata invalidator must not be null");
    }

    /** 按 table 或 schema.table 读取动态表单。 */
    public DynamicForm readForm(String formId, String table) {
        InformationSchemaFormMetadataReader.TableName tableName =
                InformationSchemaFormMetadataReader.parseTable(table);
        return readForm(formId, tableName.schema(), tableName.name());
    }

    /** 按 schema 和物理表名读取动态表单。 */
    public DynamicForm readForm(String formId, String schema, String table) {
        MetadataCacheKey key = MetadataCacheKey.form(formId, executor.metadataCachePartition(), schema, table);
        if (externalTransactionActive()) {
            return loadForm(formId, schema, table);
        }
        return forms.get(key, ignored -> loadForm(formId, schema, table));
    }

    private DynamicForm loadForm(String formId, String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        String displayTable = displayTable(schema, safeTable);
        List<DynamicRow> rows = query(queries.columnQuery(), schema, safeTable);
        return FormMetadataRowConverter.toDynamicForm(formId, displayTable, rows, queries.typeMapper());
    }

    /** 按 table 或 schema.table 读取表元数据。 */
    public TableMetadata readTable(String table) {
        InformationSchemaFormMetadataReader.TableName tableName =
                InformationSchemaFormMetadataReader.parseTable(table);
        return readTable(tableName.schema(), tableName.name());
    }

    /** 按 schema 和物理表名读取表元数据。 */
    public TableMetadata readTable(String schema, String table) {
        MetadataCacheKey key = MetadataCacheKey.table(executor.metadataCachePartition(), schema, table);
        if (externalTransactionActive()) {
            return loadTable(schema, table);
        }
        return tables.get(key, ignored -> loadTable(schema, table));
    }

    private boolean externalTransactionActive() {
        return executor.currentTransaction().isPresent();
    }

    private TableMetadata loadTable(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        String displayTable = displayTable(schema, safeTable);
        DynamicForm form = readForm(displayTable, schema, safeTable);
        List<DynamicRow> indexes = query(queries.indexQuery(), schema, safeTable);
        List<DynamicRow> foreignKeys = query(queries.foreignKeyQuery(), schema, safeTable);
        return FormMetadataRowConverter.toTableMetadata(displayTable, form, indexes, foreignKeys);
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
        forms.invalidateAll();
        tables.invalidateAll();
        dependentInvalidator.invalidateAll();
    }

    private void removeMatching(String schema, String table) {
        forms.invalidateIf(key -> sameTable(key, schema, table));
        tables.invalidateIf(key -> sameTable(key, schema, table));
    }

    private static boolean sameTable(MetadataCacheKey key, String schema, String table) {
        return key.table().equals(table) && (schema == null || Objects.equals(key.schema(), schema));
    }

    private static int metadataWeight(DynamicForm form) {
        return 1 + form.fields().size();
    }

    private static int metadataWeight(TableMetadata table) {
        return 1 + table.columns().size() + table.indexes().size() + table.foreignKeys().size();
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
        return schema == null || schema.isBlank() ? table : schema.trim() + "." + table;
    }
}
