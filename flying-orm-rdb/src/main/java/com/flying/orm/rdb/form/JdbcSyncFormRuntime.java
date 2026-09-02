package com.flying.orm.rdb.form;

import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorPageResult;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;
import java.util.Objects;

/** 原生 JDBC 同步表单运行时；查询、写入和批量路径都不创建 Reactor 对象。 */
final class JdbcSyncFormRuntime implements SyncFormRuntime {

    private final SyncSqlExecutor sqlExecutor;
    private final SyncBatchExecutor batchExecutor;
    private final SyncFormConfiguration configuration;
    private final SyncFormOperations operations;
    private final NativeSyncFormBatchOperations batches;

    JdbcSyncFormRuntime(SyncSqlExecutor sqlExecutor,
                        SyncBatchExecutor batchExecutor,
                        SyncFormConfiguration configuration) {
        this.sqlExecutor = Objects.requireNonNull(sqlExecutor, "sync sql executor must not be null");
        this.batchExecutor = Objects.requireNonNull(batchExecutor, "sync batch executor must not be null");
        this.configuration = Objects.requireNonNull(configuration, "sync form configuration must not be null");
        this.operations = new SyncFormOperations(
                sqlExecutor, configuration.renderer(), configuration.resolver(), configuration.dataScope(),
                configuration.executionOptions(), configuration.entityModels());
        this.batches = new NativeSyncFormBatchOperations(
                batchExecutor, configuration.renderer(), configuration.resolver(), configuration.dataScope(),
                configuration.batchOptions());
    }

    @Override public BatchWriteOptions defaultBatchWriteOptions() { return configuration.batchOptions(); }
    @Override public EntityModelRegistry entityModels() { return configuration.entityModels(); }
    @Override public java.util.Optional<com.flying.orm.rdb.transaction.JdbcTransactionContext> currentTransaction() {
        return sqlExecutor.currentTransaction();
    }
    @Override public List<DynamicRow> select(QuerySpec spec) { return operations.select(spec); }
    @Override public List<DynamicRow> selectJoin(JoinQuerySpec spec, SqlExecutionOptions options) {
        return operations.selectJoin(spec, options);
    }
    @Override public <T> List<T> selectJoin(JoinQuerySpec spec,
                                            SqlExecutionOptions options,
                                            RowMapper<T> mapper) {
        return operations.selectJoin(spec, options, mapper);
    }
    @Override public PageResult<DynamicRow> pageJoin(JoinQuerySpec spec,
                                                     PageQuery page,
                                                     SqlExecutionOptions options) {
        return operations.pageJoin(spec, page, options);
    }
    @Override public <T> List<T> select(QuerySpec spec, Class<T> type) { return operations.select(spec, type); }
    @Override public <T> T selectOne(QuerySpec spec, Class<T> type) { return operations.selectOne(spec, type); }
    @Override public PageResult<DynamicRow> page(QuerySpec spec, PageQuery page) { return operations.page(spec, page); }
    @Override public <T> PageResult<T> page(QuerySpec spec, PageQuery page, Class<T> type) {
        return operations.page(spec, page, type);
    }
    @Override public CursorPageResult<DynamicRow> cursorPage(QuerySpec spec, CursorPageQuery page) {
        return operations.cursorPage(spec, page);
    }
    @Override public <T> CursorPageResult<T> cursorPage(QuerySpec spec, CursorPageQuery page, Class<T> type) {
        return operations.cursorPage(spec, page, type);
    }
    @Override public long insert(WriteSpec spec) { return operations.insert(spec); }
    @Override public SqlWriteResult insertReturningKeys(WriteSpec spec) {
        return operations.insertReturningKeys(spec);
    }
    @Override public long update(WriteSpec spec) { return operations.update(spec); }
    @Override public long delete(WriteSpec spec) { return operations.delete(spec); }
    @Override public long physicalDelete(WriteSpec spec) { return operations.physicalDelete(spec); }
    @Override public BatchWriteResult writeBatch(BatchSpec spec) { return batches.writeBatch(spec); }
    @Override public List<BatchChunkResult> writeBatchChunks(BatchSpec spec) { return batches.writeBatchChunks(spec); }
    @Override public SyncFormRuntime withResolver(StructuredConditionResolver resolver) {
        return configured(configuration.withResolver(resolver));
    }
    @Override public SyncFormRuntime withExecutionOptions(SqlExecutionOptions options) {
        return configured(configuration.withExecutionOptions(options));
    }
    @Override public SyncFormRuntime withDataScope(DataScope scope) {
        return configured(configuration.withDataScope(configuration.dataScope().and(scope)));
    }
    @Override public SyncFormRuntime withBatchOptions(BatchWriteOptions options) {
        return configured(configuration.withBatchOptions(options));
    }
    @Override public SyncFormRuntime withEntityModels(EntityModelRegistry entityModels) {
        return configured(configuration.withEntityModels(entityModels));
    }
    private JdbcSyncFormRuntime configured(SyncFormConfiguration value) {
        return new JdbcSyncFormRuntime(sqlExecutor, batchExecutor, value);
    }
}
