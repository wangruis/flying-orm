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
import com.flying.orm.rdb.result.DynamicRow;

import java.util.List;

/**
 * 同步表单门面背后的包内运行时契约。
 *
 * <p>公开客户端只依赖这组表单能力，具体实现负责原生 JDBC 查询、写入、批量和配置派生。
 * 接口留在包内，避免使用方绕过统一装配直接替换数据库执行语义。</p>
 */
interface SyncFormRuntime {

    BatchWriteOptions defaultBatchWriteOptions();
    EntityModelRegistry entityModels();
    java.util.Optional<com.flying.orm.rdb.transaction.JdbcTransactionContext> currentTransaction();
    List<DynamicRow> select(QuerySpec spec);
    List<DynamicRow> selectJoin(JoinQuerySpec spec, SqlExecutionOptions options);
    PageResult<DynamicRow> pageJoin(JoinQuerySpec spec, PageQuery page, SqlExecutionOptions options);
    <T> List<T> select(QuerySpec spec, Class<T> type);
    PageResult<DynamicRow> page(QuerySpec spec, PageQuery page);
    <T> PageResult<T> page(QuerySpec spec, PageQuery page, Class<T> type);
    CursorPageResult<DynamicRow> cursorPage(QuerySpec spec, CursorPageQuery page);
    <T> CursorPageResult<T> cursorPage(QuerySpec spec, CursorPageQuery page, Class<T> type);
    long insert(WriteSpec spec);
    SqlWriteResult insertReturningKeys(WriteSpec spec);
    long update(WriteSpec spec);
    long delete(WriteSpec spec);
    long physicalDelete(WriteSpec spec);
    BatchWriteResult writeBatch(BatchSpec spec);
    List<BatchChunkResult> writeBatchChunks(BatchSpec spec);
    SyncFormRuntime withResolver(StructuredConditionResolver resolver);
    SyncFormRuntime withExecutionOptions(SqlExecutionOptions options);
    SyncFormRuntime withDataScope(DataScope scope);
    SyncFormRuntime withBatchOptions(BatchWriteOptions options);
    SyncFormRuntime withEntityModels(EntityModelRegistry entityModels);
}
