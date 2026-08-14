package com.flying.orm.rdb.bootstrap;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.sync.SyncBatchExecutor;

import java.util.List;
import java.util.Objects;

/**
 * 为同步批量执行入口施加客户端硬上限。
 *
 * <p>校验发生在委派前；因此超限配置不会订阅请求行流，也不会触发 JDBC 连接获取。实例不可变且可安全共享。</p>
 *
 * @author wangr
 * @date 2026-08-08
 * @version v1.0
 */
final class BatchMemoryLimitedSyncBatchExecutor implements SyncBatchExecutor {

    private final SyncBatchExecutor delegate;
    private final BatchMemoryLimits limits;

    private BatchMemoryLimitedSyncBatchExecutor(SyncBatchExecutor delegate, BatchMemoryLimits limits) {
        this.delegate = Objects.requireNonNull(delegate, "sync batch executor must not be null");
        this.limits = Objects.requireNonNull(limits, "batch memory limits must not be null");
    }

    static SyncBatchExecutor create(SyncBatchExecutor delegate, BatchMemoryLimits limits) {
        return new BatchMemoryLimitedSyncBatchExecutor(delegate, limits);
    }

    @Override
    public BatchWriteResult writeBatch(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = requireWithinLimits(request);
        return delegate.writeBatch(safeRequest);
    }

    @Override
    public BatchWriteResult writeProtectedBatch(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = requireWithinLimits(request);
        return delegate.writeProtectedBatch(safeRequest);
    }

    @Override
    public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = requireWithinLimits(request);
        return delegate.writeBatchChunks(safeRequest);
    }

    @Override
    public List<BatchChunkResult> writeProtectedBatchChunks(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = requireWithinLimits(request);
        return delegate.writeProtectedBatchChunks(safeRequest);
    }

    private BatchWriteRequest requireWithinLimits(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        limits.check(safeRequest.options());
        return safeRequest;
    }
}
