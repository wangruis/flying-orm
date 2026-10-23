package com.flying.orm.rdb.sync;

import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import java.util.Objects;
import java.util.function.LongConsumer;

/**
 * 原生同步有界批量执行，只报告实际 SQL 执行事实。
 * @author wangr
 * @version v4.1.0
 */
public interface SyncBatchExecutor {
    /** 是否理解内部逐行 SQL 布局；普通自定义适配器保持固定 SQL 契约。 */
    @InternalApi
    default boolean supportsBatchRowLayouts() { return false; }

    static SyncBatchExecutor jdbc(JdbcConnectionAccess access, RdbDialect dialect) {
        return JdbcBatchWriter.create(access, dialect);
    }

    default BatchExecutionEvidence writeBatch(BatchWriteRequest request) {
        return writeBatch(request, null);
    }

    @InternalApi
    BatchExecutionEvidence writeBatch(BatchWriteRequest request, LongConsumer rowCompleted);


    default BatchExecutionEvidence writeProtectedBatch(BatchWriteRequest request) {
        return writeProtectedBatch(request, null);
    }

    @InternalApi
    default BatchExecutionEvidence writeProtectedBatch(BatchWriteRequest request, LongConsumer rowCompleted) {
        Objects.requireNonNull(request, "protected batch request must not be null");
        throw new UnsupportedOperationException("sync batch executor does not support protected batch writes");
    }

}
