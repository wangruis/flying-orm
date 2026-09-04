package com.flying.orm.rdb.sync;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;

import javax.sql.DataSource;
import java.util.List;
import java.util.Objects;

/**
 * 同步批量执行契约。V2 的正式实现使用原生 JDBC，不能把 Publisher 包装成 Reactor 后再阻塞等待。
 *
 * <p>请求仍使用共享的 {@link BatchWriteRequest}，所以参数布局、行数策略、内存预算和结果模型与 R2DBC 一致。
 * 同步实现必须按背压有界消费输入，不能先把整批收进 List。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public interface SyncBatchExecutor {

    /** 从上层管理的 DataSource 创建原生 JDBC 有界批量执行器。 */
    static SyncBatchExecutor jdbc(DataSource dataSource) {
        return JdbcBatchWriter.create(dataSource);
    }

    /** 执行完整批次并返回提交、回滚、部分完成或未知结果。 */
    BatchWriteResult writeBatch(BatchWriteRequest request);

    /** 执行批量并返回独立执行证据；旧实现必须显式拒绝，不能退化为 legacy 结果。 */
    default BatchExecutionEvidence writeBatchEvidence(BatchWriteRequest request) {
        Objects.requireNonNull(request, "batch evidence request must not be null");
        throw new UnsupportedOperationException("sync batch executor does not support execution evidence");
    }

    /** 执行含侧索引维护的批量；不能控制同连接事务的自定义实现必须显式失败。 */
    default BatchWriteResult writeProtectedBatch(BatchWriteRequest request) {
        Objects.requireNonNull(request, "protected batch request must not be null");
        throw new UnsupportedOperationException("sync batch executor does not support protected batch writes");
    }

    /** 执行含侧索引维护的证据批量；默认实现禁止静默漏写侧索引。 */
    default BatchExecutionEvidence writeProtectedBatchEvidence(BatchWriteRequest request) {
        Objects.requireNonNull(request, "protected batch evidence request must not be null");
        throw new UnsupportedOperationException("sync batch executor does not support protected batch evidence");
    }

    /** 执行显式 INDEPENDENT 批次并按输入分片顺序返回明细。 */
    List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request);

    /** 执行 INDEPENDENT 受保护批量并返回分片；默认实现禁止静默漏写侧索引。 */
    default List<BatchChunkResult> writeProtectedBatchChunks(BatchWriteRequest request) {
        Objects.requireNonNull(request, "protected batch request must not be null");
        throw new UnsupportedOperationException("sync batch executor does not support protected batch writes");
    }
}
