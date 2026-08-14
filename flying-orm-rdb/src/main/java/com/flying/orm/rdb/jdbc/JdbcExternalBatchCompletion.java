package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import com.flying.orm.rdb.transaction.TransactionOutcome;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.enlisted;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rolledBack;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.unknown;

/**
 * 把 JDBC 外部事务中的 ENLISTED 结果接到事务最终完成通知。
 *
 * <p>这里只保存受批量上限保护的分片结果快照，不保存 SQL 参数或输入 Publisher。上层确认提交、回滚或
 * UNKNOWN 后，Repository 才会收到最终结果并执行对应的 after 生命周期。完成通知注册失败不能改变已经
 * 执行的 SQL，因此会保守按 UNKNOWN 释放暂存引用，同时仍向业务返回 ENLISTED。</p>
 */
final class JdbcExternalBatchCompletion {

    BatchWriteResult enlist(JdbcTransactionContext transaction,
                            BatchWriteRequest request,
                            List<BatchChunkResult> executed,
                            JdbcBatchExecutionObservationSupport.BatchContext observation) {
        JdbcTransactionContext safeTransaction = Objects.requireNonNull(
                transaction, "jdbc transaction context must not be null");
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        List<BatchChunkResult> snapshot = List.copyOf(Objects.requireNonNull(
                executed, "executed batch chunks must not be null"));
        BatchWriteResult result = BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, enlisted(snapshot));
        if (!register(safeTransaction, safeRequest.completion(), snapshot,
                      Objects.requireNonNull(observation, "batch observation context must not be null"))) {
            // 没有完成通知时，不能无限保留 Repository 的实体快照；UNKNOWN 明确告诉生命周期不要当成提交。
            completeSynchronously(safeRequest.completion(), finalResult(snapshot, TransactionOutcome.UNKNOWN));
        }
        return result;
    }

    private boolean register(JdbcTransactionContext transaction,
                             BatchWriteCompletion completion,
                             List<BatchChunkResult> executed,
                             JdbcBatchExecutionObservationSupport.BatchContext observation) {
        AtomicBoolean notified = new AtomicBoolean();
        try {
            return transaction.completion().register(outcome -> {
                if (!notified.compareAndSet(false, true)) {
                    return BatchWriteCompletion.noop().afterCompletion(
                            finalResult(executed, TransactionOutcome.UNKNOWN));
                }
                BatchWriteResult finalResult = finalResult(executed, outcome);
                observation.finalized(finalResult);
                return Objects.requireNonNull(
                        completion.afterCompletion(finalResult),
                        "batch completion publisher must not be null");
            });
        } catch (RuntimeException failure) {
            VirtualMachineError fatal = JdbcThrowableGraph.findVirtualMachineError(failure);
            if (fatal != null) {
                throw fatal;
            }
            // 注册器是上层协作设施；它异常时不能把已经成功加入事务的 SQL 改成执行失败。
            return false;
        }
    }

    private BatchWriteResult finalResult(List<BatchChunkResult> executed, TransactionOutcome outcome) {
        TransactionOutcome safeOutcome = outcome == null ? TransactionOutcome.UNKNOWN : outcome;
        return switch (safeOutcome) {
            case COMMITTED -> BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, executed);
            case ROLLED_BACK -> BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, rolledBack(executed));
            case UNKNOWN -> BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, unknown(
                    executed, new IllegalStateException("external jdbc transaction outcome is unknown")));
        };
    }

    /** JDBC 无法登记外部事务完成通知时，仅调用明确的同步收尾钩子。 */
    private void completeSynchronously(BatchWriteCompletion completion, BatchWriteResult result) {
        try {
            completion.afterCompletionUnavailable(result);
        } catch (RuntimeException failure) {
            VirtualMachineError fatal = JdbcThrowableGraph.findVirtualMachineError(failure);
            if (fatal != null) {
                throw fatal;
            }
            // 清理协作失败不能覆盖 SQL 已经加入外部事务这一事实。
        }
    }
}
