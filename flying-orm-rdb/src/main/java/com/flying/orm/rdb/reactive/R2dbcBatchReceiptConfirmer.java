package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * 在未知提交连接完成清理后，按回执配置主动确认数据库中的提交事实。
 *
 * <p>确认查询只能发生在原事务资源域之外，否则容量为一的连接池会因旧连接尚未释放而自锁。只有携带完整
 * payload 摘要和精确行数的恢复令牌才允许把 UNKNOWN 提升为 COMMITTED；查不到或普通确认失败都保留原结果。</p>
 *
 * @author wangr
 * @date 2026-08-13
 * @version v1.0
 */
final class R2dbcBatchReceiptConfirmer {

    private final BatchReceiptStore receiptStore;

    R2dbcBatchReceiptConfirmer(BatchReceiptStore receiptStore) {
        this.receiptStore = Objects.requireNonNull(receiptStore, "batch receipt store must not be null");
    }

    Mono<BatchWriteResult> confirmAtomic(BatchWriteRequest request,
                                         BatchWriteException unknown) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        BatchWriteException safeUnknown = Objects.requireNonNull(unknown, "unknown batch failure must not be null");
        VirtualMachineError fatal = ReactiveSqlExecutionProtection.findVirtualMachineError(safeUnknown);
        if (fatal != null) {
            return Mono.error(fatal);
        }
        BatchChunkResult.RecoveryToken token = completeToken(safeUnknown.result());
        if (!canConfirm(safeRequest.options().recovery(), safeUnknown.result().status(), token)) {
            return Mono.error(safeUnknown);
        }
        return lookup(safeRequest, token)
                .map(receipt -> BatchWriteResult.from(
                        BatchWriteOptions.Mode.ATOMIC,
                        List.of(BatchChunkResult.committed(0,
                                                           0L,
                                                           Math.toIntExact(receipt.exactInputRowCount()),
                                                           receipt.affectedRows()))))
                .onErrorResume(confirmError -> preserveUnknown(safeUnknown, confirmError))
                .switchIfEmpty(Mono.error(safeUnknown));
    }

    Mono<BatchChunkResult> confirmChunk(BatchWriteRequest request,
                                        BatchChunkResult unknown) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        BatchChunkResult safeUnknown = Objects.requireNonNull(unknown, "unknown batch chunk must not be null");
        BatchChunkResult.RecoveryToken token = safeUnknown.recoveryToken();
        if (!canConfirm(safeRequest.options().recovery(),
                        safeUnknown.status() == BatchChunkResult.Status.UNKNOWN
                                ? BatchWriteResult.Status.UNKNOWN : BatchWriteResult.Status.PARTIAL,
                        token)) {
            return Mono.just(safeUnknown);
        }
        return lookup(safeRequest, token)
                .map(receipt -> committedChunk(safeUnknown, receipt))
                .onErrorResume(confirmError -> propagateFatalOrReturn(safeUnknown, confirmError))
                .switchIfEmpty(Mono.just(safeUnknown));
    }

    Mono<BatchChunkResult> confirmChunkFailure(BatchWriteRequest request,
                                               R2dbcBatchChunkWriteFailure unknown) {
        R2dbcBatchChunkWriteFailure safeUnknown = Objects.requireNonNull(
                unknown, "unknown batch chunk failure must not be null");
        VirtualMachineError fatal = ReactiveSqlExecutionProtection.findVirtualMachineError(safeUnknown);
        if (fatal != null) {
            return Mono.error(fatal);
        }
        BatchChunkResult exactResult = safeUnknown.exactResult();
        if (exactResult == null) {
            return Mono.error(safeUnknown);
        }
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        BatchChunkResult.RecoveryToken token = exactResult.recoveryToken();
        if (!canConfirm(safeRequest.options().recovery(),
                        exactResult.status() == BatchChunkResult.Status.UNKNOWN
                                ? BatchWriteResult.Status.UNKNOWN : BatchWriteResult.Status.PARTIAL,
                        token)) {
            return Mono.error(safeUnknown);
        }
        return lookup(safeRequest, token)
                .map(receipt -> committedChunk(exactResult, receipt))
                .onErrorResume(confirmError -> preserveUnknown(safeUnknown, confirmError))
                .switchIfEmpty(Mono.error(safeUnknown));
    }

    private Mono<BatchReceiptStore.Receipt> lookup(BatchWriteRequest request,
                                                   BatchChunkResult.RecoveryToken token) {
        Duration confirmTimeout = request.options().recovery().confirmTimeout();
        return receiptStore.find(token, confirmTimeout);
    }

    private static BatchChunkResult committedChunk(BatchChunkResult unknown, BatchReceiptStore.Receipt receipt) {
        return BatchChunkResult.committed(unknown.chunkIndex(),
                                          unknown.startOffset(),
                                          Math.toIntExact(receipt.exactInputRowCount()),
                                          receipt.affectedRows());
    }

    private static BatchChunkResult.RecoveryToken completeToken(BatchWriteResult result) {
        if (result.status() != BatchWriteResult.Status.UNKNOWN) {
            return null;
        }
        return result.chunks().stream()
                     .map(BatchChunkResult::recoveryToken)
                     .filter(Objects::nonNull)
                     .filter(BatchChunkResult.RecoveryToken::hasCompleteEvidence)
                     .findFirst()
                     .orElse(null);
    }

    private static boolean canConfirm(BatchWriteOptions.Recovery recovery,
                                      BatchWriteResult.Status status,
                                      BatchChunkResult.RecoveryToken token) {
        return recovery.mode() == BatchWriteOptions.RecoveryMode.RECEIPT
                && !recovery.confirmTimeout().isZero()
                && status == BatchWriteResult.Status.UNKNOWN
                && token != null
                && token.hasCompleteEvidence();
    }

    private static <T> Mono<T> preserveUnknown(Throwable unknown, Throwable confirmationError) {
        VirtualMachineError fatal = ReactiveSqlExecutionProtection.promoteVirtualMachineError(
                unknown, confirmationError);
        if (fatal != null) {
            return Mono.error(fatal);
        }
        ReactiveSqlExecutionProtection.addSuppressedIfAcyclic(unknown, confirmationError);
        return Mono.error(unknown);
    }

    private static Mono<BatchChunkResult> propagateFatalOrReturn(BatchChunkResult unknown,
                                                                  Throwable confirmationError) {
        VirtualMachineError fatal = ReactiveSqlExecutionProtection.findVirtualMachineError(confirmationError);
        return fatal == null ? Mono.just(unknown) : Mono.error(fatal);
    }
}
