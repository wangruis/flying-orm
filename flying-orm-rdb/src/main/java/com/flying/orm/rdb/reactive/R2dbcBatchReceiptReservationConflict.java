package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.core.internal.error.ThrowableGraph;

import java.util.Objects;

/** 回执表预留唯一键冲突；只用于触发资源域外的幂等回执重放。 */
final class R2dbcBatchReceiptReservationConflict extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private R2dbcBatchReceiptReservationConflict(Throwable cause) {
        super(cause);
    }

    /** 只把回执 reserve 自身的确定唯一键冲突标记为并发重放信号。 */
    static Throwable classify(Throwable failure) {
        Throwable safeFailure = Objects.requireNonNull(failure, "batch receipt reservation failure must not be null");
        Throwable translated = ReactiveSqlExecutionProtection.translate(safeFailure);
        if (translated instanceof VirtualMachineError) {
            return translated;
        }
        if (translated instanceof RdbException databaseFailure
                && databaseFailure.kind() == RdbErrorKind.DUPLICATE_KEY) {
            return new R2dbcBatchReceiptReservationConflict(safeFailure);
        }
        return safeFailure;
    }

    /** 在事务、回滚和资源清理形成的有限 cause 链中恢复内部冲突标记。 */
    static R2dbcBatchReceiptReservationConflict find(Throwable failure) {
        Objects.requireNonNull(failure, "batch receipt failure must not be null");
        return ThrowableGraph.findCause(failure, R2dbcBatchReceiptReservationConflict.class);
    }
}
