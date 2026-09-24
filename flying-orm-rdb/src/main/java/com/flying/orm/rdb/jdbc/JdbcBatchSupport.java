package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/** JDBC 普通批量的有界输入读取及同步错误优先级。 */
final class JdbcBatchSupport {
    private JdbcBatchSupport() {}

    static List<ProtectedBatchRows.RowView> readBuffer(JdbcBatchRows rows, BatchWriteRequest request)
            throws InterruptedException, TimeoutException {
        var options = request.options();
        List<ProtectedBatchRows.RowView> result = new ArrayList<>(Math.min(options.bufferSize(), 16));
        long bytes = 0;
        long reservedLimit = options.maxBufferedBytes() - options.maxRowBytes();
        while (result.size() < options.bufferSize() && bytes <= reservedLimit) {
            ProtectedBatchRows.RowView row = rows.nextRowView();
            if (row == null) break;
            bytes += row.estimatedBytes();
            result.add(row);
        }
        return result;
    }

    static void restoreInterrupt(Throwable error) {
        if (error instanceof InterruptedException) Thread.currentThread().interrupt();
    }

    static void rethrowVirtualMachineError(Throwable error) {
        if (error instanceof VirtualMachineError fatal) throw fatal;
    }

    static void rethrowTryWithResourcesVirtualMachineError(Throwable error) {
        rethrowVirtualMachineError(error);
        for (Throwable suppressed : error.getSuppressed()) {
            if (suppressed instanceof VirtualMachineError fatal) {
                com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic(fatal, error);
                throw fatal;
            }
        }
    }
}
