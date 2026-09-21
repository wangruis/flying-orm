package com.flying.orm.rdb.batch;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.LongStream;

/**
 * Immutable driver-proven SQL execution facts, independent of connection and transaction policy.
 * A long successful prefix and at most one bounded terminal window avoid retaining per-buffer results.
 *
 * @author wangr
 * @version v4.1.0
 */
public final class BatchExecutionEvidence {
    private final BatchExecutionState state;
    private final long inputCount;
    private final BatchAffectedRows affectedRows;
    private final long successfulPrefix;
    private final BitSet successful;
    private final BitSet failed;
    private final List<BatchRowConflict> conflicts;
    private final Failure failure;

    private BatchExecutionEvidence(Accumulator source, BatchExecutionState state, Failure failure) {
        this.state = Objects.requireNonNull(state, "batch evidence state must not be null");
        this.inputCount = source.inputCount;
        this.affectedRows = source.affectedRows;
        this.successfulPrefix = source.successfulPrefix;
        // The accumulator owns these immutable snapshots and never mutates either bitmap.
        this.successful = source.successful;
        this.failed = source.failed;
        this.conflicts = source.conflicts;
        this.failure = failure;
    }

    public BatchExecutionState state() { return state; }
    public long inputCount() { return inputCount; }
    public BatchAffectedRows affectedRows() { return affectedRows; }
    public long successfulCount() { return successfulPrefix + successful.cardinality(); }
    public long failedCount() { return failed.cardinality(); }
    public List<BatchRowConflict> conflicts() { return conflicts; }
    public Failure failure() { return failure; }

    public boolean isSuccessful(long offset) {
        return offset >= 0 && (offset < successfulPrefix || contains(successful, offset));
    }

    public boolean isFailed(long offset) { return offset >= 0 && contains(failed, offset); }

    private boolean contains(BitSet positions, long offset) {
        long relative = offset - successfulPrefix;
        return relative >= 0 && relative < Integer.MAX_VALUE && positions.get((int) relative);
    }

    public LongStream successfulOffsets() {
        return LongStream.concat(LongStream.range(0, successfulPrefix),
                                 successful.stream().mapToLong(offset -> successfulPrefix + offset));
    }

    public LongStream failedOffsets() {
        return failed.stream().mapToLong(offset -> successfulPrefix + offset);
    }

    /**
     * Internal execution owner. All methods synchronize mutation and cancellation snapshots.
     * Adapters must order SQL, callbacks and recording; a snapshot includes completed mutations only.
     * Input acceptance is independent and can record an already-arriving tail after a terminal window.
     */
    @InternalApi
    public static final class Accumulator {
        private long inputCount;
        private long successfulPrefix;
        private BatchAffectedRows affectedRows = BatchAffectedRows.known(0);
        private BitSet successful = new BitSet();
        private BitSet failed = new BitSet();
        private List<BatchRowConflict> conflicts = List.of();
        private boolean terminal;

        public synchronized void accept(long count) {
            requireNonNegative(count);
            inputCount = add(inputCount, count);
        }

        /** Extends the contiguous successful SQL prefix, even when affected row counts are unknown. */
        public synchronized void succeeded(long count, BatchAffectedRows rows) {
            requireRecording();
            requireNonNegative(count);
            long end = add(successfulPrefix, count);
            requireAccepted(end);
            BatchAffectedRows total = sum(affectedRows, rows);
            successfulPrefix = end;
            affectedRows = total;
        }

        /**
         * Records the sole terminal window, starting at the current successful prefix.
         * Bitmap positions are relative to this window; conflict offsets are absolute.
         * Unmarked positions remain unproven, including accepted inputs beyond the window.
         */
        public synchronized void terminal(int windowSize, BitSet successful, BitSet failed,
                                          BatchAffectedRows rows, List<BatchRowConflict> conflicts) {
            requireRecording();
            requireNonNegative(windowSize);
            long end = add(successfulPrefix, windowSize);
            requireAccepted(end);
            BitSet successes = (BitSet) Objects.requireNonNull(successful, "successful positions must not be null").clone();
            BitSet failures = (BitSet) Objects.requireNonNull(failed, "failed positions must not be null").clone();
            if (successes.length() > windowSize || failures.length() > windowSize) {
                throw new IllegalArgumentException("batch positions must fit within the terminal window");
            }
            if (successes.intersects(failures)) {
                throw new IllegalArgumentException("batch successful and failed positions must not overlap");
            }
            Objects.requireNonNull(conflicts, "batch conflicts must not be null");
            if (conflicts.size() > failures.cardinality()) {
                throw new IllegalArgumentException("batch conflicts must fit within failed terminal positions");
            }
            List<BatchRowConflict> safeConflicts = List.copyOf(conflicts);
            BitSet conflictPositions = new BitSet();
            for (BatchRowConflict conflict : safeConflicts) {
                long relative = conflict.inputOffset() - successfulPrefix;
                if (relative < 0 || relative >= windowSize || !failures.get((int) relative)
                        || conflictPositions.get((int) relative)) {
                    throw new IllegalArgumentException("batch conflicts must identify a failed terminal-window position");
                }
                conflictPositions.set((int) relative);
            }
            BatchAffectedRows total = sum(affectedRows, rows);
            this.successful = successes;
            this.failed = failures;
            this.conflicts = safeConflicts;
            this.affectedRows = total;
            this.terminal = true;
        }

        public synchronized BatchExecutionEvidence snapshot(BatchExecutionState state, Failure failure) {
            return new BatchExecutionEvidence(this, state, failure);
        }

        private void requireRecording() {
            if (terminal) throw new IllegalStateException("batch execution already has a terminal window");
        }

        private void requireAccepted(long end) {
            if (end > inputCount) throw new IllegalArgumentException("batch execution positions exceed accepted inputs");
        }

        private static void requireNonNegative(long count) {
            if (count < 0) throw new IllegalArgumentException("batch count must not be negative");
        }

        private static BatchAffectedRows sum(BatchAffectedRows left, BatchAffectedRows right) {
            Objects.requireNonNull(right, "batch affected rows must not be null");
            return left.isKnown() && right.isKnown()
                    ? BatchAffectedRows.known(add(left.value(), right.value()))
                    : BatchAffectedRows.unknown();
        }

        private static long add(long left, long right) {
            try {
                return Math.addExact(left, right);
            } catch (ArithmeticException overflow) {
                throw new RdbException(RdbErrorKind.UNKNOWN, "database execution count exceeds supported range",
                                       null, null, overflow);
            }
        }
    }

    /**
     * 可安全暴露的失败摘要，不包含 SQL 参数。
     *
     * @param type      异常类名
     * @param message   异常消息
     * @param sqlState  数据库 SQL state
     * @param errorCode 数据库错误码
     * @param kind      flying-orm 的稳定错误分类
     */
    public record Failure(String type,
                          String message,
                          String sqlState,
                          int errorCode,
                          RdbErrorKind kind) {

        public Failure {
            type = Objects.requireNonNull(type, "batch failure type must not be null");
            message = Objects.requireNonNull(message, "batch failure message must not be null");
            sqlState = publicSqlState(sqlState);
            kind = Objects.requireNonNull(kind, "batch failure kind must not be null");
        }

        /**
         * 从异常提取公开失败信息。
         *
         * @param error 原始异常
         * @return 失败摘要
         */
        public static Failure from(Throwable error) {
            Throwable safeError = Objects.requireNonNull(error, "batch chunk error must not be null");
            RuntimeException translated = RdbExceptionTranslator.translate(safeError);
            if (translated instanceof RdbException rdbError) {
                Throwable driverError = rdbError.getCause();
                return new Failure(driverError.getClass().getName(),
                                   publicMessage(rdbError.kind()),
                                   rdbError.sqlState(),
                                   rdbError.errorCode() == null ? 0 : rdbError.errorCode(),
                                   rdbError.kind());
            }
            return new Failure(safeError.getClass().getName(),
                               publicMessage(RdbErrorKind.UNKNOWN),
                               null,
                               0,
                               RdbErrorKind.UNKNOWN);
        }

        /**
         * 驱动消息经常把冲突值、表名甚至 SQL 片段一起带出来，不能放进会返回给使用方的批量结果。
         * 公开层只按稳定分类给固定说明，详细驱动消息仍保留在原异常和受控观测链路里。
         */
        private static String publicMessage(RdbErrorKind kind) {
            return switch (kind) {
                case DUPLICATE_KEY -> "database duplicate key conflict";
                case CONSTRAINT -> "database integrity constraint failed";
                case BAD_SQL -> "database rejected sql";
                case CONNECTION -> "database connection failed";
                case TIMEOUT -> "database operation timed out";
                case DEADLOCK -> "database transaction deadlocked";
                case LOCK_TIMEOUT -> "database lock wait timed out";
                case CANCELLED -> "database operation was cancelled";
                case UNKNOWN -> "database operation failed";
            };
        }

        private static String publicSqlState(String value) {
            if (value == null || value.length() != 5) {
                return null;
            }
            for (int index = 0; index < value.length(); index++) {
                char character = value.charAt(index);
                if (!(character >= '0' && character <= '9')
                        && !(character >= 'A' && character <= 'Z')) {
                    return null;
                }
            }
            return value;
        }
    }
}
