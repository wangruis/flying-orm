package com.flying.orm.rdb.batch;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;

import java.util.List;
import java.util.Objects;

/**
 * BatchChunkResult 说明一个分片最后落到数据库里的真实状态。
 * @param chunkIndex   分片编号，从 0 开始
 * @param startOffset  分片第一行在输入流里的位置
 * @param inputCount   分片接收的行数
 * @param affectedRows 已确认提交的影响行数
 * @param status       分片最终状态
 * @param failure      失败摘要，成功时为空
 * @param recoveryToken UNKNOWN 的恢复令牌，没有回执时为空
 * @param conflicts    已确认的行级冲突，只有 CONFLICTED 状态会带
 * @author wangr
 * @date 2026-07-23
 * @version v1.0
 */
public record BatchChunkResult(int chunkIndex,
                               long startOffset,
                               int inputCount,
                               long affectedRows,
                               Status status,
                               Failure failure,
                               RecoveryToken recoveryToken,
                               List<BatchRowConflict> conflicts) {

    /**
     * 检查分片位置和计数，避免错误结果继续向上汇总。
     */
    public BatchChunkResult {
        if (chunkIndex < 0) {
            throw new IllegalArgumentException("batch chunk index must not be negative");
        }
        if (startOffset < 0) {
            throw new IllegalArgumentException("batch chunk start offset must not be negative");
        }
        if (inputCount < 0) {
            throw new IllegalArgumentException("batch chunk input count must not be negative");
        }
        if (inputCount > 0 && startOffset > Long.MAX_VALUE - ((long) inputCount - 1L)) {
            throw new IllegalArgumentException("batch chunk input range must not overflow");
        }
        if (affectedRows < 0) {
            throw new IllegalArgumentException("batch chunk affected rows must not be negative");
        }
        status = Objects.requireNonNull(status, "batch chunk status must not be null");
        conflicts = List.copyOf(Objects.requireNonNull(conflicts, "batch chunk conflicts must not be null"));
        if (status != Status.COMMITTED && affectedRows != 0) {
            throw new IllegalArgumentException("only committed batch chunk can include affected rows");
        }
        boolean needsFailure = status == Status.FAILED || status == Status.UNKNOWN;
        if (needsFailure && failure == null) {
            throw new IllegalArgumentException("failed or unknown batch chunk must include failure details");
        }
        if (!needsFailure && failure != null) {
            throw new IllegalArgumentException("non-failed batch chunk cannot include failure details");
        }
        if (status != Status.UNKNOWN && recoveryToken != null) {
            throw new IllegalArgumentException("only unknown batch chunk can include recovery token");
        }
        if (status == Status.CONFLICTED && conflicts.isEmpty()) {
            throw new IllegalArgumentException("conflicted batch chunk must include conflict details");
        }
        if (status != Status.CONFLICTED && !conflicts.isEmpty()) {
            throw new IllegalArgumentException("only conflicted batch chunk can include conflict details");
        }
    }

    /**
     * 创建已经确认提交的分片结果。
     *
     * @param chunkIndex  分片编号
     * @param startOffset 输入起点
     * @param inputCount  输入行数
     * @param affectedRows 影响行数
     * @return 已提交结果
     */
    public static BatchChunkResult committed(int chunkIndex,
                                             long startOffset,
                                             int inputCount,
                                             long affectedRows) {
        return new BatchChunkResult(chunkIndex,
                                    startOffset,
                                    inputCount,
                                    affectedRows,
                                    Status.COMMITTED,
                                    null,
                                    null,
                                    List.of());
    }

    /**
     * 创建执行失败且没有提交的分片结果。
     *
     * @param chunkIndex  分片编号
     * @param startOffset 输入起点
     * @param inputCount  输入行数
     * @param error       原始异常
     * @return 失败结果
     */
    public static BatchChunkResult failed(int chunkIndex,
                                          long startOffset,
                                          int inputCount,
                                          Throwable error) {
        return new BatchChunkResult(chunkIndex,
                                    startOffset,
                                    inputCount,
                                    0,
                                    Status.FAILED,
                                    Failure.from(error),
                                    null,
                                    List.of());
    }

    /**
     * 创建已经确认回滚的冲突分片。冲突偏移是整批输入里的位置，不是分片内下标。
     */
    public static BatchChunkResult conflicted(int chunkIndex,
                                              long startOffset,
                                              int inputCount,
                                              List<BatchRowConflict> conflicts) {
        return new BatchChunkResult(chunkIndex,
                                    startOffset,
                                    inputCount,
                                    0,
                                    Status.CONFLICTED,
                                    null,
                                    null,
                                    conflicts);
    }

    /**
     * 创建已执行但随整批事务回滚的分片结果。
     *
     * @param chunkIndex  分片编号
     * @param startOffset 输入起点
     * @param inputCount  输入行数
     * @return 已回滚结果
     */
    public static BatchChunkResult rolledBack(int chunkIndex, long startOffset, int inputCount) {
        return new BatchChunkResult(chunkIndex,
                                    startOffset,
                                    inputCount,
                                    0,
                                    Status.ROLLED_BACK,
                                    null,
                                    null,
                                    List.of());
    }

    /**
     * 创建提交结果暂时无法确认的分片结果。
     *
     * @param chunkIndex    分片编号
     * @param startOffset   输入起点
     * @param inputCount    输入行数
     * @param error         导致确认丢失的异常
     * @param recoveryToken 后续确认使用的恢复令牌
     * @return 状态未知结果
     */
    public static BatchChunkResult unknown(int chunkIndex,
                                           long startOffset,
                                           int inputCount,
                                           Throwable error,
                                           RecoveryToken recoveryToken) {
        return new BatchChunkResult(chunkIndex,
                                    startOffset,
                                    inputCount,
                                    0,
                                    Status.UNKNOWN,
                                    Failure.from(error),
                                    Objects.requireNonNull(recoveryToken, "batch recovery token must not be null"),
                                    List.of());
    }

    /**
     * 创建没有框架回执、只能由业务自行确认的 UNKNOWN 结果。
     *
     * @param chunkIndex  分片编号
     * @param startOffset 输入起点
     * @param inputCount  输入行数
     * @param error       导致确认丢失的异常
     * @return 状态未知结果
     */
    public static BatchChunkResult unknown(int chunkIndex,
                                           long startOffset,
                                           int inputCount,
                                           Throwable error) {
        return new BatchChunkResult(chunkIndex,
                                    startOffset,
                                    inputCount,
                                    0,
                                    Status.UNKNOWN,
                                    Failure.from(error),
                                    null,
                                    List.of());
    }

    /** 分片最终状态。 */
    public enum Status {
        /** 数据库已经确认提交。 */
        COMMITTED,
        /** SQL 已在外部事务中执行，最终提交或回滚仍由外层事务管理器决定。 */ ENLISTED,
        /** 分片执行成功，但所在的整批事务已经确认回滚。 */
        ROLLED_BACK,
        /** 分片执行失败，并且没有提交。 */
        FAILED,
        /** SQL 执行成功，但至少一行没有得到期望影响行数，分片已经回滚。 */
        CONFLICTED,
        /** 提交结果暂时无法确认。 */
        UNKNOWN
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

    /**
     * UNKNOWN 后用于查询回执的最小信息。
     *
     * @param operationId  操作编号
     * @param chunkIndex   分片编号
     * @param receiptTable 回执表名
     * @param planHash     SQL 计划摘要
     * @param payloadHash  参数摘要
     * @param expectedRowCount 已完整消费并写入事务的输入行数；流尚未完整消费时为 null
     * @param expectedAffectedRows 提交前已经确认的影响行数；策略不要求精确影响行数时为 null
     */
    public record RecoveryToken(String operationId,
                                int chunkIndex,
                                String receiptTable,
                                String planHash,
                                String payloadHash,
                                Long expectedRowCount,
                                Long expectedAffectedRows) {

        /**
         * 检查恢复令牌，空编号或摘要无法可靠确认提交结果。
         */
        public RecoveryToken {
            operationId = BatchWriteOptions.Recovery.requireOperationId(operationId);
            if (chunkIndex < 0) {
                throw new IllegalArgumentException("batch recovery chunk index must not be negative");
            }
            // 恢复令牌可能来自持久化或远程传输，不能假设它一定由 Recovery 配置生成。
            receiptTable = BatchWriteOptions.Recovery.requireReceiptTable(receiptTable);
            planHash = requireText(planHash, "batch recovery plan hash");
            if (payloadHash == null) {
                if (expectedRowCount != null || expectedAffectedRows != null) {
                    throw new IllegalArgumentException(
                            "incomplete batch recovery evidence must not contain expected counts");
                }
            } else {
                payloadHash = requireText(payloadHash, "batch recovery payload hash");
                expectedRowCount = requireNonNegative(
                        Objects.requireNonNull(expectedRowCount,
                                               "complete batch recovery expected row count must not be null"),
                        "batch recovery expected row count");
                if (expectedAffectedRows != null) {
                    expectedAffectedRows = requireNonNegative(expectedAffectedRows,
                                                              "batch recovery expected affected rows");
                }
            }
        }

        /**
         * 判断令牌是否拥有足以确认 COMMITTED 的完整事实。
         *
         * <p>流式输入尚未消费结束时没有 payload 摘要和精确行数，只能保留 UNKNOWN 操作标识用于诊断，
         * 不能仅凭 operation id 把结果提升为 COMMITTED。</p>
         *
         * @return 同时拥有 payload 摘要和预期输入行数时返回 true
         */
        public boolean hasCompleteEvidence() {
            return payloadHash != null && expectedRowCount != null;
        }

        private static long requireNonNegative(long value, String name) {
            if (value < 0L) {
                throw new IllegalArgumentException(name + " must not be negative");
            }
            return value;
        }
    }

    private static String requireText(String value, String name) {
        String safeValue = Objects.requireNonNull(value, name + " must not be null");
        if (safeValue.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return safeValue;
    }
}
