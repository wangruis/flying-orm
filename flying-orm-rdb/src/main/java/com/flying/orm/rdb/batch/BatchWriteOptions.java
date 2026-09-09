package com.flying.orm.rdb.batch;

import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * 批量大小、事务参与方式和兼容恢复配置。默认 ATOMIC 只参与上层外部事务。
 *
 * <p>只有显式 {@link Mode#INDEPENDENT} 模式允许 ORM 拥有分片局部事务。
 * 批量总时限、回执幂等和事务恢复由上层治理；正时限及 {@link RecoveryMode#RECEIPT} 配置明确拒绝。</p>
 *
 * @param mode        提交方式
 * @param chunkSize   每个分片最多包含多少行
 * @param concurrency 独立分片最多同时执行多少个
 * @param maxRows     本次任务允许接收的最大行数，0 表示不限
 * @param maxBufferedBytes 全部在途分片允许持有的输入估算重量上限，不是 JVM 堆占用上限
 * @param maxRowBytes 单行输入估算重量上限；每片在请求下一行前预留此额度
 * @param maxResultChunks 聚合结果允许保留的分片明细数
 * @param timeout     兼容字段，仅支持 0；正数抛出 UnsupportedOperationException
 * @param recovery    兼容恢复字段，仅支持 NONE 和零确认时限
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
public record BatchWriteOptions(Mode mode,
                                int chunkSize,
                                int concurrency,
                                long maxRows,
                                long maxBufferedBytes,
                                long maxRowBytes,
                                int maxResultChunks,
                                Duration timeout,
                                Recovery recovery) {

    /** 默认分片大小。 */
    public static final int DEFAULT_CHUNK_SIZE = 500;
    public static final long DEFAULT_MAX_ROWS = 100_000L;
    public static final long DEFAULT_MAX_BUFFERED_BYTES = 32L * 1024 * 1024;
    public static final int DEFAULT_MAX_RESULT_CHUNKS = 4_096;
    /** ORM 不拥有批量总时限，保留零值兼容默认配置。 */
    public static final Duration DEFAULT_TIMEOUT = Duration.ZERO;
    /**
     * 检查配置，错误参数在拿数据库连接前就直接报出来。
     */
    public BatchWriteOptions {
        mode = Objects.requireNonNull(mode, "batch write mode must not be null");
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("batch chunk size must be greater than zero");
        }
        if (concurrency <= 0) {
            throw new IllegalArgumentException("batch concurrency must be greater than zero");
        }
        if (mode == Mode.ATOMIC && concurrency != 1) {
            throw new IllegalArgumentException("atomic batch concurrency must be one");
        }
        if (maxRows < 0) {
            throw new IllegalArgumentException("batch max rows must not be negative");
        }
        if (maxBufferedBytes <= 0) {
            throw new IllegalArgumentException("batch max buffered bytes must be greater than zero");
        }
        if (maxRowBytes <= 0 || maxRowBytes > maxBufferedBytes / concurrency) {
            throw new IllegalArgumentException("batch max row bytes must fit within one concurrent chunk budget");
        }
        if (maxResultChunks <= 0) {
            throw new IllegalArgumentException("batch max result chunks must be greater than zero");
        }
        timeout = requireNonNegative(timeout, "batch timeout");
        if (!timeout.isZero()) {
            throw new UnsupportedOperationException("batch timeout must be managed by the caller");
        }
        recovery = Objects.requireNonNull(recovery, "batch recovery must not be null");
    }

    /**
     * 返回默认配置，每 500 行一个分片，整批只参与上层外部事务。
     *
     * @return 默认配置
     */
    public static BatchWriteOptions defaults() {
        return atomic(DEFAULT_CHUNK_SIZE);
    }

    /**
     * 创建整批外部事务参与配置；非空输入要求上层已经提供事务。
     *
     * @param chunkSize 每个分片最多包含多少行
     * @return 原子提交配置
     */
    public static BatchWriteOptions atomic(int chunkSize) {
        return new BatchWriteOptions(Mode.ATOMIC,
                                     chunkSize,
                                     1,
                                     DEFAULT_MAX_ROWS,
                                     DEFAULT_MAX_BUFFERED_BYTES,
                                     defaultMaxRowBytes(chunkSize, 1, DEFAULT_MAX_BUFFERED_BYTES),
                                     DEFAULT_MAX_RESULT_CHUNKS,
                                     DEFAULT_TIMEOUT,
                                     Recovery.none());
    }

    /**
     * 创建顺序执行的独立分片配置。
     *
     * @param chunkSize 每个分片最多包含多少行
     * @return 独立分片配置
     */
    public static BatchWriteOptions independent(int chunkSize) {
        return independent(chunkSize, 1);
    }

    /**
     * 创建有界并发的独立分片配置。
     *
     * @param chunkSize   每个分片最多包含多少行
     * @param concurrency 最多同时执行多少个分片
     * @return 独立分片配置
     */
    public static BatchWriteOptions independent(int chunkSize, int concurrency) {
        return new BatchWriteOptions(Mode.INDEPENDENT,
                                     chunkSize,
                                     concurrency,
                                     DEFAULT_MAX_ROWS,
                                     DEFAULT_MAX_BUFFERED_BYTES,
                                     defaultMaxRowBytes(chunkSize, concurrency, DEFAULT_MAX_BUFFERED_BYTES),
                                     DEFAULT_MAX_RESULT_CHUNKS,
                                     DEFAULT_TIMEOUT,
                                     Recovery.none());
    }

    /**
     * 创建不限制任务总行数和执行时间的原子批量配置。分片、缓冲和结果明细仍保持有界。
     *
     * @param chunkSize 每个分片的最大行数
     * @return 无超时边界的原子批量配置
     */
    public static BatchWriteOptions unlimitedAtomic(int chunkSize) {
        return atomic(chunkSize)
                .withMaxRows(0)
                .withTimeout(Duration.ZERO);
    }

    /**
     * 创建不限制任务总行数和执行时间的独立分片配置。分片、并发、缓冲和结果明细仍保持有界。
     *
     * @param chunkSize 每个分片的最大行数
     * @param concurrency 最大并发分片数
     * @return 无超时边界的独立分片配置
     */
    public static BatchWriteOptions unlimitedIndependent(int chunkSize, int concurrency) {
        return independent(chunkSize, concurrency)
                .withMaxRows(0)
                .withTimeout(Duration.ZERO);
    }

    /**
     * 返回带最大行数限制的新配置。
     *
     * @param maxRows 最大行数，0 表示不限
     * @return 新配置
     */
    public BatchWriteOptions withMaxRows(long maxRows) {
        return new BatchWriteOptions(mode,
                                     chunkSize,
                                     concurrency,
                                     maxRows,
                                     maxBufferedBytes,
                                     maxRowBytes,
                                     maxResultChunks,
                                     timeout,
                                     recovery);
    }

    /**
     * 设置行数、缓冲字节和结果明细边界，并重新计算单行上限。
     * 每片默认预留其额度的一半（至少 1 字节），单行分片使用全部额度；
     * 需要其他单行上限时，在此方法之后调用 {@link #withMaxRowBytes(long)}。
     */
    public BatchWriteOptions withMemoryLimits(long maxRows,
                                              long maxBufferedBytes,
                                              int maxResultChunks) {
        return new BatchWriteOptions(mode,
                                     chunkSize,
                                     concurrency,
                                     maxRows,
                                     maxBufferedBytes,
                                     defaultMaxRowBytes(chunkSize, concurrency, maxBufferedBytes),
                                     maxResultChunks,
                                     timeout,
                                     recovery);
    }

    /** 返回使用指定单行输入估算重量上限的新配置，该上限必须不超过每个并发分片的额度。 */
    public BatchWriteOptions withMaxRowBytes(long maxRowBytes) {
        return new BatchWriteOptions(mode,
                                     chunkSize,
                                     concurrency,
                                     maxRows,
                                     maxBufferedBytes,
                                     maxRowBytes,
                                     maxResultChunks,
                                     timeout,
                                     recovery);
    }

    /**
     * 保留旧时限配置签名；ORM 不再拥有批量总时限。
     *
     * @param timeout 仅支持 0，执行时限由上层和驱动治理
     * @return 零时限的新配置
     * @throws UnsupportedOperationException 配置正时限时
     */
    public BatchWriteOptions withTimeout(Duration timeout) {
        return new BatchWriteOptions(mode,
                                     chunkSize,
                                     concurrency,
                                     maxRows,
                                     maxBufferedBytes,
                                     maxRowBytes,
                                     maxResultChunks,
                                     timeout,
                                     recovery);
    }

    /**
     * 保留旧回执恢复入口；回执幂等和事务恢复必须由上层实现。
     *
     * @param operationId 稳定且唯一的操作编号
     * @return 不返回配置
     * @throws UnsupportedOperationException ORM 不再提供回执恢复
     */
    public BatchWriteOptions withReceipt(String operationId) {
        throw new UnsupportedOperationException("batch receipt recovery must be managed by the caller");
    }

    /**
     * 保留旧回执恢复入口，不将确认时限静默转换为其他预算。
     *
     * @param operationId 稳定且唯一的操作编号
     * @param confirmTimeout 原回执确认时限
     * @return 不返回配置
     * @throws UnsupportedOperationException ORM 不再提供回执恢复
     */
    public BatchWriteOptions withReceipt(String operationId, Duration confirmTimeout) {
        throw new UnsupportedOperationException("batch receipt recovery must be managed by the caller");
    }

    private static long defaultMaxRowBytes(int chunkSize, int concurrency, long maxBufferedBytes) {
        long perChunk = maxBufferedBytes / Math.max(1, concurrency);
        return Math.max(1L, perChunk / (chunkSize == 1 ? 1 : 2));
    }

    private static Duration requireNonNegative(Duration duration, String name) {
        Duration safeDuration = Objects.requireNonNull(duration, name + " must not be null");
        if (safeDuration.isNegative()) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return safeDuration;
    }

    /** 批量提交方式。 */
    public enum Mode {
        /** 整批参与上层外部事务；提交和回滚均由上层决定，非空输入缺事务时拒绝。 */
        ATOMIC,
        /** 每个分片使用自己的事务，允许部分成功。 */
        INDEPENDENT
    }

    /** UNKNOWN 的恢复方式。 */
    public enum RecoveryMode {
        /** 不写框架回执，由业务自行确认。 */
        NONE,
        /** 兼容枚举值；配置该模式时明确拒绝，由上层治理回执和恢复。 */
        RECEIPT
    }

    /**
     * UNKNOWN 恢复配置。
     *
     * @param mode           恢复方式
     * @param operationId    调用方提供的稳定操作编号
     * @param receiptTable   回执表名
     * @param confirmTimeout 兼容字段，仅支持 0；正数明确拒绝
     */
    public record Recovery(RecoveryMode mode,
                           String operationId,
                           String receiptTable,
                           Duration confirmTimeout) {

        /** 默认回执表名。 */
        public static final String DEFAULT_RECEIPT_TABLE = "flying_orm_batch_receipt";

        private static final int MAX_OPERATION_ID_LENGTH = 128;

        private static final Pattern RECEIPT_TABLE_PATTERN = Pattern.compile(
                "[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?");

        /**
         * 创建不写回执的恢复配置。
         *
         * @return 无回执配置
         */
        public static Recovery none() {
            return new Recovery(RecoveryMode.NONE, "", DEFAULT_RECEIPT_TABLE, Duration.ZERO);
        }

        /**
         * 检查恢复配置的基础字段。
         */
        public Recovery {
            mode = Objects.requireNonNull(mode, "batch recovery mode must not be null");
            operationId = Objects.requireNonNull(operationId, "batch operation id must not be null");
            receiptTable = requireReceiptTable(receiptTable);
            confirmTimeout = requireNonNegative(confirmTimeout, "batch confirm timeout");
            if (mode == RecoveryMode.RECEIPT || !confirmTimeout.isZero()) {
                throw new UnsupportedOperationException("batch receipt recovery must be managed by the caller");
            }
            if (operationId.length() > MAX_OPERATION_ID_LENGTH) {
                throw new IllegalArgumentException(
                        "batch operation id must not exceed 128 characters");
            }
        }

        /** 外部恢复令牌必须遵守与标准回执表相同的操作编号边界。 */
        static String requireOperationId(String operationId) {
            String safeOperationId = Objects.requireNonNull(
                    operationId, "batch recovery operation id must not be null");
            if (safeOperationId.isBlank()) {
                throw new IllegalArgumentException("batch recovery operation id must not be blank");
            }
            if (safeOperationId.length() > MAX_OPERATION_ID_LENGTH) {
                throw new IllegalArgumentException(
                        "batch operation id must not exceed 128 characters");
            }
            return safeOperationId;
        }

        /*
         * 回执表名会直接进入框架生成的 SQL，所有能构造恢复令牌的入口都必须走同一条白名单。
         * 这里只允许普通表名或 schema.table，不接受引号、空格和 SQL 片段。
         */
        static String requireReceiptTable(String receiptTable) {
            String safeReceiptTable = Objects.requireNonNull(receiptTable,
                                                              "batch receipt table must not be null");
            if (safeReceiptTable.isBlank()) {
                throw new IllegalArgumentException("batch receipt table must not be blank");
            }
            if (!RECEIPT_TABLE_PATTERN.matcher(safeReceiptTable).matches()) {
                throw new IllegalArgumentException("batch receipt table must be a plain table identifier");
            }
            return safeReceiptTable;
        }
    }
}
