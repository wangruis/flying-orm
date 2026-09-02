package com.flying.orm.rdb.batch;

import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * BatchWriteOptions 把批量大小、提交方式和恢复策略放在一起，默认选择整批原子提交。
 *
 * <p>{@link RecoveryMode#RECEIPT} 是当前 R2DBC 批量执行器的恢复能力。JDBC 批量仍支持
 * {@link Mode#ATOMIC} 和 {@link Mode#INDEPENDENT}，但会在订阅输入 Publisher 和获取连接前拒绝回执恢复配置。</p>
 *
 * @param mode        提交方式
 * @param chunkSize   每个分片最多包含多少行
 * @param concurrency 独立分片最多同时执行多少个
 * @param maxRows     本次任务允许接收的最大行数，0 表示不限
 * @param maxBufferedBytes 全部在途分片允许持有的输入估算重量上限，不是 JVM 堆占用上限
 * @param maxRowBytes 单行输入估算重量上限；每片在请求下一行前预留此额度
 * @param maxResultChunks 聚合结果允许保留的分片明细数
 * @param timeout     连接可用后，每个自有批量事务执行 SQL 和提交的兜底时限；0 表示不限制
 * @param recovery    提交结果不确定时使用的恢复策略
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
    /** 默认不叠加 ORM 总超时；自有原子事务仍保证提交或回滚终态。 */
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
        recovery = Objects.requireNonNull(recovery, "batch recovery must not be null");
    }

    /**
     * 返回默认配置，等价于每 500 行一个分片并整批原子提交。
     *
     * @return 默认配置
     */
    public static BatchWriteOptions defaults() {
        return atomic(DEFAULT_CHUNK_SIZE);
    }

    /**
     * 创建整批原子提交配置。
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
     * 返回带批量事务 SQL 兜底时限的新配置。连接排队仍完全服从上层连接池；
     * {@code INDEPENDENT} 模式下每个自有分片事务分别计时。JDBC 会在进入提交前检查同一截止点，
     * 但标准 JDBC 没有单独限制 {@code Connection.commit()} 阻塞时长的 API，提交调用本身仍受驱动和网络配置约束。
     *
     * @param timeout 连接可用后的事务执行时限，0 表示不限制
     * @return 新配置
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
     * 使用默认回执表开启 UNKNOWN 恢复。
     *
     * <p>该恢复模式当前由 R2DBC 批量执行器提供；JDBC 批量会在执行前明确拒绝。</p>
     *
     * @param operationId 稳定且唯一的操作编号
     * @return 新配置
     */
    public BatchWriteOptions withReceipt(String operationId) {
        return withReceipt(operationId, Duration.ofSeconds(3));
    }

    /**
     * 使用默认回执表开启 UNKNOWN 恢复，并指定主动确认等待时间。
     *
     * <p>该恢复模式当前由 R2DBC 批量执行器提供；JDBC 批量会在执行前明确拒绝。</p>
     *
     * @param operationId    稳定且唯一的操作编号
     * @param confirmTimeout 确认连接可用后，回执查询最多执行多久
     * @return 新配置
     */
    public BatchWriteOptions withReceipt(String operationId, Duration confirmTimeout) {
        Recovery configured = new Recovery(RecoveryMode.RECEIPT,
                                           operationId,
                                           Recovery.DEFAULT_RECEIPT_TABLE,
                                           confirmTimeout);
        return new BatchWriteOptions(mode,
                                     chunkSize,
                                     concurrency,
                                     maxRows,
                                     maxBufferedBytes,
                                     maxRowBytes,
                                     maxResultChunks,
                                     timeout,
                                     configured);
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
        /** 整批共用一个事务，任一分片失败就全部回滚。 */
        ATOMIC,
        /** 每个分片使用自己的事务，允许部分成功。 */
        INDEPENDENT
    }

    /** UNKNOWN 的恢复方式。 */
    public enum RecoveryMode {
        /** 不写框架回执，由业务自行确认。 */
        NONE,
        /** 在同一事务中写回执，用于确认和幂等重放。 */
        RECEIPT
    }

    /**
     * UNKNOWN 恢复配置。
     *
     * @param mode           恢复方式
     * @param operationId    调用方提供的稳定操作编号
     * @param receiptTable   回执表名
     * @param confirmTimeout 确认连接可用后，回执查询最多执行多久
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
            if (mode == RecoveryMode.RECEIPT && operationId.isBlank()) {
                throw new IllegalArgumentException("batch operation id must not be blank");
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
