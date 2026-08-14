package com.flying.orm.rdb.batch;

import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * BatchWriteOptions 把批量大小、提交方式和恢复策略放在一起，默认选择整批原子提交。
 *
 * @param mode        提交方式
 * @param chunkSize   每个分片最多包含多少行
 * @param concurrency 独立分片最多同时执行多少个
 * @param maxRows     本次任务允许接收的最大行数，必须是正数
 * @param timeout                  整个批量任务的超时时间，0 表示不限制
 * @param connectionAcquireTimeout R2DBC 每个批量事务最多等待连接多久，0 表示交给连接池控制；JDBC
 *                                 由 DataSource/连接池配置连接等待上限
 * @param recovery                 提交结果不确定时使用的恢复策略
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
public record BatchWriteOptions(Mode mode,
                                int chunkSize,
                                int concurrency,
                                long maxRows,
                                long maxBufferedBytes,
                                int maxResultChunks,
                                Duration timeout,
                                Duration connectionAcquireTimeout,
                                Recovery recovery) {

    /** 默认分片大小。 */
    public static final int DEFAULT_CHUNK_SIZE = 500;
    public static final long DEFAULT_MAX_ROWS = 100_000L;
    public static final long DEFAULT_MAX_BUFFERED_BYTES = 32L * 1024 * 1024;
    public static final int DEFAULT_MAX_RESULT_CHUNKS = 4_096;
    /** 普通批量任务默认最多运行五分钟。 */
    public static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);
    /** R2DBC 每个批量事务默认最多等待连接五秒；JDBC 使用 DataSource/连接池自己的等待上限。 */
    public static final Duration DEFAULT_CONNECTION_ACQUIRE_TIMEOUT = Duration.ofSeconds(5);

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
        if (maxRows <= 0) {
            throw new IllegalArgumentException("batch max rows must be greater than zero");
        }
        if (maxBufferedBytes <= 0) {
            throw new IllegalArgumentException("batch max buffered bytes must be greater than zero");
        }
        if (maxResultChunks <= 0) {
            throw new IllegalArgumentException("batch max result chunks must be greater than zero");
        }
        timeout = requireNonNegative(timeout, "batch timeout");
        connectionAcquireTimeout = requireNonNegative(connectionAcquireTimeout,
                                                       "batch connection acquisition timeout");
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
                                     DEFAULT_MAX_RESULT_CHUNKS,
                                     DEFAULT_TIMEOUT,
                                     DEFAULT_CONNECTION_ACQUIRE_TIMEOUT,
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
                                     DEFAULT_MAX_RESULT_CHUNKS,
                                     DEFAULT_TIMEOUT,
                                     DEFAULT_CONNECTION_ACQUIRE_TIMEOUT,
                                     Recovery.none());
    }

    /**
     * 显式创建不限制任务和连接等待时间的原子批量配置；只应在外部资源已经提供等价边界时使用。
     *
     * @param chunkSize 每个分片的最大行数
     * @return 无超时边界的原子批量配置
     */
    public static BatchWriteOptions unlimitedAtomic(int chunkSize) {
        return atomic(chunkSize).withTimeout(Duration.ZERO).withConnectionAcquireTimeout(Duration.ZERO);
    }

    /**
     * 显式创建不限制任务和连接等待时间的独立分片配置。
     *
     * @param chunkSize 每个分片的最大行数
     * @param concurrency 最大并发分片数
     * @return 无超时边界的独立分片配置
     */
    public static BatchWriteOptions unlimitedIndependent(int chunkSize, int concurrency) {
        return independent(chunkSize, concurrency)
                .withTimeout(Duration.ZERO)
                .withConnectionAcquireTimeout(Duration.ZERO);
    }

    /**
     * 返回带最大行数限制的新配置。
     *
     * @param maxRows 最大行数，必须是正数
     * @return 新配置
     */
    public BatchWriteOptions withMaxRows(long maxRows) {
        return new BatchWriteOptions(mode,
                                     chunkSize,
                                     concurrency,
                                     maxRows,
                                     maxBufferedBytes,
                                     maxResultChunks,
                                     timeout,
                                     connectionAcquireTimeout,
                                     recovery);
    }

    /** 一次设置行数、缓冲字节和结果明细三条硬边界。 */
    public BatchWriteOptions withMemoryLimits(long maxRows,
                                              long maxBufferedBytes,
                                              int maxResultChunks) {
        return new BatchWriteOptions(mode,
                                     chunkSize,
                                     concurrency,
                                     maxRows,
                                     maxBufferedBytes,
                                     maxResultChunks,
                                     timeout,
                                     connectionAcquireTimeout,
                                     recovery);
    }

    /**
     * 返回带任务超时的新配置。
     *
     * @param timeout 整个批量任务的超时时间，0 表示不限制
     * @return 新配置
     */
    public BatchWriteOptions withTimeout(Duration timeout) {
        return new BatchWriteOptions(mode,
                                     chunkSize,
                                     concurrency,
                                     maxRows,
                                     maxBufferedBytes,
                                     maxResultChunks,
                                     timeout,
                                     connectionAcquireTimeout,
                                     recovery);
    }

    /**
     * 限制 R2DBC 每个批量事务等待连接的时间。独立分片并发时，每个分片都受这个上限保护。
     * 标准 JDBC 没有可靠的单次 {@code DataSource.getConnection()} 超时 API，因此 JDBC 调用方要在
     * HikariCP 等 DataSource 上配置同类边界；ORM 不会另起隐藏线程来伪造无法可靠取消的超时。
     *
     * @param connectionAcquireTimeout 最多等待连接多久，0 表示交给连接池控制
     * @return 带新限制的不可变配置
     */
    public BatchWriteOptions withConnectionAcquireTimeout(Duration connectionAcquireTimeout) {
        return new BatchWriteOptions(mode,
                                     chunkSize,
                                     concurrency,
                                     maxRows,
                                     maxBufferedBytes,
                                     maxResultChunks,
                                     timeout,
                                     connectionAcquireTimeout,
                                     recovery);
    }

    /**
     * 使用默认回执表开启 UNKNOWN 恢复。
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
     * @param operationId    稳定且唯一的操作编号
     * @param confirmTimeout 主动确认最多等待多久
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
                                     maxResultChunks,
                                     timeout,
                                     connectionAcquireTimeout,
                                     configured);
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
     * @param confirmTimeout 主动确认最多等待多久
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
