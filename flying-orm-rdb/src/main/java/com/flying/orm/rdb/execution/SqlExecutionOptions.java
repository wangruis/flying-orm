package com.flying.orm.rdb.execution;

import java.time.Duration;
import java.util.Objects;

/**
 * SQL 执行保护选项。框架默认使用保守的有界配置，避免慢 SQL、无限结果、超大 LOB 或连接池耗尽
 * 把单次请求放大成进程级故障；只有明确受控的基础设施入口才应显式使用 {@link #unlimited()}。
 *
 * @param timeout                  SQL 调用的总超时，0 表示不限
 * @param maxRows                  查询最多返回多少行，0 表示不限
 * @param maxResultBytes           单次订阅累计返回的估算字节上限，0 表示不限
 * @param maxLargeObjectBytes      单个二进制大字段最多物化多少字节，0 表示不限
 * @param maxLargeObjectChars      单个文本大字段最多物化多少字符，0 表示不限
 * @param connectionAcquireTimeout 最多等待连接多久，0 表示交给连接池控制
 * @param cleanupTimeout           数据库结果确定后最多等待资源清理多久，0 表示不限
 * @param fetchSize                每次建议驱动预取多少行，0 表示使用驱动默认值
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record SqlExecutionOptions(Duration timeout,
                                  long maxRows,
                                  long maxResultBytes,
                                  long maxLargeObjectBytes,
                                  long maxLargeObjectChars,
                                  Duration connectionAcquireTimeout,
                                  Duration cleanupTimeout,
                                  int fetchSize) {

    /** 普通 SQL 默认最多执行 30 秒。 */
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    /** 普通查询默认最多返回十万行，防止误用无分页查询拖垮堆内存。 */
    public static final long DEFAULT_MAX_ROWS = 100_000L;

    /** 普通查询默认最多累计返回 64 MiB，避免宽表或同步 collectList 拖垮堆内存。 */
    public static final long DEFAULT_MAX_RESULT_BYTES = 64L * 1024 * 1024;

    /** 单个二进制 LOB 默认最多物化 16 MiB。 */
    public static final long DEFAULT_MAX_LARGE_OBJECT_BYTES = 16L * 1024 * 1024;

    /** 单个文本 LOB 默认最多物化一千六百万字符。 */
    public static final long DEFAULT_MAX_LARGE_OBJECT_CHARS = 16_000_000L;

    /** 默认最多等待连接池五秒，快速隔离连接耗尽。 */
    public static final Duration DEFAULT_CONNECTION_ACQUIRE_TIMEOUT = Duration.ofSeconds(5);

    /** 数据库结果确定后默认最多等待资源清理五秒，防止关闭 Publisher 永久挂起。 */
    public static final Duration DEFAULT_CLEANUP_TIMEOUT = Duration.ofSeconds(5);

    /** 默认保留驱动抓取策略；大结果流可通过 {@link #withFetchSize(int)} 显式启用分批抓取。 */
    private static final int SAFE_FETCH_SIZE = 0;

    /** 旧的七参数构造仍可继续使用，未声明 fetchSize 时交给驱动自己决定。 */
    public SqlExecutionOptions(Duration timeout,
                               long maxRows,
                               long maxResultBytes,
                               long maxLargeObjectBytes,
                               long maxLargeObjectChars,
                               Duration connectionAcquireTimeout,
                               Duration cleanupTimeout) {
        this(timeout, maxRows, maxResultBytes, maxLargeObjectBytes, maxLargeObjectChars,
             connectionAcquireTimeout, cleanupTimeout, 0);
    }

    /**
     * 创建普通业务 SQL 的保守安全默认值。
     *
     * @return 可作为单例共享的不可变执行保护
     */
    public static SqlExecutionOptions safeDefaults() {
        return new SqlExecutionOptions(DEFAULT_TIMEOUT,
                                       DEFAULT_MAX_ROWS,
                                       DEFAULT_MAX_RESULT_BYTES,
                                       DEFAULT_MAX_LARGE_OBJECT_BYTES,
                                       DEFAULT_MAX_LARGE_OBJECT_CHARS,
                                       DEFAULT_CONNECTION_ACQUIRE_TIMEOUT,
                                       DEFAULT_CLEANUP_TIMEOUT,
                                       SAFE_FETCH_SIZE);
    }

    /**
     * 不加超时，也不限制返回行数、结果内存、LOB 和连接等待。该方法是显式的安全逃生口，不作为框架默认值。
     */
    public static SqlExecutionOptions unlimited() {
        return new SqlExecutionOptions(Duration.ZERO, 0, 0, 0, 0, Duration.ZERO, Duration.ZERO);
    }

    /**
     * 以普通业务安全默认值为基线替换执行时间。需要解除其他保护时必须先显式选择 {@link #unlimited()}。
     *
     * @param timeout 超时时间，0 表示不限制
     * @return 执行保护选项
     */
    public static SqlExecutionOptions timeout(Duration timeout) {
        Duration safeTimeout = Objects.requireNonNull(timeout, "sql execution timeout must not be null");
        Duration cleanupBoundary = safeTimeout.isZero()
                ? Duration.ZERO
                : (safeTimeout.compareTo(DEFAULT_CLEANUP_TIMEOUT) < 0
                        ? safeTimeout
                        : DEFAULT_CLEANUP_TIMEOUT);
        // usingWhen 会等待异步清理后再转发超时错误。只限制业务阶段却无限等待 close，会让“300ms 超时”
        // 在驱动取消卡住时永远不返回；因此便捷工厂同步给清理阶段设置不大于业务截止时间的边界。
        return new SqlExecutionOptions(safeTimeout,
                                       DEFAULT_MAX_ROWS,
                                       DEFAULT_MAX_RESULT_BYTES,
                                       DEFAULT_MAX_LARGE_OBJECT_BYTES,
                                       DEFAULT_MAX_LARGE_OBJECT_CHARS,
                                       DEFAULT_CONNECTION_ACQUIRE_TIMEOUT,
                                       cleanupBoundary,
                                       SAFE_FETCH_SIZE);
    }

    /**
     * 以普通业务安全默认值为基线替换查询返回行数。需要解除其他保护时必须先显式选择 {@link #unlimited()}。
     *
     * @param maxRows 最多允许返回多少行，0 表示不限制
     * @return 执行保护选项
     */
    public static SqlExecutionOptions maxRows(long maxRows) {
        return safeDefaults().withMaxRows(maxRows);
    }

    public SqlExecutionOptions {
        timeout = Objects.requireNonNull(timeout, "sql execution timeout must not be null");
        connectionAcquireTimeout = Objects.requireNonNull(connectionAcquireTimeout,
                                                          "connection acquisition timeout must not be null");
        cleanupTimeout = Objects.requireNonNull(cleanupTimeout, "resource cleanup timeout must not be null");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("sql execution timeout must not be negative");
        }
        if (maxRows < 0) {
            throw new IllegalArgumentException("sql execution max rows must not be negative");
        }
        if (maxResultBytes < 0) {
            throw new IllegalArgumentException("sql execution max result bytes must not be negative");
        }
        if (maxLargeObjectBytes < 0) {
            throw new IllegalArgumentException("sql execution max large object bytes must not be negative");
        }
        if (maxLargeObjectChars < 0) {
            throw new IllegalArgumentException("sql execution max large object chars must not be negative");
        }
        if (connectionAcquireTimeout.isNegative()) {
            throw new IllegalArgumentException("connection acquisition timeout must not be negative");
        }
        if (cleanupTimeout.isNegative()) {
            throw new IllegalArgumentException("resource cleanup timeout must not be negative");
        }
        if (fetchSize < 0) {
            throw new IllegalArgumentException("sql execution fetch size must not be negative");
        }
    }

    public SqlExecutionOptions withTimeout(Duration timeout) {
        return new SqlExecutionOptions(timeout,
                                       maxRows,
                                       maxResultBytes,
                                       maxLargeObjectBytes,
                                       maxLargeObjectChars,
                                       connectionAcquireTimeout,
                                       cleanupTimeout,
                                       fetchSize);
    }

    public SqlExecutionOptions withMaxRows(long maxRows) {
        return new SqlExecutionOptions(timeout,
                                       maxRows,
                                       maxResultBytes,
                                       maxLargeObjectBytes,
                                       maxLargeObjectChars,
                                       connectionAcquireTimeout,
                                       cleanupTimeout,
                                       fetchSize);
    }

    /** 单次查询订阅最多累计返回多少估算字节，0 表示显式不限。 */
    public SqlExecutionOptions withMaxResultBytes(long maxResultBytes) {
        return new SqlExecutionOptions(timeout,
                                       maxRows,
                                       maxResultBytes,
                                       maxLargeObjectBytes,
                                       maxLargeObjectChars,
                                       connectionAcquireTimeout,
                                       cleanupTimeout,
                                       fetchSize);
    }

    /** 单个二进制大字段最多允许物化多少字节，0 表示不限。 */
    public SqlExecutionOptions withMaxLargeObjectBytes(long maxLargeObjectBytes) {
        return new SqlExecutionOptions(timeout,
                                       maxRows,
                                       maxResultBytes,
                                       maxLargeObjectBytes,
                                       maxLargeObjectChars,
                                       connectionAcquireTimeout,
                                       cleanupTimeout,
                                       fetchSize);
    }

    /** 单个文本大字段最多允许物化多少个字符，0 表示不限。 */
    public SqlExecutionOptions withMaxLargeObjectChars(long maxLargeObjectChars) {
        return new SqlExecutionOptions(timeout,
                                       maxRows,
                                       maxResultBytes,
                                       maxLargeObjectBytes,
                                       maxLargeObjectChars,
                                       connectionAcquireTimeout,
                                       cleanupTimeout,
                                       fetchSize);
    }

    /**
     * 单独限制等待连接的时间。0 表示交给连接池自己控制。
     *
     * @param connectionAcquireTimeout 最多等待连接多久
     * @return 带新限制的不可变选项
     */
    public SqlExecutionOptions withConnectionAcquireTimeout(Duration connectionAcquireTimeout) {
        return new SqlExecutionOptions(timeout,
                                       maxRows,
                                       maxResultBytes,
                                       maxLargeObjectBytes,
                                       maxLargeObjectChars,
                                       connectionAcquireTimeout,
                                       cleanupTimeout,
                                       fetchSize);
    }

    /**
     * 单独限制数据库结果确定后的资源清理时间。该上限不参与普通 SQL 的操作截止时间，避免已成功的
     * 数据库事实被关闭连接的延迟改写。
     *
     * @param cleanupTimeout 最多等待资源清理多久，0 表示不限
     * @return 带新清理上限的不可变选项
     */
    public SqlExecutionOptions withCleanupTimeout(Duration cleanupTimeout) {
        return new SqlExecutionOptions(timeout,
                                       maxRows,
                                       maxResultBytes,
                                       maxLargeObjectBytes,
                                       maxLargeObjectChars,
                                       connectionAcquireTimeout,
                                       cleanupTimeout,
                                       fetchSize);
    }

    /**
     * 给 JDBC/R2DBC 驱动一个有界预取提示。0 不覆盖驱动默认值，正数由具体驱动决定如何实现。
     *
     * @param fetchSize 建议每次预取的行数
     * @return 带新预取大小的不可变选项
     */
    public SqlExecutionOptions withFetchSize(int fetchSize) {
        return new SqlExecutionOptions(timeout,
                                       maxRows,
                                       maxResultBytes,
                                       maxLargeObjectBytes,
                                       maxLargeObjectChars,
                                       connectionAcquireTimeout,
                                       cleanupTimeout,
                                       fetchSize);
    }

}
