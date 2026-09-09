package com.flying.orm.rdb.execution;

import java.time.Duration;
import java.util.Objects;

/**
 * SQL 参数透传和结果物化保护选项，默认保留单个 LOB 物化上限。
 * 执行和清理生命周期由上层与驱动治理；普通单条 JDBC 可显式透传 Statement 查询超时，
 * 但该值不代表 ORM 总时限，多语句或不支持透传的执行入口必须明确拒绝正时限。
 *
 * @param timeout                  普通单条 JDBC 的 Statement 查询超时提示，0 不覆盖驱动默认值
 * @param maxRows                  查询最多返回多少行，0 表示不限
 * @param maxResultBytes           单次订阅累计返回的估算字节上限，0 表示不限
 * @param maxLargeObjectBytes      单个二进制大字段最多物化多少字节，0 表示不限
 * @param maxLargeObjectChars      单个文本大字段最多物化多少字符，0 表示不限
 * @param cleanupTimeout           兼容字段，仅支持 0；正数抛出 UnsupportedOperationException
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
                                  Duration cleanupTimeout,
                                  int fetchSize) {

    /** 默认不叠加 ORM 总超时，SQL 生命周期交给调用方、事务管理器和驱动。 */
    public static final Duration DEFAULT_TIMEOUT = Duration.ZERO;

    /** 显式安全策略可复用的十万行上限；普通流式默认不自动启用。 */
    public static final long DEFAULT_MAX_ROWS = 100_000L;

    /** 显式安全策略可复用的 64 MiB 结果预算；普通流式默认不自动启用。 */
    public static final long DEFAULT_MAX_RESULT_BYTES = 64L * 1024 * 1024;

    /** 单个二进制 LOB 默认最多物化 16 MiB。 */
    public static final long DEFAULT_MAX_LARGE_OBJECT_BYTES = 16L * 1024 * 1024;

    /** 单个文本 LOB 默认最多物化一千六百万字符。 */
    public static final long DEFAULT_MAX_LARGE_OBJECT_CHARS = 16_000_000L;

    /** ORM 不拥有清理时限；正常资源释放仍由原执行路径完成。 */
    public static final Duration DEFAULT_CLEANUP_TIMEOUT = Duration.ZERO;

    /** 默认保留驱动抓取策略；大结果流可通过 {@link #withFetchSize(int)} 显式启用分批抓取。 */
    private static final int SAFE_FETCH_SIZE = 0;

    /** 未声明 fetchSize 时交给驱动自己决定。 */
    public SqlExecutionOptions(Duration timeout,
                               long maxRows,
                               long maxResultBytes,
                               long maxLargeObjectBytes,
                               long maxLargeObjectChars,
                               Duration cleanupTimeout) {
        this(timeout, maxRows, maxResultBytes, maxLargeObjectBytes, maxLargeObjectChars,
             cleanupTimeout, 0);
    }

    /**
     * 创建普通业务 SQL 的轻量默认值。结果流保持直通，单个 LOB 物化仍有界，时限默认归零。
     *
     * @return 可作为单例共享的不可变执行保护
     */
    public static SqlExecutionOptions safeDefaults() {
        return new SqlExecutionOptions(DEFAULT_TIMEOUT,
                                       0,
                                       0,
                                       DEFAULT_MAX_LARGE_OBJECT_BYTES,
                                       DEFAULT_MAX_LARGE_OBJECT_CHARS,
                                       DEFAULT_CLEANUP_TIMEOUT,
                                       SAFE_FETCH_SIZE);
    }

    /**
     * 不加执行和清理超时，也不限制返回行数、结果内存和 LOB。该方法是显式的安全逃生口，不作为框架默认值。
     */
    public static SqlExecutionOptions unlimited() {
        return new SqlExecutionOptions(Duration.ZERO, 0, 0, 0, 0, Duration.ZERO);
    }

    /**
     * 以普通业务安全默认值配置单条 JDBC Statement 超时提示，不派生清理时限。
     *
     * @param timeout 超时时间，0 表示不限制
     * @return 执行保护选项
     */
    public static SqlExecutionOptions timeout(Duration timeout) {
        Duration safeTimeout = Objects.requireNonNull(timeout, "sql execution timeout must not be null");
        return new SqlExecutionOptions(safeTimeout,
                                       0,
                                       0,
                                       DEFAULT_MAX_LARGE_OBJECT_BYTES,
                                       DEFAULT_MAX_LARGE_OBJECT_CHARS,
                                       DEFAULT_CLEANUP_TIMEOUT,
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
        if (cleanupTimeout.isNegative()) {
            throw new IllegalArgumentException("resource cleanup timeout must not be negative");
        }
        if (!cleanupTimeout.isZero()) {
            throw new UnsupportedOperationException("resource cleanup timeout must be managed by the caller");
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
                                       cleanupTimeout,
                                       fetchSize);
    }

    public SqlExecutionOptions withMaxRows(long maxRows) {
        return new SqlExecutionOptions(timeout,
                                       maxRows,
                                       maxResultBytes,
                                       maxLargeObjectBytes,
                                       maxLargeObjectChars,
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
                                       cleanupTimeout,
                                       fetchSize);
    }

    /**
     * 保留旧清理时限签名；正常资源释放保留，时限由上层治理。
     *
     * @param cleanupTimeout 仅支持 0
     * @return 不启用 ORM 清理时限的不可变选项
     * @throws UnsupportedOperationException 配置正时限时
     */
    public SqlExecutionOptions withCleanupTimeout(Duration cleanupTimeout) {
        return new SqlExecutionOptions(timeout,
                                       maxRows,
                                       maxResultBytes,
                                       maxLargeObjectBytes,
                                       maxLargeObjectChars,
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
                                       cleanupTimeout,
                                       fetchSize);
    }

}
