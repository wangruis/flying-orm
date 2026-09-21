package com.flying.orm.rdb.execution;

/**
 * SQL 结果容量保护和驱动预取提示。所有时间政策由上层拥有。
 *
 * @param maxRows 查询最多返回多少行，0 表示不限
 * @param maxResultBytes 单次订阅累计返回的估算字节上限，0 表示不限
 * @param maxLargeObjectBytes 单个二进制大字段最多物化多少字节，0 表示不限
 * @param maxLargeObjectChars 单个文本大字段最多物化多少字符，0 表示不限
 * @param fetchSize 每次建议驱动预取多少行，0 表示使用驱动默认值
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record SqlExecutionOptions(long maxRows,
                                  long maxResultBytes,
                                  long maxLargeObjectBytes,
                                  long maxLargeObjectChars,
                                  int fetchSize) {

    /** 显式安全策略可复用的十万行上限；普通流式默认不自动启用。 */
    public static final long DEFAULT_MAX_ROWS = 100_000L;

    /** 显式安全策略可复用的 64 MiB 结果预算；普通流式默认不自动启用。 */
    public static final long DEFAULT_MAX_RESULT_BYTES = 64L * 1024 * 1024;

    /** 单个二进制 LOB 默认最多物化 16 MiB。 */
    public static final long DEFAULT_MAX_LARGE_OBJECT_BYTES = 16L * 1024 * 1024;

    /** 单个文本 LOB 默认最多物化一千六百万字符。 */
    public static final long DEFAULT_MAX_LARGE_OBJECT_CHARS = 16_000_000L;

    /** 未声明 fetchSize 时交给驱动自己决定。 */
    public SqlExecutionOptions(long maxRows,
                               long maxResultBytes,
                               long maxLargeObjectBytes,
                               long maxLargeObjectChars) {
        this(maxRows, maxResultBytes, maxLargeObjectBytes, maxLargeObjectChars, 0);
    }

    /** 普通结果流保持直通，单个 LOB 物化仍有界。 */
    public static SqlExecutionOptions safeDefaults() {
        return new SqlExecutionOptions(0, 0, DEFAULT_MAX_LARGE_OBJECT_BYTES,
                                       DEFAULT_MAX_LARGE_OBJECT_CHARS, 0);
    }

    /** 显式解除返回行数、结果内存和 LOB 容量限制，不作为框架默认值。 */
    public static SqlExecutionOptions unlimited() {
        return new SqlExecutionOptions(0, 0, 0, 0, 0);
    }

    /** 以安全默认值替换查询返回行数，0 表示不限制。 */
    public static SqlExecutionOptions maxRows(long maxRows) {
        return safeDefaults().withMaxRows(maxRows);
    }

    public SqlExecutionOptions {
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
        if (fetchSize < 0) {
            throw new IllegalArgumentException("sql execution fetch size must not be negative");
        }
    }

    public SqlExecutionOptions withMaxRows(long maxRows) {
        return new SqlExecutionOptions(maxRows, maxResultBytes, maxLargeObjectBytes, maxLargeObjectChars, fetchSize);
    }

    /** 单次查询订阅最多累计返回多少估算字节，0 表示显式不限。 */
    public SqlExecutionOptions withMaxResultBytes(long maxResultBytes) {
        return new SqlExecutionOptions(maxRows, maxResultBytes, maxLargeObjectBytes, maxLargeObjectChars, fetchSize);
    }

    /** 单个二进制大字段最多允许物化多少字节，0 表示不限。 */
    public SqlExecutionOptions withMaxLargeObjectBytes(long maxLargeObjectBytes) {
        return new SqlExecutionOptions(maxRows, maxResultBytes, maxLargeObjectBytes, maxLargeObjectChars, fetchSize);
    }

    /** 单个文本大字段最多允许物化多少个字符，0 表示不限。 */
    public SqlExecutionOptions withMaxLargeObjectChars(long maxLargeObjectChars) {
        return new SqlExecutionOptions(maxRows, maxResultBytes, maxLargeObjectBytes, maxLargeObjectChars, fetchSize);
    }

    /** 给 JDBC/R2DBC 驱动预取提示，0 不覆盖驱动默认值。 */
    public SqlExecutionOptions withFetchSize(int fetchSize) {
        return new SqlExecutionOptions(maxRows, maxResultBytes, maxLargeObjectBytes, maxLargeObjectChars, fetchSize);
    }
}
