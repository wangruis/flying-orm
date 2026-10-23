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

    private static final SqlExecutionOptions UNLIMITED = new SqlExecutionOptions(0, 0, 0, 0, 0);

    /** 未声明 fetchSize 时交给驱动自己决定。 */
    public SqlExecutionOptions(long maxRows,
                               long maxResultBytes,
                               long maxLargeObjectBytes,
                               long maxLargeObjectChars) {
        this(maxRows, maxResultBytes, maxLargeObjectBytes, maxLargeObjectChars, 0);
    }

    /** 不限制返回行数、结果内存和 LOB 容量。 */
    public static SqlExecutionOptions unlimited() {
        return UNLIMITED;
    }

    /** 仅设置查询返回行数，0 表示不限制，其余容量仍由调用方决定。 */
    public static SqlExecutionOptions maxRows(long maxRows) {
        return unlimited().withMaxRows(maxRows);
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
