package com.flying.orm.rdb.execution;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;

import java.io.Serial;
import java.util.Objects;

/**
 * BLOB/CLOB 物化后会超过调用方上限时抛出，不把字段内容放进异常消息。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public final class SqlLargeObjectLimitExceededException extends IllegalStateException implements OrmErrorReportProvider {

    @Serial
    private static final long serialVersionUID = 1L;

    public enum Kind {
        /** BLOB 或其他二进制大对象。 */
        BINARY,
        /** CLOB 或其他字符大对象。 */
        CHARACTER
    }

    private final Kind kind;

    private final long maxSize;

    private final long actualSize;

    /**
     * @param kind 大对象种类
     * @param maxSize 允许物化的最大字节数或字符数
     * @param actualSize 检测到的实际字节数或字符数
     */
    public SqlLargeObjectLimitExceededException(Kind kind, long maxSize, long actualSize) {
        super("sql large object exceeds max size: kind=" + kind
                      + ", maxSize=" + maxSize
                      + ", actualSize=" + actualSize);
        this.kind = Objects.requireNonNull(kind, "large object kind must not be null");
        if (maxSize < 0 || actualSize < 0) {
            throw new IllegalArgumentException("large object sizes must not be negative");
        }
        this.maxSize = maxSize;
        this.actualSize = actualSize;
    }

    /** @return 大对象种类 */
    public Kind kind() {
        return kind;
    }

    /** @return 允许物化的最大字节数或字符数 */
    public long maxSize() {
        return maxSize;
    }

    /** @return 检测到的实际字节数或字符数 */
    public long actualSize() {
        return actualSize;
    }

    /** @return 只记录大对象种类和大小，不包含字段内容 */
    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("EXECUTION",
                                  "LARGE_OBJECT_LIMIT_EXCEEDED",
                                  kind.name(),
                                  null,
                                  null,
                                  getMessage());
    }
}
