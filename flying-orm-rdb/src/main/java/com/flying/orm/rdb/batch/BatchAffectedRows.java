package com.flying.orm.rdb.batch;

import java.util.Objects;

/**
 * 驱动能够证明的批量影响行数。
 *
 * <p>KNOWN(0) 表示驱动明确返回零；UNKNOWN 表示驱动没有提供可证明的总数。两者不能用
 * {@code 0} 或 {@code null} 互相代替。</p>
 *
 * @author wangr
 * @date 2026-09-03
 * @version v3.2
 */
public final class BatchAffectedRows {

    private static final BatchAffectedRows UNKNOWN = new BatchAffectedRows(null);

    private final Long value;

    private BatchAffectedRows(Long value) {
        this.value = value;
    }

    public static BatchAffectedRows known(long value) {
        if (value < 0L) {
            throw new IllegalArgumentException("known batch affected rows must not be negative");
        }
        return new BatchAffectedRows(value);
    }

    public static BatchAffectedRows unknown() {
        return UNKNOWN;
    }

    public boolean isKnown() {
        return value != null;
    }

    /** @throws IllegalStateException 驱动没有提供可证明行数时 */
    public long value() {
        if (value == null) {
            throw new IllegalStateException("batch affected rows are unknown");
        }
        return value;
    }

    @Override
    public boolean equals(Object candidate) {
        return this == candidate || candidate instanceof BatchAffectedRows other
                && Objects.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public String toString() {
        return value == null ? "UNKNOWN" : "KNOWN(" + value + ')';
    }
}
