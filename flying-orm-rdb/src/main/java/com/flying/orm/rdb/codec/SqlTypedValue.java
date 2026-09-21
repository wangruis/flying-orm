package com.flying.orm.rdb.codec;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * SQL 参数需要保留数据库类型时使用的驱动无关值。
 *
 * <p>普通参数仍直接保存 Java 值，只有 BLOB/CLOB 这类驱动必须知道目标类型的参数才需要这个外壳。
 * R2DBC 和 JDBC 在最后绑定时分别转换成自己的驱动类型，codec 不再把某一种驱动的对象塞进共享 SQL 请求。</p>
 *
 * @param kind  参数的数据库语义
 * @param value 尚未交给驱动的 Java 值
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public record SqlTypedValue(Kind kind, Object value) {

    /** 当前确实需要跨驱动保留的三种大字段类型。 */
    public enum Kind {
        BLOB,
        CLOB,
        NCLOB
    }

    public SqlTypedValue {
        kind = Objects.requireNonNull(kind, "sql typed value kind must not be null");
        value = Objects.requireNonNull(value, "sql typed value must not be null");
        switch (kind) {
            case BLOB -> {
                if (!(value instanceof byte[]) && !(value instanceof ByteBuffer)) {
                    throw new IllegalArgumentException("BLOB value must be byte[] or ByteBuffer");
                }
            }
            case CLOB, NCLOB -> {
                if (!(value instanceof CharSequence)) {
                    throw new IllegalArgumentException("CLOB value must be CharSequence");
                }
            }
        }
    }
}
