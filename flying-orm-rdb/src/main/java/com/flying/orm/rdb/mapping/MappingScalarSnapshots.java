package com.flying.orm.rdb.mapping;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * 复制映射事件正式支持的可变标量，避免监听器或调用方在发布后改写映射现场。
 *
 * <p>这里只处理 ByteBuffer 和 JDK 标准旧时间类型；未知子类继续按可信自定义对象交接，不把映射边界扩成通用深复制。</p>
 *
 * @author wangr
 * @date 2026-08-18
 * @version v2.0
 */
final class MappingScalarSnapshots {

    private MappingScalarSnapshots() {
    }

    /** @return 支持的可变标量副本，其他值保持原对象 */
    static Object copy(Object value) {
        if (value instanceof ByteBuffer buffer) {
            return copyBuffer(buffer);
        }
        if (value instanceof java.util.Date date) {
            return copyLegacyTemporal(date);
        }
        return value;
    }

    private static Object copyLegacyTemporal(java.util.Date value) {
        if (value.getClass() == Timestamp.class) {
            Timestamp source = (Timestamp) value;
            Timestamp copy = new Timestamp(source.getTime());
            copy.setNanos(source.getNanos());
            return copy;
        }
        if (value.getClass() == java.sql.Date.class) {
            return new java.sql.Date(((java.sql.Date) value).getTime());
        }
        if (value.getClass() == Time.class) {
            return new Time(((Time) value).getTime());
        }
        if (value.getClass() == java.util.Date.class) {
            return new java.util.Date(((java.util.Date) value).getTime());
        }
        return value;
    }

    /** @return 当前值是否会在映射事件边界产生不同运行时对象 */
    static boolean supports(Object value) {
        return value instanceof ByteBuffer
                || value instanceof java.util.Date && (value.getClass() == Timestamp.class
                || value.getClass() == java.sql.Date.class
                || value.getClass() == Time.class
                || value.getClass() == java.util.Date.class);
    }

    /** @return 快照后的运行时类型，供对象数组选择安全组件类型 */
    static Class<?> snapshotType(Object value) {
        return value instanceof ByteBuffer ? ByteBuffer.class : value.getClass();
    }

    private static ByteBuffer copyBuffer(ByteBuffer source) {
        ByteBuffer readable = source.duplicate();
        int position = readable.position();
        int limit = readable.limit();
        readable.position(0);
        ByteBuffer copy = ByteBuffer.allocate(limit).order(source.order());
        copy.put(readable);
        copy.flip();
        copy.position(position);
        return copy;
    }
}
