package com.flying.orm.core.internal.value;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;

/**
 * 冻结已经发布到查询、Scope 和表单配置中的可绑定值。
 *
 * <p>这里只复制数组可达图、{@link ByteBuffer} 的当前可读区域、标准绑定中的可变文本和 JDK 旧式可变时间值。普通业务对象
 * 仍保持身份，避免把 Core 的不可变边界扩大成通用对象深复制；底层 SQL 请求的可信参数交接也不经过这里。</p>
 *
 * @author wangr
 * @date 2026-08-16
 * @version v2.0
 */
public final class BindableValueSnapshots {

    private BindableValueSnapshots() {
    }

    /**
     * 冻结标准可绑定值。
     *
     * @param value 原始值
     * @return 数组、ByteBuffer、可变文本与 JDK 旧式可变时间值已隔离的值
     */
    public static Object immutableValue(Object value) {
        return snapshot(value, true, new IdentityHashMap<>());
    }

    /**
     * 冻结一组标准可绑定值，并在同一次快照中保留数组共享关系和数组环。
     *
     * @param values 原始值列表
     * @return 不可修改的值列表
     */
    public static List<Object> immutableValues(List<?> values) {
        List<Object> snapshot = new ArrayList<>(values.size());
        IdentityHashMap<Object, Object> arrayCopies = new IdentityHashMap<>();
        for (Object value : values) {
            snapshot.add(snapshot(value, true, arrayCopies));
        }
        return Collections.unmodifiableList(snapshot);
    }

    /**
     * 只复制数组可达图，供可信自定义条件保持既有非数组值身份。
     *
     * @param value 原始值
     * @return 数组图副本，或原非数组值
     */
    public static Object arrayGraph(Object value) {
        return snapshot(value, false, new IdentityHashMap<>());
    }

    private static Object snapshot(Object value,
                                   boolean freezeMutableBindables,
                                   IdentityHashMap<Object, Object> arrayCopies) {
        if (freezeMutableBindables && value instanceof ByteBuffer buffer) {
            return copyBuffer(buffer);
        }
        if (freezeMutableBindables && value instanceof CharSequence text && !(text instanceof String)) {
            return text.toString();
        }
        if (freezeMutableBindables && value instanceof java.util.Date date) {
            Object temporalCopy = copyLegacyTemporal(date);
            if (temporalCopy != value) {
                return temporalCopy;
            }
        }
        if (value == null || !value.getClass().isArray()) {
            return value;
        }
        Object existing = arrayCopies.get(value);
        if (existing != null) {
            return existing;
        }
        Object rootCopy = newArray(value);
        arrayCopies.put(value, rootCopy);
        ArrayDeque<Object> sources = new ArrayDeque<>();
        ArrayDeque<Object> targets = new ArrayDeque<>();
        sources.addLast(value);
        targets.addLast(rootCopy);
        while (!sources.isEmpty()) {
            Object source = sources.removeFirst();
            Object target = targets.removeFirst();
            int length = Array.getLength(source);
            Class<?> componentType = source.getClass().getComponentType();
            if (componentType.isPrimitive()) {
                System.arraycopy(source, 0, target, 0, length);
                continue;
            }
            for (int index = 0; index < length; index++) {
                Object item = Array.get(source, index);
                if (freezeMutableBindables && item instanceof ByteBuffer buffer) {
                    Array.set(target, index, copyBuffer(buffer));
                    continue;
                }
                if (freezeMutableBindables && item instanceof CharSequence text && !(text instanceof String)) {
                    Array.set(target, index, copyTextForArray(text, componentType));
                    continue;
                }
                if (freezeMutableBindables && item instanceof java.util.Date date) {
                    Object itemTemporalCopy = copyLegacyTemporal(date);
                    if (itemTemporalCopy != item) {
                        Array.set(target, index, itemTemporalCopy);
                        continue;
                    }
                }
                if (item == null || !item.getClass().isArray()) {
                    Array.set(target, index, item);
                    continue;
                }
                Object itemCopy = arrayCopies.get(item);
                if (itemCopy == null) {
                    itemCopy = newArray(item);
                    arrayCopies.put(item, itemCopy);
                    sources.addLast(item);
                    targets.addLast(itemCopy);
                }
                Array.set(target, index, itemCopy);
            }
        }
        return rootCopy;
    }

    private static Object newArray(Object value) {
        return Array.newInstance(value.getClass().getComponentType(), Array.getLength(value));
    }

    private static ByteBuffer copyBuffer(ByteBuffer value) {
        ByteBuffer source = value.duplicate();
        ByteBuffer copy = ByteBuffer.allocate(source.remaining());
        copy.put(source);
        copy.flip();
        return copy.asReadOnlyBuffer();
    }

    private static Object copyTextForArray(CharSequence value, Class<?> componentType) {
        if (componentType.isAssignableFrom(String.class)) {
            return value.toString();
        }
        if (componentType == StringBuilder.class) {
            return new StringBuilder(value);
        }
        if (componentType == StringBuffer.class) {
            return new StringBuffer(value);
        }
        if (componentType == CharBuffer.class) {
            return CharBuffer.wrap(value.toString()).asReadOnlyBuffer();
        }
        throw new IllegalArgumentException("mutable text array component type cannot be snapshotted safely");
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
}
