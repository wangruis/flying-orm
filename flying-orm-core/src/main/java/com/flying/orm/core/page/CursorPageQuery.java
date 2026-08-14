package com.flying.orm.core.page;

import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;

/**
 * 基于稳定排序值的游标分页请求。
 *
 * <p>cursor 为空表示第一页；非空值禁止包含 {@code null}。游标值数量由关系型模块结合实体主键自动补齐后的
 * 稳定排序进行校验，而不是在这个框架无关值对象中只按调用方显式排序过早拒绝。这里不编码字符串令牌，
 * 上层可以按自己的签名、加密或 JSON 规则传输游标。</p>
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record CursorPageQuery(int size, List<CursorSort> sorts, List<Object> cursor) {

    public CursorPageQuery {
        if (size < 1 || size >= PageQuery.MAX_SIZE) {
            throw new IllegalArgumentException("cursor page size must be between 1 and " + (PageQuery.MAX_SIZE - 1));
        }
        sorts = List.copyOf(Objects.requireNonNull(sorts, "cursor sorts must not be null"));
        cursor = snapshotCursor(Objects.requireNonNull(cursor, "cursor values must not be null"));
        if (sorts.isEmpty()) {
            throw new IllegalArgumentException("cursor pagination requires at least one sort field");
        }
    }

    public static CursorPageQuery first(int size, CursorSort... sorts) {
        return new CursorPageQuery(size, Arrays.asList(sorts), List.of());
    }

    public static CursorPageQuery after(int size, List<?> cursor, CursorSort... sorts) {
        return new CursorPageQuery(size, Arrays.asList(sorts), List.copyOf(cursor));
    }

    public boolean firstPage() {
        return cursor.isEmpty();
    }

    /**
     * 返回游标值的只读快照；数组可达图每次返回独立副本，避免影响后续 SQL 参数绑定。
     *
     * @return 当前游标的不可变快照
     */
    @Override
    public List<Object> cursor() {
        return snapshotCursor(cursor);
    }

    static List<Object> snapshotCursor(List<?> values) {
        List<?> safeValues = Objects.requireNonNull(values, "cursor values must not be null");
        List<Object> snapshot = new ArrayList<>(safeValues.size());
        IdentityHashMap<Object, Object> arrayCopies = new IdentityHashMap<>();
        for (Object value : safeValues) {
            snapshot.add(snapshotArray(value, arrayCopies));
        }
        return List.copyOf(snapshot);
    }

    /** 仅深复制数组可达图；列表中的共享数组身份和数组环在同一次快照中保持。 */
    private static Object snapshotArray(Object value, IdentityHashMap<Object, Object> copies) {
        if (value == null || !value.getClass().isArray()) {
            return value;
        }
        Object existing = copies.get(value);
        if (existing != null) {
            return existing;
        }
        Object rootCopy = Array.newInstance(value.getClass().getComponentType(), Array.getLength(value));
        copies.put(value, rootCopy);
        ArrayDeque<Object> sources = new ArrayDeque<>();
        ArrayDeque<Object> targets = new ArrayDeque<>();
        sources.addLast(value);
        targets.addLast(rootCopy);
        while (!sources.isEmpty()) {
            Object source = sources.removeFirst();
            Object target = targets.removeFirst();
            Class<?> componentType = source.getClass().getComponentType();
            int length = Array.getLength(source);
            if (componentType.isPrimitive()) {
                System.arraycopy(source, 0, target, 0, length);
                continue;
            }
            for (int index = 0; index < length; index++) {
                Object item = Array.get(source, index);
                if (item == null || !item.getClass().isArray()) {
                    Array.set(target, index, item);
                    continue;
                }
                Object itemCopy = copies.get(item);
                if (itemCopy == null) {
                    itemCopy = Array.newInstance(item.getClass().getComponentType(), Array.getLength(item));
                    copies.put(item, itemCopy);
                    sources.addLast(item);
                    targets.addLast(itemCopy);
                }
                Array.set(target, index, itemCopy);
            }
        }
        return rootCopy;
    }
}
