package com.flying.orm.rdb.internal;

import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.IdentityHashMap;

/**
 * 为发布后不可变的 RDB 规格复制直接数组值的完整数组可达图。
 *
 * <p>只沿数组节点继续复制，普通业务对象仍按既有可信交接语义保留；迭代式身份遍历同时支持共享数组、
 * 自引用和互引用，不会因为深数组耗尽 JVM 栈。</p>
 *
 * @author wangr
 * @date 2026-08-14
 * @version v1.0
 */
@InternalApi
public final class MutableValueSnapshots {

    private MutableValueSnapshots() {
    }

    /**
     * 复制数组值及其经由数组元素可达的全部数组节点；非数组值保持原对象。
     *
     * @param value 待发布的值
     * @return 独立数组图，或原非数组对象
     */
    public static Object arrayGraph(Object value) {
        if (value == null || !value.getClass().isArray()) {
            return value;
        }
        Class<?> componentType = value.getClass().getComponentType();
        if (componentType.isPrimitive()) {
            return primitiveArray(value, componentType);
        }
        IdentityHashMap<Object, Object> copies = new IdentityHashMap<>();
        Deque<Object> pending = new ArrayDeque<>();
        Object root = emptyArray(value);
        copies.put(value, root);
        pending.addLast(value);
        while (!pending.isEmpty()) {
            Object source = pending.removeFirst();
            Object target = copies.get(source);
            int length = Array.getLength(source);
            for (int index = 0; index < length; index++) {
                Object item = Array.get(source, index);
                if (item == null || !item.getClass().isArray()) {
                    Array.set(target, index, item);
                    continue;
                }
                Object itemCopy = copies.get(item);
                if (itemCopy == null) {
                    Class<?> itemComponent = item.getClass().getComponentType();
                    itemCopy = itemComponent.isPrimitive()
                            ? primitiveArray(item, itemComponent)
                            : emptyArray(item);
                    copies.put(item, itemCopy);
                    if (!itemComponent.isPrimitive()) {
                        pending.addLast(item);
                    }
                }
                Array.set(target, index, itemCopy);
            }
        }
        return root;
    }

    private static Object emptyArray(Object source) {
        return Array.newInstance(source.getClass().getComponentType(), Array.getLength(source));
    }

    private static Object primitiveArray(Object source, Class<?> componentType) {
        int length = Array.getLength(source);
        Object copy = Array.newInstance(componentType, length);
        System.arraycopy(source, 0, copy, 0, length);
        return copy;
    }
}
