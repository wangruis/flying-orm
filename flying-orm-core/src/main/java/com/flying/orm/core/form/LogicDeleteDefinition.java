package com.flying.orm.core.form;

import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.Objects;

/**
 * 动态表单的逻辑删除配置。
 *
 * <p>它只描述“哪个字段代表删除状态，以及未删除/已删除分别是什么值”。怎么渲染 SQL、怎么绑定参数，
 * 交给 rdb 模块处理。</p>
 *
 * @param fieldName       删除标记字段名
 * @param notDeletedValue 未删除值
 * @param deletedValue    已删除值
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public record LogicDeleteDefinition(String fieldName, Object notDeletedValue, Object deletedValue) {

    public LogicDeleteDefinition {
        fieldName = FormNames.requireText(fieldName, "logic delete field name");
        notDeletedValue = copyArray(Objects.requireNonNull(notDeletedValue, "logic not deleted value must not be null"));
        deletedValue = copyArray(Objects.requireNonNull(deletedValue, "logic deleted value must not be null"));
    }

    /**
     * 返回未删除值的快照，避免数组配置在表单发布后被外部修改。
     *
     * @return 未删除值；数组可达图返回保持共享关系的独立副本
     */
    @Override
    public Object notDeletedValue() {
        return copyArray(notDeletedValue);
    }

    /**
     * 返回已删除值的快照，避免数组配置在表单发布后被外部修改。
     *
     * @return 已删除值；数组可达图返回保持共享关系的独立副本
     */
    @Override
    public Object deletedValue() {
        return copyArray(deletedValue);
    }

    public static LogicDeleteDefinition of(String fieldName, Object notDeletedValue, Object deletedValue) {
        return new LogicDeleteDefinition(fieldName, notDeletedValue, deletedValue);
    }

    public static LogicDeleteDefinition numeric(String fieldName) {
        return new LogicDeleteDefinition(fieldName, 0, 1);
    }

    private static Object copyArray(Object value) {
        if (value == null || !value.getClass().isArray()) {
            return value;
        }
        IdentityHashMap<Object, Object> copies = new IdentityHashMap<>();
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
