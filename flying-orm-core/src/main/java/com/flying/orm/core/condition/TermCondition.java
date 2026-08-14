package com.flying.orm.core.condition;

import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;

/**
 * 单个参数驱动条件，保留字段、term id 和参数值，不在条件层提前拼接 SQL。
 *
 * @param field    字段名或属性名
 * @param operator term id，例如 `=`、`like`、`user-in-org`
 * @param value    条件参数值
 * <p>标准多值 term（`in`、`not-in`、`between`、`not-between`）会在构造时取得有界集合快照，
 * 集合中的直接数组元素和顶层数组都会独立复制。可信 direct-AST 自定义 term 的非数组值保持原对象，由其处理器定义不可变性边界，
 * 不能在条件层按 Java 容器接口猜测业务或驱动值的语义。</p>
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public record TermCondition(String field, String operator, Object value) implements ConditionNode {

    private static final int MAX_COLLECTION_SIZE = 1_000;

    /**
     * 创建 term 条件并完成字段名和 term id 的基础规范化。
     *
     * @param field    字段名或属性名
     * @param operator term id
     * @param value    条件参数值
     */
    public TermCondition {
        field = ConditionNames.requireText(field, "condition field");
        operator = ConditionNames.normalize(operator, "condition operator");
        value = snapshotValue(operator, value);
    }

    /**
     * 返回条件参数。数组可达图按次返回独立副本，避免调用方通过 record 访问器改写已经发布的条件树。
     *
     * @return 标准多值集合及其中数组可达图的只读快照、顶层数组可达图的独立副本；
     * 自定义 direct-AST term 的非数组值原样返回
     */
    @Override
    public Object value() {
        if (requiresBoundedCollection(operator) && value instanceof List<?> values) {
            return snapshotArrayElements(values);
        }
        return copyArray(value);
    }

    /**
     * 创建 term 条件。
     *
     * @param field    字段名或属性名
     * @param operator term id
     * @param value    条件参数值
     * @return term 条件
     */
    public static TermCondition of(String field, String operator, Object value) {
        return new TermCondition(field, operator, value);
    }

    private static Object snapshotValue(String operator, Object value) {
        // 直接 AST 的自定义 term 只有处理器自己知道值形状；不能因其实现 Collection 就改写驱动标量的类型。
        // 因而仅对内置多值 term 做有界集合快照，数组仍一律按值复制以阻断发布后的元素改写。
        if (requiresBoundedCollection(operator)) {
            if (value instanceof Iterable<?> iterable) {
                return snapshotIterable(iterable);
            }
            if (value != null && value.getClass().isArray() && Array.getLength(value) > MAX_COLLECTION_SIZE) {
                throw collectionTooLarge();
            }
        }
        return copyArray(value);
    }

    /**
     * 标准多值 term 只在发布 AST 时消费一次 Iterable；既防止后续可变源改写 SQL，也不重复遍历一次性数据源。
     */
    private static List<Object> snapshotIterable(Iterable<?> values) {
        List<Object> snapshot = new ArrayList<>();
        IdentityHashMap<Object, Object> arrayCopies = new IdentityHashMap<>();
        for (Object item : values) {
            if (snapshot.size() >= MAX_COLLECTION_SIZE) {
                throw collectionTooLarge();
            }
            snapshot.add(copyArray(item, arrayCopies));
        }
        return Collections.unmodifiableList(snapshot);
    }

    /**
     * 标准多值 term 已经发布后仍需隔离其直接数组元素；不递归复制业务对象或嵌套容器。
     */
    private static List<Object> snapshotArrayElements(List<?> values) {
        List<Object> snapshot = new ArrayList<>(values.size());
        IdentityHashMap<Object, Object> arrayCopies = new IdentityHashMap<>();
        for (Object value : values) {
            snapshot.add(copyArray(value, arrayCopies));
        }
        return Collections.unmodifiableList(snapshot);
    }

    private static boolean requiresBoundedCollection(String operator) {
        return TermRegistry.standard()
                           .find(operator)
                           .map(TermHandler::shape)
                           .map(shape -> shape == ConditionValueShape.COLLECTION
                                   || shape == ConditionValueShape.RANGE)
                           .orElse(false);
    }

    private static Object copyArray(Object value) {
        return copyArray(value, new IdentityHashMap<>());
    }

    /** 仅复制数组可达图；普通业务对象保持身份，共享引用和数组环也在副本中保持。 */
    private static Object copyArray(Object value, IdentityHashMap<Object, Object> copies) {
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

    private static ConditionValueException collectionTooLarge() {
        return new ConditionValueException(ConditionValueException.Error.COLLECTION_TOO_LARGE,
                                           "condition collection exceeds limit: " + MAX_COLLECTION_SIZE);
    }
}
