package com.flying.orm.core.condition;

import com.flying.orm.core.internal.value.BindableValueSnapshots;

import java.lang.reflect.Array;
import java.util.ArrayList;
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
            return BindableValueSnapshots.immutableValues(values);
        }
        return isStandardOperator(operator)
                ? BindableValueSnapshots.immutableValue(value)
                : BindableValueSnapshots.arrayGraph(value);
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
        return isStandardOperator(operator)
                ? BindableValueSnapshots.immutableValue(value)
                : BindableValueSnapshots.arrayGraph(value);
    }

    /**
     * 标准多值 term 只在发布 AST 时消费一次 Iterable；既防止后续可变源改写 SQL，也不重复遍历一次性数据源。
     */
    private static List<Object> snapshotIterable(Iterable<?> values) {
        List<Object> snapshot = new ArrayList<>();
        for (Object item : values) {
            if (snapshot.size() >= MAX_COLLECTION_SIZE) {
                throw collectionTooLarge();
            }
            snapshot.add(item);
        }
        return BindableValueSnapshots.immutableValues(snapshot);
    }

    private static boolean requiresBoundedCollection(String operator) {
        return TermRegistry.standard()
                           .find(operator)
                           .map(TermHandler::shape)
                           .map(shape -> shape == ConditionValueShape.COLLECTION
                                   || shape == ConditionValueShape.RANGE)
                           .orElse(false);
    }

    private static boolean isStandardOperator(String operator) {
        return TermRegistry.standard().find(operator).isPresent();
    }

    private static ConditionValueException collectionTooLarge() {
        return new ConditionValueException(ConditionValueException.Error.COLLECTION_TOO_LARGE,
                                           "condition collection exceeds limit: " + MAX_COLLECTION_SIZE);
    }
}
