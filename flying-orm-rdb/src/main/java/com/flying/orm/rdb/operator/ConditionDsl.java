package com.flying.orm.rdb.operator;

import java.util.List;
import java.util.Objects;

/**
 * 查询、更新、删除共用的条件便利方法，不持有状态或执行器。
 * 严格条件保留原值，只有 IfPresent 使用可选值清理规则。
 * 根条件按 AND 连接；嵌套条件按所在组的连接方式追加。
 *
 * @param <F> 字段引用：动态字段名或关联实体类型的 EntityProperty
 * @param <S> 当前构建器类型
 * @author wangr
 * @version v4.1.2
 */
public interface ConditionDsl<F, S extends ConditionDsl<F, S>> {
    /** 添加标准或已注册的扩展条件，字段和值不作为原始 SQL。 */
    S where(F field, String operator, Object value);

    /** 添加可选条件；清理后没有有效值时不生成节点。 */
    S whereIfPresent(F field, String operator, Object value);

    default S where(F field, Object value) {
        return where(field, "=", value);
    }

    default S whereIfPresent(F field, Object value) {
        return whereIfPresent(field, "=", value);
    }

    default S and(F field, Object value) {
        return where(field, value);
    }

    default S and(F field, String operator, Object value) {
        return where(field, operator, value);
    }

    default S in(F field, Iterable<?> values) {
        return where(field, "in", values);
    }

    default S notIn(F field, Iterable<?> values) {
        return where(field, "not-in", values);
    }

    default S between(F field, Object start, Object end) {
        return where(field, "between", List.of(
                Objects.requireNonNull(start, "between start must not be null"),
                Objects.requireNonNull(end, "between end must not be null")));
    }

    default S isNull(F field) {
        return where(field, "is-null", null);
    }

    default S isNotNull(F field) {
        return where(field, "is-not-null", null);
    }
}
