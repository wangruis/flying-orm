package com.flying.orm.core.condition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 前端传来的结构化条件节点，只描述“字段、操作符、值”和 and/or 分组，不承载 SQL。
 *
 * @param field    term 字段名
 * @param operator term 操作符，比如 eq、like、user-in-org
 * @param value    term 参数值
 * @param logic    分组逻辑，只允许 and/or
 * @param terms    分组下的子条件
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
public record StructuredConditionInput(String field,
                                       String operator,
                                       Object value,
                                       String logic,
                                       List<StructuredConditionInput> terms) {

    /**
     * 复制子节点列表但保留 null 元素，让编译入口能够以稳定错误码和路径报告非法节点。
     */
    public StructuredConditionInput {
        terms = terms == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(terms));
    }

    /**
     * 创建普通 term 节点。
     *
     * @param field    字段名
     * @param operator 操作符
     * @param value    参数值
     * @return term 节点
     */
    public static StructuredConditionInput term(String field, String operator, Object value) {
        return new StructuredConditionInput(field, operator, value, null, List.of());
    }

    /**
     * 创建 AND 分组。
     *
     * @param terms 子条件
     * @return AND 分组
     */
    public static StructuredConditionInput and(StructuredConditionInput... terms) {
        return group("and", terms);
    }

    /**
     * 创建 OR 分组。
     *
     * @param terms 子条件
     * @return OR 分组
     */
    public static StructuredConditionInput or(StructuredConditionInput... terms) {
        return group("or", terms);
    }

    private static StructuredConditionInput group(String logic, StructuredConditionInput... terms) {
        return new StructuredConditionInput(null, null, null, logic, Arrays.asList(terms));
    }
}
