package com.flying.orm.core.form;

import java.util.Objects;

/**
 * FieldChange 描述同名动态字段在两个表单版本之间的定义变化。
 *
 * @param source 原字段定义
 * @param target 目标字段定义
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public record FieldChange(DynamicField source, DynamicField target) {

    /**
     * 创建字段变更并校验来源与目标字段。
     *
     * @param source 原字段定义
     * @param target 目标字段定义
     */
    public FieldChange {
        source = Objects.requireNonNull(source, "source field must not be null");
        target = Objects.requireNonNull(target, "target field must not be null");
        if (!source.name().equals(target.name())) {
            throw new IllegalArgumentException("field change must keep the same field name");
        }
        if (source.equals(target)) {
            throw new IllegalArgumentException("field change must contain a changed definition");
        }
    }
}
