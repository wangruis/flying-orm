package com.flying.orm.core.form;

import com.flying.orm.core.internal.value.BindableValueSnapshots;

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
        notDeletedValue = BindableValueSnapshots.immutableValue(
                Objects.requireNonNull(notDeletedValue, "logic not deleted value must not be null"));
        deletedValue = BindableValueSnapshots.immutableValue(
                Objects.requireNonNull(deletedValue, "logic deleted value must not be null"));
    }

    /**
     * 返回未删除值的快照，避免数组配置在表单发布后被外部修改。
     *
     * @return 未删除值；数组可达图返回保持共享关系的独立副本
     */
    @Override
    public Object notDeletedValue() {
        return BindableValueSnapshots.immutableValue(notDeletedValue);
    }

    /**
     * 返回已删除值的快照，避免数组配置在表单发布后被外部修改。
     *
     * @return 已删除值；数组可达图返回保持共享关系的独立副本
     */
    @Override
    public Object deletedValue() {
        return BindableValueSnapshots.immutableValue(deletedValue);
    }

    public static LogicDeleteDefinition of(String fieldName, Object notDeletedValue, Object deletedValue) {
        return new LogicDeleteDefinition(fieldName, notDeletedValue, deletedValue);
    }

    public static LogicDeleteDefinition numeric(String fieldName) {
        return new LogicDeleteDefinition(fieldName, 0, 1);
    }
}
