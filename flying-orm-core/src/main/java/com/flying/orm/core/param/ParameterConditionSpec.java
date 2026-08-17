package com.flying.orm.core.param;

import com.flying.orm.core.internal.value.BindableValueSnapshots;

import java.util.Objects;
import java.util.function.Function;

/**
 * ParameterConditionSpec 描述一个请求参数如何映射为字段条件。
 *
 * @param parameter       参数名
 * @param field           条件字段
 * @param operator        term id
 * @param defaultValue    默认参数值
 * @param hasDefaultValue 是否声明了默认值
 * @param converter       参数值转换器
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
public record ParameterConditionSpec(String parameter,
                                     String field,
                                     String operator,
                                     Object defaultValue,
                                     boolean hasDefaultValue,
                                     Function<Object, Object> converter) {

    /**
     * 创建参数条件规则并完成基础校验。
     *
     * @param parameter       参数名
     * @param field           条件字段
     * @param operator        term id
     * @param defaultValue    默认参数值
     * @param hasDefaultValue 是否声明了默认值
     * @param converter       参数值转换器
     */
    public ParameterConditionSpec {
        parameter = ParameterNames.requireText(parameter, "parameter name");
        field = ParameterNames.requireText(field, "condition field");
        operator = ParameterNames.normalize(operator, "condition operator");
        defaultValue = BindableValueSnapshots.immutableValue(defaultValue);
        converter = Objects.requireNonNullElse(converter, Function.identity());
    }

    /**
     * 返回默认参数值。
     *
     * <p>数组可达图每次返回独立副本，防止已发布规则被调用方反向修改；非数组保留原有对象引用与 converter 语义。
     *
     * @return 默认参数值；未声明默认值时为 {@code null}
     */
    @Override
    public Object defaultValue() {
        return BindableValueSnapshots.immutableValue(defaultValue);
    }

    /**
     * 创建参数条件规则。
     *
     * @param parameter 参数名
     * @param field     条件字段
     * @param operator  term id
     * @return 参数条件规则
     */
    public static ParameterConditionSpec of(String parameter, String field, String operator) {
        return builder(parameter, field, operator).build();
    }

    /**
     * 创建参数条件规则构建器。
     *
     * @param parameter 参数名
     * @param field     条件字段
     * @param operator  term id
     * @return 参数条件规则构建器
     */
    public static Builder builder(String parameter, String field, String operator) {
        return new Builder(parameter, field, operator);
    }

    /**
     * 返回规范化参数名。
     *
     * @return 规范化参数名
     */
    public String normalizedParameter() {
        return ParameterNames.normalize(parameter, "parameter name");
    }

    /**
     * 转换参数值。
     *
     * @param value 原始参数值
     * @return 转换后的参数值
     */
    public Object convert(Object value) {
        return converter.apply(value);
    }

    /**
     * ParameterConditionSpec 构建器。
     *
     * @author wangr
     * @date 2026-07-22
     * @version v1.0
     */
    public static final class Builder {

        private final String parameter;

        private final String field;

        private final String operator;

        private Object defaultValue;

        private boolean hasDefaultValue;

        private Function<Object, Object> converter = Function.identity();

        private Builder(String parameter, String field, String operator) {
            this.parameter = ParameterNames.requireText(parameter, "parameter name");
            this.field = ParameterNames.requireText(field, "condition field");
            this.operator = ParameterNames.normalize(operator, "condition operator");
        }

        /**
         * 设置默认参数值。
         *
         * @param defaultValue 默认参数值
         * @return 当前构建器
         */
        public Builder defaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
            this.hasDefaultValue = true;
            return this;
        }

        /**
         * 设置参数值转换器。
         *
         * @param converter 参数值转换器
         * @return 当前构建器
         */
        public Builder convert(Function<Object, Object> converter) {
            this.converter = Objects.requireNonNull(converter, "parameter converter must not be null");
            return this;
        }

        /**
         * 构建参数条件规则。
         *
         * @return 参数条件规则
         */
        public ParameterConditionSpec build() {
            return new ParameterConditionSpec(parameter, field, operator, defaultValue, hasDefaultValue, converter);
        }
    }
}
