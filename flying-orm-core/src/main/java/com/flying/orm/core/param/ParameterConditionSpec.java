package com.flying.orm.core.param;

import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.internal.Names;
import com.flying.orm.core.internal.value.BindableValueSnapshots;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * ParameterConditionSpec 描述一个请求参数如何映射为字段条件。
 *
 * @param parameter       参数名
 * @param identity        条件字段身份
 * @param operator        term id
 * @param defaultValue    默认参数值
 * @param hasDefaultValue 是否声明了默认值
 * @param converter       参数值转换器
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
public record ParameterConditionSpec(String parameter,
                                     FieldIdentity identity,
                                     String operator,
                                     Object defaultValue,
                                     boolean hasDefaultValue,
                                     Function<Object, Object> converter) {

    private static final Function<Object, Object> IDENTITY_CONVERTER = value -> value;

    /**
     * 创建参数条件规则并完成基础校验。
     *
     * @param parameter       参数名
     * @param identity        条件字段身份
     * @param operator        term id
     * @param defaultValue    默认参数值
     * @param hasDefaultValue 是否声明了默认值
     * @param converter       参数值转换器
     */
    public ParameterConditionSpec {
        if (defaultValue instanceof OwnedDefault owned) {
            ParameterConditionSpec source = owned.source();
            parameter = source.parameter;
            identity = source.identity;
            operator = source.operator;
            defaultValue = owned.value();
            hasDefaultValue = source.hasDefaultValue;
            converter = source.converter;
        } else {
            parameter = Names.requireText(parameter, "parameter name");
            identity = Objects.requireNonNull(identity, "condition field identity must not be null");
            operator = Names.key(operator, "condition operator");
            defaultValue = snapshotDefaultValue(defaultValue);
            converter = Objects.requireNonNullElse(converter, IDENTITY_CONVERTER);
        }
    }

    /**
     * 由字段名创建参数条件规则。
     */
    public ParameterConditionSpec(String parameter,
                                  String field,
                                  String operator,
                                  Object defaultValue,
                                  boolean hasDefaultValue,
                                  Function<Object, Object> converter) {
        this(parameter, FieldIdentity.of(field), operator, defaultValue, hasDefaultValue, converter);
    }

    /** @return 保留声明大小写的条件字段名 */
    public String field() {
        return identity.name();
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
        return snapshotDefaultValue(ownedDefaultValue());
    }

    /** 包内编译链读取已经规范化并拥有的默认值，不再生成逐请求副本。 */
    Object ownedDefaultValue() {
        return defaultValue;
    }

    /** 复用默认值时只在真实 converter 扩展边界为可变值创建本次调用的隔离副本。 */
    Object isolateOwnedDefaultForConverter(Object ownedValue) {
        return usesIdentityConverter() ? ownedValue : snapshotDefaultValue(ownedValue);
    }

    /** 仅信任本类的内置无转换规则，用户 converter 即使返回原引用也不进入此路径。 */
    boolean usesIdentityConverter() {
        return converter == IDENTITY_CONVERTER;
    }

    /** 创建持有已规范化默认值的编译表示，同时保留公共访问器的防御语义。 */
    static ParameterConditionSpec compiled(ParameterConditionSpec source,
                                           Object defaultValue) {
        return new ParameterConditionSpec(source.parameter(),
                                          source.identity(),
                                          source.operator(),
                                          new OwnedDefault(source, defaultValue),
                                          source.hasDefaultValue(),
                                          source.converter());
    }

    private static Object snapshotDefaultValue(Object value) {
        return value instanceof List<?> values
                ? BindableValueSnapshots.logicalValues(values)
                : BindableValueSnapshots.logicalValue(value);
    }

    private record OwnedDefault(ParameterConditionSpec source, Object value) {
        private OwnedDefault {
            Objects.requireNonNull(source, "source parameter condition must not be null");
        }
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
        return Names.key(parameter, "parameter name");
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

        private final FieldIdentity identity;

        private final String operator;

        private Object defaultValue;

        private boolean hasDefaultValue;

        private Function<Object, Object> converter = IDENTITY_CONVERTER;

        private Builder(String parameter, String field, String operator) {
            this.parameter = Names.requireText(parameter, "parameter name");
            this.identity = FieldIdentity.of(field);
            this.operator = Names.key(operator, "condition operator");
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
            return new ParameterConditionSpec(parameter, identity, operator, defaultValue, hasDefaultValue, converter);
        }
    }
}
