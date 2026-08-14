package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodecRegistry;

import java.util.Objects;

/**
 * 常用结构化条件编译入口放这里，调用方不用每次都手动拼 customizer。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public final class StructuredConditionResolvers {

    private StructuredConditionResolvers() {
    }

    public static StructuredConditionResolver defaults() {
        return StructuredConditionResolver.defaults();
    }

    /**
     * 创建只带内置安全规则的解析器，并让前端值使用应用配置的类型转换规则。
     *
     * <p>正常情况下 {@link com.flying.orm.rdb.bootstrap.FlyingOrmClients} 会替调用方完成这一步；
     * 只有调用方自己拼装解析器时，才需要直接调用这个重载。</p>
     *
     * @param valueCodecs 应用启动时确定的只读 codec 注册表
     * @return 使用指定类型转换规则的默认解析器
     */
    public static StructuredConditionResolver defaults(ValueCodecRegistry valueCodecs) {
        return StructuredConditionResolver.defaults(valueCodecs);
    }

    public static StructuredConditionResolver composite(StructuredConditionCustomizer... customizers) {
        return StructuredConditionResolver.composite(customizers);
    }

    /**
     * 组合自定义条件定制器，并把应用级 codec 交给标准字段条件的值转换。
     *
     * @param valueCodecs 应用级只读 codec 注册表
     * @param customizers 按顺序执行的条件定制器
     * @return 组合后的解析器
     */
    public static StructuredConditionResolver composite(ValueCodecRegistry valueCodecs,
                                                        StructuredConditionCustomizer... customizers) {
        return StructuredConditionResolver.composite(valueCodecs, customizers);
    }

    public static StructuredConditionCustomizer allowOperator(String operator) {
        return StructuredConditionCustomizer.allowOperator(operator);
    }

    public static StructuredConditionCustomizer allowOperator(String externalOperator, String internalOperator) {
        return StructuredConditionCustomizer.allowOperator(externalOperator, internalOperator);
    }

    public static StructuredConditionResolver allowOperators(String... operators) {
        return allowOperators(ValueCodecRegistry.standard(), operators);
    }

    /**
     * 放行一组自定义 operator，同时让同一棵条件树里的标准字段条件继续使用应用级 codec。
     *
     * <p>自定义 operator 的值不一定是当前字段值，因此不会按字段类型强转；需要 JSON、数组或其他
     * 结构化值时，应再提供专用 customizer 完成白名单校验和输入适配。</p>
     *
     * @param valueCodecs 应用启动时确定的只读 codec 注册表
     * @param operators 允许前端使用的 operator 名称
     * @return 带 operator 白名单和指定类型转换规则的解析器
     */
    public static StructuredConditionResolver allowOperators(ValueCodecRegistry valueCodecs, String... operators) {
        return composite(valueCodecs, allowOperatorCustomizers(operators));
    }

    private static StructuredConditionCustomizer[] allowOperatorCustomizers(String... operators) {
        Objects.requireNonNull(operators, "condition operators must not be null");
        StructuredConditionCustomizer[] customizers = new StructuredConditionCustomizer[operators.length];
        for (int i = 0; i < operators.length; i++) {
            customizers[i] = allowOperator(operators[i]);
        }
        return customizers;
    }
}
