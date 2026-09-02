package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionCompiler;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicForm;

import java.util.List;
import java.util.Objects;

/**
 * 客户端把前端结构化条件变成内部条件树时会走这里。
 * 默认只做通用条件，自定义 term 或 JSON 这种特殊值可以通过扩展实现接进来。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
@FunctionalInterface
public interface StructuredConditionResolver {

    ConditionGroup compile(DynamicForm form, StructuredConditionInput input, StructuredConditionPolicy policy);

    static StructuredConditionResolver defaults() {
        return defaults(ValueCodecRegistry.standard());
    }

    /**
     * 创建使用应用级 codec 的默认结构化条件解析器。
     *
     * <p>标准比较条件里的字符串、数字等值会先按动态字段类型转换，再进入条件 AST。自定义 term
     * 可能接收机构 ID、JSON 条件对象等非字段值，因此保留自己的值语义，只在专用 customizer 中适配。</p>
     *
     * @param valueCodecs 应用级只读 codec 注册表
     * @return 默认结构化条件解析器
     */
    static StructuredConditionResolver defaults(ValueCodecRegistry valueCodecs) {
        return StructuredConditionResolvers.compiling(valueCodecs);
    }

    static StructuredConditionResolver composite(StructuredConditionCustomizer... customizers) {
        return composite(ValueCodecRegistry.standard(), customizers);
    }

    /**
     * 创建带 customizer 的条件解析器。标准字段条件复用应用级 codec，自定义 term 则由对应
     * customizer 决定值形状和适配方式，不能误按挂载字段的 Java 类型强转。
     *
     * @param valueCodecs 应用级只读 codec 注册表
     * @param customizers JSON、自定义 operator 等输入和策略定制器，按传入顺序执行
     * @return 组合后的结构化条件解析器
     */
    static StructuredConditionResolver composite(ValueCodecRegistry valueCodecs,
                                                  StructuredConditionCustomizer... customizers) {
        List<StructuredConditionCustomizer> safeCustomizers = List.of(Objects.requireNonNull(customizers,
                                                                                             "structured condition customizers must not be null"));
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create(valueCodecs);
        StructuredConditionResolver validatedSeam = (form, input, policy) -> {
            StructuredConditionInput adaptedInput = Objects.requireNonNull(input,
                                                                           "structured condition input must not be null");
            StructuredConditionPolicy adaptedPolicy = Objects.requireNonNull(policy,
                                                                             "structured condition policy must not be null");
            for (StructuredConditionCustomizer customizer : safeCustomizers) {
                StructuredConditionCustomizer safeCustomizer = Objects.requireNonNull(customizer,
                                                                                     "structured condition customizer must not be null");
                safeCustomizer.validate(form, adaptedInput, adaptedPolicy);
                adaptedInput = safeCustomizer.adapt(form, adaptedInput);
                adaptedPolicy = safeCustomizer.customize(adaptedPolicy);
            }
            return compiler.compile(form, adaptedInput, adaptedPolicy);
        };
        return StructuredConditionResolvers.validating(validatedSeam);
    }
}
