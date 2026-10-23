package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionCompiler;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicField;

import java.util.Objects;
import java.util.List;
import java.util.function.Function;

/**
 * 结构化条件编译的内部实现；公开工厂归 StructuredConditionResolver。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
final class StructuredConditionResolvers {

    private StructuredConditionResolvers() {
    }

    static StructuredConditionResolver validating(StructuredConditionResolver resolver) {
        StructuredConditionResolver safeResolver = Objects.requireNonNull(
                resolver, "structured condition resolver must not be null");
        return safeResolver instanceof ValidatingResolver || safeResolver instanceof CompilerResolver
                ? safeResolver : new ValidatingResolver(safeResolver);
    }

    static StructuredConditionResolver compiling(ValueCodecRegistry valueCodecs) {
        return compiling(valueCodecs, List.of());
    }

    static StructuredConditionResolver compiling(ValueCodecRegistry valueCodecs,
                                                  List<StructuredConditionCustomizer> customizers) {
        return new CompilerResolver(StructuredConditionCompiler.create(valueCodecs), customizers);
    }

    /** Core 编译器自身在一次 AST 遍历中完成预算、授权和值规范化，不再套外层结构遍历。 */
    private record CompilerResolver(StructuredConditionCompiler compiler,
                                    List<StructuredConditionCustomizer> customizers) implements StructuredConditionResolver {

        @Override
        public ConditionGroup compileFields(DynamicForm form, Function<String, DynamicField> fields,
                                              StructuredConditionInput input, StructuredConditionPolicy policy) {
            if (!customizers.isEmpty()) {
                StructuredConditionCompiler.validateStructure(input, policy);
                for (StructuredConditionCustomizer customizer : customizers) {
                    customizer.validate(form, input, policy);
                    input = customizer.adaptFields(form, fields, input);
                    policy = customizer.customize(policy);
                }
            }
            return compiler.compileFields(fields, input, policy);
        }
    }

    private record ValidatingResolver(StructuredConditionResolver delegate) implements StructuredConditionResolver {

        @Override
        public ConditionGroup compileFields(DynamicForm form, Function<String, DynamicField> fields,
                                              StructuredConditionInput input, StructuredConditionPolicy policy) {
            StructuredConditionCompiler.validateStructure(input, policy);
            return delegate.compileFields(form, fields, input, policy);
        }
    }
}
