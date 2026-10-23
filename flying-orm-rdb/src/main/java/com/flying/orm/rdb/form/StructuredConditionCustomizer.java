package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicField;

import java.util.Objects;
import java.util.function.Function;

/**
 * 给前端结构化条件编译加一点扩展：可以改输入，也可以改安全策略。
 * 比如 JSON 条件要先把 Map 变成值对象，业务 term 只需要放行 operator。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public interface StructuredConditionCustomizer {

    /**
     * 在扩展值被包装、复制或序列化前执行专用容量校验。
     *
     * <p>通用条件编译器只负责原始值图的深度、节点和字符串边界；JSON、SQL Array、Vector 等扩展
     * 各自知道业务容器的真实含义，不能由 Core 猜测。</p>
     *
     * @param form 可信动态表单
     * @param input 原始结构化条件
     * @param policy 当前安全策略
     */
    default void validate(DynamicForm form,
                          StructuredConditionInput input,
                          StructuredConditionPolicy policy) {
        Objects.requireNonNull(form, "dynamic form must not be null");
        Objects.requireNonNull(input, "structured condition input must not be null");
        Objects.requireNonNull(policy, "structured condition policy must not be null");
    }

    /** 使用查询已解析的字段元数据适配输入；只修改 operator 的定制器直接保留原输入。 */
    default StructuredConditionInput adaptFields(DynamicForm form, Function<String, DynamicField> fields,
                                                 StructuredConditionInput input) {
        return input;
    }

    default StructuredConditionPolicy customize(StructuredConditionPolicy policy) {
        return Objects.requireNonNull(policy, "structured condition policy must not be null");
    }

    static StructuredConditionCustomizer allowOperator(String operator) {
        return allowOperator(operator, operator);
    }

    static StructuredConditionCustomizer allowOperator(String externalOperator, String internalOperator) {
        Objects.requireNonNull(externalOperator, "external condition operator must not be null");
        Objects.requireNonNull(internalOperator, "internal condition operator must not be null");
        return new StructuredConditionCustomizer() {

            @Override
            public StructuredConditionPolicy customize(StructuredConditionPolicy policy) {
                return StructuredConditionCustomizer.super.customize(policy)
                                                   .allowOperator(externalOperator, internalOperator);
            }
        };
    }
}
